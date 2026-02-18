import os
import time
import requests
import xmltodict
from datetime import datetime, date
from dotenv import load_dotenv
import logging

from db import execute

load_dotenv()

EUTILS_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EUTILS_EFETCH  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# --- Logging (cron-safe) ---
# If you want logs next to this script, use this:
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(BASE_DIR, "clawbot.log")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


def get_source_id(source_name: str) -> int:
    row = execute("SELECT id FROM sources WHERE name=%s", (source_name,), fetch="one")
    if not row:
        raise RuntimeError(f"Source not found in DB: {source_name}")
    return row["id"]


def get_last_run(source_id: int):
    row = execute("SELECT last_run FROM scanner_state WHERE source_id=%s", (source_id,), fetch="one")
    return row["last_run"] if row else None


def set_last_run(source_id: int):
    execute(
        """
        INSERT INTO scanner_state (source_id, last_run)
        VALUES (%s, NOW())
        ON CONFLICT (source_id) DO UPDATE SET last_run=EXCLUDED.last_run
        """,
        (source_id,),
    )


def parse_pub_date(article_dict) -> date | None:
    """
    PubMed dates can be messy. We try a few common fields.
    """
    try:
        journal = article_dict.get("Journal", {})
        issue = journal.get("JournalIssue", {})
        pub_date = issue.get("PubDate", {})

        y = pub_date.get("Year")
        m = pub_date.get("Month")
        d = pub_date.get("Day")

        if not y:
            return None

        # Month can be "Jan" etc.
        if m and isinstance(m, str) and not m.isdigit():
            months = {
                "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
                "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
                "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
            }
            m = months.get(m[:3], 1)

        y = int(y)
        m = int(m) if m else 1
        d = int(d) if d else 1
        return date(y, m, d)
    except Exception:
        return None


def extract_authors(article_dict) -> list[str]:
    authors = []
    author_list = article_dict.get("AuthorList", {}).get("Author", [])
    if isinstance(author_list, dict):
        author_list = [author_list]

    for a in author_list:
        last = a.get("LastName")
        fore = a.get("ForeName")
        if last and fore:
            authors.append(f"{fore} {last}")
        elif last:
            authors.append(last)
    return authors


def upsert_paper(source_id: int, pmid: str, title: str, abstract: str, journal: str,
                 pub_date: date | None, url: str, authors: list[str]) -> bool:
    """
    Inserts paper if new; returns True if inserted, False if already existed.
    """
    # Insert paper (dedupe by UNIQUE(source_id, external_id))
    row = execute(
        """
        INSERT INTO papers (source_id, external_id, title, abstract, journal, publication_date, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, external_id) DO NOTHING
        RETURNING id
        """,
        (source_id, pmid, title, abstract, journal, pub_date, url),
        fetch="one",
    )

    if not row:
        return False  # already existed

    paper_id = row["id"]

    # Insert authors + join table
    for name in authors:
        arow = execute(
            """
            INSERT INTO authors (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
            RETURNING id
            """,
            (name,),
            fetch="one",
        )
        author_id = arow["id"]
        execute(
            """
            INSERT INTO paper_authors (paper_id, author_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (paper_id, author_id),
        )

    return True


def pubmed_esearch(term: str, retmax: int = 20, mindate: str | None = None) -> list[str]:
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": str(retmax),
        "sort": "date",
    }

    # Optional: limit by date (PubMed supports mindate/maxdate with datetype)
    if mindate:
        params["datetype"] = "pdat"
        params["mindate"] = mindate

    r = requests.get(EUTILS_ESEARCH, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("esearchresult", {}).get("idlist", [])


def pubmed_efetch(pmids: list[str]) -> dict:
    if not pmids:
        return {}

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    r = requests.get(EUTILS_EFETCH, params=params, timeout=60)
    r.raise_for_status()
    return xmltodict.parse(r.content)


def scan_pubmed_neuro():
    source_id = get_source_id("pubmed")
    last_run = get_last_run(source_id)

    mindate = None
    if last_run:
        mindate = last_run.date().isoformat()

    retmax = int(os.getenv("PUBMED_RETMAX", "20"))

    queries = [
        ("Parkinson Disease", "Parkinson"),
        ("Alzheimer Disease", "Alzheimer"),
    ]

    total_inserted = 0
    total_existing = 0
    total_errors = 0

    logger.info("=== PubMed scan start | mindate=%s retmax=%s ===", mindate, retmax)

    for term, label in queries:
        logger.info("[PubMed] Searching term=%s label=%s mindate=%s retmax=%s", term, label, mindate, retmax)

        try:
            pmids = pubmed_esearch(term=term, retmax=retmax, mindate=mindate)
            logger.info("[PubMed] Found %s PMIDs for term=%s", len(pmids), term)

            if not pmids:
                continue

            data = pubmed_efetch(pmids)

            article_set = data.get("PubmedArticleSet", {})
            articles = article_set.get("PubmedArticle", [])

            if isinstance(articles, dict):
                articles = [articles]

            for item in articles:
                try:
                    mc = item.get("MedlineCitation", {})
                    pmid = mc.get("PMID")
                    article = mc.get("Article", {})

                    title = article.get("ArticleTitle") or "(no title)"
                    journal = (article.get("Journal", {}) or {}).get("Title") or "Unknown Journal"

                    abstract = ""
                    abs_obj = article.get("Abstract", {}).get("AbstractText")
                    if isinstance(abs_obj, list):
                        abstract = " ".join([str(x) for x in abs_obj])
                    elif isinstance(abs_obj, dict):
                        abstract = str(abs_obj.get("#text", ""))
                    elif abs_obj:
                        abstract = str(abs_obj)

                    pub_date = parse_pub_date(article)
                    url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
                    authors = extract_authors(article)

                    inserted = upsert_paper(
                        source_id=source_id,
                        pmid=str(pmid),
                        title=str(title),
                        abstract=abstract,
                        journal=str(journal),
                        pub_date=pub_date,
                        url=url,
                        authors=authors,
                    )

                    if inserted:
                        total_inserted += 1
                        logger.info("INSERTED pmid=%s label=%s title=%s", pmid, label, str(title)[:160])
                    else:
                        total_existing += 1
                        logger.info("EXISTS   pmid=%s label=%s title=%s", pmid, label, str(title)[:160])

                    # be polite to NCBI
                    time.sleep(0.1)

                except Exception:
                    total_errors += 1
                    logger.exception("ERROR processing article item (term=%s)", term)

        except Exception:
            total_errors += 1
            logger.exception("ERROR in term scan (term=%s)", term)

    set_last_run(source_id)
    logger.info(
        "=== PubMed scan done | inserted=%s existing=%s errors=%s | updated scanner_state ===",
        total_inserted, total_existing, total_errors
    )


if __name__ == "__main__":
    scan_pubmed_neuro()