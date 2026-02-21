import os
import sys
import json
import requests
from datetime import datetime, timedelta, timezone, date
from db import get_conn

PUBMED_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# NOTE: This uses the "esearch + efetch xml" pattern.
# We keep it minimal and robust, no XML libraries beyond stdlib.
import xml.etree.ElementTree as ET


def utc_now():
    return datetime.now(timezone.utc)


def ensure_source(cur, name="pubmed", base_url="https://pubmed.ncbi.nlm.nih.gov/"):
    cur.execute(
        """
        INSERT INTO sources (name, base_url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO NOTHING
        """,
        (name, base_url),
    )
    cur.execute("SELECT id FROM sources WHERE name=%s", (name,))
    return cur.fetchone()[0]


def start_fetch_run(cur, source_id):
    cur.execute(
        """
        INSERT INTO fetch_runs (source_id, started_at, status)
        VALUES (%s, now(), 'running')
        RETURNING id
        """,
        (source_id,),
    )
    return cur.fetchone()[0]


def finish_fetch_run(cur, run_id, status="ok", new_count=0, updated_count=0, error_message=None):
    cur.execute(
        """
        UPDATE fetch_runs
        SET finished_at = now(),
            status = %s,
            new_count = %s,
            updated_count = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, new_count, updated_count, error_message, run_id),
    )


def pubmed_esearch(term: str, days: int = 1, retmax: int = 200):
    # "reldate" + "datetype=pdat" = published date window
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": str(retmax),
        "retmode": "json",
        "reldate": str(days),
        "datetype": "pdat",
        "sort": "pub_date",
    }
    r = requests.get(PUBMED_ESEARCH, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    idlist = data.get("esearchresult", {}).get("idlist", [])
    return idlist


def pubmed_efetch_xml(pmids):
    if not pmids:
        return None
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    r = requests.get(PUBMED_EFETCH, params=params, timeout=60)
    r.raise_for_status()
    return r.text


def text_or_none(elem):
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t if t else None


def parse_pubdate(article_node):
    # PubDate can be messy: Year/Month/Day or MedlineDate
    pub_date = article_node.find(".//Journal/JournalIssue/PubDate")
    if pub_date is None:
        return None

    year = text_or_none(pub_date.find("Year"))
    month = text_or_none(pub_date.find("Month"))
    day = text_or_none(pub_date.find("Day"))

    if year and month:
        # Month could be "Jan" etc.
        try:
            m = int(month)
        except ValueError:
            month_map = {
                "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
            }
            m = month_map.get(month[:3], 1)

        d = int(day) if day and day.isdigit() else 1
        try:
            return date(int(year), int(m), int(d))
        except Exception:
            return None

    # Try MedlineDate fallback (e.g., "2024 Jan-Feb")
    medline = text_or_none(pub_date.find("MedlineDate"))
    if medline:
        # grab first 4 digits as year if present
        try:
            y = int(medline[:4])
            return date(y, 1, 1)
        except Exception:
            return None

    return None


def parse_articles_from_xml(xml_text):
    root = ET.fromstring(xml_text)
    out = []

    for pubmed_article in root.findall(".//PubmedArticle"):
        pmid = text_or_none(pubmed_article.find(".//PMID"))
        if not pmid:
            continue

        article = pubmed_article.find(".//Article")
        title = text_or_none(article.find("ArticleTitle")) if article is not None else None

        abstract_parts = []
        if article is not None:
            for abst in article.findall(".//Abstract/AbstractText"):
                label = abst.get("Label")
                txt = "".join(abst.itertext()).strip()
                if label:
                    abstract_parts.append(f"{label}: {txt}")
                else:
                    abstract_parts.append(txt)
        abstract = "\n\n".join([p for p in abstract_parts if p])

        journal = None
        if article is not None:
            journal = text_or_none(article.find(".//Journal/Title"))

        pub_date = parse_pubdate(pubmed_article)

        # DOI
        doi = None
        for aid in pubmed_article.findall(".//ArticleId"):
            if aid.get("IdType") == "doi":
                doi = (aid.text or "").strip()
                break

        # Authors
        authors = []
        if article is not None:
            for a in article.findall(".//AuthorList/Author"):
                last = text_or_none(a.find("LastName"))
                fore = text_or_none(a.find("ForeName"))
                coll = text_or_none(a.find("CollectiveName"))
                if coll:
                    authors.append({"collective": coll})
                elif last or fore:
                    authors.append({"last": last, "fore": fore})

        # Keywords (best-effort)
        keywords = []
        for kw in pubmed_article.findall(".//KeywordList/Keyword"):
            t = "".join(kw.itertext()).strip()
            if t:
                keywords.append(t)

        # MeSH terms
        mesh_terms = []
        for mh in pubmed_article.findall(".//MeshHeading/DescriptorName"):
            t = "".join(mh.itertext()).strip()
            if t:
                mesh_terms.append(t)

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        out.append(
            {
                "pmid": int(pmid),
                "doi": doi,
                "url": url,
                "title": title or f"(untitled) PMID {pmid}",
                "abstract": abstract or None,
                "journal": journal,
                "publication_date": pub_date,
                "authors": authors or None,
                "keywords": keywords or None,
                "mesh_terms": mesh_terms or None,
                "source_record_id": pmid,  # stable per-source identifier
            }
        )

    return out


def upsert_article(cur, source_id, a):
    """
    Returns (inserted: bool)
    We key on PMID primarily (unique), DOI secondarily (unique).
    We'll upsert by PMID (since PubMed guarantees it).
    """
    cur.execute(
        """
        INSERT INTO articles (
            pmid, doi, url,
            title, abstract, journal, publication_date,
            authors, keywords, mesh_terms,
            source_id, source_record_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (pmid) DO UPDATE SET
            doi = COALESCE(EXCLUDED.doi, articles.doi),
            url = COALESCE(EXCLUDED.url, articles.url),
            title = EXCLUDED.title,
            abstract = COALESCE(EXCLUDED.abstract, articles.abstract),
            journal = COALESCE(EXCLUDED.journal, articles.journal),
            publication_date = COALESCE(EXCLUDED.publication_date, articles.publication_date),
            authors = COALESCE(EXCLUDED.authors, articles.authors),
            keywords = COALESCE(EXCLUDED.keywords, articles.keywords),
            mesh_terms = COALESCE(EXCLUDED.mesh_terms, articles.mesh_terms),
            source_id = EXCLUDED.source_id,
            source_record_id = EXCLUDED.source_record_id
        RETURNING (xmax = 0) AS inserted
        """,
        (
            a["pmid"], a["doi"], a["url"],
            a["title"], a["abstract"], a["journal"], a["publication_date"],
            json.dumps(a["authors"]) if a["authors"] else None,
            json.dumps(a["keywords"]) if a["keywords"] else None,
            json.dumps(a["mesh_terms"]) if a["mesh_terms"] else None,
            source_id, str(a["source_record_id"])
        ),
    )
    return cur.fetchone()[0]


def main():
    # Very simple seed term: refine later
    term = '(parkinson*[Title/Abstract] OR alzheimer*[Title/Abstract]) AND (2020:3000[pdat])'
    days = int(os.environ.get("PUBMED_DAYS", "1"))
    retmax = int(os.environ.get("PUBMED_RETMAX", "200"))

    print(f"[pubmed] searching last {days} day(s), retmax={retmax} ...", flush=True)
    pmids = pubmed_esearch(term=term, days=days, retmax=retmax)
    print(f"[pubmed] found {len(pmids)} PMIDs", flush=True)

    with get_conn() as conn:
        run_id = None
        try:
            with conn.cursor() as cur:
                source_id = ensure_source(cur)
                run_id = start_fetch_run(cur, source_id)

                xml_text = pubmed_efetch_xml(pmids)
                if not xml_text:
                    finish_fetch_run(cur, run_id, status="ok", new_count=0, updated_count=0)
                    conn.commit()
                    print("[pubmed] no results; done.")
                    return

                articles = parse_articles_from_xml(xml_text)
                new_count = 0
                updated_count = 0

                for a in articles:
                    inserted = upsert_article(cur, source_id, a)
                    if inserted:
                        new_count += 1
                    else:
                        updated_count += 1

                finish_fetch_run(cur, run_id, status="ok", new_count=new_count, updated_count=updated_count)
            conn.commit()

            print(f"[pubmed] done. new={new_count} updated={updated_count}", flush=True)

        except Exception as e:
            conn.rollback()
            # try to mark run as error
            try:
                if run_id is not None:
                    with conn.cursor() as cur:
                        finish_fetch_run(cur, run_id, status="error", error_message=str(e))
                    conn.commit()
            except Exception:
                pass
            print(f"[pubmed] ERROR: {e}", file=sys.stderr)
            raise


if __name__ == "__main__":
    main()

