#!/usr/bin/env python3
"""
ingest_pubmed_backfill.py

Historical PubMed backfill for Parkinson's papers, starting at year 2000 by default.

Features:
- year-bucket ingest
- retry handling for ESearch / EFetch
- safe chunked fetching
- idempotent upsert into public.articles
- resets AI fields to pending when title/abstract changes

Env:
  DATABASE_URL=postgresql://user:pass@localhost:5432/neurocompute
  NCBI_EMAIL=you@example.com
  NCBI_API_KEY=xxxxx

Optional env:
  BACKFILL_START_YEAR=2000
  BACKFILL_END_YEAR=2026
  BACKFILL_FETCH_CHUNK=50
  BACKFILL_ESEARCH_PAGE=2000
  BACKFILL_MAX_PER_YEAR=0
"""

import os
import sys
import time
import json
import math
import random
import requests
import psycopg
from datetime import datetime
from dotenv import load_dotenv
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout, HTTPError

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "").strip()
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "").strip()

BACKFILL_START_YEAR = int(os.getenv("BACKFILL_START_YEAR", "2000"))
BACKFILL_END_YEAR = int(os.getenv("BACKFILL_END_YEAR", str(datetime.now().year)))
BACKFILL_FETCH_CHUNK = int(os.getenv("BACKFILL_FETCH_CHUNK", "50"))
BACKFILL_ESEARCH_PAGE = int(os.getenv("BACKFILL_ESEARCH_PAGE", "2000"))
BACKFILL_MAX_PER_YEAR = int(os.getenv("BACKFILL_MAX_PER_YEAR", "0"))  # 0 = unlimited

ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

QUERY = '("Parkinson Disease"[MeSH Terms] OR parkinson*[Title/Abstract])'


def die(msg: str, code: int = 1):
    print(f"[ingest_pubmed_backfill] ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _params(extra: dict) -> dict:
    p = dict(extra)
    if NCBI_EMAIL:
        p["email"] = NCBI_EMAIL
    if NCBI_API_KEY:
        p["api_key"] = NCBI_API_KEY
    return p


def _sleep():
    if NCBI_API_KEY:
        time.sleep(0.05)
    else:
        time.sleep(0.34)


def request_with_retry(url: str, params: dict, timeout: int = 60, max_retries: int = 5):
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            _sleep()
            return r

        except (ChunkedEncodingError, ConnectionError, Timeout, HTTPError) as e:
            last_err = e
            wait_s = min(20.0, (1.5 * attempt) + random.random())
            print(
                f"[ingest_pubmed_backfill] retry {attempt}/{max_retries} "
                f"for {url} after error: {e} (sleep {wait_s:.1f}s)"
            )
            time.sleep(wait_s)

    raise last_err


def year_query(year: int) -> str:
    return f'({QUERY}) AND ("{year}/01/01"[PDAT] : "{year}/12/31"[PDAT])'


def month_to_int(mm: str | None):
    if not mm:
        return None
    mm = mm.strip()
    if mm.isdigit():
        return int(mm)

    lookup = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    return lookup.get(mm[:3].lower())


def esearch_count(term: str) -> int:
    params = _params({
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": "0",
    })
    r = request_with_retry(ESEARCH, params=params, timeout=60)
    data = r.json()
    return int(data["esearchresult"]["count"])


def esearch_ids(term: str, max_per_year: int = 0) -> list[int]:
    total = esearch_count(term)
    if total == 0:
        return []

    if max_per_year > 0:
        total = min(total, max_per_year)

    ids: list[int] = []
    pages = math.ceil(total / BACKFILL_ESEARCH_PAGE)

    for page in range(pages):
        retstart = page * BACKFILL_ESEARCH_PAGE
        remaining = total - retstart
        retmax = min(BACKFILL_ESEARCH_PAGE, remaining)

        params = _params({
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retstart": str(retstart),
            "retmax": str(retmax),
        })

        r = request_with_retry(ESEARCH, params=params, timeout=60)
        data = r.json()
        batch = [int(x) for x in data["esearchresult"].get("idlist", [])]
        ids.extend(batch)

        print(
            f"[ingest_pubmed_backfill]   esearch page {page + 1}/{pages} "
            f"got {len(batch)} ids"
        )

    return ids


def efetch_xml(pmids: list[int]) -> str:
    if not pmids:
        return ""

    params = _params({
        "db": "pubmed",
        "id": ",".join(str(x) for x in pmids),
        "retmode": "xml",
    })

    r = request_with_retry(EFETCH, params=params, timeout=120)
    return r.text


def parse_pubmed_xml(xml_text: str) -> list[dict]:
    import xml.etree.ElementTree as ET

    if not xml_text.strip():
        return []

    root = ET.fromstring(xml_text)
    out: list[dict] = []

    for article in root.findall(".//PubmedArticle"):
        pmid_el = article.find(".//MedlineCitation/PMID")
        pmid = int(pmid_el.text) if pmid_el is not None and pmid_el.text else None
        if not pmid:
            continue

        doi = None
        for aid in article.findall(".//ArticleIdList/ArticleId"):
            if (aid.get("IdType") or "").lower() == "doi" and aid.text:
                doi = aid.text.strip()
                break

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        title_el = article.find(".//Article/ArticleTitle")
        title = "".join(title_el.itertext()).strip() if title_el is not None else ""

        abs_parts = []
        for a in article.findall(".//Article/Abstract/AbstractText"):
            label = a.get("Label")
            txt = "".join(a.itertext()).strip() if a is not None else ""
            if not txt:
                continue
            abs_parts.append(f"{label}: {txt}" if label else txt)
        abstract = "\n\n".join(abs_parts).strip()

        j_el = article.find(".//Article/Journal/Title")
        journal = j_el.text.strip() if j_el is not None and j_el.text else None

        pub_date = None
        y = article.findtext(".//Article/Journal/JournalIssue/PubDate/Year")
        m = article.findtext(".//Article/Journal/JournalIssue/PubDate/Month")
        d = article.findtext(".//Article/Journal/JournalIssue/PubDate/Day")

        try:
            if y:
                yy = int(y)
                mm_i = month_to_int(m) or 1
                dd = int(d) if d and d.isdigit() else 1
                pub_date = f"{yy:04d}-{mm_i:02d}-{dd:02d}"
        except Exception:
            pub_date = None

        authors = []
        for au in article.findall(".//Article/AuthorList/Author"):
            last = au.findtext("LastName") or ""
            fore = au.findtext("ForeName") or ""
            collective = au.findtext("CollectiveName") or ""
            name = (fore + " " + last).strip() or collective.strip()
            if name:
                authors.append(name)

        keywords = []
        for kw in article.findall(".//KeywordList/Keyword"):
            text = "".join(kw.itertext()).strip() if kw is not None else ""
            if text:
                keywords.append(text)

        mesh_terms = []
        for mh in article.findall(".//MeshHeadingList/MeshHeading/DescriptorName"):
            text = "".join(mh.itertext()).strip() if mh is not None else ""
            if text:
                mesh_terms.append(text)

        out.append({
            "pmid": pmid,
            "doi": doi,
            "url": url,
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "publication_date": pub_date,
            "authors": json.dumps(authors),
            "keywords": json.dumps(keywords),
            "mesh_terms": json.dumps(mesh_terms),
        })

    return out


UPSERT_SQL = """
INSERT INTO public.articles
    (pmid, doi, url, title, abstract, journal, publication_date,
     authors, keywords, mesh_terms,
     created_at, updated_at,
     agent_status, ai_score, summary_1s, tags, score_components, scored_at)
VALUES
    (%(pmid)s, %(doi)s, %(url)s, %(title)s, %(abstract)s, %(journal)s, %(publication_date)s,
     %(authors)s::jsonb, %(keywords)s::jsonb, %(mesh_terms)s::jsonb,
     now(), now(),
     'pending', NULL, NULL, NULL, NULL, NULL)
ON CONFLICT (pmid)
DO UPDATE SET
    doi = COALESCE(EXCLUDED.doi, public.articles.doi),
    url = COALESCE(EXCLUDED.url, public.articles.url),
    title = COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title),
    abstract = COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract),
    journal = COALESCE(EXCLUDED.journal, public.articles.journal),
    publication_date = COALESCE(EXCLUDED.publication_date, public.articles.publication_date),
    authors = CASE WHEN EXCLUDED.authors IS NOT NULL THEN EXCLUDED.authors ELSE public.articles.authors END,
    keywords = CASE WHEN EXCLUDED.keywords IS NOT NULL THEN EXCLUDED.keywords ELSE public.articles.keywords END,
    mesh_terms = CASE WHEN EXCLUDED.mesh_terms IS NOT NULL THEN EXCLUDED.mesh_terms ELSE public.articles.mesh_terms END,

    agent_status = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN 'pending'
        ELSE public.articles.agent_status
    END,

    ai_score = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.ai_score
    END,

    summary_1s = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.summary_1s
    END,

    tags = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.tags
    END,

    score_components = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.score_components
    END,

    scored_at = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title, ''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract, ''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.scored_at
    END,

    updated_at = now();
"""


def db_preflight(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user;")
        dbname, dbuser = cur.fetchone()
        print(f"[ingest_pubmed_backfill] connected: db={dbname} user={dbuser}")

        cur.execute("SELECT to_regclass('public.articles');")
        reg = cur.fetchone()[0]
        if not reg:
            die("public.articles table not found.")

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.articles'::regclass
                  AND contype = 'u'
            );
        """)
        has_unique = cur.fetchone()[0]
        if not has_unique:
            print("[ingest_pubmed_backfill] WARNING: no unique constraint detected generically; continuing.")


def upsert_articles(rows: list[dict]) -> int:
    if not rows:
        return 0
    if not DATABASE_URL:
        die("DATABASE_URL is not set.")

    with psycopg.connect(DATABASE_URL) as conn:
        db_preflight(conn)
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(UPSERT_SQL, row)
        conn.commit()

    return len(rows)


def run():
    print(
        f"[ingest_pubmed_backfill] starting backfill "
        f"{BACKFILL_START_YEAR} → {BACKFILL_END_YEAR}"
    )

    if not NCBI_EMAIL:
        print("[ingest_pubmed_backfill] NOTE: NCBI_EMAIL not set (recommended by NCBI).")
    if not NCBI_API_KEY:
        print("[ingest_pubmed_backfill] NOTE: NCBI_API_KEY not set (slower and more fragile).")

    total_seen = 0

    for year in range(BACKFILL_START_YEAR, BACKFILL_END_YEAR + 1):
        print(f"\n[ingest_pubmed_backfill] YEAR {year}")

        term = year_query(year)
        pmids = esearch_ids(term, max_per_year=BACKFILL_MAX_PER_YEAR)

        print(f"[ingest_pubmed_backfill] year {year}: total pmids={len(pmids)}")
        if not pmids:
            continue

        total_batches = math.ceil(len(pmids) / BACKFILL_FETCH_CHUNK)

        for i in range(0, len(pmids), BACKFILL_FETCH_CHUNK):
            batch = pmids[i:i + BACKFILL_FETCH_CHUNK]

            try:
                xml = efetch_xml(batch)
                rows = parse_pubmed_xml(xml)
                count = upsert_articles(rows)
                total_seen += count

                print(
                    f"[ingest_pubmed_backfill]   batch {i // BACKFILL_FETCH_CHUNK + 1}/{total_batches} "
                    f"pmids={len(batch)} parsed={len(rows)} upserted={count}"
                )

            except Exception as e:
                print(
                    f"[ingest_pubmed_backfill]   batch failed for year={year} "
                    f"offset={i} size={len(batch)} error={e}"
                )
                # keep going; restartability matters more than perfect first pass
                continue

    print(f"\n[ingest_pubmed_backfill] done. total upserted rows processed={total_seen}")


if __name__ == "__main__":
    run()