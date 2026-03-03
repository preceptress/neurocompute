#!/usr/bin/env python3
"""
ingest_pubmed.py
PubMed ingest for Parkinson's + Alzheimer's (last N hours), upserting into Postgres.

Requirements:
  pip install requests psycopg[binary] python-dotenv

Env:
  DATABASE_URL=postgresql://user:pass@localhost:5432/neurocompute
  NCBI_EMAIL=you@example.com          (recommended by NCBI)
  NCBI_API_KEY=xxxxx                 (optional but helps rate limits)
  INGEST_HOURS=24                    (optional; default 24)
  INGEST_MAX=0                       (optional; 0 = no cap)
"""

import os
import sys
import time
import json
import math
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "").strip()
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "").strip()

INGEST_HOURS = int(os.getenv("INGEST_HOURS", "24"))
INGEST_MAX = int(os.getenv("INGEST_MAX", "0"))  # 0 = unlimited

ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

QUERIES = [
    # Parkinson’s
    '("Parkinson Disease"[MeSH Terms] OR parkinson*[Title/Abstract])',
    # Alzheimer’s
    '("Alzheimer Disease"[MeSH Terms] OR alzheimer*[Title/Abstract])',
]


def die(msg: str, code: int = 1):
    print(f"[ingest_pubmed] ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _ncbi_params(extra: dict) -> dict:
    p = dict(extra)
    if NCBI_EMAIL:
        p["email"] = NCBI_EMAIL
    if NCBI_API_KEY:
        p["api_key"] = NCBI_API_KEY
    return p


def _sleep_if_no_key():
    # NCBI rate limiting is much friendlier with an API key.
    if not NCBI_API_KEY:
        time.sleep(0.34)


def esearch_ids(term: str, hours: int) -> list[int]:
    """
    Find PubMed IDs matching term in the last `hours`.

    NOTE: PubMed ESearch `reldate` is in DAYS, not hours.
    So we approximate: ceil(hours / 24).
    """
    days = max(1, math.ceil(hours / 24))

    params = _ncbi_params({
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "datetype": "edat",
        "reldate": str(days),
        "retmax": "0",
    })

    r = requests.get(ESEARCH, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    count = int(data["esearchresult"]["count"])
    if count == 0:
        return []

    ids: list[int] = []
    retmax = 5000
    pages = math.ceil(count / retmax)

    for page in range(pages):
        p2 = dict(params)
        p2["retmax"] = str(retmax)
        p2["retstart"] = str(page * retmax)

        r2 = requests.get(ESEARCH, params=p2, timeout=30)
        r2.raise_for_status()
        d2 = r2.json()

        batch = [int(x) for x in d2["esearchresult"].get("idlist", [])]
        ids.extend(batch)

        if INGEST_MAX > 0 and len(ids) >= INGEST_MAX:
            return ids[:INGEST_MAX]

        _sleep_if_no_key()

    return ids


def efetch_xml(pmids: list[int]) -> str:
    if not pmids:
        return ""
    params = _ncbi_params({
        "db": "pubmed",
        "id": ",".join(str(x) for x in pmids),
        "retmode": "xml",
    })
    r = requests.get(EFETCH, params=params, timeout=60)
    r.raise_for_status()
    _sleep_if_no_key()
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

        def month_to_int(mm: str):
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
            name = (fore + " " + last).strip() or au.findtext("CollectiveName") or ""
            if name:
                authors.append(name)

        keywords = []
        for kw in article.findall(".//KeywordList/Keyword"):
            if kw.text:
                keywords.append(kw.text.strip())

        mesh_terms = []
        for mh in article.findall(".//MeshHeadingList/MeshHeading/DescriptorName"):
            if mh.text:
                mesh_terms.append(mh.text.strip())

        out.append({
            "pmid": pmid,
            "doi": doi,
            "url": url,
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "publication_date": pub_date,  # YYYY-MM-DD or None
            "authors": authors,
            "keywords": keywords,
            "mesh_terms": mesh_terms,
        })

    return out


UPSERT_SQL = """
INSERT INTO public.articles
    (pmid, doi, url, title, abstract, journal, publication_date,
     authors, keywords, mesh_terms,
     created_at, updated_at,
     agent_status, agent_score, summary_1s, tags, score_components, scored_at)
VALUES
    (%(pmid)s, %(doi)s, %(url)s, %(title)s, %(abstract)s, %(journal)s, %(publication_date)s,
     %(authors)s::jsonb, %(keywords)s::jsonb, %(mesh_terms)s::jsonb,
     now(), now(),
     'pending', NULL, NULL, NULL, NULL, NULL)
ON CONFLICT (pmid)
DO UPDATE SET
    doi = COALESCE(EXCLUDED.doi, public.articles.doi),
    url = COALESCE(EXCLUDED.url, public.articles.url),
    title = COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title),
    abstract = COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract),
    journal = COALESCE(EXCLUDED.journal, public.articles.journal),
    publication_date = COALESCE(EXCLUDED.publication_date, public.articles.publication_date),
    authors = CASE WHEN EXCLUDED.authors IS NOT NULL THEN EXCLUDED.authors ELSE public.articles.authors END,
    keywords = CASE WHEN EXCLUDED.keywords IS NOT NULL THEN EXCLUDED.keywords ELSE public.articles.keywords END,
    mesh_terms = CASE WHEN EXCLUDED.mesh_terms IS NOT NULL THEN EXCLUDED.mesh_terms ELSE public.articles.mesh_terms END,

    agent_status = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN 'pending'
        ELSE public.articles.agent_status
    END,

    agent_score = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.agent_score
    END,
    summary_1s = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.summary_1s
    END,
    tags = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.tags
    END,
    score_components = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.score_components
    END,
    scored_at = CASE
        WHEN (COALESCE(NULLIF(EXCLUDED.title,''), public.articles.title) IS DISTINCT FROM public.articles.title)
          OR (COALESCE(NULLIF(EXCLUDED.abstract,''), public.articles.abstract) IS DISTINCT FROM public.articles.abstract)
        THEN NULL
        ELSE public.articles.scored_at
    END,

    updated_at = now()
RETURNING (xmax = 0) AS inserted;
"""


def db_preflight(conn: psycopg.Connection):
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user;")
        dbname, dbuser = cur.fetchone()
        print(f"[ingest_pubmed] connected: db={dbname} user={dbuser}")

        cur.execute("SELECT to_regclass('public.articles');")
        reg = cur.fetchone()[0]
        if not reg:
            die("public.articles table not found. Wrong DB or schema not applied.")

        # Ensure pmid has a unique constraint or unique index.
        cur.execute("""
            SELECT
              EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE c.relname = 'articles'
                  AND i.indisunique
                  AND a.attname = 'pmid'
              ) AS has_unique_pmid;
        """)
        has_unique = cur.fetchone()[0]
        if not has_unique:
            die("No UNIQUE index/constraint found on articles.pmid. ON CONFLICT (pmid) will fail.")


def upsert_articles(rows: list[dict]) -> tuple[int, int, int]:
    """
    Returns (seen, inserted, updated)
    """
    if not DATABASE_URL:
        die("DATABASE_URL is not set (check your .env and systemd EnvironmentFile).")

    if not rows:
        return (0, 0, 0)

    seen = len(rows)

    payloads = []
    for r in rows:
        payloads.append({
            **r,
            "authors": json.dumps(r.get("authors") or []),
            "keywords": json.dumps(r.get("keywords") or []),
            "mesh_terms": json.dumps(r.get("mesh_terms") or []),
        })

    inserted = 0
    updated = 0

    with psycopg.connect(DATABASE_URL) as conn:
        db_preflight(conn)

        with conn.cursor() as cur:
            # Batch insert/upsert, but still get per-row RETURNING.
            # psycopg3: executemany + RETURNING is supported via cur.executemany then fetchall per statement
            # We'll just loop in chunks for clarity and reliability.
            chunk = 200
            for i in range(0, len(payloads), chunk):
                batch = payloads[i:i+chunk]
                for p in batch:
                    cur.execute(UPSERT_SQL, p)
                    was_inserted = cur.fetchone()[0]
                    if was_inserted:
                        inserted += 1
                    else:
                        updated += 1

        conn.commit()

    return (seen, inserted, updated)


def ingest_last_n_hours():
    print(f"[ingest_pubmed] starting (hours={INGEST_HOURS}, max={INGEST_MAX or 'unlimited'})")
    if not NCBI_EMAIL:
        print("[ingest_pubmed] NOTE: NCBI_EMAIL not set (recommended by NCBI).")

    combined_ids: set[int] = set()
    for q in QUERIES:
        ids = esearch_ids(q, INGEST_HOURS)
        print(f"[ingest_pubmed] query matched {len(ids)} ids: {q}")
        combined_ids.update(ids)

    pmids = sorted(combined_ids)
    print(f"[ingest_pubmed] total unique pmids: {len(pmids)}")

    if not pmids:
        print("[ingest_pubmed] nothing to ingest")
        return

    fetch_chunk = 200
    total = len(pmids)

    parsed_total = 0
    inserted_total = 0
    updated_total = 0

    for i in range(0, total, fetch_chunk):
        batch = pmids[i:i+fetch_chunk]
        xml = efetch_xml(batch)
        rows = parse_pubmed_xml(xml)

        seen, ins, upd = upsert_articles(rows)

        parsed_total += seen
        inserted_total += ins
        updated_total += upd

        print(
            f"[ingest_pubmed] batch {i//fetch_chunk+1}/{math.ceil(total/fetch_chunk)}: "
            f"pmids={len(batch)} parsed={seen} inserted={ins} updated={upd}"
        )

    print(f"[ingest_pubmed] done. parsed={parsed_total} inserted={inserted_total} updated={updated_total}")


if __name__ == "__main__":
    ingest_last_n_hours()