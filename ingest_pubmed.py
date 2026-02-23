#!/usr/bin/env python3
"""
ingest_pubmed.py
One-shot PubMed ingest for Parkinson's + Alzheimer's (last 24 hours).

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

# PubMed E-utilities endpoints
ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# Two “disease radar” queries. You can tweak these later.
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

def esearch_ids(term: str, hours: int) -> list[int]:
    """
    Find all PubMed IDs matching the term in the last `hours`.
    We use datetype=edat (Entrez date) so it tracks PubMed indexing/entry.
    """
    # Get total count first
    params = _ncbi_params({
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "datetype": "edat",
        "reldate": str(hours * 24 // 24),  # reldate is in days. We'll approximate via days.
        # If you want exact hours, use mindate/maxdate in YYYY/MM/DD and accept day precision.
        "retmax": "0",
    })
    r = requests.get(ESEARCH, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    count = int(data["esearchresult"]["count"])
    if count == 0:
        return []

    # Page through IDs
    ids: list[int] = []
    retmax = 5000  # PubMed allows big batches
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

        # be nice to NCBI if no API key
        if not NCBI_API_KEY:
            time.sleep(0.34)

    return ids

def efetch_xml(pmids: list[int]) -> str:
    """
    Fetch PubMed records in XML for a list of IDs.
    """
    if not pmids:
        return ""
    params = _ncbi_params({
        "db": "pubmed",
        "id": ",".join(str(x) for x in pmids),
        "retmode": "xml",
    })
    r = requests.get(EFETCH, params=params, timeout=60)
    r.raise_for_status()

    if not NCBI_API_KEY:
        time.sleep(0.34)

    return r.text

def parse_pubmed_xml(xml_text: str) -> list[dict]:
    """
    Minimal XML parse without heavy deps.
    For “million dollar” polish later we can switch to BioPython or lxml.
    """
    # To keep this dependency-light, we'll do a very pragmatic parse using stdlib xml.
    import xml.etree.ElementTree as ET

    if not xml_text.strip():
        return []

    root = ET.fromstring(xml_text)
    out: list[dict] = []

    for article in root.findall(".//PubmedArticle"):
        # PMID
        pmid_el = article.find(".//MedlineCitation/PMID")
        pmid = int(pmid_el.text) if pmid_el is not None and pmid_el.text else None
        if not pmid:
            continue

        # DOI + URL
        doi = None
        for aid in article.findall(".//ArticleIdList/ArticleId"):
            if (aid.get("IdType") or "").lower() == "doi" and aid.text:
                doi = aid.text.strip()
                break
        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        # Title
        title_el = article.find(".//Article/ArticleTitle")
        title = "".join(title_el.itertext()).strip() if title_el is not None else ""

        # Abstract
        abs_parts = []
        for a in article.findall(".//Article/Abstract/AbstractText"):
            label = a.get("Label")
            txt = "".join(a.itertext()).strip() if a is not None else ""
            if not txt:
                continue
            abs_parts.append(f"{label}: {txt}" if label else txt)
        abstract = "\n\n".join(abs_parts).strip()

        # Journal
        j_el = article.find(".//Article/Journal/Title")
        journal = j_el.text.strip() if j_el is not None and j_el.text else None

        # Publication date (best-effort)
        pub_date = None
        y = article.findtext(".//Article/Journal/JournalIssue/PubDate/Year")
        m = article.findtext(".//Article/Journal/JournalIssue/PubDate/Month")
        d = article.findtext(".//Article/Journal/JournalIssue/PubDate/Day")

        def month_to_int(mm: str) -> int | None:
            if not mm:
                return None
            mm = mm.strip()
            if mm.isdigit():
                return int(mm)
            lookup = {
                "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
            }
            key = mm[:3].lower()
            return lookup.get(key)

        try:
            if y:
                yy = int(y)
                mm_i = month_to_int(m) or 1
                dd = int(d) if d and d.isdigit() else 1
                pub_date = f"{yy:04d}-{mm_i:02d}-{dd:02d}"
        except Exception:
            pub_date = None

        # Authors
        authors = []
        for au in article.findall(".//Article/AuthorList/Author"):
            last = au.findtext("LastName") or ""
            fore = au.findtext("ForeName") or ""
            name = (fore + " " + last).strip() or au.findtext("CollectiveName") or ""
            if name:
                authors.append(name)

        # Keywords
        keywords = []
        for kw in article.findall(".//KeywordList/Keyword"):
            if kw.text:
                keywords.append(kw.text.strip())

        # MeSH terms
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
INSERT INTO articles
    (pmid, doi, url, title, abstract, journal, publication_date, authors, keywords, mesh_terms, created_at, updated_at)
VALUES
    (%(pmid)s, %(doi)s, %(url)s, %(title)s, %(abstract)s, %(journal)s, %(publication_date)s,
     %(authors)s::jsonb, %(keywords)s::jsonb, %(mesh_terms)s::jsonb,
     now(), now())
ON CONFLICT (pmid)
DO UPDATE SET
    doi = COALESCE(EXCLUDED.doi, articles.doi),
    url = COALESCE(EXCLUDED.url, articles.url),
    title = COALESCE(NULLIF(EXCLUDED.title,''), articles.title),
    abstract = COALESCE(NULLIF(EXCLUDED.abstract,''), articles.abstract),
    journal = COALESCE(EXCLUDED.journal, articles.journal),
    publication_date = COALESCE(EXCLUDED.publication_date, articles.publication_date),
    authors = CASE WHEN EXCLUDED.authors IS NOT NULL THEN EXCLUDED.authors ELSE articles.authors END,
    keywords = CASE WHEN EXCLUDED.keywords IS NOT NULL THEN EXCLUDED.keywords ELSE articles.keywords END,
    mesh_terms = CASE WHEN EXCLUDED.mesh_terms IS NOT NULL THEN EXCLUDED.mesh_terms ELSE articles.mesh_terms END,
    updated_at = now()
;
"""

def upsert_articles(rows: list[dict]) -> tuple[int, int]:
    """
    Returns (seen, upserted)
    """
    if not DATABASE_URL:
        die("DATABASE_URL is not set (check your .env and systemd EnvironmentFile).")

    if not rows:
        return (0, 0)

    now = datetime.now(timezone.utc).isoformat()
    seen = len(rows)
    upserted = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for r in rows:
                payload = dict(r)
                payload["authors"] = json.dumps(r.get("authors") or [])
                payload["keywords"] = json.dumps(r.get("keywords") or [])
                payload["mesh_terms"] = json.dumps(r.get("mesh_terms") or [])

                cur.execute(UPSERT_SQL, payload)
                upserted += 1
        conn.commit()

    return (seen, upserted)

def ingest_last_24h():
    print(f"[ingest_pubmed] starting (hours={INGEST_HOURS}, max={INGEST_MAX or 'unlimited'})")

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

    # EFETCH in chunks
    chunk = 200
    total = len(pmids)
    inserted_total = 0
    seen_total = 0

    for i in range(0, total, chunk):
        batch = pmids[i:i+chunk]
        xml = efetch_xml(batch)
        rows = parse_pubmed_xml(xml)
        seen, upserted = upsert_articles(rows)

        seen_total += seen
        inserted_total += upserted

        print(f"[ingest_pubmed] batch {i//chunk+1}/{math.ceil(total/chunk)}: pmids={len(batch)} parsed={seen} upserted={upserted}")

    print(f"[ingest_pubmed] done. parsed={seen_total} upserted={inserted_total}")

if __name__ == "__main__":
    ingest_last_24h()

