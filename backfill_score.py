#!/usr/bin/env python3
"""
Backfill public.articles.score for rows where score IS NULL.

Requirements:
  pip install psycopg[binary] python-dotenv
Env:
  DATABASE_URL=...
"""

import os
import re
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BATCH = int(os.getenv("BATCH", "200"))  # rows per run

# ---- Replace this with YOUR REAL scoring logic (best) ----
# If you already have compute_score(title, abstract, journal, mesh_terms, keywords, publication_date),
# import it and call it here.
KEYWORDS = [
    # neurodegeneration core
    "parkinson", "alzheimer", "alpha-synuclein", "amyloid", "tau", "neuroinflammation",
    "microglia", "mitochondria", "synapse", "dopamine", "autophagy", "lysosome",
    # therapeutic signals
    "drug", "therapy", "therapeutic", "inhibitor", "agonist", "antagonist",
    "clinical", "trial", "phase", "biomarker", "target", "intervention",
]

def compute_score_fallback(title: str | None, abstract: str | None) -> float:
    """
    Fallback heuristic score (0-100). Use only if you don't yet wire in your real scoring logic.
    """
    text = f"{title or ''} {abstract or ''}".lower()
    hits = 0
    for k in KEYWORDS:
        if k in text:
            hits += 1

    # simple saturating function
    score = min(100.0, hits * 8.0)

    # small boosts
    if "randomized" in text or "double-blind" in text:
        score += 10
    if "meta-analysis" in text or "systematic review" in text:
        score += 5

    return max(0.0, min(100.0, score))

def main():
    if not DATABASE_URL:
        raise SystemExit("ERROR: DATABASE_URL not set")

    updated = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Grab a batch of NULL-score rows
            cur.execute("""
                SELECT id, title, abstract
                FROM public.articles
                WHERE score IS NULL
                ORDER BY created_at ASC, id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            """, (BATCH,))
            rows = cur.fetchall()

            if not rows:
                print("No rows with score IS NULL. Done.")
                return

            for (article_id, title, abstract) in rows:
                score = compute_score_fallback(title, abstract)

                cur.execute("""
                    UPDATE public.articles
                    SET score = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (score, article_id))
                updated += 1

        conn.commit()

    print(f"Backfill complete: updated {updated} rows (batch size={BATCH}).")

if __name__ == "__main__":
    main()

