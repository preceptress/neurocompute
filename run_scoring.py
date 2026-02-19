#!/usr/bin/env python3
"""
Compute and upsert paper scores into paper_scores.

Why this exists:
- Flask UI reads COALESCE(paper_scores.total_score, 0) as "score"
- Your cron is trying to run /home/chief/neurocompute/run_scoring.py
- But that file didn't exist, so scores never update (always 0.00)
"""

from datetime import date
from db import execute

# Simple, explainable scoring model (tweak weights as you like)
TAG_WEIGHTS = {
    "repurpose": 3.0,
    "natural": 2.0,
    "orphan": 1.0,
    "general": 0.0,
}

KEYWORD_BONUS = {
    "amyloid": 1.0,
    "tau": 1.0,
    "alpha-synuclein": 1.0,
    "parkinson": 1.0,
    "alzheimer": 1.0,
    "neurodegener": 1.0,   # matches neurodegeneration/neurodegenerative
}

def recency_score(pub_date):
    """
    0..5 points based on how recent the paper is.
    Very recent papers score higher.
    """
    if not pub_date:
        return 0.0
    days = (date.today() - pub_date).days
    if days < 0:
        days = 0
    # 0-30 days => ~5..4
    # 30-180 days => decays to ~1
    # older => approaches 0
    if days <= 30:
        return 5.0 - (days / 30.0) * 1.0
    if days <= 180:
        # decay from 4 -> 1
        return 4.0 - ((days - 30) / 150.0) * 3.0
    # slow tail
    return max(0.0, 1.0 - ((days - 180) / 365.0) * 1.0)

def keyword_bonus(title, abstract):
    text = f"{title or ''} {abstract or ''}".lower()
    bonus = 0.0
    for kw, w in KEYWORD_BONUS.items():
        if kw in text:
            bonus += w
    # cap keyword bonus so it doesn't dominate
    return min(bonus, 4.0)

def main():
    # Safety: ensure table exists (won't harm if it already does)
    execute("""
        CREATE TABLE IF NOT EXISTS paper_scores (
            paper_id BIGINT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
            total_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    papers = execute("""
        SELECT id, title, abstract, publication_date
        FROM papers
        ORDER BY id DESC
        LIMIT 5000
    """, fetch="all")

    # Pull tags in one go
    tag_rows = execute("""
        SELECT pt.paper_id, t.name
        FROM paper_tags pt
        JOIN tags t ON t.id = pt.tag_id
    """, fetch="all")

    tags_by_paper = {}
    for r in tag_rows:
        tags_by_paper.setdefault(r["paper_id"], set()).add((r["name"] or "").strip().lower())

    updated = 0
    for p in papers:
        pid = p["id"]
        tags = tags_by_paper.get(pid, set())

        tag_score = sum(TAG_WEIGHTS.get(t, 0.0) for t in tags)
        r_score = recency_score(p.get("publication_date"))
        k_bonus = keyword_bonus(p.get("title"), p.get("abstract"))

        total = float(tag_score + r_score + k_bonus)

        execute("""
            INSERT INTO paper_scores (paper_id, total_score, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (paper_id)
            DO UPDATE SET total_score = EXCLUDED.total_score, updated_at = NOW()
        """, (pid, total))

        updated += 1

    print(f"Scored {updated} papers.")

if __name__ == "__main__":
    main()

