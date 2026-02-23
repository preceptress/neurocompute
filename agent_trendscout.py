#!/usr/bin/env python3
import os
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from db import get_conn

load_dotenv()

WINDOW_HOURS = int(os.environ.get("TRENDSCOUT_WINDOW_HOURS", "72"))
LIMIT = int(os.environ.get("TRENDSCOUT_LIMIT", "400"))       # how many recent signal rows to read
TOP_N = int(os.environ.get("TRENDSCOUT_TOP_N", "10"))         # how many themes to output

def norm(s: str) -> str:
    """
    Normalize strings for stable counting (canonicalize common disease names).
    """
    s = " ".join((s or "").strip().lower().split())
    s = s.replace("’", "'")

    # Canonicalize common variants
    if s == "parkinson's disease":
        return "parkinson's"
    if s == "alzheimer's disease":
        return "alzheimer's"

    return s

def flatten_list(x):
    if not x:
        return []
    if isinstance(x, list):
        return x
    return [x]

def novelty_weight(novelty: str) -> float:
    """
    Weight counts by the model's novelty_signal.
    """
    novelty = (novelty or "low").strip().lower()
    if novelty == "high":
        return 1.6
    if novelty == "medium":
        return 1.3
    return 1.0

def main():
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=WINDOW_HOURS)

    # Pull recent signals
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.article_id,
                       a.title,
                       a.publication_date,
                       s.metadata->'signals' AS signals
                FROM article_summaries s
                JOIN articles a ON a.id = s.article_id
                WHERE s.summary_type = 'signals'
                  AND a.publication_date >= %s
                ORDER BY a.publication_date DESC NULLS LAST, s.article_id DESC
                LIMIT %s
                """,
                (window_start, LIMIT),
            )
            rows = cur.fetchall()

    if not rows:
        print("[TrendScout] no signals found in window")
        return

    # Weighted theme counters
    mech = Counter()
    targets = Counter()
    compounds = Counter()
    diseases = Counter()

    # Evidence mapping: key -> list of article_ids
    evidence_map = defaultdict(list)

    for article_id, title, pub_date, signals in rows:
        signals = signals or {}

        w = novelty_weight(signals.get("novelty_signal", "low"))

        for d in flatten_list(signals.get("diseases")):
            k = norm(d)
            if k:
                diseases[k] += w
                evidence_map[f"disease:{k}"].append(article_id)

        for m in flatten_list(signals.get("mechanisms")):
            k = norm(m)
            if k:
                mech[k] += w
                evidence_map[f"mech:{k}"].append(article_id)

        for t in flatten_list(signals.get("targets_pathways")):
            k = norm(t)
            if k:
                targets[k] += w
                evidence_map[f"target:{k}"].append(article_id)

        for c in flatten_list(signals.get("compounds_interventions")):
            # expected dicts like {"name": "..."}
            if isinstance(c, dict):
                name = norm(c.get("name", ""))
                if name:
                    compounds[name] += w
                    evidence_map[f"compound:{name}"].append(article_id)
            else:
                name = norm(str(c))
                if name:
                    compounds[name] += w
                    evidence_map[f"compound:{name}"].append(article_id)

    # Build ranked “themes”
    themes = []

    def add_top(counter: Counter, label: str):
        for key, count in counter.most_common(TOP_N):
            themes.append({
                "type": label,
                "key": key,
                "count": float(count),
                "evidence_article_ids": evidence_map[f"{label}:{key}"][:15],
            })

    add_top(diseases, "disease")
    add_top(mech, "mech")
    add_top(targets, "target")
    add_top(compounds, "compound")

    # Score themes: weighted count with mild log boost
    for t in themes:
        c = float(t["count"])
        t["score"] = c * (1.0 + math.log(1.0 + c))

    themes.sort(key=lambda x: x["score"], reverse=True)
    top = themes[:TOP_N]

    # Build one compact insight
    insight_title = f"TrendScout: Top {TOP_N} signals (last {WINDOW_HOURS}h)"
    summary_lines = []
    for i, t in enumerate(top, 1):
        summary_lines.append(f"{i}. {t['type']} → {t['key']} (w={t['count']:.2f})")
    insight_summary = "\n".join(summary_lines)

    evidence = {
        "window_hours": WINDOW_HOURS,
        "source_rows": len(rows),
        "top": top,
    }

    metadata = {
        "agent": "TrendScout",
        "window_start": window_start.isoformat(),
        "window_end": now.isoformat(),
        "limit": LIMIT,
        "top_n": TOP_N,
        "scoring": "weighted_count*(1+log(1+weighted_count))",
        "novelty_weights": {"low": 1.0, "medium": 1.3, "high": 1.6},
    }

    # Write to insights
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insights (agent_name, window_start, window_end, title, summary, evidence, score, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "TrendScout",
                    window_start,
                    now,
                    insight_title,
                    insight_summary,
                    json.dumps(evidence),
                    float(top[0]["score"]) if top else 0.0,
                    json.dumps(metadata),
                ),
            )
        conn.commit()

    print("[TrendScout] wrote 1 insight row")
    print(insight_summary)

if __name__ == "__main__":
    main()

