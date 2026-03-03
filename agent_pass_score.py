#!/usr/bin/env python3
"""
agent_pass_score.py
Compute articles.score from paper_signals (Score_v1) and log to clawbot_runs.

Requirements:
  pip install psycopg[binary] python-dotenv
Env:
  DATABASE_URL
  SCORE_BATCH (default 500)
"""

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BATCH = int(os.getenv("SCORE_BATCH", "500"))
CLAWBOT_NAME = "score"

if not DATABASE_URL:
    raise SystemExit("Missing DATABASE_URL")

FETCH = """
SELECT a.id,
       ps.novelty_score,
       ps.neglected_score,
       COALESCE(ps.repurpose_flag,false),
       COALESCE(ps.abandoned_trial_flag,false),
       COALESCE(ps.natural_compound_flag,false)
FROM articles a
JOIN paper_signals ps ON ps.article_id = a.id
WHERE a.score IS NULL
ORDER BY a.id DESC
LIMIT %s;
"""

UPDATE = "UPDATE articles SET score=%s WHERE id=%s;"

INSERT_RUN = """
INSERT INTO clawbot_runs (clawbot_name)
VALUES (%s)
RETURNING id;
"""

FINISH_OK = """
UPDATE clawbot_runs
SET finished_at=now(), status='ok', items_processed=%s
WHERE id=%s;
"""

FINISH_ERR = """
UPDATE clawbot_runs
SET finished_at=now(), status='error', items_processed=%s, error=%s
WHERE id=%s;
"""

def clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))

def score_v1(novelty, neglected, repurpose, abandoned, natural):
    # weights: neglected > novelty + boosts
    base = 0.45*neglected + 0.35*novelty + (10 if repurpose else 0) + (6 if abandoned else 0) + (4 if natural else 0)
    return int(round(clamp(base, 0, 100)))

def main():
    processed = 0
    with psycopg.connect(DATABASE_URL) as conn:
        run_id = conn.execute(INSERT_RUN, (CLAWBOT_NAME,)).fetchone()[0]
        conn.commit()

        try:
            rows = conn.execute(FETCH, (BATCH,)).fetchall()
            for (aid, novelty, neglected, repurpose, abandoned, natural) in rows:
                novelty = int(novelty or 0)
                neglected = int(neglected or 0)
                s = score_v1(novelty, neglected, repurpose, abandoned, natural)
                conn.execute(UPDATE, (s, aid))
                processed += 1

            conn.commit()
            conn.execute(FINISH_OK, (processed, run_id))
            conn.commit()
            print(f"✅ scored {processed} articles")

        except Exception as e:
            conn.execute(FINISH_ERR, (processed, f"{type(e).__name__}: {e}"[:8000], run_id))
            conn.commit()
            raise

if __name__ == "__main__":
    main()

