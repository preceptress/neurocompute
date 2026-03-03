#!/usr/bin/env python3
"""
Step 2 agent:
- Connects to Postgres
- Fetches 1 record from public.articles
- Writes back agent_status (and updates updated_at)
- Prints what it did

Requirements:
  pip install psycopg[binary] python-dotenv

Env:
  DATABASE_URL=postgresql://user:pass@host:5432/neurocompute
"""

import os
import sys
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)

def run() -> None:
    if not DATABASE_URL:
        die("ERROR: DATABASE_URL is not set.")

    print("Agent starting (step 2)...")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # 1) Fetch 1 record (latest created_at; tie-breaker by id)
            cur.execute("""
                SELECT id, pmid, title, score, agent_status
                FROM public.articles
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """)
            row = cur.fetchone()

            if not row:
                print("No records found in public.articles.")
                return

            article_id, pmid, title, score, agent_status = row

            print("---- READ ----")
            print("id:", article_id)
            print("pmid:", pmid)
            print("score:", score)
            print("agent_status (before):", agent_status)
            print("title:", (title or "")[:180])

            # 2) Write back
            now = datetime.now(timezone.utc)
            new_status = f"checked:{now.isoformat()}"

            cur.execute("""
                UPDATE public.articles
                SET agent_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING agent_status, updated_at
            """, (new_status, article_id))

            updated = cur.fetchone()
            conn.commit()

            print("---- WROTE ----")
            print("agent_status (after):", updated[0])
            print("updated_at:", updated[1])

if __name__ == "__main__":
    run()

