#!/usr/bin/env python3

import os
import json
import psycopg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
LOG_PATH = "/home/chief/neurocompute/logs/agent_pass.log"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

def log(message):
    with open(LOG_PATH, "a") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {message}\n")

def main():
    log("Agent pass started")

    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:

            # Select up to 5 articles needing processing
            cur.execute("""
                SELECT id, title
                FROM articles
                WHERE summary_1s IS NULL
                ORDER BY publication_date DESC NULLS LAST
                LIMIT 5
            """)
            rows = cur.fetchall()

            if not rows:
                log("No articles needing processing")
                return

            for article_id, title in rows:
                try:
                    fake_summary = f"This study investigates: {title[:120]}"
                    fake_tags = json.dumps(["stub", "auto-tag"])
                    fake_score = 50.0

                    cur.execute("""
                        UPDATE articles
                        SET summary_1s = %s,
                            tags = %s::jsonb,
                            score = COALESCE(score, %s),
                            updated_at = NOW()
                        WHERE id = %s
                    """, (fake_summary, fake_tags, fake_score, article_id))

                    log(f"Processed article {article_id}")

                except Exception as e:
                    log(f"Error processing article {article_id}: {e}")

    log("Agent pass finished")

if __name__ == "__main__":
    main()

