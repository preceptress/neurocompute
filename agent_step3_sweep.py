#!/usr/bin/env python3
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def run(limit: int = 200):
    if not DATABASE_URL:
        raise SystemExit("ERROR: DATABASE_URL not set")

    processed = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            while processed < limit:
                # get next unprocessed row
                cur.execute("""
                    SELECT id, pmid, title
                    FROM public.articles
                    WHERE agent_status IS NULL
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)
                row = cur.fetchone()
                if not row:
                    print(f"Done. No more unprocessed rows. processed={processed}")
                    break

                article_id, pmid, title = row

                # mark as checked
                cur.execute("""
                    UPDATE public.articles
                    SET agent_status = 'checked',
                        agent_checked_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                """, (article_id,))
                conn.commit()

                processed += 1
                print(f"[{processed}] checked id={article_id} pmid={pmid} title={(title or '')[:120]}")

if __name__ == "__main__":
    run(limit=int(os.getenv("AGENT_LIMIT", "200")))

