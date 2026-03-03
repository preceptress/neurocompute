# agent_step1.py

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def run():

    print("Agent starting...")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pmid, title, score
                FROM articles
                ORDER BY created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()

    if row:
        pmid, title, score = row
        print("---- RECORD FOUND ----")
        print("PMID:", pmid)
        print("TITLE:", title)
        print("SCORE:", score)
    else:
        print("No records found.")

if __name__ == "__main__":
    run()

