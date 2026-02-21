from flask import Flask, jsonify
from datetime import date
from db import get_conn

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify(ok=True)

@app.get("/db-test")
def db_test():
    """
    1) Ensure 'pubmed' exists in sources
    2) Insert a test article (idempotent)
    3) Return total article count + the inserted row id
    """
    test_doi = "10.0000/neurocompute.test.1"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Ensure source exists
            cur.execute("""
                INSERT INTO sources (name, base_url)
                VALUES (%s, %s)
                ON CONFLICT (name) DO NOTHING
            """, ("pubmed", "https://pubmed.ncbi.nlm.nih.gov/"))

            cur.execute("SELECT id FROM sources WHERE name = %s", ("pubmed",))
            source_id = cur.fetchone()[0]

            # 2) Insert test article if missing
            cur.execute("""
                INSERT INTO articles (
                    doi, title, abstract, journal, publication_date,
                    authors, keywords, source_id, source_record_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (doi) DO NOTHING
                RETURNING id
            """, (
                test_doi,
                "Neurocompute Test Article (Safe to Delete)",
                "Test insert to confirm Flask ↔ PostgreSQL connectivity.",
                "Neurocompute Internal",
                date.today(),
                [{"name": "Eduardo Potter"}],      # JSON automatically handled by psycopg
                ["parkinsons", "alzheimers", "test"],
                source_id,
                "test-1",
            ))

            inserted = cur.fetchone()
            inserted_id = inserted[0] if inserted else None

            # 3) Count articles
            cur.execute("SELECT COUNT(*) FROM articles")
            total = cur.fetchone()[0]

        conn.commit()

    return jsonify(ok=True, inserted_id=inserted_id, total_articles=total)

