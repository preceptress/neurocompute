import os
from db import get_conn

BATCH = int(os.environ.get("SUMMARY_BATCH", "10"))

def fake_summary(title, abstract):
    # Stub: replace with OpenAI call in Step 5
    base = abstract.strip() if abstract else ""
    base = base.replace("\n", " ")
    base = (base[:240] + "…") if len(base) > 240 else base
    return f"{title} — {base}" if base else title

def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Find articles with no 'plain' summary yet
            cur.execute("""
                SELECT a.id, a.title, a.abstract
                FROM articles a
                LEFT JOIN article_summaries s
                  ON s.article_id = a.id AND s.summary_type = 'plain'
                WHERE s.id IS NULL
                ORDER BY a.publication_date DESC NULLS LAST, a.id DESC
                LIMIT %s
            """, (BATCH,))
            rows = cur.fetchall()

            if not rows:
                print("[summaries] nothing to summarize")
                return

            created = 0
            for article_id, title, abstract in rows:
                summary = fake_summary(title, abstract)
                cur.execute("""
                    INSERT INTO article_summaries (article_id, model, summary_type, summary)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (article_id, summary_type) DO NOTHING
                """, (article_id, "fake-stub", "plain", summary))
                created += 1

        conn.commit()

    print(f"[summaries] created {created} summaries (stub)")

if __name__ == "__main__":
    main()

