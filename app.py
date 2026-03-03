import os
import psycopg
from dotenv import load_dotenv
from flask import Flask, render_template, request

load_dotenv()

app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True
app.config["DEBUG"] = True


def get_db():
    db = os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL not set")
    return db


@app.route("/")
def index():
    db = get_db()
    q = request.args.get("q")

    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:

            # Fetch articles
            if q:
                cur.execute("""
                    SELECT pmid, title, journal, publication_date, score
                    FROM articles
                    WHERE title ILIKE %s
                    ORDER BY score DESC NULLS LAST,
                             publication_date DESC NULLS LAST,
                             created_at DESC
                    LIMIT 50;
                """, (f"%{q}%",))
            else:
                cur.execute("""
                    SELECT pmid, title, journal, publication_date, score
                    FROM articles
                    ORDER BY score DESC NULLS LAST,
                             publication_date DESC NULLS LAST,
                             created_at DESC
                    LIMIT 50;
                """)

            rows = cur.fetchall()

            # Fetch last successful clawbot run
            cur.execute("""
                SELECT finished_at
                FROM clawbot_runs
                WHERE status = 'ok'
                ORDER BY finished_at DESC
                LIMIT 1;
            """)
            last_run = cur.fetchone()

    articles = [
        {
            "pmid": r[0],
            "title": r[1],
            "journal": r[2],
            "publication_date": r[3],
            "score": r[4],
        }
        for r in rows
    ]

    last_updated = last_run[0] if last_run else None

    return render_template(
        "index.html",
        articles=articles,
        q=q,
        last_updated=last_updated
    )

@app.route("/paper/<pmid>")
def paper(pmid):
    db = get_db()

    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pmid,
                    title,
                    journal,
                    publication_date,
                    score,
                    created_at,
                    abstract,
                    summary_1
                FROM articles
                WHERE pmid = %s
                LIMIT 1;
            """, (pmid,))
            row = cur.fetchone()

    if not row:
        return "Paper not found", 404

    paper = {
        "pmid": row[0],
        "title": row[1],
        "journal": row[2],
        "publication_date": row[3],
        "score": row[4],
        "created_at": row[5],
        "abstract": row[6] or "",
        "summary_1": row[7] or "",
    }

    return render_template("paper.html", paper=paper)


@app.route("/ping")
def ping():
    return "pong", 200


@app.route("/dbcheck")
def dbcheck():
    try:
        with psycopg.connect(get_db()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return "DB OK", 200
    except Exception as e:
        return f"DB FAIL: {type(e).__name__}: {e}", 500


@app.route("/articles_count")
def articles_count():
    try:
        with psycopg.connect(get_db()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM articles;")
                count = cur.fetchone()[0]
        return f"Articles: {count}", 200
    except Exception as e:
        return f"FAIL: {type(e).__name__}: {e}", 500


@app.route("/articles_schema")
def articles_schema():
    with psycopg.connect(get_db()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='articles'
                ORDER BY ordinal_position;
            """)
            cols = [r[0] for r in cur.fetchall()]

    return "<br>".join(cols), 200