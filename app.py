import os
import math
import psycopg
from dotenv import load_dotenv
from flask import Flask, render_template, request

load_dotenv()

app = Flask(__name__)

PER_PAGE = 25


def get_db():
    db = os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL not set")
    return db


def grade(score):
    if score is None:
        return "U"
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "E"


@app.route("/")
def index():
    db = get_db()

    page = request.args.get("page", 1, type=int)
    sort = (request.args.get("sort", "rank") or "rank").strip().lower()
    q = (request.args.get("q") or "").strip()

    if page < 1:
        page = 1

    offset = (page - 1) * PER_PAGE

    sort_sql = {
        "rank": "rank_score DESC NULLS LAST, ai_score DESC NULLS LAST, publication_date DESC NULLS LAST, created_at DESC",
        "ai": "ai_score DESC NULLS LAST, rank_score DESC NULLS LAST, publication_date DESC NULLS LAST, created_at DESC",
        "base": "base_score DESC NULLS LAST, rank_score DESC NULLS LAST, publication_date DESC NULLS LAST, created_at DESC",
        "newest": "publication_date DESC NULLS LAST, created_at DESC",
        "oldest": "publication_date ASC NULLS LAST, created_at ASC",
    }.get(sort, "rank_score DESC NULLS LAST, ai_score DESC NULLS LAST, publication_date DESC NULLS LAST, created_at DESC")

    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            if q:
                like = f"%{q}%"

                cur.execute(f"""
                    SELECT
                        pmid,
                        title,
                        journal,
                        publication_date,
                        ai_score,
                        base_score,
                        rank_score,
                        summary_1s
                    FROM articles
                    WHERE
                        title ILIKE %s
                        OR abstract ILIKE %s
                        OR journal ILIKE %s
                        OR CAST(pmid AS TEXT) ILIKE %s
                    ORDER BY {sort_sql}
                    LIMIT %s OFFSET %s
                """, (like, like, like, like, PER_PAGE, offset))
                rows = cur.fetchall()

                cur.execute("""
                    SELECT count(*)
                    FROM articles
                    WHERE
                        title ILIKE %s
                        OR abstract ILIKE %s
                        OR journal ILIKE %s
                        OR CAST(pmid AS TEXT) ILIKE %s
                """, (like, like, like, like))
                total_row = cur.fetchone()

            else:
                cur.execute(f"""
                    SELECT
                        pmid,
                        title,
                        journal,
                        publication_date,
                        ai_score,
                        base_score,
                        rank_score,
                        summary_1s
                    FROM articles
                    ORDER BY {sort_sql}
                    LIMIT %s OFFSET %s
                """, (PER_PAGE, offset))
                rows = cur.fetchall()

                cur.execute("SELECT count(*) FROM articles")
                total_row = cur.fetchone()

    total = total_row[0] if total_row else 0
    total_pages = max(1, math.ceil(total / PER_PAGE))

    if page > total_pages:
        page = total_pages

    articles = []
    for r in rows:
        score_for_grade = r[6] if r[6] is not None else (r[4] if r[4] is not None else r[5])

        articles.append({
            "pmid": r[0],
            "title": r[1],
            "journal": r[2],
            "publication_date": r[3],
            "ai_score": r[4],
            "base_score": r[5],
            "rank_score": r[6],
            "summary_1s": r[7] or "",
            "grade": grade(score_for_grade)
        })

    return render_template(
        "index.html",
        articles=articles,
        page=page,
        total_pages=total_pages,
        total=total,
        sort=sort,
        q=q
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
                    ai_score,
                    base_score,
                    rank_score,
                    abstract,
                    summary_1s
                FROM articles
                WHERE pmid = %s
                LIMIT 1
            """, (pmid,))
            row = cur.fetchone()

    if not row:
        return "Not found", 404

    paper_data = {
        "pmid": row[0],
        "title": row[1],
        "journal": row[2],
        "publication_date": row[3],
        "ai_score": row[4],
        "base_score": row[5],
        "rank_score": row[6],
        "abstract": row[7] or "",
        "summary_1s": row[8] or "",
        "grade": grade(row[6] if row[6] is not None else (row[4] if row[4] is not None else row[5]))
    }

    return render_template("paper.html", paper=paper_data)


@app.route("/ping")
def ping():
    return "pong", 200


if __name__ == "__main__":
    app.run(debug=True)
