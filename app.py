#!/usr/bin/env python3
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg
from flask import Flask, render_template, request

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

app = Flask(__name__)

def get_conn():
    return psycopg.connect(DATABASE_URL)

@app.route("/")
def index():
    q = (request.args.get("q") or "").strip()
    limit = int(request.args.get("limit") or 50)

    with get_conn() as conn:
        # clawbots + last run
        clawbots = conn.execute("""
            SELECT
              cb.name,
              cb.description,
              cb.schedule,
              cb.is_enabled,
              cr.started_at,
              cr.finished_at,
              cr.status,
              cr.items_processed
            FROM clawbots cb
            LEFT JOIN LATERAL (
              SELECT started_at, finished_at, status, items_processed
              FROM clawbot_runs
              WHERE clawbot_name = cb.name
              ORDER BY started_at DESC
              LIMIT 1
            ) cr ON true
            ORDER BY cb.name;
        """).fetchall()

        # articles list with signals joined
        if q:
            rows = conn.execute("""
                SELECT
                  a.id, a.title, a.journal, a.pub_date, a.source, a.url,
                  ps.summary_1_sentence, ps.tags,
                  ps.repurpose_flag, ps.natural_compound_flag, ps.abandoned_trial_flag,
                  ps.novelty_score, ps.neglected_score
                FROM articles a
                LEFT JOIN paper_signals ps ON ps.article_id = a.id
                WHERE a.title ILIKE %s OR COALESCE(a.abstract,'') ILIKE %s
                ORDER BY a.id DESC
                LIMIT %s;
            """, (f"%{q}%", f"%{q}%", limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT
                  a.id, a.title, a.journal, a.pub_date, a.source, a.url,
                  ps.summary_1_sentence, ps.tags,
                  ps.repurpose_flag, ps.natural_compound_flag, ps.abandoned_trial_flag,
                  ps.novelty_score, ps.neglected_score
                FROM articles a
                LEFT JOIN paper_signals ps ON ps.article_id = a.id
                ORDER BY a.id DESC
                LIMIT %s;
            """, (limit,)).fetchall()

    # convert tags jsonb -> python list for template
    def parse_tags(t):
        if t is None:
            return []
        if isinstance(t, list):
            return t
        try:
            return json.loads(t) if isinstance(t, str) else t
        except Exception:
            return []

    articles = []
    for r in rows:
        (aid, title, journal, pub_date, source, url,
         summary, tags,
         repurpose, natural, abandoned,
         novelty, neglected) = r

        articles.append({
            "id": aid,
            "title": title,
            "journal": journal,
            "pub_date": pub_date,
            "source": source,
            "url": url,
            "summary": summary,
            "tags": parse_tags(tags),
            "repurpose": repurpose,
            "natural": natural,
            "abandoned": abandoned,
            "novelty": novelty,
            "neglected": neglected,
        })

    clawbot_cards = []
    for r in clawbots:
        (name, desc, sched, enabled, started_at, finished_at, status, items_processed) = r
        clawbot_cards.append({
            "name": name,
            "description": desc,
            "schedule": sched,
            "enabled": enabled,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "items_processed": items_processed,
        })

    return render_template("index.html", q=q, articles=articles, clawbots=clawbot_cards)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9056, debug=True)

