import json
from datetime import date, timedelta
from openai import OpenAI
import psycopg
from .config import DATABASE_URL, OPENAI_API_KEY, OPENAI_MODEL, require_env

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM = """You write a daily research briefing for Parkinson's and Alzheimer's.
Input is JSON rows with: title, agent_score, summary_1s, tags.
Output STRICT JSON only with keys:
- top_picks: array of 5 objects {title, why, score, tags}
- rising_themes: array of 3 strings
- actionable_leads: array of 2 objects {lead, why}
"""

def load_last_24h_scored(conn):
    q = """
    SELECT title, agent_score, summary_1s, tags
    FROM public.articles
    WHERE agent_status='done'
      AND scored_at >= NOW() - INTERVAL '24 hours'
      AND agent_score IS NOT NULL
    ORDER BY agent_score DESC
    LIMIT 50
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    items = []
    for (title, score, summary_1s, tags) in rows:
        items.append({
            "title": title,
            "agent_score": score,
            "summary_1s": summary_1s,
            "tags": tags or {}
        })
    return items

def upsert_briefing(conn, briefing_date: date, top_picks_json: dict):
    q = """
    INSERT INTO public.daily_briefings (briefing_date, top_picks)
    VALUES (%s, %s::jsonb)
    ON CONFLICT (briefing_date)
    DO UPDATE SET top_picks = EXCLUDED.top_picks, created_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(q, (briefing_date, psycopg.types.json.Json(top_picks_json)))

def main():
    require_env()
    today = date.today()

    with psycopg.connect(DATABASE_URL) as conn:
        items = load_last_24h_scored(conn)
        if not items:
            print("No scored items in last 24h; skipping.")
            return

        resp = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role":"system","content": SYSTEM},
                {"role":"user","content": json.dumps({"items": items})}
            ],
            temperature=0.3,
        )
        out = json.loads(resp.output_text.strip())
        upsert_briefing(conn, today, out)
        conn.commit()

    print(f"Saved daily briefing for {today}.")

if __name__ == "__main__":
    main()

