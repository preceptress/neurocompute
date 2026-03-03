from contextlib import contextmanager
import psycopg
from .config import DATABASE_URL

@contextmanager
def get_conn():
    with psycopg.connect(DATABASE_URL) as conn:
        yield conn

def fetch_pending_articles(limit: int):
    """
    Atomically claims rows for processing using SKIP LOCKED.
    Returns list of tuples.
    """
    q = """
    SELECT id, pmid, title, abstract, journal, publication_date
    FROM public.articles
    WHERE agent_status = 'pending'
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(q, (limit,))
            rows = cur.fetchall()

            # Mark claimed as processing
            if rows:
                ids = [r[0] for r in rows]
                cur.execute(
                    "UPDATE public.articles SET agent_status='processing' WHERE id = ANY(%s)",
                    (ids,),
                )
            cur.execute("COMMIT;")
            return rows

def mark_done(article_id: int, agent_score: int, summary_1s: str, tags: dict, components: dict):
    q = """
    UPDATE public.articles
    SET agent_status='done',
        agent_score=%s,
        summary_1s=%s,
        tags=%s::jsonb,
        score_components=%s::jsonb,
        scored_at=NOW()
    WHERE id=%s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (agent_score, summary_1s, psycopg.types.json.Json(tags),
                            psycopg.types.json.Json(components), article_id))

def mark_error(article_id: int, err: str):
    q = """
    UPDATE public.articles
    SET agent_status='error',
        summary_1s=LEFT(%s, 240)
    WHERE id=%s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (f"Agent error: {err}", article_id))

