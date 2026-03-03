#!/usr/bin/env python3
import os, json, time, sys
import psycopg
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

# Safety knobs
AGENT_LIMIT = int(os.getenv("AGENT_LIMIT", "10"))          # how many rows per run
SLEEP_SECONDS = float(os.getenv("AGENT_SLEEP", "0.2"))     # small pause between calls
MIN_TEXT = int(os.getenv("AGENT_MIN_TEXT", "40"))          # skip rows with too-little content

client = OpenAI(api_key=OPENAI_API_KEY)

def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    raise SystemExit(code)

def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))

def llm_score_and_summary(title: str, abstract: str, ingest_score):
    """
    Returns (agent_score: float, summary_1s: str)
    """
    system = (
        "You are a biomedical research triage agent for Alzheimer's and Parkinson's drug discovery. "
        "Return ONLY valid JSON with keys: agent_score (0-100 number), summary_1s (one sentence)."
    )

    user_obj = {
        "task": "Score the paper's importance for ALZ/PD drug discovery and write a single-sentence summary.",
        "inputs": {
            "title": title or "",
            "abstract": abstract or "",
            "ingest_score": ingest_score,
        },
        "scoring_rubric": [
            "High score if it suggests actionable targets/compounds, strong mechanistic insight, or clinical translation.",
            "Boost human/clinical relevance, biomarkers, interventions, novel delivery, or clear measurable effects.",
            "Penalize vague review/editorial content, missing abstract, or low actionable signal."
        ],
        "output_format": {"agent_score": 0, "summary_1s": "..." }
    }

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0.2,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_obj)},
        ],
        response_format={"type": "json_object"},
    )

    data = json.loads(resp.choices[0].message.content)
    agent_score = clamp(float(data.get("agent_score", 0)))
    summary_1s = (data.get("summary_1s", "") or "").strip()

    # Keep summary short-ish
    if len(summary_1s) > 240:
        summary_1s = summary_1s[:240].rsplit(" ", 1)[0] + "…"

    return agent_score, summary_1s

def fetch_one_to_score(cur):
    """
    Grab 1 row ready for LLM scoring.
    """
    cur.execute("""
        SELECT id, title, abstract, score
        FROM public.articles
        WHERE agent_status = 'checked'
          AND agent_score IS NULL
        ORDER BY agent_checked_at ASC NULLS LAST, id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """)
    return cur.fetchone()

def mark_processing(cur, article_id: int):
    cur.execute("""
        UPDATE public.articles
        SET agent_status = 'processing',
            agent_last_error = NULL,
            updated_at = NOW()
        WHERE id = %s
    """, (article_id,))

def mark_scored(cur, article_id: int, agent_score: float, summary_1s: str):
    cur.execute("""
        UPDATE public.articles
        SET agent_score = %s,
            summary_1s = %s,
            agent_status = 'scored',
            updated_at = NOW()
        WHERE id = %s
    """, (agent_score, summary_1s, article_id))

def mark_error(cur, article_id: int, err: str):
    cur.execute("""
        UPDATE public.articles
        SET agent_status = 'error',
            agent_last_error = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (err[:2000], article_id))

def main():
    if not DATABASE_URL:
        die("ERROR: DATABASE_URL not set.")
    if not OPENAI_API_KEY:
        die("ERROR: OPENAI_API_KEY not set.")

    processed = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            while processed < AGENT_LIMIT:
                row = fetch_one_to_score(cur)
                if not row:
                    print(f"Done. No more rows ready for scoring. processed={processed}")
                    break

                article_id, title, abstract, ingest_score = row
                title = title or ""
                abstract = abstract or ""

                text_len = len(title.strip()) + len(abstract.strip())
                if text_len < MIN_TEXT:
                    # Not enough content to justify LLM cost; mark as error-like but non-fatal
                    mark_error(cur, article_id, f"Too little text to score (len={text_len}).")
                    conn.commit()
                    processed += 1
                    print(f"[skip] id={article_id} len={text_len} (marked error)")
                    continue

                try:
                    mark_processing(cur, article_id)
                    conn.commit()

                    agent_score, summary_1s = llm_score_and_summary(title, abstract, ingest_score)

                    mark_scored(cur, article_id, agent_score, summary_1s)
                    conn.commit()

                    processed += 1
                    print(f"[{processed}] scored id={article_id} ingest_score={ingest_score} agent_score={agent_score}")
                    print(f"     summary_1s: {summary_1s}")

                    time.sleep(SLEEP_SECONDS)

                except Exception as e:
                    mark_error(cur, article_id, str(e))
                    conn.commit()
                    processed += 1
                    print(f"[error] id={article_id} {e}", file=sys.stderr)

if __name__ == "__main__":
    main()

