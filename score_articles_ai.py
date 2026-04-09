#!/usr/bin/env python3

import json
import os
import time
import psycopg
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
AI_BATCH_SIZE = int(os.getenv("AI_SCORE_BATCH_SIZE", "500"))
AI_MODEL = os.getenv("AI_SCORE_MODEL", "gpt-5-mini")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set")

client = OpenAI(api_key=OPENAI_API_KEY)


SYSTEM_PROMPT = """
You are analyzing scientific literature related to Parkinson's disease.
Your job is to identify therapeutic relevance, hidden signals, and translational potential.

Return STRICT JSON with these fields:
{
  "therapeutic_relevance": integer 0-100,
  "disease_modifying_potential": integer 0-100,
  "repurposing_signal": integer 0-100,
  "mechanistic_novelty": integer 0-100,
  "clinical_translation_potential": integer 0-100,
  "confidence": integer 0-100,
  "primary_mechanisms": [string, ... up to 5],
  "candidate_interventions": [string, ... up to 5],
  "red_flags": [string, ... up to 5],
  "why_it_matters": "short paragraph",
  "ai_summary": "1-3 sentence summary"
}

Be careful, evidence-based, and avoid hype.
"""


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def compute_ai_score(payload: dict) -> float:
    tr = payload.get("therapeutic_relevance", 0)
    dm = payload.get("disease_modifying_potential", 0)
    rp = payload.get("repurposing_signal", 0)
    mn = payload.get("mechanistic_novelty", 0)
    ct = payload.get("clinical_translation_potential", 0)
    cf = payload.get("confidence", 0)

    score = (
        0.24 * tr +
        0.22 * dm +
        0.18 * rp +
        0.14 * mn +
        0.14 * ct +
        0.08 * cf
    )
    return round(clamp(score), 2)


def compute_rank_score(base_score, ai_score, narrative_score=None):
    parts = []
    weights = []

    if base_score is not None:
        parts.append(float(base_score))
        weights.append(0.55)
    if ai_score is not None:
        parts.append(float(ai_score))
        weights.append(0.35)
    if narrative_score is not None:
        parts.append(float(narrative_score))
        weights.append(0.10)

    if not parts:
        return None

    total_weight = sum(weights)
    weighted = sum(v * w for v, w in zip(parts, weights)) / total_weight
    return round(weighted, 2)


def ask_ai(title: str, abstract: str, journal: str, publication_date):
    user_prompt = f"""
TITLE:
{title or ""}

JOURNAL:
{journal or ""}

PUBLICATION_DATE:
{publication_date or ""}

ABSTRACT:
{abstract or ""}

Analyze this paper for Parkinson's-related therapeutic discovery signals.
Return only valid JSON.
"""

    response = client.responses.create(
        model=AI_MODEL,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        text={"format": {"type": "text"}},
    )

    text = response.output_text.strip()
    return json.loads(text)


def main():
    processed = 0
    failed = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT
                    pmid,
                    title,
                    abstract,
                    journal,
                    publication_date,
                    base_score,
                    ai_score,
                    narrative_score
                FROM public.articles
                WHERE ai_score IS NULL
                ORDER BY base_score DESC NULLS LAST, publication_date DESC NULLS LAST
                LIMIT %s
                """,
                (AI_BATCH_SIZE,),
            )
            rows = cur.fetchall()

            for row in rows:
                try:
                    payload = ask_ai(
                        title=row.get("title"),
                        abstract=row.get("abstract"),
                        journal=row.get("journal"),
                        publication_date=row.get("publication_date"),
                    )

                    ai_score = compute_ai_score(payload)
                    rank_score = compute_rank_score(
                        base_score=row.get("base_score"),
                        ai_score=ai_score,
                        narrative_score=row.get("narrative_score"),
                    )

                    cur.execute(
                        """
                        UPDATE public.articles
                        SET
                            ai_score = %s,
                            ai_confidence = %s,
                            ai_summary = %s,
                            why_it_matters = %s,
                            mechanisms = %s::jsonb,
                            candidate_interventions = %s::jsonb,
                            red_flags = %s::jsonb,
                            ai_model = %s,
                            rank_score = %s,
                            agent_status = %s,
                            scored_at = NOW()
                        WHERE pmid = %s
                        """,
                        (
                            ai_score,
                            payload.get("confidence"),
                            payload.get("ai_summary"),
                            payload.get("why_it_matters"),
                            json.dumps(payload.get("primary_mechanisms", [])),
                            json.dumps(payload.get("candidate_interventions", [])),
                            json.dumps(payload.get("red_flags", [])),
                            AI_MODEL,
                            rank_score,
                            "scored_ai_v1",
                            row["pmid"],
                        ),
                    )
                    processed += 1
                    time.sleep(0.2)

                except Exception as e:
                    cur.execute(
                        """
                        UPDATE public.articles
                        SET agent_status = %s
                        WHERE pmid = %s
                        """,
                        (f"ai_error: {str(e)[:180]}", row["pmid"]),
                    )
                    failed += 1

        conn.commit()

    print(f"AI scored {processed} articles, failed {failed}, model={AI_MODEL}")


if __name__ == "__main__":
    main()

