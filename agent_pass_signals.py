#!/usr/bin/env python3
"""
agent_pass_signals.py
Populate paper_signals for articles missing signals, and log runs to clawbot_runs.

Requirements:
  pip install openai psycopg[binary] python-dotenv

Env:
  DATABASE_URL
  OPENAI_API_KEY
  SIGNAL_BATCH (default 25)
  SIGNAL_MODEL (default gpt-4.1-mini)
"""

import os
import json
import hashlib
import traceback

import psycopg
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

BATCH = int(os.getenv("SIGNAL_BATCH", "25"))
MODEL = os.getenv("SIGNAL_MODEL", "gpt-4.1-mini").strip()
AGENT_VERSION = "signals-v1"
CLAWBOT_NAME = "signals"

if not DATABASE_URL:
    raise SystemExit("Missing DATABASE_URL")
if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM = (
    "You are a biomedical research triage agent. "
    "Return ONLY valid JSON that matches the schema exactly. "
    "Do not add extra keys. If unknown, use null or empty list."
)

SCHEMA = {
    "summary_1_sentence": "string (<= 240 chars)",
    "mechanism_of_action": "string or null",
    "sponsor_name": "string or null",
    "repurpose_flag": "boolean",
    "natural_compound_flag": "boolean",
    "abandoned_trial_flag": "boolean",
    "novelty_score": "integer 0-100",
    "neglected_score": "integer 0-100",
    "tags": "array of short strings (3-12)"
}

def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]

def prompt_for(title: str, abstract: str | None) -> str:
    abstract = (abstract or "").strip()
    return f"""
Extract structured signals from this article for a Parkinson's/Alzheimer's discovery dashboard.

JSON schema (must match exactly):
{json.dumps(SCHEMA, indent=2)}

Rules:
- summary_1_sentence: one sentence, plain English, <= 240 characters.
- tags: 3 to 12 short strings, no sentences.
- repurpose_flag: true if suggests repurposing/repositioning/approved drug/off-label reuse.
- natural_compound_flag: true if a natural product / phytochemical / plant-derived compound is central.
- abandoned_trial_flag: true if indicates halted/terminated/withdrawn/failed trial or sponsor abandonment.
- novelty_score: 0-100 (higher = more novel claim/mechanism/approach).
- neglected_score: 0-100 (higher = overlooked/under-discussed "hidden gem" potential).

Article:
Title: {title}

Abstract:
{abstract if abstract else "(no abstract provided)"}
""".strip()

FETCH_BATCH = """
SELECT a.id, a.title, a.abstract
FROM articles a
LEFT JOIN paper_signals ps ON ps.article_id = a.id
WHERE ps.article_id IS NULL
ORDER BY a.id DESC
LIMIT %s;
"""

UPSERT_SIGNALS = """
INSERT INTO paper_signals (
  article_id,
  summary_1_sentence,
  mechanism_of_action,
  sponsor_name,
  repurpose_flag,
  natural_compound_flag,
  abandoned_trial_flag,
  novelty_score,
  neglected_score,
  tags,
  agent_version,
  model_name,
  prompt_hash,
  confidence
) VALUES (
  %(article_id)s,
  %(summary_1_sentence)s,
  %(mechanism_of_action)s,
  %(sponsor_name)s,
  %(repurpose_flag)s,
  %(natural_compound_flag)s,
  %(abandoned_trial_flag)s,
  %(novelty_score)s,
  %(neglected_score)s,
  %(tags)s::jsonb,
  %(agent_version)s,
  %(model_name)s,
  %(prompt_hash)s,
  %(confidence)s
)
ON CONFLICT (article_id) DO UPDATE SET
  summary_1_sentence       = EXCLUDED.summary_1_sentence,
  mechanism_of_action      = EXCLUDED.mechanism_of_action,
  sponsor_name             = EXCLUDED.sponsor_name,
  repurpose_flag           = EXCLUDED.repurpose_flag,
  natural_compound_flag    = EXCLUDED.natural_compound_flag,
  abandoned_trial_flag     = EXCLUDED.abandoned_trial_flag,
  novelty_score            = EXCLUDED.novelty_score,
  neglected_score          = EXCLUDED.neglected_score,
  tags                     = EXCLUDED.tags,
  agent_version            = EXCLUDED.agent_version,
  model_name               = EXCLUDED.model_name,
  prompt_hash              = EXCLUDED.prompt_hash,
  confidence               = EXCLUDED.confidence,
  updated_at               = now();
"""

INSERT_RUN = """
INSERT INTO clawbot_runs (clawbot_name)
VALUES (%s)
RETURNING id;
"""

FINISH_RUN_OK = """
UPDATE clawbot_runs
SET finished_at = now(), status='ok', items_processed=%s
WHERE id=%s;
"""

FINISH_RUN_ERR = """
UPDATE clawbot_runs
SET finished_at = now(), status='error', items_processed=%s, error=%s
WHERE id=%s;
"""

def parse_json_strict(content: str) -> dict:
    content = content.strip()
    try:
        return json.loads(content)
    except Exception:
        # salvage if wrapped
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1:
            raise
        return json.loads(content[start:end+1])

def main():
    processed = 0
    run_id = None

    with psycopg.connect(DATABASE_URL) as conn:
        # start run log
        run_id = conn.execute(INSERT_RUN, (CLAWBOT_NAME,)).fetchone()[0]
        conn.commit()

        try:
            rows = conn.execute(FETCH_BATCH, (BATCH,)).fetchall()
            if not rows:
                conn.execute(FINISH_RUN_OK, (0, run_id))
                conn.commit()
                print("No articles missing signals. ✅")
                return

            print(f"Found {len(rows)} articles missing signals. Processing...")

            for (article_id, title, abstract) in rows:
                user_prompt = prompt_for(title, abstract)
                phash = stable_hash(user_prompt)

                resp = client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.2,
                )

                content = resp.choices[0].message.content or ""
                data = parse_json_strict(content)

                # normalize
                tags = data.get("tags", [])
                if not isinstance(tags, list):
                    tags = []

                payload = {
                    "article_id": article_id,
                    "summary_1_sentence": data.get("summary_1_sentence"),
                    "mechanism_of_action": data.get("mechanism_of_action"),
                    "sponsor_name": data.get("sponsor_name"),
                    "repurpose_flag": bool(data.get("repurpose_flag", False)),
                    "natural_compound_flag": bool(data.get("natural_compound_flag", False)),
                    "abandoned_trial_flag": bool(data.get("abandoned_trial_flag", False)),
                    "novelty_score": int(max(0, min(100, data.get("novelty_score", 0) or 0))),
                    "neglected_score": int(max(0, min(100, data.get("neglected_score", 0) or 0))),
                    "tags": json.dumps(tags[:20]),
                    "agent_version": AGENT_VERSION,
                    "model_name": MODEL,
                    "prompt_hash": phash,
                    "confidence": 0.75,  # v1 constant; later compute
                }

                conn.execute(UPSERT_SIGNALS, payload)
                conn.commit()
                processed += 1
                print(f"✅ signals saved for article_id={article_id}")

            conn.execute(FINISH_RUN_OK, (processed, run_id))
            conn.commit()

        except Exception as e:
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            conn.execute(FINISH_RUN_ERR, (processed, err[:8000], run_id))
            conn.commit()
            raise

if __name__ == "__main__":
    main()

