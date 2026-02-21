#!/usr/bin/env python3
import os
import json
import time
import traceback
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from openai import OpenAI

from db import get_conn

# =========================
# Debug Controls
# =========================
DEBUG = os.environ.get("DEBUG", "1") == "1"
DEBUG_DIR = os.environ.get("DEBUG_DIR", ".")  # where debug files get written
os.makedirs(DEBUG_DIR, exist_ok=True)

def dprint(*args):
    if DEBUG:
        print(*args)

def write_debug_file(name: str, content: str):
    path = os.path.join(DEBUG_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    dprint(f"[debug] wrote {path}")

# =========================
# Env / Client
# =========================
load_dotenv()

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
BATCH = int(os.environ.get("SUMMARY_BATCH", "5"))
SLEEP_SECS = float(os.environ.get("SUMMARY_SLEEP", "0.4"))

API_KEY = os.environ.get("OPENAI_API_KEY", "")
if not API_KEY:
    raise RuntimeError("OPENAI_API_KEY not found in environment. Put it in .env or export it.")

client = OpenAI(api_key=API_KEY)

# =========================
# Prompting
# =========================
SYSTEM = (
    "You are Neurocompute, a biomedical research assistant. "
    "Summarize neurodegenerative research (Parkinson’s/Alzheimer’s) precisely. "
    "Never invent facts not present in title/abstract. Be concise, cautious, and structured."
)

PROMPT = """\
Given the paper title and abstract, produce THREE outputs:

(1) plain_summary: 2–3 sentences for an educated non-specialist.
(2) technical_summary: 4–6 bullets for a scientist.
(3) signals: structured extraction for downstream analysis.

Return STRICT JSON ONLY as a single object that starts with "{{" and ends with "}}".
No markdown, no commentary.

Schema:
{{
  "plain_summary": "string",
  "technical_summary": ["string", "..."],
  "signals": {{
    "diseases": ["Parkinson's", "Alzheimer's"],
    "study_type": "basic|preclinical|clinical|review|other",
    "models": ["mouse", "cell culture", "human cohort"],
    "mechanisms": ["string"],
    "targets_pathways": ["string"],
    "compounds_interventions": [
      {{
        "name": "string",
        "type": "drug|natural|device|behavioral|other",
        "notes": "string"
      }}
    ],
    "biomarkers": ["string"],
    "outcomes": ["string"],
    "trial_phase": "preclinical|phase1|phase2|phase3|phase4|na",
    "repurposing_signal": true,
    "novelty_signal": "low|medium|high",
    "confidence": 0.0,
    "notes": "string"
  }}
}}

Title: {title}

Abstract:
{abstract}
"""


# =========================
# JSON repair (fallback only)
# =========================
def repair_json(text: str) -> str:
    s = (text or "").strip()

    # Strip code fences if present
    if s.startswith("```"):
        # remove fences crudely
        s = s.strip()
        s = s.strip("`").strip()
        if "\n" in s:
            first, rest = s.split("\n", 1)
            if first.strip().lower().startswith("json"):
                s = rest.strip()

    s = s.strip()

    # If model returned a fragment missing braces, wrap it
    if s.startswith('"plain_summary"') or s.startswith("'plain_summary'"):
        s = "{\n" + s + "\n}"

    # If there are braces somewhere, slice to outermost
    if "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            s = s[start : end + 1]

    return s

# =========================
# Response parsing helpers
# =========================
def try_extract_structured_json(resp) -> Optional[Dict[str, Any]]:
    """
    Tries to extract a parsed JSON object from OpenAI Responses API result.
    We prefer structured access; if SDK shape changes, we gracefully return None.
    """
    try:
        out = getattr(resp, "output", None)
        if not out:
            return None

        for item in out:
            content = getattr(item, "content", None)
            if not content:
                continue

            for part in content:
                ptype = getattr(part, "type", None)

                # Newer SDKs typically provide JSON parts like output_json
                if ptype in ("output_json", "json", "response_json"):
                    pj = getattr(part, "json", None)
                    if isinstance(pj, dict):
                        return pj

                # Sometimes part.json exists even if type differs
                pj = getattr(part, "json", None)
                if isinstance(pj, dict):
                    return pj

        return None
    except Exception:
        return None

def call_llm(title: str, abstract: str) -> dict:
    text = PROMPT.format(
        title=title.strip(),
        abstract=(abstract or "").strip()
    )

    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": text},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )

    # This content is guaranteed to be valid JSON text
    content = resp.choices[0].message.content
    return json.loads(content)


# =========================
# DB upsert
# =========================
def upsert_summary(cur, article_id: int, summary_type: str, summary: str, metadata: Dict[str, Any]):
    cur.execute(
        """
        INSERT INTO article_summaries (article_id, model, summary_type, summary, metadata)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (article_id, summary_type) DO UPDATE
        SET summary = EXCLUDED.summary,
            model = EXCLUDED.model,
            metadata = EXCLUDED.metadata
        """,
        (article_id, MODEL, summary_type, summary, json.dumps(metadata)),
    )

# =========================
# Main
# =========================
def main():
    dprint("[env] DEBUG=1 (enabled)")
    dprint("[env] MODEL=", MODEL)
    dprint("[env] SUMMARY_BATCH=", BATCH)
    dprint("[env] cwd=", os.getcwd())

    # Fetch articles missing signals summary
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.title, a.abstract
                FROM articles a
                WHERE NOT EXISTS (
                    SELECT 1 FROM article_summaries s
                    WHERE s.article_id = a.id AND s.summary_type = 'signals'
                )
                ORDER BY a.publication_date DESC NULLS LAST, a.id DESC
                LIMIT %s
                """,
                (BATCH,),
            )
            rows = cur.fetchall()

    if not rows:
        print("[summaries] nothing to summarize")
        return

    processed = 0

    for article_id, title, abstract in rows:
        print(f"[summaries] working on article_id={article_id}")

        # IMPORTANT: do not swallow errors during debugging
        data = call_llm(title, abstract or "")

        # Validate schema keys early (crash if missing)
        plain = (data.get("plain_summary") or "").strip()
        technical_list = data.get("technical_summary") or []
        signals = data.get("signals") or {}

        if not isinstance(technical_list, list):
            raise RuntimeError("technical_summary is not a list")
        if not isinstance(signals, dict):
            raise RuntimeError("signals is not an object")

        technical = "\n".join([f"- {str(x).strip()}" for x in technical_list if str(x).strip()])
        metadata = {"signals": signals}
        signals_text = json.dumps(signals, ensure_ascii=False, indent=2)

        with get_conn() as conn2:
            with conn2.cursor() as cur2:
                upsert_summary(cur2, article_id, "plain", plain, metadata)
                upsert_summary(cur2, article_id, "technical", technical, metadata)
                upsert_summary(cur2, article_id, "signals", signals_text, metadata)
            conn2.commit()

        processed += 1
        print(f"[summaries] article_id={article_id} OK (plain+technical+signals)")
        time.sleep(SLEEP_SECS)

    print(f"[summaries] done. processed={processed}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Always write a traceback file during debug
        tb = traceback.format_exc()
        write_debug_file("last_traceback.txt", tb)
        print(tb)
        raise

