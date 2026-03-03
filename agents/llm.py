import json
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import OPENAI_API_KEY, OPENAI_MODEL

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM = """You are a biomedical research scoring agent for Parkinson's disease and Alzheimer's disease.
You must output STRICT JSON only—no extra text.

Goal: produce a 0–100 Therapeutic Signal Score for each article and structured tags.
Be conservative: if unclear, score lower.

Return JSON with keys:
- agent_score (integer 0-100)
- summary_1s (one sentence, max 220 chars)
- tags (object with arrays: compounds, targets, pathways, models)
- score_components (object with integers 0-100: relevance, novelty, evidence, actionability)
"""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def score_article(title: str, abstract: str, journal: str = "", pub_date: str = "") -> dict:
    user = {
        "title": title or "",
        "abstract": abstract or "",
        "journal": journal or "",
        "publication_date": pub_date or "",
        "constraints": {
            "diseases": ["Parkinson's disease", "Alzheimer's disease"],
            "output": "json_only"
        }
    }

    # Responses API (recommended migration path)  [oai_citation:2‡OpenAI Developers](https://developers.openai.com/api/reference/resources/responses/?utm_source=chatgpt.com)
    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": json.dumps(user)}
        ],
        # Hint: keep temperature low for stable scoring
        temperature=0.2,
    )

    # The SDK returns output text; parse JSON
    text = resp.output_text.strip()
    data = json.loads(text)

    # Basic validation / clamps
    data["agent_score"] = int(max(0, min(100, data.get("agent_score", 0))))
    data["summary_1s"] = (data.get("summary_1s", "") or "")[:220]

    tags = data.get("tags", {}) or {}
    for k in ("compounds", "targets", "pathways", "models"):
        if k not in tags or not isinstance(tags[k], list):
            tags[k] = []

    comps = data.get("score_components", {}) or {}
    for k in ("relevance", "novelty", "evidence", "actionability"):
        v = comps.get(k, 0)
        comps[k] = int(max(0, min(100, v if isinstance(v, (int, float)) else 0)))

    data["tags"] = tags
    data["score_components"] = comps
    return data

