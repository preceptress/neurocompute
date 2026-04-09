#!/usr/bin/env python3

import os
import json
import re
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BATCH_SIZE = int(os.getenv("SCORE_BATCH_SIZE", "500"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def contains_any(text: str, terms: list[str]) -> bool:
    t = text.lower()
    return any(term in t for term in terms)


def count_any(text: str, terms: list[str]) -> int:
    t = text.lower()
    return sum(1 for term in terms if term in t)


def compute_base_score(row: dict) -> tuple[float, dict]:
    title = row.get("title") or ""
    abstract = row.get("abstract") or ""
    journal = row.get("journal") or ""
    combined = f"{title}\n{abstract}".lower()

    score = 0.0
    reasons = []

    positive_therapy_terms = [
        "therapy", "therapeutic", "treatment", "treated", "intervention",
        "drug", "compound", "small molecule", "repurpos", "neuroprotect",
        "disease-modifying", "disease modifying", "ameliorat", "improv"
    ]

    clinical_terms = [
        "clinical trial", "randomized", "randomised", "double-blind",
        "double blind", "placebo", "phase ii", "phase iii", "phase 2",
        "phase 3", "patient", "patients", "cohort", "human study", "humans"
    ]

    mechanism_terms = [
        "alpha-synuclein", "synuclein", "mitochondria", "mitochondrial",
        "inflammation", "neuroinflammation", "microglia", "dopamine",
        "oxidative stress", "lysosome", "autophagy", "protein aggregation",
        "gut microbiota", "mitophagy"
    ]

    weak_or_non_therapeutic_terms = [
        "review", "systematic review", "meta-analysis", "meta analysis",
        "bibliometric", "protocol"
    ]

    preclinical_terms = [
        "mouse", "mice", "murine", "rat", "zebrafish", "drosophila",
        "cell line", "sh-sy5y", "in vitro", "animal model"
    ]

    strong_positive_hits = count_any(combined, positive_therapy_terms)
    if strong_positive_hits:
        add = min(30, strong_positive_hits * 8)
        score += add
        reasons.append({"factor": "therapy_terms", "delta": add})

    clinical_hits = count_any(combined, clinical_terms)
    if clinical_hits:
        add = min(25, clinical_hits * 10)
        score += add
        reasons.append({"factor": "clinical_terms", "delta": add})

    mechanism_hits = count_any(combined, mechanism_terms)
    if mechanism_hits:
        add = min(15, mechanism_hits * 3)
        score += add
        reasons.append({"factor": "mechanism_terms", "delta": add})

    if "parkinson" in combined:
        score += 15
        reasons.append({"factor": "parkinson_direct_match", "delta": 15})

    if "alzheimer" in combined and "parkinson" in combined:
        score += 4
        reasons.append({"factor": "multi_neuro_context", "delta": 4})

    if contains_any(combined, weak_or_non_therapeutic_terms):
        score -= 12
        reasons.append({"factor": "review_or_meta_penalty", "delta": -12})

    preclinical_hits = count_any(combined, preclinical_terms)
    if preclinical_hits:
        penalty = min(12, preclinical_hits * 4)
        score -= penalty
        reasons.append({"factor": "preclinical_penalty", "delta": -penalty})

    if journal:
        j = journal.lower()
        if any(x in j for x in ["neurology", "movement disorders", "jama", "lancet", "nature", "brain"]):
            score += 6
            reasons.append({"factor": "journal_bonus", "delta": 6})

    pub_date = row.get("publication_date")
    if pub_date:
        year_match = re.match(r"(\d{4})", str(pub_date))
        if year_match:
            year = int(year_match.group(1))
            if year >= 2022:
                score += 6
                reasons.append({"factor": "recency_bonus_recent", "delta": 6})
            elif year >= 2018:
                score += 3
                reasons.append({"factor": "recency_bonus_modern", "delta": 3})

    abstract_len = len(abstract.strip())
    if abstract_len >= 1200:
        score += 4
        reasons.append({"factor": "rich_abstract_bonus", "delta": 4})
    elif abstract_len < 120:
        score -= 6
        reasons.append({"factor": "thin_abstract_penalty", "delta": -6})

    final_score = round(clamp(score), 2)

    components = {
        "method": "rules_v1",
        "base_score_raw": score,
        "base_score_final": final_score,
        "reasons": reasons,
        "title_length": len(title),
        "abstract_length": abstract_len,
    }

    return final_score, components


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


def main():
    processed = 0

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
                WHERE base_score IS NULL
                ORDER BY publication_date DESC NULLS LAST, created_at DESC
                LIMIT %s
                """,
                (BATCH_SIZE,)
            )
            rows = cur.fetchall()

            for row in rows:
                base_score, components = compute_base_score(row)
                rank_score = compute_rank_score(
                    base_score=base_score,
                    ai_score=row.get("ai_score"),
                    narrative_score=row.get("narrative_score"),
                )

                cur.execute(
                    """
                    UPDATE public.articles
                    SET
                        base_score = %s,
                        rank_score = %s,
                        score_components = %s::jsonb,
                        agent_status = %s,
                        scored_at = NOW()
                    WHERE pmid = %s
                    """,
                    (
                        base_score,
                        rank_score,
                        json.dumps(components),
                        "scored_rules_v1",
                        row["pmid"],
                    ),
                )
                processed += 1

        conn.commit()

    print(f"Scored {processed} articles with rules_v1")


if __name__ == "__main__":
    main()
    