import os
import time
import requests
from datetime import date
from dotenv import load_dotenv

from db import execute

load_dotenv()

CTG_BASE = "https://clinicaltrials.gov/api/v2/studies"

ABANDONED_STATUSES = {"TERMINATED", "WITHDRAWN", "SUSPENDED"}

def get_source_id(name: str) -> int:
    row = execute("SELECT id FROM sources WHERE name=%s", (name,), fetch="one")
    if not row:
        raise RuntimeError(f"Source not found: {name}")
    return row["id"]

def parse_date(s):
    # clinicaltrials api sometimes returns "YYYY-MM-DD" or "YYYY-MM"
    if not s:
        return None
    try:
        parts = s.split("-")
        y = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 1
        d = int(parts[2]) if len(parts) > 2 else 1
        return date(y, m, d)
    except Exception:
        return None

def upsert_trial(source_id: int, nct_id: str, title: str, brief: str, status: str, phase: str,
                 study_type: str, conditions: str, sponsor: str,
                 start_date_v, completion_date_v, url: str) -> int | None:
    row = execute("""
        INSERT INTO trials
        (source_id, nct_id, title, brief_summary, status, phase, study_type, conditions, sponsor,
         start_date, completion_date, url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source_id, nct_id) DO UPDATE SET
            title=EXCLUDED.title,
            brief_summary=EXCLUDED.brief_summary,
            status=EXCLUDED.status,
            phase=EXCLUDED.phase,
            study_type=EXCLUDED.study_type,
            conditions=EXCLUDED.conditions,
            sponsor=EXCLUDED.sponsor,
            start_date=EXCLUDED.start_date,
            completion_date=EXCLUDED.completion_date,
            url=EXCLUDED.url
        RETURNING id
    """, (
        source_id, nct_id, title, brief, status, phase, study_type, conditions, sponsor,
        start_date_v, completion_date_v, url
    ), fetch="one")
    return row["id"] if row else None

def replace_interventions(trial_id: int, interventions: list[dict]):
    execute("DELETE FROM trial_interventions WHERE trial_id=%s", (trial_id,))
    for it in interventions:
        execute("""
            INSERT INTO trial_interventions (trial_id, intervention_type, name)
            VALUES (%s, %s, %s)
        """, (trial_id, it.get("type"), it.get("name")))

def fetch_trials(query: str, page_size: int = 50, max_pages: int = 4):
    """
    Pull studies from ClinicalTrials.gov API v2.
    query examples:
      "Parkinson"
      "Alzheimer"
      "(Parkinson OR Alzheimer) AND (terminated OR withdrawn)"
    """
    params = {
        "query.term": query,
        "pageSize": str(page_size),
        "fields": ",".join([
            "protocolSection.identificationModule.nctId",
            "protocolSection.identificationModule.briefTitle",
            "protocolSection.descriptionModule.briefSummary",
            "protocolSection.statusModule.overallStatus",
            "protocolSection.designModule.phases",
            "protocolSection.designModule.studyType",
            "protocolSection.conditionsModule.conditions",
            "protocolSection.sponsorCollaboratorsModule.leadSponsor",
            "protocolSection.statusModule.startDateStruct",
            "protocolSection.statusModule.completionDateStruct",
            "protocolSection.armsInterventionsModule.interventions",
        ])
    }

    url = CTG_BASE
    for _ in range(max_pages):
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        studies = data.get("studies", [])
        next_page = data.get("nextPageToken")

        yield studies

        if not next_page:
            break

        # v2 uses a page token parameter for next request
        params["pageToken"] = next_page
        time.sleep(0.2)

def normalize_status(s: str) -> str:
    return (s or "").strip().upper()

def main():
    source_id = get_source_id("clinicaltrials")

    # Neuro-focused + abandon signals (broad but useful)
    queries = [
        "Parkinson",
        "Alzheimer",
        "(Parkinson OR Alzheimer) AND (terminated OR withdrawn OR suspended)",
    ]

    inserted_or_updated = 0
    abandoned_count = 0

    for q in queries:
        print(f"\n[CTG] Query: {q}")
        for batch in fetch_trials(q):
            for study in batch:
                ps = study.get("protocolSection", {})
                ident = ps.get("identificationModule", {})
                desc = ps.get("descriptionModule", {})
                status_m = ps.get("statusModule", {})
                design = ps.get("designModule", {})
                cond_m = ps.get("conditionsModule", {})
                sponsor_m = ps.get("sponsorCollaboratorsModule", {})
                arms = ps.get("armsInterventionsModule", {})

                nct_id = ident.get("nctId")
                title = ident.get("briefTitle") or "(no title)"
                brief = desc.get("briefSummary") or ""
                status = normalize_status(status_m.get("overallStatus"))
                phases = design.get("phases") or []
                phase = ", ".join(phases) if isinstance(phases, list) else str(phases or "")
                study_type = design.get("studyType") or ""
                conditions = cond_m.get("conditions") or []
                conditions_text = "; ".join(conditions) if isinstance(conditions, list) else str(conditions or "")

                lead = sponsor_m.get("leadSponsor") or {}
                sponsor = lead.get("name") or ""

                sd = status_m.get("startDateStruct", {}) or {}
                cd = status_m.get("completionDateStruct", {}) or {}
                start_date_v = parse_date(sd.get("date"))
                completion_date_v = parse_date(cd.get("date"))

                interventions = arms.get("interventions") or []
                if isinstance(interventions, dict):
                    interventions = [interventions]

                url = f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else ""

                if not nct_id:
                    continue

                trial_id = upsert_trial(
                    source_id=source_id,
                    nct_id=nct_id,
                    title=title,
                    brief=brief,
                    status=status,
                    phase=phase,
                    study_type=study_type,
                    conditions=conditions_text,
                    sponsor=sponsor,
                    start_date_v=start_date_v,
                    completion_date_v=completion_date_v,
                    url=url
                )
                if trial_id:
                    replace_interventions(trial_id, interventions)
                    inserted_or_updated += 1

                if status in ABANDONED_STATUSES:
                    abandoned_count += 1

    print(f"\nDone. Upserted {inserted_or_updated} trials. Abandoned statuses found: {abandoned_count}")

if __name__ == "__main__":
    main()

