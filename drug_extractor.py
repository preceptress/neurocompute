import re
from db import execute

# Intervention types we care about (ClinicalTrials uses many; drug/biological are key)
GOOD_TYPES = {"DRUG", "BIOLOGICAL", "DIETARY_SUPPLEMENT", "OTHER"}

# Simple cleanup (you can improve later)
JUNK_PATTERNS = [
    r"\bplacebo\b",
    r"\bsham\b",
    r"\bstandard of care\b",
    r"\busual care\b",
    r"\bbehavioral\b",
    r"\bdevice\b",
]

junk_re = re.compile("|".join(JUNK_PATTERNS), re.I)

def normalize_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    return name

def ensure_drug(name: str) -> int:
    row = execute("""
        INSERT INTO drugs (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
    """, (name,), fetch="one")
    return row["id"]

def link_trial_drug(trial_id: int, drug_id: int, source="intervention"):
    execute("""
        INSERT INTO trial_drugs (trial_id, drug_id, source)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (trial_id, drug_id, source))

def main():
    interventions = execute("""
        SELECT ti.trial_id, ti.intervention_type, ti.name
        FROM trial_interventions ti
        JOIN trials t ON t.id = ti.trial_id
        WHERE ti.name IS NOT NULL AND ti.name <> ''
    """, fetch="all")

    created = 0
    linked = 0

    for it in interventions:
        itype = (it.get("intervention_type") or "").upper().strip()
        name = normalize_name(it.get("name"))

        if not name:
            continue

        if itype and itype not in GOOD_TYPES:
            # still keep "OTHER" category; skip devices etc
            continue

        if junk_re.search(name):
            continue

        drug_id = ensure_drug(name)
        created += 1

        link_trial_drug(it["trial_id"], drug_id, "intervention")
        linked += 1

    print(f"Done. Processed {len(interventions)} interventions. Created/ensured {created} drugs. Linked {linked} trial_drugs.")

if __name__ == "__main__":
    main()

