import re
from db import execute

NATURAL_RE = re.compile(r"\b(plant|herbal|herb|extract|phytochemical|polyphenol|flavonoid|natural product|botanical|ayurvedic|traditional medicine)\b", re.I)
REPURPOSE_RE = re.compile(r"\b(repurpose|reposition|drug reposition|off-label|discontinued|terminated|withdrawn|failed trial|suspended)\b", re.I)
ORPHAN_RE = re.compile(r"\b(orphan drug|rare disease|rare)\b", re.I)

def ensure_tag(name: str) -> int:
    row = execute(
        """
        INSERT INTO tags (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
        RETURNING id
        """,
        (name,),
        fetch="one",
    )
    return row["id"]

def attach_tag(paper_id: int, tag_id: int):
    execute(
        """
        INSERT INTO paper_tags (paper_id, tag_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (paper_id, tag_id),
    )

def main():
    tag_general = ensure_tag("general")
    tag_natural = ensure_tag("natural")
    tag_repurpose = ensure_tag("repurpose")
    tag_orphan = ensure_tag("orphan")

    papers = execute("SELECT id, title, abstract FROM papers ORDER BY id DESC", fetch="all")

    updated = 0
    for p in papers:
        text = f"{p.get('title') or ''} {p.get('abstract') or ''}"

        # Always general
        attach_tag(p["id"], tag_general)

        if NATURAL_RE.search(text):
            attach_tag(p["id"], tag_natural)

        if REPURPOSE_RE.search(text):
            attach_tag(p["id"], tag_repurpose)

        if ORPHAN_RE.search(text):
            attach_tag(p["id"], tag_orphan)

        updated += 1

    print(f"Tagged {updated} papers.")

if __name__ == "__main__":
    main()

