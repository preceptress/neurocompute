from flask import Flask, render_template, request, abort
from db import execute

app = Flask(__name__)

@app.route("/health")
def health():
    return "ok", 200


@app.route("/")
def dashboard():
    q = (request.args.get("q") or "").strip()
    tag = (request.args.get("tag") or "").strip()

    params = []
    where = []
    join = ""

    if q:
        where.append("(p.title ILIKE %s OR p.abstract ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]

    if tag:
        join = """
            JOIN paper_tags pt ON pt.paper_id = p.id
            JOIN tags t ON t.id = pt.tag_id
        """
        where.append("t.name = %s")
        params.append(tag)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    papers = execute(f"""
        SELECT
            p.id,
            p.title,
            p.abstract,
            p.journal,
            p.publication_date,
            p.url,
            COALESCE(ps.total_score, 0) AS score
        FROM papers p
        LEFT JOIN paper_scores ps ON ps.paper_id = p.id
        {join}
        {where_sql}
        ORDER BY p.publication_date DESC NULLS LAST, p.id DESC
        LIMIT 200
    """, tuple(params) if params else None, fetch="all")

    tag_counts = execute("""
        SELECT t.name, COUNT(*) AS count
        FROM paper_tags pt
        JOIN tags t ON t.id = pt.tag_id
        GROUP BY t.name
        ORDER BY count DESC
    """, fetch="all")

    return render_template(
        "dashboard.html",
        papers=papers,
        q=q,
        tag=tag,
        tag_counts=tag_counts
    )


@app.route("/paper/<int:paper_id>")
def paper_detail(paper_id):
    paper = execute("""
        SELECT
            p.*,
            COALESCE(ps.total_score, 0) AS score
        FROM papers p
        LEFT JOIN paper_scores ps ON ps.paper_id = p.id
        WHERE p.id=%s
    """, (paper_id,), fetch="one")

    if not paper:
        abort(404)

    authors = execute("""
        SELECT a.name
        FROM paper_authors pa
        JOIN authors a ON a.id = pa.author_id
        WHERE pa.paper_id=%s
        ORDER BY a.name
    """, (paper_id,), fetch="all")

    tags = execute("""
        SELECT t.name
        FROM paper_tags pt
        JOIN tags t ON t.id = pt.tag_id
        WHERE pt.paper_id=%s
        ORDER BY t.name
    """, (paper_id,), fetch="all")

    drugs = execute("""
        SELECT d.name, pd.mention_source
        FROM paper_drugs pd
        JOIN drugs d ON d.id = pd.drug_id
        WHERE pd.paper_id=%s
        ORDER BY d.name
        LIMIT 200
    """, (paper_id,), fetch="all")

    return render_template("paper_detail.html", paper=paper, authors=authors, tags=tags, drugs=drugs)


@app.route("/abandoned")
def abandoned_trials():
    q = (request.args.get("q") or "").strip()

    params = []
    where = ["t.status IN ('TERMINATED','WITHDRAWN','SUSPENDED')"]

    if q:
        where.append("(t.title ILIKE %s OR t.brief_summary ILIKE %s OR t.conditions ILIKE %s OR t.sponsor ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]

    where_sql = "WHERE " + " AND ".join(where)

    trials = execute(f"""
        SELECT
          t.id,
          t.nct_id,
          t.title,
          t.status,
          t.phase,
          t.sponsor,
          t.start_date,
          t.completion_date,
          t.url,
          COALESCE(ts.repurpose_score, 0) AS repurpose_score,
          COALESCE(ts.years_since_end, 0) AS years_since_end,
          (SELECT COUNT(*) FROM trial_papers tp WHERE tp.trial_id = t.id) AS evidence_papers
        FROM trials t
        LEFT JOIN trial_scores ts ON ts.trial_id = t.id
        {where_sql}
        ORDER BY repurpose_score DESC, years_since_end DESC, evidence_papers DESC
        LIMIT 200
    """, tuple(params) if params else None, fetch="all")

    return render_template("abandoned.html", trials=trials, q=q)


@app.route("/drugs")
def drugs_radar():
    q = (request.args.get("q") or "").strip()
    params = []
    where = ""

    if q:
        where = "WHERE d.name ILIKE %s"
        params = [f"%{q}%"]

    drugs = execute(f"""
        SELECT
          d.id,
          d.name,
          COUNT(DISTINCT t.id) FILTER (WHERE t.status IN ('TERMINATED','WITHDRAWN','SUSPENDED')) AS abandoned_trials,
          COUNT(DISTINCT pd.paper_id) AS paper_mentions,
          COALESCE(MAX(ts.repurpose_score), 0) AS top_score
        FROM drugs d
        LEFT JOIN trial_drugs td ON td.drug_id = d.id
        LEFT JOIN trials t ON t.id = td.trial_id
        LEFT JOIN trial_scores ts ON ts.trial_id = t.id
        LEFT JOIN paper_drugs pd ON pd.drug_id = d.id
        {where}
        GROUP BY d.id
        ORDER BY top_score DESC, abandoned_trials DESC, paper_mentions DESC, d.name ASC
        LIMIT 300
    """, tuple(params) if params else None, fetch="all")

    return render_template("drugs.html", drugs=drugs, q=q)


@app.route("/sponsors")
def sponsors():
    rows = execute("""
        SELECT sponsor,
               COUNT(*) AS abandoned_trials,
               COUNT(*) FILTER (WHERE status='TERMINATED') AS terminated,
               COUNT(*) FILTER (WHERE status='WITHDRAWN') AS withdrawn,
               COUNT(*) FILTER (WHERE status='SUSPENDED') AS suspended
        FROM trials
        WHERE status IN ('TERMINATED','WITHDRAWN','SUSPENDED')
        GROUP BY sponsor
        ORDER BY abandoned_trials DESC
        LIMIT 200
    """, fetch="all")

    return render_template("sponsors.html", sponsors=rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
    