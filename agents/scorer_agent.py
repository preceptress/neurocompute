
from .llm import score_article
from .db import mark_done

def run_one(article_row):
    """
    article_row: (id, pmid, title, abstract, journal, publication_date)
    """
    article_id, pmid, title, abstract, journal, pub_date = article_row

    result = score_article(
        title=title or "",
        abstract=abstract or "",
        journal=journal or "",
        pub_date=str(pub_date) if pub_date else ""
    )

    mark_done(
        article_id=article_id,
        agent_score=result["agent_score"],
        summary_1s=result["summary_1s"],
        tags=result["tags"],
        components=result["score_components"],
    )

    return result

