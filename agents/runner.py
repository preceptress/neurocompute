import time
import traceback
from .config import require_env, AGENT_BATCH, AGENT_SLEEP_SECS
from .db import fetch_pending_articles, mark_error
from .scorer_agent import run_one

def main():
    require_env()
    print("Agent runner started.")

    while True:
        rows = fetch_pending_articles(AGENT_BATCH)

        if not rows:
            time.sleep(AGENT_SLEEP_SECS)
            continue

        for row in rows:
            article_id = row[0]
            try:
                res = run_one(row)
                print(f"scored id={article_id} score={res['agent_score']}")
            except Exception as e:
                err = f"{e}\n{traceback.format_exc()}"
                print(f"ERROR id={article_id}: {e}")
                mark_error(article_id, str(e))

        time.sleep(1)

if __name__ == "__main__":
    main()

