import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

AGENT_BATCH = int(os.getenv("AGENT_BATCH", "10"))
AGENT_SLEEP_SECS = int(os.getenv("AGENT_SLEEP_SECS", "3"))

def require_env():
    missing = []
    if not DATABASE_URL:
        missing.append("DATABASE_URL")
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")

