import os
from dotenv import load_dotenv

# Load .env file (works both locally and under systemd if EnvironmentFile is set)
load_dotenv()

# Core infrastructure
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# Model selection (safe default)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

# Agent runtime tuning
AGENT_BATCH = int(os.getenv("AGENT_BATCH", "10"))
AGENT_SLEEP_SECS = int(os.getenv("AGENT_SLEEP_SECS", "3"))

def require_env():
    """
    Hard fail early if required environment variables are missing.
    This prevents silent crashes later.
    """
    missing = []
    if not DATABASE_URL:
        missing.append("DATABASE_URL")
    if not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")

    if missing:
        raise SystemExit(
            f"[agents.config] Missing required environment variables: {', '.join(missing)}"
        )

