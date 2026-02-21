import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    dsn = os.environ["DATABASE_URL"]
    # autocommit=False gives us explicit transaction control
    return psycopg.connect(dsn, autocommit=False)

