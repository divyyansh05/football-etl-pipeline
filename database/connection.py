"""
Database connection for football-data-platform.
Read-only for API consumers.
Read-write for ETL scripts only.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:postgres@localhost:5434/football_platform'
)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def query(sql: str, params=None, as_dict=False) -> list:
    """Execute a SELECT query and return results."""
    cursor_factory = RealDictCursor if as_dict else None
    with get_conn() as conn:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def execute(sql: str, params=None) -> None:
    """Execute an INSERT/UPDATE/DELETE."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()


def execute_returning(sql: str, params=None):
    """Execute INSERT ... RETURNING and return first result."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            result = cur.fetchone()
        conn.commit()
    return result
