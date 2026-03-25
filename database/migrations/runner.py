#!/usr/bin/env python3
"""
Migration runner.

Design:
- One persistent psycopg2 connection for the entire session.
- Advisory lock prevents concurrent runs.
- conn.autocommit=False: transactions start implicitly, no BEGIN needed.
- Runner calls conn.commit() / conn.rollback() per migration file.
- SQL files must NOT contain BEGIN/COMMIT.
- All migration DDL must be guarded or idempotent (IF NOT EXISTS,
  DO $$ existence checks, ON CONFLICT DO NOTHING, etc.) so migrations
  run safely against a DB bootstrapped from end-state schema.sql.
"""
import os
import glob
import logging
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ADVISORY_LOCK_ID = 12345678


def run_migrations():
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5434/football_data",
    )
    conn = psycopg2.connect(url)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Acquire advisory lock
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            if not cur.fetchone()[0]:
                logger.error(
                    "Advisory lock unavailable — another migration may be running."
                )
                sys.exit(1)
            conn.commit()

            # Ensure tracking table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id         SERIAL PRIMARY KEY,
                    filename   VARCHAR(200) NOT NULL UNIQUE,
                    applied_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()

            migrations_dir = Path(__file__).parent
            sql_files = sorted(glob.glob(str(migrations_dir / "0*.sql")))

            for filepath in sql_files:
                filename = Path(filepath).name
                cur.execute(
                    "SELECT 1 FROM schema_migrations WHERE filename = %s",
                    (filename,),
                )
                if cur.fetchone():
                    logger.info(f"SKIP: {filename}")
                    continue

                logger.info(f"Applying: {filename}")
                with open(filepath) as f:
                    sql = f.read()

                try:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (filename,),
                    )
                    conn.commit()
                    logger.info(f"APPLIED: {filename}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"FAILED: {filename} — {e}")
                    sys.exit(1)

            # Release advisory lock
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_migrations()
    print("Migrations complete.")
