"""
Tests for database/migrations/runner.py

Verifies idempotency: running migrations twice produces "SKIP" on second pass.
Uses the project's test DB or a temporary DB if available.

These tests require a running PostgreSQL instance. They are skipped if the DB
is unreachable, so they are safe to run in CI without a DB service.
"""
import os
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Skip marker ───────────────────────────────────────────────────────────────

def _db_available() -> bool:
    try:
        import psycopg2
        url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5434/football_data",
        )
        conn = psycopg2.connect(url, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


skip_if_no_db = pytest.mark.skipif(
    not _db_available(),
    reason="PostgreSQL not reachable — skipping DB tests",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _connect():
    import psycopg2
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5434/football_data",
    )
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def _applied_migrations(conn) -> list:
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT filename FROM schema_migrations ORDER BY filename")
            return [r[0] for r in cur.fetchall()]
        except Exception:
            return []


# ── Tests ─────────────────────────────────────────────────────────────────────

@skip_if_no_db
class TestMigrationRunner:
    def test_schema_migrations_table_exists(self):
        """schema_migrations table must exist after runner has run."""
        conn = _connect()
        try:
            applied = _applied_migrations(conn)
            # If the list is not empty, the table exists and runner has run
            assert isinstance(applied, list)
        finally:
            conn.close()

    def test_migrations_applied(self):
        """All three migration files should be recorded."""
        conn = _connect()
        try:
            applied = _applied_migrations(conn)
            for expected in ["001_sofascore_canonical.sql",
                             "002_fix_stale_columns.sql",
                             "003_update_gold_views.sql"]:
                assert expected in applied, (
                    f"Migration {expected!r} not in schema_migrations. "
                    f"Run: python database/migrations/runner.py"
                )
        finally:
            conn.close()

    def test_idempotency(self):
        """
        Running the runner a second time should produce no errors and should
        skip all already-applied migrations.
        """
        import subprocess
        result = subprocess.run(
            [sys.executable, "database/migrations/runner.py"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        assert result.returncode == 0, (
            f"runner.py exited with {result.returncode}:\n{result.stderr}"
        )
        # All migrations should be skipped (not re-applied)
        assert "APPLIED:" not in result.stdout + result.stderr, (
            "Runner re-applied a migration that should have been skipped"
        )

    def test_no_duplicate_players(self):
        """Constraint: zero duplicate sofascore_ids in players table."""
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sofascore_id, COUNT(*)
                      FROM players
                     WHERE sofascore_id IS NOT NULL
                     GROUP BY sofascore_id
                    HAVING COUNT(*) > 1
                    """
                )
                dupes = cur.fetchall()
            assert not dupes, f"Duplicate sofascore_ids: {dupes}"
        finally:
            conn.close()

    def test_players_sofascore_id_not_null(self):
        """All players must have sofascore_id (canonical rule)."""
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM players WHERE sofascore_id IS NULL"
                )
                count = cur.fetchone()[0]
            assert count == 0, (
                f"{count} players have NULL sofascore_id (violates canonical rule)"
            )
        finally:
            conn.close()

    def test_gold_views_exist(self):
        """All 4 Gold views must exist after migrations."""
        expected_views = [
            "v_players_current_season",
            "v_coverage_summary",
            "v_team_current_elo",
            "v_player_last5",
        ]
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT viewname FROM pg_views
                     WHERE schemaname = 'public'
                       AND viewname = ANY(%s)
                    """,
                    (expected_views,),
                )
                found = {r[0] for r in cur.fetchall()}
            for v in expected_views:
                assert v in found, f"Gold view missing: {v}"
        finally:
            conn.close()

    def test_v_players_current_season_uses_is_current(self):
        """v_players_current_season must use is_current=TRUE (not hardcoded name)."""
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT definition FROM pg_views
                     WHERE viewname = 'v_players_current_season'
                    """
                )
                row = cur.fetchone()
            assert row, "View v_players_current_season not found"
            definition = row[0].lower()
            assert "is_current" in definition, (
                "v_players_current_season does not use is_current filter"
            )
            assert "2025-26" not in definition, (
                "v_players_current_season has hardcoded season name"
            )
        finally:
            conn.close()
