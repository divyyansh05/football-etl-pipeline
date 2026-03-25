"""
Tests for utils/identity_resolution.py

Uses an in-memory mock DB to verify the 4-step matching protocol without
touching the real PostgreSQL instance.
"""
import pytest
from unittest.mock import MagicMock, call
from utils.identity_resolution import IdentityResolver, _normalise, POSITION_MAP_UNDERSTAT


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_db(**step_results):
    """
    Build a mock DatabaseConnection where execute_query returns configured results.

    step_results keys:
      by_uid         → rows for Step 0 (understat_id lookup)
      step1          → rows for Step 1 (norm_name + team + season)
      step2          → rows for Step 2 (norm_name + league + season)
      step3          → rows for Step 3 (pg_trgm fuzzy)
      update         → not asserted, just no-op
      log_unmatched  → not asserted
    """
    db = MagicMock()

    def _side_effect(sql, params=None, fetch=True):
        sql_stripped = " ".join(sql.split()).lower()
        if "understat_id = :uid" in sql_stripped and "player_id" in sql_stripped and "!= :pid" not in sql_stripped:
            # Step 0 fast-path lookup
            if "update players" in sql_stripped:
                return None
            return step_results.get("by_uid", [])
        if "player_name_norm = :norm_name" in sql_stripped and "immutable_unaccent" in sql_stripped and "team" in sql_stripped and "league" not in sql_stripped:
            return step_results.get("step1", [])
        if "player_name_norm = :norm_name" in sql_stripped and "league_name" in sql_stripped:
            return step_results.get("step2", [])
        if "similarity" in sql_stripped:
            return step_results.get("step3", [])
        if "insert into unmatched" in sql_stripped:
            return None
        if "update players" in sql_stripped:
            return None
        return []

    db.execute_query.side_effect = _side_effect
    return db


# ── _normalise ────────────────────────────────────────────────────────────────

class TestNormalise:
    def test_strips_accents(self):
        assert _normalise("Ángel Di María") == "angel di maria"

    def test_lowercases(self):
        assert _normalise("COLE PALMER") == "cole palmer"

    def test_strips_whitespace(self):
        assert _normalise("  Phil Foden  ") == "phil foden"

    def test_empty_string(self):
        assert _normalise("") == ""

    def test_none_like_empty(self):
        assert _normalise("") == ""


# ── IdentityResolver.resolve ──────────────────────────────────────────────────

class TestResolve:
    def test_step0_fast_path_by_understat_id(self):
        db = _make_db(by_uid=[(42,)])
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Erling Haaland",
            team_name="Manchester City",
            league_name="Premier League",
            season_name="2024-25",
            understat_id=8260,
        )
        assert pid == 42

    def test_step1_exact_name_team_season(self):
        db = _make_db(step1=[(101,)])
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Mohamed Salah",
            team_name="Liverpool",
            league_name="Premier League",
            season_name="2024-25",
        )
        assert pid == 101

    def test_step2_exact_name_league_season(self):
        db = _make_db(
            step1=[],
            step2=[(202, 1)],  # (player_id, cnt)
        )
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Kylian Mbappé",
            team_name="Real Madrid",
            league_name="La Liga",
            season_name="2025-26",
        )
        assert pid == 202

    def test_step2_rejects_ambiguous(self):
        """Two players with same normalised name in same league/season → None."""
        db = _make_db(
            step1=[],
            step2=[(301, 2), (302, 2)],  # cnt=2 → ambiguous
        )
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Juan Garcia",
            team_name="Sevilla",
            league_name="La Liga",
            season_name="2024-25",
        )
        assert pid is None

    def test_step3_fuzzy_match(self):
        db = _make_db(
            step1=[],
            step2=[],
            step3=[(401, "M", 0.92)],
        )
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Vinicius Jr",
            team_name="Real Madrid",
            league_name="La Liga",
            season_name="2024-25",
        )
        assert pid == 401

    def test_step3_position_tiebreak(self):
        """When multiple fuzzy candidates, pick the one with matching position."""
        db = _make_db(
            step1=[],
            step2=[],
            step3=[
                (501, "F", 0.91),   # FWD — matches position "FW"
                (502, "D", 0.95),   # DEF — does not match
            ],
        )
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Pedro Santos",
            team_name="Lyon",
            league_name="Ligue 1",
            season_name="2024-25",
            position="FW",
        )
        assert pid == 501

    def test_step4_no_match_returns_none(self):
        db = _make_db(step1=[], step2=[], step3=[])
        resolver = IdentityResolver(db)
        pid = resolver.resolve(
            name="Unknown Player",
            team_name="Nonexistent FC",
            league_name="Premier League",
            season_name="2024-25",
        )
        assert pid is None

    def test_cache_hit_avoids_second_query(self):
        db = _make_db(step1=[(111,)])
        resolver = IdentityResolver(db)
        call_count_before = db.execute_query.call_count

        # First call
        pid1 = resolver.resolve(
            name="Bruno Fernandes",
            team_name="Manchester United",
            league_name="Premier League",
            season_name="2024-25",
        )
        calls_after_first = db.execute_query.call_count

        # Second call (same args) — should be served from cache
        pid2 = resolver.resolve(
            name="Bruno Fernandes",
            team_name="Manchester United",
            league_name="Premier League",
            season_name="2024-25",
        )
        calls_after_second = db.execute_query.call_count

        assert pid1 == 111
        assert pid2 == 111
        assert calls_after_second == calls_after_first  # no new queries

    def test_understat_id_stamped_on_match(self):
        """After a successful match, understat_id should be written to the DB."""
        from unittest.mock import MagicMock

        db = MagicMock()

        def _se(sql, params=None, fetch=True):
            sql_l = " ".join(sql.split()).lower()
            # Step 1 match
            if "player_name_norm = :norm_name" in sql_l and "team_name" in sql_l:
                return [(222,)]
            # Conflict check (SELECT for existing understat_id owner)
            if "understat_id = :uid" in sql_l and "player_id <> :pid" in sql_l:
                return []  # no conflict
            # UPDATE (stamp) — fetch=False, return None
            if "update players" in sql_l:
                return None
            return []

        db.execute_query.side_effect = _se

        resolver = IdentityResolver(db)
        resolver.resolve(
            name="Harry Kane",
            team_name="Bayern München",
            league_name="Bundesliga",
            season_name="2024-25",
            understat_id=12345,
        )

        # Check that an UPDATE players ... SET understat_id query was issued
        update_calls = [
            c for c in db.execute_query.call_args_list
            if "update players" in str(c).lower()
        ]
        assert len(update_calls) >= 1


# ── Position map ──────────────────────────────────────────────────────────────

class TestPositionMap:
    def test_all_understat_positions_mapped(self):
        expected = {"GK", "FW", "F", "D", "M", "AM", "DM", "AW", "S"}
        for pos in expected:
            assert pos in POSITION_MAP_UNDERSTAT or pos.upper() in POSITION_MAP_UNDERSTAT, \
                f"Missing position: {pos}"
