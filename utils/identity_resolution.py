"""
Player Identity Resolution — Understat → SofaScore matching.

This module is ENRICHMENT-ONLY. It finds existing players in the DB
and optionally stamps their understat_id. It NEVER creates player records.
Only SofaScore (etl/sofascore_etl.py) may create players.

Matching protocol (strict priority order):
  Step 0 — fast path: exact understat_id on players table
  Step 1 — normalised_name + team_name + season_name  (HIGH confidence)
  Step 2 — normalised_name + league_name + season_name (MEDIUM confidence)
  Step 3 — pg_trgm similarity > 0.90 + team + season  (LOW confidence)
  Step 4 — no match → log_unmatched, return None

DatabaseConnection API (confirmed from database/connection.py):
  db.execute_query(sql, params={}, fetch=True) → list[Row] | result
"""
import logging
import unicodedata
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ── Position maps ─────────────────────────────────────────────────────────────

# Understat position strings → canonical position_group
POSITION_MAP_UNDERSTAT = {
    "GK": "GK",
    "D":  "DEF",
    "M":  "MID",
    "AM": "MID",
    "DM": "MID",
    "AW": "FWD",
    "FW": "FWD",
    "F":  "FWD",
    "S":  "FWD",
}

# SofaScore single-letter codes → canonical position_group
SOFASCORE_POSITION_MAP = {
    "G": "GK",
    "D": "DEF",
    "M": "MID",
    "F": "FWD",
}

# Confidence labels returned in resolve() result
CONFIDENCE_HIGH   = "high"    # Step 1 exact match
CONFIDENCE_MEDIUM = "medium"  # Step 2 exact match
CONFIDENCE_LOW    = "low"     # Step 3 fuzzy match


def _normalise(name: str) -> str:
    """NFKD unicode → ascii → lower → strip. Mirrors BaseETL.normalise_name."""
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
    return ascii_name.lower().strip()


class IdentityResolver:
    """
    Resolve Understat player → existing SofaScore player in DB.

    Usage:
        resolver = IdentityResolver(db)
        player_id = resolver.resolve(
            name="Erling Haaland",
            team_name="Manchester City",
            league_name="Premier League",
            season_name="2024-25",
            understat_id=8260,
            position="FW",
        )
        # Returns player_id (int) or None if no match.
    """

    TRGM_THRESHOLD = 0.90  # pg_trgm similarity floor for Step 3

    def __init__(self, db):
        self.db = db
        # Cache: (norm_name, norm_team, season_name) → player_id
        self._cache: dict = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def resolve(
        self,
        name: str,
        team_name: str,
        league_name: str,
        season_name: str,
        understat_id: Optional[int] = None,
        position: Optional[str] = None,
    ) -> Optional[int]:
        """
        Find the DB player_id for an Understat player record.

        Returns player_id on match, None if no match (already logged).
        """
        norm_name = _normalise(name)
        norm_team = _normalise(team_name)
        cache_key = (norm_name, norm_team, season_name)

        if cache_key in self._cache:
            return self._cache[cache_key]

        # Step 0 — fast path via understat_id
        if understat_id is not None:
            pid = self._by_understat_id(understat_id)
            if pid:
                self._cache[cache_key] = pid
                return pid

        # Step 1 — exact: norm_name + team + season
        pid, confidence = self._step1(norm_name, team_name, season_name)

        # Step 2 — exact: norm_name + league + season
        if pid is None:
            pid, confidence = self._step2(norm_name, league_name, season_name)

        # Step 3 — fuzzy: pg_trgm + team + season (+ optional position tiebreak)
        if pid is None:
            pid, confidence = self._step3(
                norm_name, team_name, season_name, position
            )

        # Step 4 — no match
        if pid is None:
            self._log_unmatched(
                name, norm_name, team_name, league_name, season_name
            )
            self._cache[cache_key] = None
            return None

        # Stamp understat_id if we have one and match succeeded
        if understat_id is not None:
            self._update_understat_id(pid, understat_id, name)

        logger.info(
            f"Matched [{confidence}] '{name}' ({team_name}) → player_id={pid}"
        )
        self._cache[cache_key] = pid
        return pid

    # ── Step implementations ──────────────────────────────────────────────────

    def _by_understat_id(self, understat_id: int) -> Optional[int]:
        rows = self.db.execute_query(
            "SELECT player_id FROM players WHERE understat_id = :uid",
            {"uid": understat_id},
            fetch=True,
        )
        return rows[0][0] if rows else None

    def _step1(
        self, norm_name: str, team_name: str, season_name: str
    ) -> Tuple[Optional[int], str]:
        """
        Exact: immutable_unaccent(player_name_norm) + team_name + season.
        Joins player_season_stats → teams → seasons for context.
        """
        rows = self.db.execute_query(
            """
            SELECT p.player_id
              FROM players p
              JOIN player_season_stats pss ON pss.player_id = p.player_id
              JOIN teams t  ON pss.team_id   = t.team_id
              JOIN seasons s ON pss.season_id = s.season_id
             WHERE p.player_name_norm = :norm_name
               AND immutable_unaccent(lower(t.team_name)) = immutable_unaccent(lower(:team_name))
               AND s.season_name = :season_name
             LIMIT 1
            """,
            {
                "norm_name": norm_name,
                "team_name": team_name,
                "season_name": season_name,
            },
            fetch=True,
        )
        if rows:
            return rows[0][0], CONFIDENCE_HIGH
        return None, ""

    def _step2(
        self, norm_name: str, league_name: str, season_name: str
    ) -> Tuple[Optional[int], str]:
        """
        Exact: player_name_norm + league + season (team agnostic).
        Only returns a match when exactly ONE player with that name is in the
        league/season to avoid ambiguous merges.
        """
        rows = self.db.execute_query(
            """
            SELECT p.player_id, COUNT(*) OVER () AS cnt
              FROM players p
              JOIN player_season_stats pss ON pss.player_id = p.player_id
              JOIN leagues l  ON pss.league_id  = l.league_id
              JOIN seasons s  ON pss.season_id  = s.season_id
             WHERE p.player_name_norm = :norm_name
               AND l.league_name      = :league_name
               AND s.season_name      = :season_name
             LIMIT 2
            """,
            {
                "norm_name": norm_name,
                "league_name": league_name,
                "season_name": season_name,
            },
            fetch=True,
        )
        # Reject if ambiguous (two players with identical normalised name in same league/season)
        if rows and rows[0][1] == 1:
            return rows[0][0], CONFIDENCE_MEDIUM
        return None, ""

    def _step3(
        self,
        norm_name: str,
        team_name: str,
        season_name: str,
        position: Optional[str],
    ) -> Tuple[Optional[int], str]:
        """
        Fuzzy: pg_trgm similarity(player_name_norm, :norm_name) > threshold
        + team + season.  Position used as tiebreaker when multiple candidates.
        """
        rows = self.db.execute_query(
            """
            SELECT p.player_id,
                   p.position,
                   similarity(p.player_name_norm, :norm_name) AS sim
              FROM players p
              JOIN player_season_stats pss ON pss.player_id = p.player_id
              JOIN teams t  ON pss.team_id   = t.team_id
              JOIN seasons s ON pss.season_id = s.season_id
             WHERE similarity(p.player_name_norm, :norm_name) > :threshold
               AND immutable_unaccent(lower(t.team_name)) = immutable_unaccent(lower(:team_name))
               AND s.season_name = :season_name
             ORDER BY sim DESC
             LIMIT 5
            """,
            {
                "norm_name": norm_name,
                "team_name": team_name,
                "season_name": season_name,
                "threshold": self.TRGM_THRESHOLD,
            },
            fetch=True,
        )
        if not rows:
            return None, ""

        best_pid = rows[0][0]
        best_sim = float(rows[0][2])

        # Position tiebreak when multiple candidates above threshold
        if position and len(rows) > 1:
            canon_pos = POSITION_MAP_UNDERSTAT.get(position.upper())
            if canon_pos:
                for row in rows:
                    cand_pos = SOFASCORE_POSITION_MAP.get(row[1], row[1])
                    if cand_pos == canon_pos:
                        best_pid = row[0]
                        best_sim = float(row[2])
                        break

        logger.debug(
            f"Step 3 fuzzy match: '{norm_name}' → player_id={best_pid} "
            f"(similarity={best_sim:.3f})"
        )
        return best_pid, CONFIDENCE_LOW

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _update_understat_id(
        self, player_id: int, understat_id: int, name: str
    ) -> None:
        """
        Stamp understat_id onto the players row.
        Detects ID conflicts (different player already has this understat_id).
        """
        # Check for conflict
        rows = self.db.execute_query(
            """
            SELECT player_id FROM players
             WHERE understat_id = :uid AND player_id <> :pid
            """,
            {"uid": understat_id, "pid": player_id},
            fetch=True,
        )
        if rows:
            logger.warning(
                f"understat_id conflict: id={understat_id} already belongs to "
                f"player_id={rows[0][0]}, tried to assign to {player_id} ('{name}'). "
                f"Skipping stamp."
            )
            return

        self.db.execute_query(
            """
            UPDATE players
               SET understat_id = :uid,
                   updated_at   = NOW()
             WHERE player_id = :pid
               AND understat_id IS NULL
            """,
            {"uid": understat_id, "pid": player_id},
            fetch=False,
        )

    def _log_unmatched(
        self,
        name: str,
        norm_name: str,
        team_name: str,
        league_name: str,
        season_name: str,
    ) -> None:
        logger.warning(
            f"No match: '{name}' ({team_name}, {league_name}, {season_name})"
        )
        try:
            self.db.execute_query(
                """
                INSERT INTO unmatched_players_log
                    (source, player_name, player_name_norm, team_name,
                     league_name, season_name, reason)
                VALUES
                    ('understat', :name, :norm, :team, :league, :season,
                     'no_match')
                ON CONFLICT DO NOTHING
                """,
                {
                    "name": name,
                    "norm": norm_name,
                    "team": team_name,
                    "league": league_name,
                    "season": season_name,
                },
                fetch=False,
            )
        except Exception as exc:
            logger.error(f"Failed to log unmatched player '{name}': {exc}")
