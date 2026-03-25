"""
UnderstatETL — ENRICHMENT ONLY.

Adds xG, npxG, xA, xGChain, xGBuildup to existing player_season_stats rows.

CANONICAL RULE: This ETL NEVER creates player or team records.
If a player cannot be matched to an existing DB record, they are logged
to unmatched_players_log and skipped.

Data source: soccerdata library (Understat class).
Season key format: '2526' for 2025-26, '2425' for 2024-25, etc.

Bronze layer:
  data/raw/understat/{league_slug}/{season}/players.json
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from etl.base_etl import BaseETL
from utils.identity_resolution import IdentityResolver

logger = logging.getLogger(__name__)

# ── League mappings: canonical DB name → soccerdata Understat league string ──

LEAGUE_MAP = {
    "Premier League": "ENG-Premier League",
    "La Liga":        "ESP-La Liga",
    "Serie A":        "ITA-Serie A",
    "Bundesliga":     "GER-Bundesliga",
    "Ligue 1":        "FRA-Ligue 1",
}

# ── Season name → soccerdata key ('2024-25' → '2425') ────────────────────────

def _season_key(season_name: str) -> str:
    """Convert '2024-25' to '2425', '2025-26' to '2526', etc."""
    parts = season_name.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid season_name: {season_name!r}")
    return parts[0][-2:] + parts[1][-2:]


def _league_slug(league_name: str) -> str:
    return league_name.lower().replace(" ", "-")


# ── soccerdata column → DB column ─────────────────────────────────────────────
# Only the 5 xG columns that Understat contributes.

UNDERSTAT_COL_MAP = {
    "xg":         "xg",
    "np_xg":      "npxg",
    "xa":         "xa",
    "xg_chain":   "xg_chain",
    "xg_buildup": "xg_buildup",
}


class UnderstatETL(BaseETL):
    """
    Enriches player_season_stats with Understat xG metrics.

    Requires SofaScoreETL to have already populated players + player_season_stats
    for the relevant league/season.
    """

    SOURCE_NAME = "understat"
    SUPPORTED_LEAGUES = list(LEAGUE_MAP.keys())

    def __init__(self, db=None):
        super().__init__(db)
        self._resolver = IdentityResolver(self.db)

    # ── Abstract implementation ───────────────────────────────────────────────

    def run(self, league: str, season: str) -> Dict[str, Any]:
        """
        Run Understat enrichment for one league/season.

        Args:
            league: Canonical league name, e.g. 'Premier League'
            season: Season name, e.g. '2024-25'

        Returns:
            Stats dict: {processed, enriched, skipped, errors, unmatched}
        """
        if league not in LEAGUE_MAP:
            raise ValueError(
                f"Unsupported league: {league!r}. "
                f"Supported: {list(LEAGUE_MAP.keys())}"
            )

        slug      = _league_slug(league)
        us_league = LEAGUE_MAP[league]
        us_key    = _season_key(season)

        run_id = self.start_run(league, season)
        logger.info(
            f"[run={run_id}] UnderstatETL: {league} {season} "
            f"(understat key={us_key!r})"
        )

        counts = {
            "processed": 0,
            "enriched":  0,
            "skipped":   0,
            "errors":    0,
            "unmatched": 0,
        }

        # ── DB context ───────────────────────────────────────────────────────
        league_id = self.get_league_id(league)
        season_id = self.get_season_id(season)
        if not league_id or not season_id:
            logger.error(
                f"league_id or season_id not found in DB: {league}/{season}"
            )
            counts["errors"] += 1
            self.finish_run(run_id, status="error", **counts)
            return counts

        # ── Fetch from soccerdata ────────────────────────────────────────────
        try:
            df = self._fetch(us_league, us_key)
        except Exception as exc:
            logger.error(f"soccerdata fetch failed: {exc}", exc_info=True)
            counts["errors"] += 1
            self.finish_run(run_id, status="error", **counts)
            return counts

        if df.empty:
            logger.warning(f"No Understat data for {league} {season}")
            self.finish_run(run_id, status="success", **counts)
            return counts

        # ── Bronze ───────────────────────────────────────────────────────────
        self.save_bronze(
            df.reset_index().to_dict(orient="records"),
            self.SOURCE_NAME, slug, season,
            "players.json",
        )

        # ── Before count ─────────────────────────────────────────────────────
        rows_before = self._count_understat_enriched(league_id, season_id)

        # ── Process each player ───────────────────────────────────────────────
        for row in df.reset_index().to_dict(orient="records"):
            try:
                result = self._process_row(
                    row, league, league_id, season, season_id
                )
                counts["processed"] += 1
                if result == "enriched":
                    counts["enriched"] += 1
                elif result == "unmatched":
                    counts["unmatched"] += 1
                else:
                    counts["skipped"] += 1
            except Exception as exc:
                player_name = row.get("player", row.get("name", "unknown"))
                logger.error(
                    f"Failed processing '{player_name}': {exc}", exc_info=True
                )
                counts["errors"] += 1

        # ── After count assertion ─────────────────────────────────────────────
        rows_after = self._count_understat_enriched(league_id, season_id)
        if rows_after < rows_before:
            logger.error(
                f"ASSERTION FAILED: enriched rows decreased! "
                f"before={rows_before}, after={rows_after}"
            )
            counts["errors"] += 1

        logger.info(
            f"[run={run_id}] Understat enriched rows: "
            f"{rows_before} → {rows_after} "
            f"(+{rows_after - rows_before})"
        )

        status = "success" if counts["errors"] == 0 else "partial"
        self.finish_run(run_id, status=status, **counts)
        logger.info(
            f"[run={run_id}] Done: {counts['enriched']} enriched, "
            f"{counts['unmatched']} unmatched, {counts['errors']} errors"
        )
        return counts

    # ── Data fetch ────────────────────────────────────────────────────────────

    def _fetch(self, us_league: str, us_key: str) -> pd.DataFrame:
        """Fetch player season stats from soccerdata Understat."""
        import soccerdata as sd

        us = sd.Understat(
            leagues=us_league,
            seasons=us_key,
            data_dir=Path('data/cache/soccerdata/Understat'),
        )
        df = us.read_player_season_stats()
        logger.info(
            f"soccerdata returned {len(df)} player rows "
            f"for {us_league!r} {us_key!r}"
        )
        return df

    # ── Per-row processing ────────────────────────────────────────────────────

    def _process_row(
        self,
        row: Dict,
        league: str,
        league_id: int,
        season: str,
        season_id: int,
    ) -> str:
        """
        Resolve + enrich one player row.

        Returns 'enriched', 'unmatched', or 'skipped'.
        """
        # Extract key fields from soccerdata row
        # soccerdata may use 'player' or 'name' as the player name column
        player_name = (
            row.get("player") or row.get("name") or ""
        ).strip()
        team_name   = (row.get("team") or "").strip()
        position    = str(row.get("position") or "")

        if not player_name:
            return "skipped"

        # Resolve identity
        db_player_id = self._resolver.resolve(
            name=player_name,
            team_name=team_name,
            league_name=league,
            season_name=season,
            position=position if position else None,
        )

        if db_player_id is None:
            counts_unmatched = self.log_unmatched(
                source="understat",
                player_name=player_name,
                team_name=team_name,
                league_name=league,
                season_name=season,
                reason="no_match",
            )
            return "unmatched"

        # Find the team_id for this player in this season
        # Prefer to get it from the existing player_season_stats row
        db_team_id = self._get_team_id_for_player_season(
            db_player_id, league_id, season_id
        )
        if db_team_id is None:
            # Fallback: look up by team name in this league
            db_team_id = self._get_team_id_by_name(team_name, league_id)

        if db_team_id is None:
            logger.warning(
                f"No team_id for '{player_name}' (team={team_name!r}) in "
                f"{league} {season} — skipping xG upsert"
            )
            return "skipped"

        # Build xG params
        xg_params = self._extract_xg(row)
        if not xg_params:
            return "skipped"

        self._upsert_xg(
            db_player_id, db_team_id, season_id, league_id, xg_params
        )
        return "enriched"

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_xg(self, row: Dict) -> Optional[Dict]:
        """Extract xG fields from a soccerdata row dict."""
        params = {}
        for sd_col, db_col in UNDERSTAT_COL_MAP.items():
            val = row.get(sd_col)
            if val is not None:
                try:
                    params[db_col] = float(val)
                except (TypeError, ValueError):
                    pass
        return params if params else None

    def _upsert_xg(
        self,
        player_id: int,
        team_id: int,
        season_id: int,
        league_id: int,
        xg_params: Dict,
    ) -> None:
        """
        Upsert only the xG columns into player_season_stats.
        Creates the row if it doesn't exist, updates otherwise.
        Uses COALESCE so SofaScore values are never overwritten by NULL.
        """
        col_names = list(xg_params.keys())

        insert_cols = (
            "player_id, team_id, season_id, league_id, "
            + ", ".join(col_names)
            + ", understat_collected, last_updated"
        )
        insert_vals = (
            ":player_id, :team_id, :season_id, :league_id, "
            + ", ".join(f":{c}" for c in col_names)
            + ", TRUE, NOW()"
        )
        # On conflict: only update non-null incoming values (COALESCE guard)
        update_clauses = (
            ", ".join(
                f"{c} = COALESCE(EXCLUDED.{c}, player_season_stats.{c})"
                for c in col_names
            )
            + ", understat_collected = TRUE"
            + ", last_updated = NOW()"
        )

        sql = f"""
            INSERT INTO player_season_stats ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT (player_id, team_id, season_id, league_id)
            DO UPDATE SET {update_clauses}
        """

        params = {
            "player_id": player_id,
            "team_id":   team_id,
            "season_id": season_id,
            "league_id": league_id,
            **xg_params,
        }

        self.db.execute_query(sql, params, fetch=False)

    def _get_team_id_for_player_season(
        self,
        player_id: int,
        league_id: int,
        season_id: int,
    ) -> Optional[int]:
        """Find team_id from existing player_season_stats (from SofaScore run)."""
        rows = self.db.execute_query(
            """
            SELECT team_id FROM player_season_stats
             WHERE player_id  = :pid
               AND league_id  = :lid
               AND season_id  = :sid
             LIMIT 1
            """,
            {"pid": player_id, "lid": league_id, "sid": season_id},
            fetch=True,
        )
        return rows[0][0] if rows else None

    def _get_team_id_by_name(
        self, team_name: str, league_id: int
    ) -> Optional[int]:
        """Fallback: find team_id by name in the league."""
        rows = self.db.execute_query(
            """
            SELECT team_id FROM teams
             WHERE immutable_unaccent(lower(team_name))
                   = immutable_unaccent(lower(:name))
               AND league_id = :lid
             LIMIT 1
            """,
            {"name": team_name, "lid": league_id},
            fetch=True,
        )
        return rows[0][0] if rows else None

    def _count_understat_enriched(
        self, league_id: int, season_id: int
    ) -> int:
        """Count rows already enriched by Understat for this league/season."""
        rows = self.db.execute_query(
            """
            SELECT COUNT(*) FROM player_season_stats
             WHERE league_id          = :lid
               AND season_id          = :sid
               AND understat_collected = TRUE
            """,
            {"lid": league_id, "sid": season_id},
            fetch=True,
        )
        return int(rows[0][0]) if rows else 0

    # ── Convenience ───────────────────────────────────────────────────────────

    def run_all(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run for multiple leagues and seasons (defaults to all supported).
        """
        if leagues is None:
            leagues = self.SUPPORTED_LEAGUES
        if seasons is None:
            from scrapers.sofascore.constants import SEASON_NAME_TO_YEAR
            seasons = list(SEASON_NAME_TO_YEAR.keys())

        totals = {
            "processed": 0, "enriched": 0,
            "skipped":   0, "errors":   0, "unmatched": 0,
        }
        for league in leagues:
            for season in seasons:
                result = self.run(league, season)
                for k in totals:
                    totals[k] += result.get(k, 0)

        return totals
