"""
SofaScoreETL — CANONICAL creator of players and teams.

This is the ONLY ETL class authorised to INSERT into:
  - players  (with created_by = 'sofascore')
  - teams    (with created_by = 'sofascore')

Every other ETL (Understat, ClubElo) is enrichment-only.

Collection order per league/season:
  1. Fetch standings → upsert teams
  2. Fetch top-players/overall → collect unique player IDs
  3. For each player: fetch identity → upsert player record
  4. For each player: fetch season stats → upsert player_season_stats row

Bronze layer:
  data/raw/sofascore/{league_slug}/{season}/standings.json
  data/raw/sofascore/{league_slug}/{season}/top_players.json
  data/raw/sofascore/{league_slug}/{season}/players/{player_id}.json
  data/raw/sofascore/{league_slug}/{season}/stats/{player_id}.json
"""
import logging
from typing import Any, Dict, List, Optional

from etl.base_etl import BaseETL
from scrapers.sofascore.client import SofaScoreClient
from scrapers.sofascore.constants import (
    LEAGUE_IDS,
    SEASON_NAME_TO_YEAR,
    STATS_FIELD_MAP,
)

logger = logging.getLogger(__name__)

# DB column → override value set by this ETL (not a stat field)
_COLLECTION_FLAGS = {
    "sofascore_collected": True,
    "last_updated": "NOW()",
}


def _league_slug(league_name: str) -> str:
    return league_name.lower().replace(" ", "-")


class SofaScoreETL(BaseETL):
    """
    Canonical creator: builds the player/team spine from SofaScore data,
    then upserts player_season_stats rows with deep stat fields.
    """

    SOURCE_NAME = "sofascore"
    SUPPORTED_LEAGUES = list(LEAGUE_IDS.keys())

    def __init__(self, db=None, rate_limit: float = 3.0):
        super().__init__(db)
        self._client = SofaScoreClient(rate_limit=rate_limit)
        # local caches for this run
        self._team_sscid_to_dbid: Dict[int, int] = {}   # sofascore_id → team_id
        self._player_sscid_to_dbid: Dict[int, int] = {}  # sofascore_id → player_id

    # ── Abstract implementation ───────────────────────────────────────────────

    def run(self, league: str, season: str) -> Dict[str, Any]:
        """
        Run full SofaScore collection for one league/season.

        Args:
            league: Canonical league name, e.g. 'Premier League'
            season: Season name, e.g. '2024-25'

        Returns:
            Stats dict: {processed, enriched, skipped, errors, unmatched}
        """
        if league not in LEAGUE_IDS:
            raise ValueError(
                f"Unsupported league: {league!r}. "
                f"Supported: {list(LEAGUE_IDS.keys())}"
            )
        if season not in SEASON_NAME_TO_YEAR:
            raise ValueError(
                f"Unsupported season: {season!r}. "
                f"Supported: {list(SEASON_NAME_TO_YEAR.keys())}"
            )

        season_year = SEASON_NAME_TO_YEAR[season]
        slug = _league_slug(league)

        run_id = self.start_run(league, season)
        logger.info(f"[run={run_id}] SofaScoreETL: {league} {season}")

        counts = {
            "processed": 0,
            "enriched":  0,
            "skipped":   0,
            "errors":    0,
            "unmatched": 0,
        }

        try:
            # ── Step 1: teams ────────────────────────────────────────────────
            self._collect_teams(league, season_year, season, slug)

            # ── Step 2: player IDs ───────────────────────────────────────────
            player_ids = self._client.get_top_player_ids(league, season_year)
            self.save_bronze(
                player_ids, self.SOURCE_NAME, slug, season,
                "top_players.json"
            )

            if not player_ids:
                logger.error(f"No player IDs returned for {league} {season}")
                counts["errors"] += 1
                self.finish_run(run_id, status="error", **counts)
                return counts

            # ── Steps 3+4: identity + stats per player ────────────────────────
            league_id  = self.get_league_id(league)
            season_id  = self.get_season_id(season)
            if not league_id or not season_id:
                raise RuntimeError(
                    f"league_id or season_id not found in DB for {league}/{season}"
                )

            for pid in player_ids:
                try:
                    result = self._process_player(
                        pid, league, season_year, season,
                        league_id, season_id, slug
                    )
                    counts["processed"] += 1
                    if result == "enriched":
                        counts["enriched"] += 1
                    elif result == "skipped":
                        counts["skipped"] += 1
                except Exception as exc:
                    logger.error(
                        f"Player {pid} failed in {league} {season}: {exc}",
                        exc_info=True,
                    )
                    counts["errors"] += 1

        except Exception as exc:
            logger.error(
                f"SofaScoreETL run failed [{league} {season}]: {exc}",
                exc_info=True,
            )
            counts["errors"] += 1
            self.finish_run(run_id, status="error", **counts)
            return counts

        status = "success" if counts["errors"] == 0 else "partial"
        self.finish_run(run_id, status=status, **counts)
        logger.info(
            f"[run={run_id}] Done: {counts['enriched']} enriched, "
            f"{counts['skipped']} skipped, {counts['errors']} errors"
        )
        return counts

    # ── Team collection ───────────────────────────────────────────────────────

    def _collect_teams(
        self,
        league: str,
        season_year: int,
        season: str,
        slug: str,
    ) -> None:
        """Fetch standings, upsert teams, populate _team_sscid_to_dbid cache."""
        standings = self._client.get_standings(league, season_year)
        self.save_bronze(
            standings, self.SOURCE_NAME, slug, season, "standings.json"
        )

        league_id = self.get_league_id(league)
        if not league_id:
            raise RuntimeError(f"League not found in DB: {league!r}")

        for team in standings:
            ss_id    = team["sofascore_id"]
            db_id    = self._upsert_team(ss_id, team["team_name"], league_id)
            if db_id:
                self._team_sscid_to_dbid[ss_id] = db_id
                # also warm BaseETL cache
                self._team_id_cache[ss_id] = db_id

        logger.info(
            f"Teams upserted: {len(self._team_sscid_to_dbid)} for {league} {season}"
        )

    def _upsert_team(
        self, sofascore_id: int, team_name: str, league_id: int
    ) -> Optional[int]:
        rows = self.db.execute_query(
            """
            INSERT INTO teams (team_name, league_id, sofascore_id, created_by)
            VALUES (:name, :lid, :ssid, 'sofascore')
            ON CONFLICT (sofascore_id) DO UPDATE
               SET team_name  = EXCLUDED.team_name,
                   updated_at = NOW()
            RETURNING team_id
            """,
            {"name": team_name, "lid": league_id, "ssid": sofascore_id},
            fetch=True,
        )
        return rows[0][0] if rows else None

    # ── Player processing ─────────────────────────────────────────────────────

    def _process_player(
        self,
        player_ssid: int,
        league: str,
        season_year: int,
        season: str,
        league_id: int,
        season_id: int,
        slug: str,
    ) -> str:
        """
        Fetch identity + stats for one player, upsert both.

        Returns 'enriched', 'skipped' based on outcome.
        """
        # ── Identity ─────────────────────────────────────────────────────────
        identity = self._client.get_player_identity(player_ssid)
        if not identity or not identity.get("name"):
            logger.debug(f"No identity for sofascore_id={player_ssid}")
            return "skipped"

        self.save_bronze(
            identity, self.SOURCE_NAME, slug, season,
            f"players/{player_ssid}.json"
        )

        # ── Team lookup ───────────────────────────────────────────────────────
        team_ssid = identity.get("team_sofascore_id")
        db_team_id: Optional[int] = None
        if team_ssid:
            db_team_id = self._team_sscid_to_dbid.get(team_ssid)
            if db_team_id is None:
                # Player may belong to a promoted/relegated club not in standings
                db_team_id = self.get_team_id_by_sofascore(team_ssid)

        # If we still have no team, try to create one (edge case: mid-season transfer target)
        if db_team_id is None and team_ssid and identity.get("team_name"):
            db_team_id = self._upsert_team(
                team_ssid, identity["team_name"], league_id
            )
            if db_team_id:
                self._team_sscid_to_dbid[team_ssid] = db_team_id
                self._team_id_cache[team_ssid] = db_team_id
                logger.debug(
                    f"Created supplementary team: {identity['team_name']} "
                    f"(sofascore_id={team_ssid})"
                )

        if db_team_id is None:
            logger.warning(
                f"No team_id for player {identity['name']} "
                f"(team_sofascore_id={team_ssid}) — skipping stats"
            )
            # Still upsert the player record, just skip stats
            self._upsert_player(identity)
            return "skipped"

        # ── Player upsert ─────────────────────────────────────────────────────
        db_player_id = self._upsert_player(identity)
        if not db_player_id:
            return "skipped"

        # ── Season stats ──────────────────────────────────────────────────────
        raw_stats = self._client.get_player_stats(player_ssid, league, season_year)
        if raw_stats is None:
            # Player discovered via top-players but has no detailed stats
            # (common for players who transferred mid-season)
            return "skipped"

        self.save_bronze(
            raw_stats, self.SOURCE_NAME, slug, season,
            f"stats/{player_ssid}.json"
        )

        self._upsert_player_season_stats(
            db_player_id, db_team_id, season_id, league_id, raw_stats
        )
        return "enriched"

    def _upsert_player(self, identity: Dict) -> Optional[int]:
        """
        Upsert player record. Only SofaScore may call this.

        Returns player_id or None on error.
        """
        position_code  = identity.get("position")
        position_group = identity.get("position_group")

        try:
            rows = self.db.execute_query(
                """
                INSERT INTO players (
                    player_name, sofascore_id, position, position_group,
                    position_source, nationality, date_of_birth, height_cm,
                    preferred_foot, shirt_number, created_by
                )
                VALUES (
                    :name, :ssid, :pos, :pos_group,
                    'sofascore', :nationality, :dob, :height,
                    :foot, :shirt, 'sofascore'
                )
                ON CONFLICT (sofascore_id) DO UPDATE
                   SET player_name    = EXCLUDED.player_name,
                       position       = COALESCE(EXCLUDED.position, players.position),
                       position_group = COALESCE(EXCLUDED.position_group, players.position_group),
                       nationality    = COALESCE(EXCLUDED.nationality, players.nationality),
                       date_of_birth  = COALESCE(EXCLUDED.date_of_birth, players.date_of_birth),
                       height_cm      = COALESCE(EXCLUDED.height_cm, players.height_cm),
                       preferred_foot = COALESCE(EXCLUDED.preferred_foot, players.preferred_foot),
                       shirt_number   = EXCLUDED.shirt_number,
                       updated_at     = NOW()
                RETURNING player_id
                """,
                {
                    "name":        identity["name"],
                    "ssid":        identity["sofascore_id"],
                    "pos":         position_code,
                    "pos_group":   position_group,
                    "nationality": identity.get("nationality"),
                    "dob":         identity.get("dob"),
                    "height":      identity.get("height_cm"),
                    "foot":        identity.get("preferred_foot"),
                    "shirt":       identity.get("shirt_number"),
                },
                fetch=True,
            )
            if rows:
                pid = rows[0][0]
                self._player_sscid_to_dbid[identity["sofascore_id"]] = pid
                return pid
        except Exception as exc:
            logger.error(
                f"Failed to upsert player {identity.get('name')}: {exc}"
            )
        return None

    def _upsert_player_season_stats(
        self,
        player_id: int,
        team_id: int,
        season_id: int,
        league_id: int,
        raw_stats: Dict,
    ) -> None:
        """Map API stat fields to DB columns and upsert player_season_stats."""
        params: Dict = {
            "player_id": player_id,
            "team_id":   team_id,
            "season_id": season_id,
            "league_id": league_id,
        }

        # Map API fields → DB columns
        col_names: List[str] = []
        for api_key, db_col in STATS_FIELD_MAP.items():
            if db_col is None:
                continue  # explicitly excluded
            val = raw_stats.get(api_key)
            if val is not None:
                params[db_col] = val
                col_names.append(db_col)

        if not col_names:
            logger.debug(f"No mappable stats for player_id={player_id}")
            return

        insert_cols = (
            "player_id, team_id, season_id, league_id, "
            + ", ".join(col_names)
            + ", sofascore_collected, last_updated"
        )
        insert_vals = (
            ":player_id, :team_id, :season_id, :league_id, "
            + ", ".join(f":{c}" for c in col_names)
            + ", TRUE, NOW()"
        )
        update_clauses = (
            ", ".join(f"{c} = EXCLUDED.{c}" for c in col_names)
            + ", sofascore_collected = TRUE"
            + ", last_updated = NOW()"
        )

        sql = f"""
            INSERT INTO player_season_stats ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT (player_id, team_id, season_id, league_id)
            DO UPDATE SET {update_clauses}
        """

        try:
            self.db.execute_query(sql, params, fetch=False)
        except Exception as exc:
            logger.error(
                f"Stats upsert failed for player_id={player_id}: {exc}"
            )

    # ── Convenience helpers ───────────────────────────────────────────────────

    def run_all(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run for multiple leagues and seasons.

        Defaults to all supported leagues and all four seasons.
        """
        if leagues is None:
            leagues = self.SUPPORTED_LEAGUES
        if seasons is None:
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
