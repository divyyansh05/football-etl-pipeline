"""
FotMobSquadETL — squad completion enrichment using FotMob __NEXT_DATA__.

ENRICHMENT ONLY. This ETL does NOT create new player or team records.
SofaScore is the only authorised creator of player/team records (schema
enforces sofascore_id NOT NULL on players).

What this ETL does:
  1. For each team in FOTMOB_TEAM_IDS[league]:
     - Fetch squad from www.fotmob.com via __NEXT_DATA__ extraction.
     - Match each FotMob player to an existing DB player using 4-step
       identity resolution (name + team + season).
     - On match: stamp fotmob_id on players, set fotmob_collected=TRUE,
       update is_injured, update height/DOB/shirt_number if missing.
     - Ensure a player_season_stats row exists (upsert); if the player
       exists in DB but has no PSS row for this league/season, create one.
     - On no match: log to unmatched_players_log and continue.

Collection Order note:
  Run SofaScoreETL FIRST. FotMobSquadETL assumes teams + players already
  exist for the league/season being processed. Running FotMob on a
  league/season with zero SofaScore data will yield zero matches (all
  players unmatched).

Bronze layer:
  data/raw/fotmob/{league_slug}/{season}/{team_id}.json

Rate limit: 3.0s per team page request (managed by client.py).
"""
import logging
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from etl.base_etl import BaseETL
from scrapers.fotmob.client import get_squad
from scrapers.fotmob.constants import (
    FOTMOB_ROLE_TO_POSITION,
    FOTMOB_TEAM_IDS,
    RATE_LIMIT_SECONDS,
    TEAM_NAME_MAP,
)

logger = logging.getLogger(__name__)

BRONZE_BASE = Path("data/raw/fotmob")


def _league_slug(league_name: str) -> str:
    """Return a filesystem-safe slug for a league name."""
    return league_name.lower().replace(" ", "-")


def _normalise(name: str) -> str:
    """
    NFKD unicode → ascii → lower → strip.
    Matches BaseETL.normalise_name() and identity_resolution._normalise().
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
    return ascii_name.lower().strip()


def _fotmob_team_name_to_db(fotmob_name: str) -> str:
    """
    Map a FotMob team name to the canonical DB team_name.

    Falls back to the FotMob name itself if no mapping entry exists.
    """
    return TEAM_NAME_MAP.get(fotmob_name, fotmob_name)


class FotMobSquadETL(BaseETL):
    """
    FotMob squad completion ETL — enrichment-only pipeline.

    Matches FotMob squad members to existing SofaScore-created player
    records and fills in: fotmob_id, is_injured, fotmob_collected flag,
    and basic bio fields (height, DOB, shirt_number) where currently NULL.
    """

    SOURCE_NAME = "fotmob"

    def __init__(self, db=None):
        """
        Args:
            db: DatabaseConnection instance. Created automatically if None.
        """
        super().__init__(db)

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self, league: str, season: str) -> Dict[str, Any]:
        """
        Run squad enrichment for all teams in a league/season.

        Iterates over every team in FOTMOB_TEAM_IDS[league], fetches
        their current squad, and attempts to match each player to the DB.

        Args:
            league: Canonical league name (e.g. 'Premier League').
            season: Season name as stored in DB (e.g. '2025-26').

        Returns:
            Dict with keys:
              processed    — total FotMob players attempted
              matched      — players matched to existing DB records
              enriched     — players whose fotmob_id / bio was updated
              unmatched    — players with no DB match (logged)
              unmatched_teams — teams in FotMob not found in DB
              errors       — processing errors
        """
        run_id = self.start_run(league, season)
        logger.info(f"[run={run_id}] FotMobSquadETL start: {league} {season}")

        counters = {
            "processed": 0,
            "matched": 0,
            "enriched": 0,
            "unmatched": 0,
            "unmatched_teams": 0,
            "errors": 0,
        }

        league_id = self.get_league_id(league)
        season_id = self.get_season_id(season)
        if league_id is None:
            logger.error(f"Unknown league: {league!r}")
            self.finish_run(run_id, status="error", notes=f"Unknown league: {league}")
            return counters
        if season_id is None:
            logger.error(f"Unknown season: {season!r}")
            self.finish_run(run_id, status="error", notes=f"Unknown season: {season}")
            return counters

        league_teams = FOTMOB_TEAM_IDS.get(league, {})
        if not league_teams:
            logger.warning(f"No FotMob team IDs configured for league: {league!r}")
            self.finish_run(run_id, status="success", notes="No teams configured")
            return counters

        bronze_dir = BRONZE_BASE / _league_slug(league) / season
        bronze_dir.mkdir(parents=True, exist_ok=True)

        for fotmob_team_id, (fotmob_team_name, slug) in league_teams.items():
            db_team_name = _fotmob_team_name_to_db(fotmob_team_name)

            # Look up team_id in DB
            team_id = self._get_team_id_by_name(db_team_name)
            if team_id is None:
                logger.warning(
                    f"[run={run_id}] Team not in DB: "
                    f"FotMob={fotmob_team_name!r} -> DB lookup={db_team_name!r} "
                    f"(league={league}, season={season}) — skipping"
                )
                counters["unmatched_teams"] += 1
                continue

            bronze_path = bronze_dir / f"{fotmob_team_id}.json"
            squad = get_squad(fotmob_team_id, slug, bronze_path=bronze_path)

            if not squad:
                logger.warning(
                    f"[run={run_id}] Empty squad for team_id={fotmob_team_id} "
                    f"({fotmob_team_name})"
                )
                continue

            logger.info(
                f"[run={run_id}] Processing {len(squad)} players "
                f"for {db_team_name} (FotMob team {fotmob_team_id})"
            )

            for player_data in squad:
                counters["processed"] += 1
                try:
                    result = self._process_player(
                        player_data,
                        db_team_name=db_team_name,
                        team_id=team_id,
                        league=league,
                        league_id=league_id,
                        season=season,
                        season_id=season_id,
                        run_id=run_id,
                    )
                    counters["matched"] += result.get("matched", 0)
                    counters["enriched"] += result.get("enriched", 0)
                    counters["unmatched"] += result.get("unmatched", 0)
                except Exception as exc:
                    counters["errors"] += 1
                    logger.error(
                        f"[run={run_id}] Error processing player "
                        f"{player_data.get('name', '?')} "
                        f"(fotmob_id={player_data.get('fotmob_id')}): {exc}",
                        exc_info=True,
                    )

        self.finish_run(
            run_id,
            status="success",
            processed=counters["processed"],
            enriched=counters["enriched"],
            skipped=counters["unmatched_teams"],
            unmatched=counters["unmatched"],
            errors=counters["errors"],
            notes=(
                f"matched={counters['matched']}, "
                f"unmatched_teams={counters['unmatched_teams']}"
            ),
        )
        logger.info(f"[run={run_id}] FotMobSquadETL done: {counters}")
        return counters

    # ── Per-player processing ─────────────────────────────────────────────────

    def _process_player(
        self,
        player_data: Dict[str, Any],
        db_team_name: str,
        team_id: int,
        league: str,
        league_id: int,
        season: str,
        season_id: int,
        run_id: int,
    ) -> Dict[str, int]:
        """
        Match a FotMob player dict to an existing DB player and enrich it.

        Steps:
          1. Try fotmob_id fast-path (player already has fotmob_id stamped).
          2. 4-step name matching:
             a. normalised_name + team + season (HIGH)
             b. normalised_name + league + season (MEDIUM, reject if ambiguous)
             c. pg_trgm similarity > 0.90 + team + season (LOW)
          3. On match: update fotmob_id, bio fields, fotmob_collected flag.
          4. Upsert player_season_stats row (ensure it exists).
          5. On no match: log unmatched.

        Returns:
            Dict with keys matched (0/1), enriched (0/1), unmatched (0/1).
        """
        name = player_data.get("name", "")
        fotmob_id = player_data.get("fotmob_id")
        if not name or not fotmob_id:
            return {"matched": 0, "enriched": 0, "unmatched": 0}

        norm_name = _normalise(name)

        # Step 0: fast-path via fotmob_id already stamped
        player_id = self._find_by_fotmob_id(fotmob_id)

        # Steps 1-3: name-based matching
        if player_id is None:
            player_id = self._match_by_name(
                norm_name=norm_name,
                team_name=db_team_name,
                league_name=league,
                season_name=season,
            )

        if player_id is None:
            logger.warning(
                f"[run={run_id}] No match: {name!r} "
                f"(team={db_team_name}, fotmob_id={fotmob_id})"
            )
            self.log_unmatched(
                source="fotmob",
                player_name=name,
                team_name=db_team_name,
                league_name=league,
                season_name=season,
                reason="no_match",
            )
            return {"matched": 0, "enriched": 0, "unmatched": 1}

        # Enrich player bio and stamp fotmob_id
        enriched = self._enrich_player(player_id, fotmob_id, player_data)

        # Upsert PSS row to set fotmob_collected + is_injured
        self._upsert_pss_fotmob(
            player_id=player_id,
            team_id=team_id,
            season_id=season_id,
            league_id=league_id,
            goals=player_data.get("goals", 0) or 0,
            assists=player_data.get("assists", 0) or 0,
            yellow_cards=player_data.get("yellow_cards", 0) or 0,
            red_cards=player_data.get("red_cards", 0) or 0,
            is_injured=player_data.get("is_injured", False),
        )

        logger.info(
            f"[run={run_id}] Matched {name!r} → player_id={player_id}"
        )
        return {"matched": 1, "enriched": 1 if enriched else 0, "unmatched": 0}

    # ── Player lookup helpers ─────────────────────────────────────────────────

    def _find_by_fotmob_id(self, fotmob_id: int) -> Optional[int]:
        """
        Fast-path: find player_id where fotmob_id is already stamped.

        Returns player_id or None.
        """
        rows = self.db.execute_query(
            "SELECT player_id FROM players WHERE fotmob_id = :fid",
            {"fid": fotmob_id},
            fetch=True,
        )
        return rows[0][0] if rows else None

    def _match_by_name(
        self,
        norm_name: str,
        team_name: str,
        league_name: str,
        season_name: str,
    ) -> Optional[int]:
        """
        4-step name-based matching to find an existing DB player.

        Step 1: player_name_norm + team + season (HIGH confidence).
        Step 2: player_name_norm + league + season (MEDIUM, reject ambiguous).
        Step 3: pg_trgm similarity > 0.90 + team + season (LOW confidence).
        Step 4: no match → return None.

        Returns player_id or None.
        """
        # Step 1: exact name + team + season
        rows = self.db.execute_query(
            """
            SELECT p.player_id
              FROM players p
              JOIN player_season_stats pss ON pss.player_id = p.player_id
              JOIN teams t  ON pss.team_id   = t.team_id
              JOIN seasons s ON pss.season_id = s.season_id
             WHERE p.player_name_norm = :norm_name
               AND immutable_unaccent(lower(t.team_name)) =
                   immutable_unaccent(lower(:team_name))
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
            return rows[0][0]

        # Step 2: exact name + league + season (reject if ambiguous)
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
        if rows and rows[0][1] == 1:
            return rows[0][0]

        # Step 3: pg_trgm fuzzy match > 0.90 + team + season
        rows = self.db.execute_query(
            """
            SELECT p.player_id,
                   similarity(p.player_name_norm, :norm_name) AS sim
              FROM players p
              JOIN player_season_stats pss ON pss.player_id = p.player_id
              JOIN teams t  ON pss.team_id   = t.team_id
              JOIN seasons s ON pss.season_id = s.season_id
             WHERE similarity(p.player_name_norm, :norm_name) > 0.90
               AND immutable_unaccent(lower(t.team_name)) =
                   immutable_unaccent(lower(:team_name))
               AND s.season_name = :season_name
             ORDER BY sim DESC
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
            logger.debug(
                f"Fuzzy match (sim={rows[0][1]:.3f}): {norm_name!r} → "
                f"player_id={rows[0][0]}"
            )
            return rows[0][0]

        return None

    def _get_team_id_by_name(self, team_name: str) -> Optional[int]:
        """
        Look up team_id by exact team_name match (case-insensitive via DB).

        Returns team_id or None if not found.
        """
        rows = self.db.execute_query(
            "SELECT team_id FROM teams WHERE team_name = :name",
            {"name": team_name},
            fetch=True,
        )
        return rows[0][0] if rows else None

    # ── Enrichment helpers ────────────────────────────────────────────────────

    def _enrich_player(
        self,
        player_id: int,
        fotmob_id: int,
        player_data: Dict[str, Any],
    ) -> bool:
        """
        Stamp fotmob_id and fill missing bio fields on an existing player.

        Only updates fields that are currently NULL in the DB — never
        overwrites SofaScore-provided data.

        Returns True if any update was made.
        """
        # Check for fotmob_id conflict (different player already has this ID)
        conflict = self.db.execute_query(
            """
            SELECT player_id FROM players
             WHERE fotmob_id = :fid AND player_id <> :pid
            """,
            {"fid": fotmob_id, "pid": player_id},
            fetch=True,
        )
        if conflict:
            logger.warning(
                f"fotmob_id conflict: {fotmob_id} already belongs to "
                f"player_id={conflict[0][0]}, skipping stamp for "
                f"player_id={player_id}"
            )
            return False

        # Parse DOB from 'YYYY-MM-DD' string
        dob_str = player_data.get("date_of_birth")
        dob_val: Optional[str] = None
        if dob_str:
            try:
                # Validate format
                date.fromisoformat(dob_str)
                dob_val = dob_str
            except (ValueError, TypeError):
                pass

        self.db.execute_query(
            """
            UPDATE players
               SET fotmob_id       = COALESCE(fotmob_id, :fid),
                   height_cm       = COALESCE(height_cm, :height),
                   date_of_birth   = COALESCE(date_of_birth, :dob),
                   shirt_number    = COALESCE(shirt_number, :shirt),
                   updated_at      = NOW()
             WHERE player_id = :pid
            """,
            {
                "fid": fotmob_id,
                "height": player_data.get("height_cm"),
                "dob": dob_val,
                "shirt": player_data.get("shirt_number"),
                "pid": player_id,
            },
            fetch=False,
        )
        return True

    def _upsert_pss_fotmob(
        self,
        player_id: int,
        team_id: int,
        season_id: int,
        league_id: int,
        goals: int,
        assists: int,
        yellow_cards: int,
        red_cards: int,
        is_injured: bool,
    ) -> None:
        """
        Upsert player_season_stats row for FotMob collection.

        If a row already exists (player_id, team_id, season_id, league_id):
          - Set fotmob_collected = TRUE
          - Update is_injured
          - Update goals/assists/yellow_cards/red_cards only if currently NULL
            (SofaScore-collected stats take precedence)

        If no row exists: insert a minimal row with FotMob stats only.

        The unique constraint is: (player_id, team_id, season_id, league_id).
        """
        self.db.execute_query(
            """
            INSERT INTO player_season_stats
                (player_id, team_id, season_id, league_id,
                 goals, assists, yellow_cards, red_cards,
                 fotmob_collected, is_injured, last_updated)
            VALUES
                (:player_id, :team_id, :season_id, :league_id,
                 :goals, :assists, :ycards, :rcards,
                 TRUE, :is_injured, NOW())
            ON CONFLICT (player_id, team_id, season_id, league_id)
            DO UPDATE SET
                fotmob_collected = TRUE,
                is_injured       = EXCLUDED.is_injured,
                goals = COALESCE(
                    player_season_stats.goals,
                    EXCLUDED.goals
                ),
                assists = COALESCE(
                    player_season_stats.assists,
                    EXCLUDED.assists
                ),
                yellow_cards = COALESCE(
                    player_season_stats.yellow_cards,
                    EXCLUDED.yellow_cards
                ),
                red_cards = COALESCE(
                    player_season_stats.red_cards,
                    EXCLUDED.red_cards
                ),
                last_updated = NOW()
            """,
            {
                "player_id": player_id,
                "team_id": team_id,
                "season_id": season_id,
                "league_id": league_id,
                "goals": goals,
                "assists": assists,
                "ycards": yellow_cards,
                "rcards": red_cards,
                "is_injured": is_injured,
            },
            fetch=False,
        )
