"""
Scheduled job definitions — active sources only.

Schedule (UTC):
  Monday    05:00  — sofascore_weekly   : SofaScore current-season refresh
  Tuesday   05:00  — understat_weekly   : Understat xG enrichment
  Wednesday 05:00  — clubelo_weekly     : ClubElo ELO snapshot
  Thursday  05:00  — catchup_weekly     : Catch-up (re-runs Monday+Tuesday for failures)
  Saturday  06:00  — compute_scores     : Recompute analytics performance scores

No FotMob, API-Football, FBref, or StatsBomb — those sources are dead/removed.
"""
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from utils.season_utils import SeasonUtils

logger = logging.getLogger(__name__)

ALL_LEAGUES = [
    "Premier League",
    "La Liga",
    "Serie A",
    "Bundesliga",
    "Ligue 1",
]


def _get_db():
    from database.connection import DatabaseConnection
    return DatabaseConnection()


def _current_season() -> str:
    """Return current season name from DB or date fallback."""
    try:
        db = _get_db()
        rows = db.execute_query(
            "SELECT season_name FROM seasons WHERE is_current = TRUE LIMIT 1",
            fetch=True,
        )
        if rows:
            return rows[0][0]
    except Exception as exc:
        logger.warning(f"DB season lookup failed: {exc}")
    return SeasonUtils.get_current_season()


# ── Job 1: SofaScore weekly ───────────────────────────────────────────────────

def sofascore_weekly_job(
    leagues: Optional[List[str]] = None,
    season: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Weekly SofaScore refresh — updates player identities and season stats.

    Runs Monday 05:00 UTC.
    """
    from etl.sofascore_etl import SofaScoreETL

    if leagues is None:
        leagues = ALL_LEAGUES
    if season is None:
        season = _current_season()

    logger.info(f"sofascore_weekly_job: {len(leagues)} leagues, season={season}")

    db  = _get_db()
    etl = SofaScoreETL(db=db)

    totals: Dict[str, Any] = {
        "job": "sofascore_weekly",
        "season": season,
        "processed": 0, "enriched": 0,
        "skipped": 0, "errors": 0, "unmatched": 0,
    }

    for league in leagues:
        try:
            result = etl.run(league, season)
            for k in ("processed", "enriched", "skipped", "errors", "unmatched"):
                totals[k] += result.get(k, 0)
            logger.info(
                f"  {league}: enriched={result.get('enriched', 0)}, "
                f"errors={result.get('errors', 0)}"
            )
        except Exception as exc:
            logger.error(f"SofaScore job failed [{league}]: {exc}", exc_info=True)
            totals["errors"] += 1

    logger.info(f"sofascore_weekly_job done: {totals}")
    return totals


# ── Job 2: Understat weekly ───────────────────────────────────────────────────

def understat_weekly_job(
    leagues: Optional[List[str]] = None,
    season: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Weekly Understat xG enrichment.

    Runs Tuesday 05:00 UTC (after SofaScore on Monday).
    """
    from etl.understat_etl import UnderstatETL

    if leagues is None:
        leagues = ALL_LEAGUES
    if season is None:
        season = _current_season()

    logger.info(f"understat_weekly_job: {len(leagues)} leagues, season={season}")

    db  = _get_db()
    etl = UnderstatETL(db=db)

    totals: Dict[str, Any] = {
        "job": "understat_weekly",
        "season": season,
        "processed": 0, "enriched": 0,
        "skipped": 0, "errors": 0, "unmatched": 0,
    }

    for league in leagues:
        try:
            result = etl.run(league, season)
            for k in ("processed", "enriched", "skipped", "errors", "unmatched"):
                totals[k] += result.get(k, 0)
            logger.info(
                f"  {league}: enriched={result.get('enriched', 0)}, "
                f"unmatched={result.get('unmatched', 0)}, "
                f"errors={result.get('errors', 0)}"
            )
        except Exception as exc:
            logger.error(f"Understat job failed [{league}]: {exc}", exc_info=True)
            totals["errors"] += 1

    logger.info(f"understat_weekly_job done: {totals}")
    return totals


# ── Job 3: ClubElo weekly ─────────────────────────────────────────────────────

def clubelo_weekly_job(snapshot_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Weekly ClubElo ELO snapshot.

    Runs Wednesday 05:00 UTC.
    Takes a snapshot for today (or a provided date).
    """
    from etl.clubelo_etl import ClubEloETL

    if snapshot_date is None:
        snapshot_date = date.today().isoformat()

    logger.info(f"clubelo_weekly_job: date={snapshot_date}")

    db  = _get_db()
    etl = ClubEloETL(db=db)

    try:
        result = etl.run_date(snapshot_date)
        result["job"] = "clubelo_weekly"
        logger.info(f"clubelo_weekly_job done: {result}")
        return result
    except Exception as exc:
        logger.error(f"ClubElo job failed: {exc}", exc_info=True)
        return {"job": "clubelo_weekly", "error": str(exc), "errors": 1}


# ── Job 4: Catch-up weekly ────────────────────────────────────────────────────

def catchup_weekly_job(season: Optional[str] = None) -> Dict[str, Any]:
    """
    Thursday catch-up: re-runs SofaScore and Understat for any league/season
    combinations that have < 300 players or < 200 Understat-enriched players.

    Runs Thursday 05:00 UTC.
    """
    if season is None:
        season = _current_season()

    logger.info(f"catchup_weekly_job: season={season}")

    db = _get_db()

    # Find leagues with insufficient SofaScore coverage
    rows = db.execute_query(
        """
        SELECT l.league_name,
               COUNT(DISTINCT pss.player_id) AS player_count
          FROM leagues l
          LEFT JOIN player_season_stats pss ON pss.league_id = l.league_id
          LEFT JOIN seasons s ON pss.season_id = s.season_id
            AND s.season_name = :season
         WHERE l.league_name = ANY(:leagues)
         GROUP BY l.league_name
        HAVING COUNT(DISTINCT pss.player_id) < 300
        """,
        {"season": season, "leagues": ALL_LEAGUES},
        fetch=True,
    )
    needs_sofascore = [r[0] for r in rows] if rows else []

    # Find leagues with insufficient Understat enrichment
    rows2 = db.execute_query(
        """
        SELECT l.league_name,
               COUNT(CASE WHEN pss.understat_collected THEN 1 END) AS enriched_count
          FROM leagues l
          LEFT JOIN player_season_stats pss ON pss.league_id = l.league_id
          LEFT JOIN seasons s ON pss.season_id = s.season_id
            AND s.season_name = :season
         WHERE l.league_name = ANY(:leagues)
         GROUP BY l.league_name
        HAVING COUNT(CASE WHEN pss.understat_collected THEN 1 END) < 200
        """,
        {"season": season, "leagues": ALL_LEAGUES},
        fetch=True,
    )
    needs_understat = [r[0] for r in rows2] if rows2 else []

    totals: Dict[str, Any] = {
        "job": "catchup_weekly",
        "season": season,
        "sofascore_reruns": len(needs_sofascore),
        "understat_reruns": len(needs_understat),
        "errors": 0,
    }

    if needs_sofascore:
        logger.info(f"Catch-up SofaScore for: {needs_sofascore}")
        result = sofascore_weekly_job(leagues=needs_sofascore, season=season)
        totals["errors"] += result.get("errors", 0)

    if needs_understat:
        logger.info(f"Catch-up Understat for: {needs_understat}")
        result = understat_weekly_job(leagues=needs_understat, season=season)
        totals["errors"] += result.get("errors", 0)

    if not needs_sofascore and not needs_understat:
        logger.info("Catch-up: all leagues at threshold — nothing to do")

    logger.info(f"catchup_weekly_job done: {totals}")
    return totals


# ── Job 5: Analytics score computation ───────────────────────────────────────

def compute_scores_job() -> Dict[str, Any]:
    """
    Recompute analytics performance scores after weekly enrichment.

    Runs Saturday 06:00 UTC — after Mon/Tue/Wed/Thu pipeline jobs have
    updated player_season_stats with the latest SofaScore and Understat data.

    Calls analytics/compute_scores.py via subprocess so it runs in a clean
    Python process with its own sys.path, matching how it is invoked manually.
    """
    import subprocess
    import sys
    from pathlib import Path

    project_root = str(Path(__file__).parent.parent)
    logger.info("compute_scores_job: starting analytics score recomputation")

    try:
        result = subprocess.run(
            [sys.executable, 'analytics/compute_scores.py'],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        if result.returncode != 0:
            logger.error(
                f"Score computation failed (exit {result.returncode}): "
                f"{result.stderr[:500]}"
            )
            return {
                "job": "compute_scores",
                "success": False,
                "error": result.stderr[:500],
                "errors": 1,
            }

        # Log last 500 chars of stdout for summary visibility
        stdout_tail = result.stdout[-500:] if result.stdout else ""
        logger.info(f"compute_scores_job complete:\n{stdout_tail}")
        return {"job": "compute_scores", "success": True, "errors": 0}

    except Exception as exc:
        logger.error(f"compute_scores_job error: {exc}", exc_info=True)
        return {"job": "compute_scores", "success": False, "error": str(exc), "errors": 1}
