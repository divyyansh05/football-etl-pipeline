#!/usr/bin/env python3
"""
quality_audit.py — Data quality gates for football-etl-pipeline.

Runs the 7 quality checks defined in .claude/rules/data-quality.md and
prints a pass/fail report. Exits with code 1 if any gate fails.

Checks:
  1. Duplicate players (= 0 required)
  2. Orphaned player_season_stats (= 0 required)
  3. Stats rows with NULL team_id (= 0 required)
  4. Players created by non-SofaScore source (= 0 required)
  5. Position coverage (> 92% of players with ≥450 mins)
  6. Understat match rate per league/season (> 75%)
  7. SofaScore collection rate per league/season (> 70%)

Usage:
    python scripts/quality_audit.py
    python scripts/quality_audit.py --fail-fast
    python scripts/quality_audit.py --league "Premier League" --season "2024-25"
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()
logger = logging.getLogger(__name__)

# ── Thresholds (from .claude/rules/data-quality.md) ──────────────────────────

POSITION_COVERAGE_MIN = 0.92   # 92% of active players must have a position
UNDERSTAT_MATCH_MIN   = 0.75   # 75% of SofaScore players matched in Understat
SOFASCORE_RATE_MIN    = 0.70   # 70% of expected players collected via SofaScore
MIN_MINUTES           = 450    # player threshold for analytics

ALL_LEAGUES = ["Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1"]
ALL_SEASONS = ["2022-23", "2023-24", "2024-25", "2025-26"]


# ── DB ────────────────────────────────────────────────────────────────────────

def get_db():
    from database.connection import DatabaseConnection
    return DatabaseConnection()


# ── Individual checks ─────────────────────────────────────────────────────────

def check_duplicate_players(db) -> Tuple[bool, str]:
    """No two rows in players should refer to the same real person (sofascore_id unique)."""
    rows = db.execute_query(
        """
        SELECT sofascore_id, COUNT(*) AS cnt
          FROM players
         WHERE sofascore_id IS NOT NULL
         GROUP BY sofascore_id
        HAVING COUNT(*) > 1
        LIMIT 10
        """,
        fetch=True,
    )
    if rows:
        dupes = ", ".join(str(r[0]) for r in rows[:5])
        return False, f"{len(rows)} duplicate sofascore_ids: {dupes}…"
    return True, "0 duplicates"


def check_orphaned_stats(db) -> Tuple[bool, str]:
    """player_season_stats rows must point to an existing player."""
    rows = db.execute_query(
        """
        SELECT COUNT(*) FROM player_season_stats pss
         WHERE NOT EXISTS (
             SELECT 1 FROM players p WHERE p.player_id = pss.player_id
         )
        """,
        fetch=True,
    )
    count = rows[0][0] if rows else 0
    if count > 0:
        return False, f"{count} orphaned stat rows"
    return True, "0 orphaned rows"


def check_null_team_id(db) -> Tuple[bool, str]:
    """player_season_stats.team_id must never be NULL."""
    rows = db.execute_query(
        "SELECT COUNT(*) FROM player_season_stats WHERE team_id IS NULL",
        fetch=True,
    )
    count = rows[0][0] if rows else 0
    if count > 0:
        return False, f"{count} rows with NULL team_id"
    return True, "0 NULL team_id rows"


def check_canonical_creator(db) -> Tuple[bool, str]:
    """All players must have created_by = 'sofascore'."""
    rows = db.execute_query(
        """
        SELECT created_by, COUNT(*) FROM players
         WHERE created_by <> 'sofascore'
         GROUP BY created_by
        """,
        fetch=True,
    )
    if rows:
        detail = ", ".join(f"{r[1]}×{r[0]}" for r in rows)
        return False, f"Non-sofascore creators: {detail}"
    return True, "All players created_by='sofascore'"


def check_position_coverage(
    db,
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
) -> List[Tuple[bool, str, str, str]]:
    """Position must be set for >92% of players with ≥450 mins per league/season."""
    league_filter = leagues or ALL_LEAGUES
    season_filter = seasons or ALL_SEASONS
    results = []

    for league in league_filter:
        for season in season_filter:
            rows = db.execute_query(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(p.position) AS with_position
                  FROM player_season_stats pss
                  JOIN players p  ON pss.player_id  = p.player_id
                  JOIN leagues l  ON pss.league_id  = l.league_id
                  JOIN seasons s  ON pss.season_id  = s.season_id
                 WHERE l.league_name = :league
                   AND s.season_name = :season
                   AND pss.minutes  >= :mins
                """,
                {"league": league, "season": season, "mins": MIN_MINUTES},
                fetch=True,
            )
            if not rows or rows[0][0] == 0:
                continue

            total, with_pos = rows[0]
            pct = with_pos / total
            ok  = pct >= POSITION_COVERAGE_MIN
            msg = f"{with_pos}/{total} ({pct:.1%})"
            results.append((ok, msg, league, season))

    return results


def check_understat_match_rate(
    db,
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
) -> List[Tuple[bool, str, str, str]]:
    """≥75% of SofaScore players (≥450 mins) should have Understat xG data."""
    league_filter = leagues or ALL_LEAGUES
    season_filter = seasons or ALL_SEASONS
    results = []

    for league in league_filter:
        for season in season_filter:
            rows = db.execute_query(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN pss.understat_collected THEN 1 END) AS enriched
                  FROM player_season_stats pss
                  JOIN leagues l ON pss.league_id = l.league_id
                  JOIN seasons s ON pss.season_id = s.season_id
                 WHERE l.league_name = :league
                   AND s.season_name = :season
                   AND pss.minutes  >= :mins
                """,
                {"league": league, "season": season, "mins": MIN_MINUTES},
                fetch=True,
            )
            if not rows or rows[0][0] == 0:
                continue

            total, enriched = rows[0]
            pct = enriched / total
            ok  = pct >= UNDERSTAT_MATCH_MIN
            msg = f"{enriched}/{total} ({pct:.1%})"
            results.append((ok, msg, league, season))

    return results


def check_sofascore_rate(
    db,
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
) -> List[Tuple[bool, str, str, str]]:
    """≥70% of player_season_stats rows should have sofascore_collected=TRUE."""
    league_filter = leagues or ALL_LEAGUES
    season_filter = seasons or ALL_SEASONS
    results = []

    for league in league_filter:
        for season in season_filter:
            rows = db.execute_query(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN pss.sofascore_collected THEN 1 END) AS collected
                  FROM player_season_stats pss
                  JOIN leagues l ON pss.league_id = l.league_id
                  JOIN seasons s ON pss.season_id = s.season_id
                 WHERE l.league_name = :league
                   AND s.season_name = :season
                """,
                {"league": league, "season": season},
                fetch=True,
            )
            if not rows or rows[0][0] == 0:
                continue

            total, collected = rows[0]
            pct = collected / total
            ok  = pct >= SOFASCORE_RATE_MIN
            msg = f"{collected}/{total} ({pct:.1%})"
            results.append((ok, msg, league, season))

    return results


# ── Report ────────────────────────────────────────────────────────────────────

def run_audit(
    leagues: Optional[List[str]],
    seasons: Optional[List[str]],
    fail_fast: bool,
) -> bool:
    """Run all checks. Returns True if all pass."""
    try:
        db = get_db()
    except Exception as exc:
        console.print(f"[red]DB connection failed: {exc}[/red]")
        sys.exit(1)

    all_pass = True

    # ── Scalar checks ────────────────────────────────────────────────────────
    scalar_table = Table(title="Integrity Checks", show_lines=True)
    scalar_table.add_column("Check", style="cyan")
    scalar_table.add_column("Status")
    scalar_table.add_column("Detail")

    scalar_checks = [
        ("Duplicate players",      check_duplicate_players(db)),
        ("Orphaned stats",         check_orphaned_stats(db)),
        ("NULL team_id in stats",  check_null_team_id(db)),
        ("Canonical creator",      check_canonical_creator(db)),
    ]

    for name, (ok, detail) in scalar_checks:
        icon   = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
        scalar_table.add_row(name, icon, detail)
        if not ok:
            all_pass = False
            if fail_fast:
                console.print(scalar_table)
                console.print(f"[red]FAIL FAST: {name}[/red]")
                return False

    console.print(scalar_table)

    # ── Per-league/season checks ──────────────────────────────────────────────
    per_ls_checks = [
        ("Position coverage",      check_position_coverage(db, leagues, seasons),     POSITION_COVERAGE_MIN),
        ("Understat match rate",   check_understat_match_rate(db, leagues, seasons),  UNDERSTAT_MATCH_MIN),
        ("SofaScore collection",   check_sofascore_rate(db, leagues, seasons),        SOFASCORE_RATE_MIN),
    ]

    for check_name, results, threshold in per_ls_checks:
        if not results:
            console.print(f"[dim]{check_name}: no data[/dim]")
            continue

        t = Table(
            title=f"{check_name} (threshold: {threshold:.0%})",
            show_lines=True
        )
        t.add_column("League",  style="cyan")
        t.add_column("Season",  style="cyan")
        t.add_column("Status")
        t.add_column("Coverage")

        for ok, msg, league, season in results:
            icon = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
            t.add_row(league, season, icon, msg)
            if not ok:
                all_pass = False
                if fail_fast:
                    console.print(t)
                    console.print(f"[red]FAIL FAST: {check_name} {league} {season}[/red]")
                    return False

        console.print(t)

    # ── Final verdict ─────────────────────────────────────────────────────────
    if all_pass:
        console.print(Panel.fit("[bold green]All quality gates PASSED[/bold green]", border_style="green"))
    else:
        console.print(Panel.fit("[bold red]Quality gates FAILED — see above[/bold red]", border_style="red"))

    return all_pass


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run data quality audit")
    parser.add_argument(
        "--fail-fast", action="store_true",
        help="Stop at first failing check"
    )
    parser.add_argument(
        "--league", nargs="+", dest="leagues", default=None,
        metavar="LEAGUE",
        help="Filter to specific leagues"
    )
    parser.add_argument(
        "--season", nargs="+", dest="seasons", default=None,
        metavar="SEASON",
        help="Filter to specific seasons (e.g. '2024-25')"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    passed = run_audit(args.leagues, args.seasons, args.fail_fast)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
