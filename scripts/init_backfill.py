#!/usr/bin/env python3
"""
init_backfill.py — Full historical backfill for football-etl-pipeline.

Runs all three ETLs in the correct canonical order:
  1. SofaScoreETL  — creates players, teams, player_season_stats
  2. UnderstatETL  — enriches xG fields (enrichment only)
  3. ClubEloETL    — team ELO date snapshots (independent)

Scope: top-5 EU leagues × 4 seasons (2022-23 → 2025-26)

Usage:
    python scripts/init_backfill.py
    python scripts/init_backfill.py --dry-run
    python scripts/init_backfill.py --leagues "Premier League" "La Liga"
    python scripts/init_backfill.py --seasons "2024-25" "2025-26"
    python scripts/init_backfill.py --skip-sofascore --skip-clubelo
    python scripts/init_backfill.py --elo-dates "2025-01-01" "2025-06-01"
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# ── Constants ─────────────────────────────────────────────────────────────────

ALL_LEAGUES = [
    "Premier League",
    "La Liga",
    "Serie A",
    "Bundesliga",
    "Ligue 1",
]

ALL_SEASONS = ["2022-23", "2023-24", "2024-25", "2025-26"]

# Default ELO snapshot dates: first of each month for current season
# (run with --elo-dates to customise)
DEFAULT_ELO_DATES = [
    f"2025-{m:02d}-01" for m in range(8, 13)
] + [
    f"2026-{m:02d}-01" for m in range(1, 7)
]

POPULATED_THRESHOLD = 300  # min players to consider a league/season populated

# ── Setup ─────────────────────────────────────────────────────────────────────

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/init_backfill.log"),
    ],
)
logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    from database.connection import DatabaseConnection
    return DatabaseConnection()


def get_existing_counts(db) -> dict:
    """Return {'{season}_{league}': player_count} for already-populated combos."""
    rows = db.execute_query(
        """
        SELECT s.season_name, l.league_name, COUNT(DISTINCT pss.player_id)
          FROM player_season_stats pss
          JOIN seasons s ON pss.season_id = s.season_id
          JOIN leagues l ON pss.league_id = l.league_id
         WHERE l.league_name = ANY(:leagues)
         GROUP BY s.season_name, l.league_name
        """,
        {"leagues": ALL_LEAGUES},
        fetch=True,
    )
    return {f"{r[0]}_{r[1]}": r[2] for r in rows}


def is_sofascore_populated(existing: dict, season: str, league: str) -> bool:
    return existing.get(f"{season}_{league}", 0) >= POPULATED_THRESHOLD


def is_understat_enriched(db, season: str, league: str) -> bool:
    """Return True if ≥200 players already have xG data for this league/season."""
    rows = db.execute_query(
        """
        SELECT COUNT(DISTINCT pss.player_id)
          FROM player_season_stats pss
          JOIN seasons s ON pss.season_id = s.season_id
          JOIN leagues l ON pss.league_id = l.league_id
         WHERE s.season_name = :season
           AND l.league_name = :league
           AND pss.understat_collected = TRUE
        """,
        {"season": season, "league": league},
        fetch=True,
    )
    return bool(rows and rows[0][0] >= 200)


# ── ETL runners ───────────────────────────────────────────────────────────────

def run_sofascore(db, leagues, seasons, dry_run: bool) -> dict:
    from etl.sofascore_etl import SofaScoreETL

    console.print("\n[bold cyan]Phase 1/3 — SofaScore (canonical creator)[/bold cyan]")
    existing = get_existing_counts(db)
    results = {}

    etl = SofaScoreETL(db=db)
    for league in leagues:
        for season in seasons:
            key = f"{season}_{league}"
            if is_sofascore_populated(existing, season, league):
                console.print(
                    f"  [dim]{season} {league} — {existing.get(key, 0):,} players "
                    f"(≥{POPULATED_THRESHOLD}), skipping[/dim]"
                )
                continue

            if dry_run:
                console.print(f"  [yellow]DRY RUN[/yellow] {season} {league}")
                results[key] = {"dry_run": True}
                continue

            console.print(f"  [cyan]Running[/cyan] {season} {league}…")
            try:
                result = etl.run(league, season)
                results[key] = result
                console.print(
                    f"  [green]✓[/green] {season} {league}: "
                    f"{result.get('enriched', 0)} enriched, "
                    f"{result.get('errors', 0)} errors"
                )
            except Exception as exc:
                console.print(f"  [red]✗[/red] {season} {league}: {exc}")
                logger.exception(f"SofaScore failed: {league} {season}")
                results[key] = {"error": str(exc)}

    return results


def run_understat(db, leagues, seasons, dry_run: bool) -> dict:
    from etl.understat_etl import UnderstatETL

    console.print("\n[bold cyan]Phase 2/3 — Understat (xG enrichment)[/bold cyan]")
    results = {}
    etl = UnderstatETL(db=db)

    for league in leagues:
        for season in seasons:
            key = f"{season}_{league}"
            if is_understat_enriched(db, season, league):
                console.print(
                    f"  [dim]{season} {league} — Understat already enriched, skipping[/dim]"
                )
                continue

            if dry_run:
                console.print(f"  [yellow]DRY RUN[/yellow] {season} {league}")
                results[key] = {"dry_run": True}
                continue

            console.print(f"  [cyan]Running[/cyan] {season} {league}…")
            try:
                result = etl.run(league, season)
                results[key] = result
                console.print(
                    f"  [green]✓[/green] {season} {league}: "
                    f"{result.get('enriched', 0)} enriched, "
                    f"{result.get('unmatched', 0)} unmatched, "
                    f"{result.get('errors', 0)} errors"
                )
            except Exception as exc:
                console.print(f"  [red]✗[/red] {season} {league}: {exc}")
                logger.exception(f"Understat failed: {league} {season}")
                results[key] = {"error": str(exc)}

    return results


def run_clubelo(db, elo_dates, dry_run: bool) -> dict:
    from etl.clubelo_etl import ClubEloETL

    console.print("\n[bold cyan]Phase 3/3 — ClubElo (team ELO snapshots)[/bold cyan]")
    results = {}
    etl = ClubEloETL(db=db)

    for date_str in elo_dates:
        if dry_run:
            console.print(f"  [yellow]DRY RUN[/yellow] ELO {date_str}")
            results[date_str] = {"dry_run": True}
            continue

        console.print(f"  [cyan]ELO snapshot[/cyan] {date_str}…")
        try:
            result = etl.run_date(date_str)
            results[date_str] = result
            console.print(
                f"  [green]✓[/green] {date_str}: "
                f"{result.get('enriched', 0)} upserted, "
                f"{result.get('unmatched', 0)} unmatched"
            )
        except Exception as exc:
            console.print(f"  [red]✗[/red] {date_str}: {exc}")
            logger.exception(f"ClubElo failed: {date_str}")
            results[date_str] = {"error": str(exc)}

    return results


# ── Summary table ─────────────────────────────────────────────────────────────

def print_summary(ss_results, us_results, elo_results, dry_run: bool) -> None:
    total_errors = sum(
        1 for r in {**ss_results, **us_results, **elo_results}.values()
        if isinstance(r, dict) and r.get("error")
    )
    style = "yellow" if dry_run else ("red" if total_errors else "green")
    label = "DRY RUN — no data written" if dry_run else (
        "Backfill Complete (with errors)" if total_errors else "Backfill Complete"
    )

    table = Table(title="Backfill Summary", show_lines=True)
    table.add_column("Phase", style="cyan")
    table.add_column("Runs")
    table.add_column("Errors", style="red")

    def _errs(d): return sum(1 for r in d.values() if isinstance(r, dict) and r.get("error"))

    table.add_row("SofaScore",  str(len(ss_results)),  str(_errs(ss_results)))
    table.add_row("Understat",  str(len(us_results)),  str(_errs(us_results)))
    table.add_row("ClubElo",    str(len(elo_results)), str(_errs(elo_results)))

    console.print(table)
    console.print(Panel.fit(f"[bold {style}]{label}[/bold {style}]", border_style=style))


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Full historical backfill: SofaScore → Understat → ClubElo"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would run without executing any ETL"
    )
    parser.add_argument(
        "--leagues", nargs="+", default=ALL_LEAGUES,
        metavar="LEAGUE",
        help=f"Leagues to process. Default: all 5. Options: {ALL_LEAGUES}"
    )
    parser.add_argument(
        "--seasons", nargs="+", default=ALL_SEASONS,
        metavar="SEASON",
        help=f"Seasons to process. Default: all 4. Options: {ALL_SEASONS}"
    )
    parser.add_argument(
        "--elo-dates", nargs="+", default=DEFAULT_ELO_DATES,
        metavar="DATE",
        help="ISO dates for ClubElo snapshots (default: monthly 2025-08 to 2026-06)"
    )
    parser.add_argument(
        "--skip-sofascore", action="store_true",
        help="Skip Phase 1 (SofaScore)"
    )
    parser.add_argument(
        "--skip-understat", action="store_true",
        help="Skip Phase 2 (Understat)"
    )
    parser.add_argument(
        "--skip-clubelo", action="store_true",
        help="Skip Phase 3 (ClubElo)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    console.print(Panel.fit(
        "[bold blue]Football ETL — Historical Backfill[/bold blue]\n"
        f"Leagues: {', '.join(args.leagues)}\n"
        f"Seasons: {', '.join(args.seasons)}\n"
        f"ELO dates: {len(args.elo_dates)} snapshots\n"
        + ("[bold yellow]DRY RUN MODE[/bold yellow]" if args.dry_run else ""),
        border_style="blue",
    ))

    # DB connection
    try:
        db = get_db()
        existing = get_existing_counts(db)
        console.print(f"DB connected. Existing player-season records: {sum(existing.values()):,}")
    except Exception as exc:
        console.print(f"[red]DB connection failed: {exc}[/red]")
        console.print("Check DATABASE_URL or DB_* environment variables.")
        sys.exit(1)

    # Run phases in strict order
    ss_results  = run_sofascore(db, args.leagues, args.seasons, args.dry_run) \
                  if not args.skip_sofascore else {}
    us_results  = run_understat(db, args.leagues, args.seasons, args.dry_run) \
                  if not args.skip_understat else {}
    elo_results = run_clubelo(db, args.elo_dates, args.dry_run) \
                  if not args.skip_clubelo else {}

    print_summary(ss_results, us_results, elo_results, args.dry_run)


if __name__ == "__main__":
    main()
