"""
Initial backfill script for fresh machine setup.
Runs once, detects existing data, skips already-populated league/seasons.
"""
import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/init_backfill.log')
    ]
)
logger = logging.getLogger(__name__)

TOP_5_LEAGUES = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

UNDERSTAT_LEAGUE_MAP = {
    'premier-league': 'epl',
    'la-liga': 'la_liga',
    'serie-a': 'serie_a',
    'bundesliga': 'bundesliga',
    'ligue-1': 'ligue_1',
}

SEASON_NAME_MAP = {
    2022: '2022-23',
    2023: '2023-24',
    2024: '2024-25',
    2025: '2025-26',
}

LEAGUE_DISPLAY_MAP = {
    'premier-league': 'Premier League',
    'la-liga': 'La Liga',
    'serie-a': 'Serie A',
    'bundesliga': 'Bundesliga',
    'ligue-1': 'Ligue 1',
}

BACKFILL_SEASONS = [2022, 2023, 2024, 2025]


def get_db():
    from database.connection import DatabaseConnection
    return DatabaseConnection()


def check_existing_data(db) -> dict:
    query = """
        SELECT s.season_name, l.league_name, COUNT(*) as player_count
        FROM player_season_stats pss
        JOIN seasons s ON pss.season_id = s.season_id
        JOIN leagues l ON pss.league_id = l.league_id
        WHERE l.league_name IN (
            'Premier League','La Liga','Serie A','Bundesliga','Ligue 1'
        )
        GROUP BY s.season_name, l.league_name
    """
    rows = db.execute_query(query, fetch=True)
    return {f"{r[0]}_{r[1]}": r[2] for r in rows}


def is_populated(existing, season_name, league_name, threshold=300):
    return existing.get(f"{season_name}_{league_name}", 0) >= threshold


def run_fotmob_backfill(existing):
    from scheduler.jobs import fotmob_weekly_deep_job
    console.print("\n[bold cyan]Step 1/3 — FotMob basic stats[/bold cyan]")
    results = {}
    for season_year in BACKFILL_SEASONS:
        season_name = SEASON_NAME_MAP[season_year]
        season_str = f"{season_year}-{str(season_year + 1)[-2:]}"
        missing = [l for l in TOP_5_LEAGUES
                   if not is_populated(existing, season_name, LEAGUE_DISPLAY_MAP[l])]
        if not missing:
            console.print(f"  [dim]{season_name} — all populated, skipping[/dim]")
            continue
        console.print(f"  Collecting {season_name} ({len(missing)} leagues)...")
        try:
            result = fotmob_weekly_deep_job(season=season_str)
            results[season_name] = result
            console.print(f"  [green]✓[/green] {season_name} complete")
        except Exception as e:
            console.print(f"  [red]✗[/red] {season_name}: {e}")
            results[season_name] = {'error': str(e)}
    return results


async def run_understat_backfill(existing):
    import aiohttp
    from understat import Understat
    from etl.understat_etl import UnderstatETL
    console.print("\n[bold cyan]Step 2/3 — Understat xG metrics[/bold cyan]")
    results = {}
    etl = UnderstatETL()
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        for season_year in BACKFILL_SEASONS:
            season_name = SEASON_NAME_MAP[season_year]
            for league_slug, understat_league in UNDERSTAT_LEAGUE_MAP.items():
                league_display = LEAGUE_DISPLAY_MAP[league_slug]
                if is_populated(existing, season_name, league_display, threshold=200):
                    db = get_db()
                    xg_check = db.execute_query("""
                        SELECT COUNT(*) as cnt FROM player_season_stats pss
                        JOIN seasons s ON pss.season_id = s.season_id
                        JOIN leagues l ON pss.league_id = l.league_id
                        WHERE s.season_name = :season AND l.league_name = :league
                        AND pss.xg > 0
                    """, {'season': season_name, 'league': league_display}, fetch=True)
                    if xg_check and xg_check[0][0] > 200:
                        console.print(f"  [dim]{season_name} {league_display} xG filled, skipping[/dim]")
                        continue
                console.print(f"  xG: {season_name} {league_display}...")
                try:
                    players = await understat.get_league_players(understat_league, season_year)
                    enriched = etl.enrich_players_with_understat(players, season_name, league_display)
                    results[f"{season_name}_{league_slug}"] = {'enriched': enriched}
                    console.print(f"  [green]✓[/green] {season_name} {league_display}: {enriched} players")
                except Exception as e:
                    console.print(f"  [red]✗[/red] {season_name} {league_display}: {e}")
                    results[f"{season_name}_{league_slug}"] = {'error': str(e)}
    return results


def main():
    console.print(Panel.fit(
        "[bold blue]Football ETL — Initial Backfill[/bold blue]\n"
        "Seasons: 2022-23, 2023-24, 2024-25, 2025-26\n"
        "Leagues: Top 5 EU",
        border_style="blue"
    ))

    try:
        db = get_db()
        existing = check_existing_data(db)
    except Exception as e:
        console.print(f"[red]DB connection failed: {e}[/red]")
        console.print("Ensure DB_HOST=db is set correctly in environment.")
        sys.exit(1)

    console.print(f"Existing records: {sum(existing.values()):,}")

    fotmob_results = run_fotmob_backfill(existing)
    existing = check_existing_data(get_db())
    understat_results = asyncio.run(run_understat_backfill(existing))

    # Step 3: SofaScore deep stats
    console.print("\n[bold cyan]Step 3/3 — SofaScore deep stats[/bold cyan]")
    from etl.sofascore_etl import SofaScoreETL
    sofascore_results = {}
    with SofaScoreETL() as etl:
        for season_year in BACKFILL_SEASONS:
            season_name = SEASON_NAME_MAP[season_year]
            for league_slug in TOP_5_LEAGUES:
                league_display = LEAGUE_DISPLAY_MAP[league_slug]
                # Check if SofaScore data already exists
                db = get_db()
                check = db.execute_query("""
                    SELECT COUNT(*) FROM player_season_stats pss
                    JOIN seasons s ON pss.season_id = s.season_id
                    JOIN leagues l ON pss.league_id = l.league_id
                    WHERE s.season_name = :season
                    AND l.league_name = :league
                    AND pss.aerial_duels_won > 0
                """, {'season': season_name, 'league': league_display}, fetch=True)
                count = check[0][0] if check else 0
                if count > 100:
                    console.print(f"  [dim]{season_name} {league_display} SofaScore filled, skipping[/dim]")
                    continue
                console.print(f"  Deep stats: {season_name} {league_display}...")
                try:
                    result = etl.process_league_season(league_slug, season_year)
                    sofascore_results[f"{season_name}_{league_slug}"] = result
                    console.print(f"  [green]✓[/green] {season_name} {league_display}: {result['enriched']} enriched")
                except Exception as e:
                    console.print(f"  [red]✗[/red] {season_name} {league_display}: {e}")

    console.print(Panel.fit(
        "[bold green]Backfill Complete[/bold green]\n"
        f"FotMob runs: {len(fotmob_results)}\n"
        f"Understat runs: {len(understat_results)}\n"
        f"SofaScore runs: {len(sofascore_results)}",
        border_style="green"
    ))


if __name__ == '__main__':
    main()
