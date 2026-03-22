"""
Football Data Pipeline CLI

Command-line interface for data ingestion from multiple sources:
- FotMob (Web API) - Primary source (no request limit)
- API-Football (RapidAPI) - Secondary source (100 req/day)
- StatsBomb Open Data
"""

import os
import click
import logging
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from datetime import datetime

from etl.statsbomb_etl import StatsBombETL
from etl.api_football_etl import APIFootballETL
from etl.fotmob_etl import FotMobETL
from scrapers.statsbomb.client import StatsBombClient
from scrapers.api_football.client import APIFootballClient, LEAGUE_IDS
from scrapers.fotmob.constants import (
    LEAGUE_IDS as FOTMOB_LEAGUE_IDS,
    LEAGUE_NAMES as FOTMOB_LEAGUE_NAMES,
    ALL_LEAGUE_KEYS as FOTMOB_ALL_LEAGUES,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
console = Console()

@click.group()
def cli():
    """Football Data Pipeline CLI."""
    pass

@cli.group()
def statsbomb():
    """StatsBomb Open Data operations - progressive actions, SCA/GCA."""
    pass


@cli.group()
def understat():
    """Understat operations - xA, npxG, xGChain, xGBuildup."""
    pass


@cli.group()
def sofascore():
    """SofaScore deep stats collection commands."""
    pass


@cli.group()
def fotmob():
    """FotMob data operations - PRIMARY SOURCE (no API key needed)."""
    pass


@cli.group('api-football')
def api_football():
    """API-Football (RapidAPI) data operations - SECONDARY SOURCE."""
    pass


@cli.group()
def system():
    """System health and monitoring commands."""
    pass


@cli.group()
def soccerdata():
    """SoccerData library integration - enhanced event data with x/y coordinates."""
    pass


# ============================================
# SYSTEM COMMANDS
# ============================================

@system.command('health')
def system_health():
    """
    Run health checks on all system components.

    Example:
        python cli.py system health
    """
    console.print("\n[bold blue]Running System Health Checks...[/bold blue]\n")

    try:
        from utils.monitoring import get_health_monitor, HealthStatus

        monitor = get_health_monitor()

        # Register additional checks if not already registered
        try:
            from utils.monitoring import check_api_football_health
            if "api_football" not in monitor.checks:
                monitor.register_check("api_football", check_api_football_health)
        except:
            pass

        # Run all checks
        results = monitor.run_all_checks()

        # Display results
        table = Table(title="System Health Report", show_header=True)
        table.add_column("Component", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Message")
        table.add_column("Response Time", justify="right")

        status_colors = {
            HealthStatus.HEALTHY: "green",
            HealthStatus.DEGRADED: "yellow",
            HealthStatus.UNHEALTHY: "red",
            HealthStatus.UNKNOWN: "dim"
        }

        for name, result in results.items():
            color = status_colors.get(result.status, "white")
            status_icon = {
                HealthStatus.HEALTHY: "[green]✓ HEALTHY[/green]",
                HealthStatus.DEGRADED: "[yellow]⚠ DEGRADED[/yellow]",
                HealthStatus.UNHEALTHY: "[red]✗ UNHEALTHY[/red]",
                HealthStatus.UNKNOWN: "[dim]? UNKNOWN[/dim]"
            }.get(result.status, "?")

            response_time = f"{result.response_time_ms:.0f}ms" if result.response_time_ms else "N/A"

            table.add_row(
                name,
                status_icon,
                result.message[:50] + "..." if len(result.message) > 50 else result.message,
                response_time
            )

        console.print(table)

        # Overall status
        overall = monitor.get_overall_status()
        overall_color = status_colors.get(overall, "white")
        console.print(f"\n[bold]Overall Status:[/bold] [{overall_color}]{overall.value.upper()}[/{overall_color}]")

    except ImportError as e:
        console.print(f"[yellow]Could not load monitoring module: {e}[/yellow]")
        console.print("Run 'pip install -r requirements.txt' to install dependencies")
    except Exception as e:
        console.print(f"[red]Health check failed: {e}[/red]")
        logger.error(f"Health check error: {e}", exc_info=True)


@system.command('stats')
def system_stats():
    """
    Show system statistics and data counts.

    Example:
        python cli.py system stats
    """
    console.print("\n[bold blue]System Statistics[/bold blue]\n")

    try:
        from database.connection import get_db
        db = get_db()

        # Get table counts
        tables = ['leagues', 'teams', 'players', 'matches', 'player_season_stats', 'team_season_stats']
        counts = {}

        for table in tables:
            try:
                result = db.execute_query(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result[0][0] if result else 0
            except:
                counts[table] = -1

        # Display counts
        table = Table(title="Database Statistics", show_header=True)
        table.add_column("Table", style="cyan")
        table.add_column("Records", justify="right", style="green")

        for name, count in counts.items():
            count_str = str(count) if count >= 0 else "[red]Error[/red]"
            table.add_row(name.replace('_', ' ').title(), count_str)

        console.print(table)

        # Get API usage if tracking table exists
        try:
            result = db.execute_query("""
                SELECT tracking_date, requests_count, 100 - requests_count as remaining
                FROM api_usage_tracking
                WHERE source_name = 'api_football'
                ORDER BY tracking_date DESC
                LIMIT 5
            """)

            if result:
                console.print("\n")
                usage_table = Table(title="Recent API Usage", show_header=True)
                usage_table.add_column("Date", style="cyan")
                usage_table.add_column("Used", justify="right")
                usage_table.add_column("Remaining", justify="right")

                for row in result:
                    remaining_color = "green" if row[2] > 30 else "yellow" if row[2] > 10 else "red"
                    usage_table.add_row(
                        str(row[0]),
                        str(row[1]),
                        f"[{remaining_color}]{row[2]}[/{remaining_color}]"
                    )

                console.print(usage_table)

        except Exception as e:
            console.print(f"\n[dim]API usage tracking not available: {e}[/dim]")

    except Exception as e:
        console.print(f"[red]Error getting stats: {e}[/red]")
        logger.error(f"Stats error: {e}", exc_info=True)


@system.command('logs')
@click.option('--lines', '-n', default=50, help='Number of lines to show')
@click.option('--level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO', help='Minimum log level')
def system_logs(lines, level):
    """
    Show recent log entries.

    Example:
        python cli.py system logs -n 20 --level ERROR
    """
    from pathlib import Path

    log_file = Path(__file__).parent / "logs" / "football_etl.log"

    if not log_file.exists():
        console.print("[yellow]No log file found. Logs will appear after running ETL jobs.[/yellow]")
        return

    console.print(f"\n[bold blue]Recent Logs (last {lines} lines, level >= {level})[/bold blue]\n")

    try:
        with open(log_file, 'r') as f:
            all_lines = f.readlines()

        # Filter by level
        level_order = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        min_level_idx = level_order.index(level)

        filtered = []
        for line in all_lines:
            for lvl in level_order[min_level_idx:]:
                if f"| {lvl}" in line or f"|{lvl}" in line:
                    filtered.append(line)
                    break

        # Show last N lines
        for line in filtered[-lines:]:
            # Color based on level
            if '| ERROR' in line or '|ERROR' in line:
                console.print(f"[red]{line.strip()}[/red]")
            elif '| WARNING' in line or '|WARNING' in line:
                console.print(f"[yellow]{line.strip()}[/yellow]")
            elif '| DEBUG' in line or '|DEBUG' in line:
                console.print(f"[dim]{line.strip()}[/dim]")
            else:
                console.print(line.strip())

    except Exception as e:
        console.print(f"[red]Error reading logs: {e}[/red]")


# ============================================
# API-FOOTBALL COMMANDS
# ============================================

@api_football.command('collect-league')
@click.option(
    '--league',
    required=True,
    type=click.Choice(['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1', 'eredivisie', 'brasileiro-serie-a', 'argentina-primera']),
    help='League to collect data for'
)
@click.option(
    '--season',
    default=2024,
    type=int,
    help='Season year (e.g., 2024 for 2024-25)'
)
@click.option(
    '--with-players/--no-players',
    default=False,
    help='Include player statistics (costs ~40 extra requests)'
)
@click.option(
    '--max-requests',
    default=90,
    type=int,
    help='Maximum API requests to use (default: 90)'
)
def collect_league(league, season, with_players, max_requests):
    """
    Collect all data for a league/season from API-Football.

    Example:
        python cli.py api-football collect-league --league premier-league --season 2024
        python cli.py api-football collect-league --league la-liga --with-players
    """
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        console.print("[bold red]ERROR: API_FOOTBALL_KEY environment variable not set[/bold red]")
        console.print("Set it with: export API_FOOTBALL_KEY='your_key_here'")
        raise click.Abort()

    console.print(Panel.fit(
        f"[bold blue]API-Football Data Collection[/bold blue]\n"
        f"League: {league.replace('-', ' ').title()}\n"
        f"Season: {season}-{str(season+1)[-2:]}\n"
        f"Include Players: {'Yes' if with_players else 'No'}\n"
        f"Max Requests: {max_requests}",
        border_style="blue"
    ))

    try:
        etl = APIFootballETL(api_key=api_key)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Collecting data from API-Football...", total=None)

            stats = etl.process_league_season(
                league_key=league,
                season=season,
                include_players=with_players
            )

            progress.update(task, completed=True)

        # Display results
        _display_api_football_stats(stats)

        console.print(f"\n[bold green]✓ Data collection complete![/bold green]")
        console.print(f"  API requests used: {stats.get('requests_used', 'N/A')}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"API-Football collection failed: {e}", exc_info=True)
        raise click.Abort()


@api_football.command('update-standings')
@click.option(
    '--league',
    required=True,
    type=click.Choice(['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1', 'eredivisie', 'brasileiro-serie-a', 'argentina-primera']),
    help='League to update standings for'
)
@click.option(
    '--season',
    default=2024,
    type=int,
    help='Season year (e.g., 2024 for 2024-25)'
)
def update_standings(league, season):
    """
    Quick standings update (low API cost - ~3 requests).

    Example:
        python cli.py api-football update-standings --league premier-league
    """
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        console.print("[bold red]ERROR: API_FOOTBALL_KEY environment variable not set[/bold red]")
        raise click.Abort()

    console.print(f"\n[bold blue]Updating {league.replace('-', ' ').title()} standings[/bold blue]\n")

    try:
        client = APIFootballClient(api_key=api_key)
        league_id = LEAGUE_IDS.get(league)

        if not league_id:
            console.print(f"[red]Unknown league: {league}[/red]")
            raise click.Abort()

        with console.status("[bold green]Fetching standings..."):
            standings = client.get_standings(league_id, season)

        if standings and len(standings) > 0:
            league_standings = standings[0].get('league', {}).get('standings', [[]])[0]

            table = Table(title=f"Standings - {league.replace('-', ' ').title()} {season}-{str(season+1)[-2:]}")
            table.add_column("Pos", style="cyan", justify="right")
            table.add_column("Team", style="white")
            table.add_column("P", justify="right")
            table.add_column("W", justify="right", style="green")
            table.add_column("D", justify="right", style="yellow")
            table.add_column("L", justify="right", style="red")
            table.add_column("GD", justify="right")
            table.add_column("Pts", justify="right", style="bold")

            for team in league_standings[:10]:  # Show top 10
                table.add_row(
                    str(team['rank']),
                    team['team']['name'],
                    str(team['all']['played']),
                    str(team['all']['win']),
                    str(team['all']['draw']),
                    str(team['all']['lose']),
                    str(team['goalsDiff']),
                    str(team['points'])
                )

            console.print(table)
            console.print(f"\n[dim]Showing top 10 of {len(league_standings)} teams[/dim]")
        else:
            console.print("[yellow]No standings data available[/yellow]")

        console.print("\n[bold green]✓ Standings updated![/bold green]")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        raise click.Abort()


@api_football.command('check-quota')
def check_quota():
    """
    Check remaining API quota for today.

    Example:
        python cli.py api-football check-quota
    """
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        console.print("[bold red]ERROR: API_FOOTBALL_KEY environment variable not set[/bold red]")
        raise click.Abort()

    console.print("\n[bold blue]Checking API-Football Quota...[/bold blue]\n")

    try:
        # Try to get quota from database tracking
        from database.connection import get_db
        db = get_db()

        result = db.execute_query(
            "SELECT requests_count FROM api_usage_tracking WHERE tracking_date = CURRENT_DATE AND source_name = 'api_football'"
        )

        if result:
            used = result[0][0]
            remaining = 100 - used

            # Color based on remaining
            if remaining <= 10:
                color = "red"
                status = "CRITICAL"
            elif remaining <= 30:
                color = "yellow"
                status = "LOW"
            else:
                color = "green"
                status = "GOOD"

            console.print(Panel.fit(
                f"[bold]API-Football Quota Status[/bold]\n\n"
                f"Requests Used Today: [cyan]{used}[/cyan]\n"
                f"Remaining: [{color}]{remaining}[/{color}]\n"
                f"Daily Limit: 100\n"
                f"Status: [{color}]{status}[/{color}]",
                border_style=color
            ))
        else:
            console.print("[green]No requests made today. Full quota available (100 requests).[/green]")

    except Exception as e:
        console.print(f"[yellow]Could not check database tracking: {e}[/yellow]")
        console.print("Note: Run schema_api_football.sql to enable quota tracking")


@api_football.command('run-daily')
@click.option(
    '--leagues',
    multiple=True,
    type=click.Choice(['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1', 'eredivisie', 'brasileiro-serie-a', 'argentina-primera']),
    default=['premier-league', 'la-liga', 'serie-a'],
    help='Leagues to collect (default: premier-league, la-liga, serie-a)'
)
@click.option(
    '--season',
    default=2024,
    type=int,
    help='Season year'
)
@click.option(
    '--max-requests',
    default=90,
    type=int,
    help='Maximum API requests to use'
)
def run_daily(leagues, season, max_requests):
    """
    Run the daily collection job manually.

    Example:
        python cli.py api-football run-daily
        python cli.py api-football run-daily --leagues premier-league --leagues la-liga
    """
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        console.print("[bold red]ERROR: API_FOOTBALL_KEY environment variable not set[/bold red]")
        raise click.Abort()

    leagues_list = list(leagues)

    console.print(Panel.fit(
        f"[bold blue]Daily Collection Job[/bold blue]\n"
        f"Leagues: {', '.join([l.replace('-', ' ').title() for l in leagues_list])}\n"
        f"Season: {season}-{str(season+1)[-2:]}\n"
        f"Max Requests: {max_requests}",
        border_style="blue"
    ))

    try:
        from scheduler.jobs import daily_collection_job

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running daily collection...", total=None)

            result = daily_collection_job(
                leagues=leagues_list,
                season=season,
                max_requests=max_requests
            )

            progress.update(task, completed=True)

        console.print(f"\n[bold green]✓ Daily collection complete![/bold green]")
        console.print(f"  Result: {result}")

    except ImportError:
        console.print("[yellow]Scheduler module not found. Running direct collection...[/yellow]")

        etl = APIFootballETL(api_key=api_key)
        total_stats = {'teams': 0, 'matches': 0, 'requests_used': 0}

        for league in leagues_list:
            console.print(f"\n[cyan]Processing {league}...[/cyan]")
            stats = etl.process_league_season(league, season, include_players=False)
            total_stats['teams'] += stats.get('teams', 0)
            total_stats['matches'] += stats.get('matches', 0)
            total_stats['requests_used'] += stats.get('requests_used', 0)

        console.print(f"\n[bold green]✓ Collection complete![/bold green]")
        console.print(f"  Teams: {total_stats['teams']}")
        console.print(f"  Matches: {total_stats['matches']}")
        console.print(f"  Requests used: {total_stats['requests_used']}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"Daily collection failed: {e}", exc_info=True)
        raise click.Abort()


@api_football.command('test-connection')
def test_api_connection():
    """
    Test API-Football connection and show account status.

    Example:
        python cli.py api-football test-connection
    """
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        console.print("[bold red]ERROR: API_FOOTBALL_KEY environment variable not set[/bold red]")
        console.print("Set it with: export API_FOOTBALL_KEY='your_key_here'")
        raise click.Abort()

    console.print("\n[bold blue]Testing API-Football Connection...[/bold blue]\n")

    try:
        client = APIFootballClient(api_key=api_key)

        with console.status("[bold green]Connecting..."):
            # Try to get Premier League info as a test
            leagues = client.get_leagues(country='England')

        if leagues:
            premier_league = next((l for l in leagues if l['league']['id'] == 39), None)

            if premier_league:
                console.print("[bold green]✓ Connection successful![/bold green]\n")

                table = Table(title="API-Football Status")
                table.add_column("Property", style="cyan")
                table.add_column("Value", style="green")

                table.add_row("API Key", f"{api_key[:8]}...{api_key[-4:]}")
                table.add_row("Test League", premier_league['league']['name'])
                table.add_row("Country", premier_league['country']['name'])
                table.add_row("Current Season", str(premier_league['seasons'][-1]['year']) if premier_league['seasons'] else 'N/A')

                console.print(table)
            else:
                console.print("[green]✓ Connection successful![/green]")
                console.print(f"  Retrieved {len(leagues)} leagues from England")
        else:
            console.print("[yellow]⚠ Connection succeeded but no data returned[/yellow]")

    except Exception as e:
        console.print(f"[bold red]✗ Connection failed: {e}[/bold red]")
        console.print("\nTroubleshooting:")
        console.print("  1. Check your API key is correct")
        console.print("  2. Ensure you've subscribed on RapidAPI")
        console.print("  3. Check if you've exceeded the daily limit")
        raise click.Abort()


@api_football.command('list-leagues')
def list_leagues():
    """
    List supported leagues and their API IDs.

    Example:
        python cli.py api-football list-leagues
    """
    console.print("\n[bold blue]Supported Leagues[/bold blue]\n")

    table = Table(title="API-Football League IDs")
    table.add_column("League Key", style="cyan")
    table.add_column("League Name", style="white")
    table.add_column("API ID", justify="right", style="green")

    league_names = {
        'premier-league': 'English Premier League',
        'la-liga': 'Spanish La Liga',
        'serie-a': 'Italian Serie A',
        'bundesliga': 'German Bundesliga',
        'ligue-1': 'French Ligue 1'
    }

    for key, api_id in LEAGUE_IDS.items():
        table.add_row(key, league_names.get(key, key), str(api_id))

    console.print(table)

    console.print("\n[dim]Use --league with any of these keys in commands[/dim]")


def _display_api_football_stats(stats: dict):
    """Display API-Football ETL statistics in a formatted table."""
    table = Table(title="API-Football Collection Statistics", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="green")

    table.add_row("Teams", str(stats.get('teams', 0)))
    table.add_row("Team Season Stats", str(stats.get('team_season_stats', 0)))
    table.add_row("Matches", str(stats.get('matches', 0)))
    table.add_row("Players", str(stats.get('players', 0)))
    table.add_row("Player Season Stats", str(stats.get('player_season_stats', 0)))
    table.add_row("API Requests Used", str(stats.get('requests_used', 0)))

    if stats.get('errors'):
        table.add_row("Errors", str(len(stats['errors'])), style="red")

    console.print(table)

    if stats.get('errors'):
        console.print("\n[bold red]Errors encountered:[/bold red]")
        for i, error in enumerate(stats['errors'][:5], 1):
            console.print(f"  {i}. {error}")
        if len(stats['errors']) > 5:
            console.print(f"  ... and {len(stats['errors']) - 5} more")


# ============================================
# FOTMOB COMMANDS
# ============================================

@fotmob.command('collect-league')
@click.option(
    '--league',
    required=True,
    type=click.Choice(FOTMOB_ALL_LEAGUES),
    help='League to collect data for'
)
@click.option(
    '--season',
    default=None,
    type=str,
    help='Season in DB format (e.g., 2024-25). Defaults to current.'
)
def fotmob_collect_league(league, season):
    """
    Collect league standings and team data from FotMob.

    Basic collection: standings + teams + match IDs.
    No API key needed.

    Example:
        python cli.py fotmob collect-league --league premier-league
        python cli.py fotmob collect-league --league la-liga --season 2023-24
    """
    console.print(Panel.fit(
        f"[bold blue]FotMob Data Collection[/bold blue]\n"
        f"League: {FOTMOB_LEAGUE_NAMES.get(league, league)}\n"
        f"Season: {season or 'Current'}\n"
        f"Mode: Basic (standings + teams)",
        border_style="blue"
    ))

    try:
        with FotMobETL() as etl:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task("Collecting league data from FotMob...", total=None)
                stats = etl.process_league_season(league, season)
                progress.update(task, completed=True)

            _display_fotmob_stats(stats, "League Collection")
            console.print(f"\n[bold green]✓ League collection complete![/bold green]")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"FotMob collection failed: {e}", exc_info=True)
        raise click.Abort()


@fotmob.command('collect-deep')
@click.option(
    '--league',
    required=True,
    type=click.Choice(FOTMOB_ALL_LEAGUES),
    help='League to collect data for'
)
@click.option(
    '--skip-players/--with-players',
    default=False,
    help='Skip individual player stats collection (faster, no xG/xA data)'
)
@click.option(
    '--season',
    default=None,
    type=str,
    help='Season in DB format (e.g., 2024-25). Defaults to current.'
)
@click.option(
    '--max-matches',
    default=None,
    type=int,
    help='Limit number of matches to process (for testing)'
)
@click.option(
    '--skip-squads/--with-squads',
    default=False,
    help='Skip squad collection (faster, standings + matches only)'
)
def fotmob_collect_deep(league, season, max_matches, skip_squads, skip_players):
    """
    Deep collection: standings + squads + player stats (xG/xA) + match details.

    This is the full collection pipeline. Takes longer due to
    individual API calls per team, match, and player.

    Example:
        python cli.py fotmob collect-deep --league premier-league
        python cli.py fotmob collect-deep --league serie-a --max-matches 10
        python cli.py fotmob collect-deep --league bundesliga --skip-squads
        python cli.py fotmob collect-deep --league la-liga --skip-players
    """
    league_name = FOTMOB_LEAGUE_NAMES.get(league, league)

    console.print(Panel.fit(
        f"[bold blue]FotMob Deep Collection[/bold blue]\n"
        f"League: {league_name}\n"
        f"Season: {season or 'Current'}\n"
        f"Squads: {'Skip' if skip_squads else 'Include'}\n"
        f"Player Stats (xG/xA): {'Skip' if skip_players else 'Include'}\n"
        f"Max Matches: {max_matches or 'All'}",
        border_style="blue"
    ))

    try:
        with FotMobETL() as etl:
            # Step 1: Basic league data (standings + teams)
            console.print(f"\n[cyan]Step 1/4: Collecting standings...[/cyan]")
            league_stats = etl.process_league_season(league, season)
            _display_fotmob_stats(league_stats, "Standings")

            # Step 2: Deep team data (squads)
            if not skip_squads:
                console.print(f"\n[cyan]Step 2/4: Collecting team squads...[/cyan]")
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task(f"Fetching squads for {league_name}...", total=None)
                    team_stats = etl.process_league_teams_deep(league)
                    progress.update(task, completed=True)
                _display_fotmob_stats(team_stats, "Squads")
            else:
                console.print(f"\n[dim]Step 2/4: Skipping squads[/dim]")

            # Step 3: Deep player stats (xG, xA, npxG, percentiles)
            if not skip_players:
                console.print(f"\n[cyan]Step 3/4: Collecting player stats (xG, xA, npxG)...[/cyan]")
                console.print(f"[dim]This fetches individual player pages - may take 10-15 minutes per league[/dim]")
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task(f"Fetching player stats for {league_name}...", total=None)
                    player_stats = etl.process_league_players_deep(league)
                    progress.update(task, completed=True)
                _display_fotmob_stats(player_stats, "Player Stats")
            else:
                console.print(f"\n[dim]Step 3/4: Skipping player stats (use --with-players to include)[/dim]")

            # Step 4: Deep match data
            console.print(f"\n[cyan]Step 4/4: Collecting match details...[/cyan]")
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(f"Fetching matches for {league_name}...", total=None)
                match_stats = etl.process_league_matches_deep(league, season, max_matches)
                progress.update(task, completed=True)
            _display_fotmob_stats(match_stats, "Matches")

            # Final statistics
            final_stats = etl.get_statistics()
            console.print(f"\n[bold green]✓ Deep collection complete for {league_name}![/bold green]")
            console.print(f"  Total API requests: {final_stats.get('api_requests', 0)}")
            console.print(f"  Errors: {final_stats.get('errors', 0)}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"FotMob deep collection failed: {e}", exc_info=True)
        raise click.Abort()


@fotmob.command('collect-player-stats')
@click.option(
    '--league',
    required=True,
    type=click.Choice(FOTMOB_ALL_LEAGUES),
    help='League to collect player stats for'
)
def fotmob_collect_player_stats(league):
    """
    Collect deep player stats (xG, xA, npxG, percentiles) for a league.

    This fetches individual /playerData endpoints for each player in the league
    and updates player_season_stats with advanced metrics.

    Prerequisites: Run 'collect-deep --skip-players' first to populate players.

    Example:
        python cli.py fotmob collect-player-stats --league premier-league
    """
    league_name = FOTMOB_LEAGUE_NAMES.get(league, league)

    console.print(Panel.fit(
        f"[bold blue]FotMob Player Stats Collection[/bold blue]\n"
        f"League: {league_name}\n"
        f"Fetching: xG, xA, npxG, progressive actions, percentiles",
        border_style="blue"
    ))

    console.print(f"\n[dim]This fetches individual player pages - may take 10-15 minutes[/dim]")

    try:
        with FotMobETL() as etl:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(f"Fetching player stats for {league_name}...", total=None)
                player_stats = etl.process_league_players_deep(league)
                progress.update(task, completed=True)

            _display_fotmob_stats(player_stats, "Player Stats")

            final_stats = etl.get_statistics()
            console.print(f"\n[bold green]✓ Player stats collection complete![/bold green]")
            console.print(f"  Players processed: {player_stats.get('players_processed', 0)}")
            console.print(f"  Season stats inserted: {player_stats.get('player_season_stats', 0)}")
            console.print(f"  API requests: {final_stats.get('api_requests', 0)}")
            console.print(f"  Errors: {final_stats.get('errors', 0)}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"FotMob player stats collection failed: {e}", exc_info=True)
        raise click.Abort()


@fotmob.command('collect-all')
@click.option(
    '--season',
    default=None,
    type=str,
    help='Season in DB format (e.g., 2024-25). Defaults to current.'
)
@click.option(
    '--basic/--deep',
    default=True,
    help='Basic (standings only) or deep (full) collection'
)
def fotmob_collect_all(season, basic):
    """
    Collect data for ALL 8 leagues.

    Use --basic for quick standings-only collection (~8 API calls).
    Use --deep for full collection (teams + matches + players).

    Example:
        python cli.py fotmob collect-all
        python cli.py fotmob collect-all --deep --season 2023-24
    """
    mode = "Basic" if basic else "Deep"
    console.print(Panel.fit(
        f"[bold blue]FotMob All-League Collection[/bold blue]\n"
        f"Leagues: {len(FOTMOB_ALL_LEAGUES)} leagues\n"
        f"Season: {season or 'Current'}\n"
        f"Mode: {mode}",
        border_style="blue"
    ))

    try:
        with FotMobETL() as etl:
            all_stats = {}

            for league_key in FOTMOB_ALL_LEAGUES:
                league_name = FOTMOB_LEAGUE_NAMES.get(league_key, league_key)
                console.print(f"\n[cyan]Processing {league_name}...[/cyan]")

                try:
                    stats = etl.process_league_season(league_key, season)
                    all_stats[league_key] = stats

                    if not basic:
                        team_stats = etl.process_league_teams_deep(league_key)
                        match_stats = etl.process_league_matches_deep(league_key, season)
                        all_stats[league_key].update({
                            'players': team_stats.get('players_inserted', 0) + team_stats.get('players_updated', 0),
                            'matches_processed': match_stats.get('matches_processed', 0),
                        })

                    console.print(f"  [green]✓ {league_name}: {stats.get('teams', 0)} teams, "
                                  f"{stats.get('team_season_stats', 0)} season stats[/green]")

                except Exception as e:
                    console.print(f"  [red]✗ {league_name}: {e}[/red]")
                    logger.error(f"Error collecting {league_name}: {e}")

            # Summary
            console.print(f"\n[bold green]✓ All-league collection complete![/bold green]")
            final = etl.get_statistics()
            console.print(f"  Total API requests: {final.get('api_requests', 0)}")
            console.print(f"  Total errors: {final.get('errors', 0)}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"FotMob all-league collection failed: {e}", exc_info=True)
        raise click.Abort()


@fotmob.command('list-leagues')
def fotmob_list_leagues():
    """
    List all supported FotMob leagues.

    Example:
        python cli.py fotmob list-leagues
    """
    console.print("\n[bold blue]FotMob Supported Leagues[/bold blue]\n")

    table = Table(title="FotMob Leagues", show_header=True)
    table.add_column("Key", style="cyan")
    table.add_column("League", style="white")
    table.add_column("FotMob ID", justify="right", style="green")

    for key in FOTMOB_ALL_LEAGUES:
        table.add_row(
            key,
            FOTMOB_LEAGUE_NAMES.get(key, key),
            str(FOTMOB_LEAGUE_IDS.get(key, '?')),
        )

    console.print(table)
    console.print(f"\n[dim]Use --league with any key above. No API key needed.[/dim]")


@fotmob.command('test-connection')
def fotmob_test_connection():
    """
    Test FotMob API connectivity.

    Fetches Premier League data as a quick smoke test.

    Example:
        python cli.py fotmob test-connection
    """
    console.print("\n[bold blue]Testing FotMob API Connection...[/bold blue]\n")

    try:
        from scrapers.fotmob.client import FotMobClient
        from scrapers.fotmob.data_parser import FotMobDataParser

        with FotMobClient() as client:
            with console.status("[bold green]Connecting to FotMob..."):
                data = client.get_league(47)  # Premier League

            if data:
                details = FotMobDataParser.parse_league_details(data)
                standings = FotMobDataParser.parse_league_standings(data)
                seasons = FotMobDataParser.parse_available_seasons(data)

                console.print("[bold green]✓ Connection successful![/bold green]\n")

                table = Table(title="FotMob API Status")
                table.add_column("Property", style="cyan")
                table.add_column("Value", style="green")

                if details:
                    table.add_row("League", details.get('name', 'N/A'))
                    table.add_row("Country", details.get('country', 'N/A'))
                    table.add_row("Current Season", str(details.get('selected_season', 'N/A')))
                table.add_row("Teams in Standings", str(len(standings)))
                table.add_row("Available Seasons", str(len(seasons)))
                table.add_row("API Key Required", "No")
                table.add_row("Rate Limit", "~2s between requests")

                console.print(table)

                if standings:
                    console.print(f"\n[dim]Top 5 teams:[/dim]")
                    for team in standings[:5]:
                        console.print(f"  {team['position']}. {team['team_name']} - {team['points']} pts")

            else:
                console.print("[yellow]⚠ Connected but no data returned[/yellow]")

    except Exception as e:
        console.print(f"[bold red]✗ Connection failed: {e}[/bold red]")
        console.print("\nTroubleshooting:")
        console.print("  1. Check your internet connection")
        console.print("  2. FotMob may be temporarily unavailable")
        console.print("  3. Check logs/football_etl.log for details")
        raise click.Abort()


def _display_fotmob_stats(stats: dict, title: str = "FotMob Collection"):
    """Display FotMob ETL statistics in a formatted table."""
    table = Table(title=f"{title} Statistics", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="green")

    for key, value in stats.items():
        display_key = key.replace('_', ' ').title()
        table.add_row(display_key, str(value))

    console.print(table)


@statsbomb.command('scrape-open-data')
@click.option('--competition-id', default=11, help='Competition ID (default 11 for La Liga)')
@click.option('--season-id', default=90, help='Season ID (default 90 for 2020/21)')
def scrape_open_data(competition_id, season_id):
    """
    Scrape StatsBomb Open Data (basic).
    Default is La Liga 2020/21 (Messi's last season).
    """
    console.print(f"\n[bold blue]Scraping StatsBomb Open Data (Comp: {competition_id}, Season: {season_id})[/bold blue]\n")

    try:
        etl = StatsBombETL()
        with console.status("[bold green]Processing matches..."):
            stats = etl.process_competition_matches(competition_id, season_id)

        console.print("[bold green]✓ Processing Complete![/bold green]")
        console.print(f"Matches: {stats['matches_processed']}")
        console.print(f"Teams: {stats['teams_processed']}")
        console.print(f"Players: {stats['players_processed']}")

        if stats['errors']:
            console.print(f"[red]Errors: {len(stats['errors'])}[/red]")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")


@statsbomb.command('collect-advanced')
@click.option('--competition-id', default=11, help='Competition ID (default 11 for La Liga)')
@click.option('--season-id', default=90, help='Season ID (default 90 for 2020/21)')
def statsbomb_collect_advanced(competition_id, season_id):
    """
    Collect advanced stats from StatsBomb Open Data.

    Extracts progressive actions, pressing stats, SCA/GCA that are
    not available from FotMob or API-Football.

    Example:
        python cli.py statsbomb collect-advanced --competition-id 11 --season-id 90
    """
    console.print(Panel.fit(
        f"[bold blue]StatsBomb Advanced Collection[/bold blue]\n"
        f"Competition ID: {competition_id}\n"
        f"Season ID: {season_id}\n"
        f"Stats: Progressive actions, pressures, SCA/GCA",
        border_style="blue"
    ))

    try:
        from etl.statsbomb_advanced_etl import StatsBombAdvancedETL

        with StatsBombAdvancedETL() as etl:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task("Processing StatsBomb event data...", total=None)
                stats = etl.process_competition(competition_id, season_id)
                progress.update(task, completed=True)

            # Display results
            table = Table(title="StatsBomb Advanced Collection", show_header=True)
            table.add_column("Metric", style="cyan")
            table.add_column("Count", justify="right", style="green")

            table.add_row("Matches Processed", str(stats.get('matches_processed', 0)))
            table.add_row("Players Processed", str(stats.get('players_processed', 0)))
            table.add_row("Teams Processed", str(stats.get('teams_processed', 0)))
            table.add_row("Events Analyzed", str(stats.get('events_analyzed', 0)))

            if stats.get('errors'):
                table.add_row("Errors", str(len(stats['errors'])), style="red")

            console.print(table)
            console.print(f"\n[bold green]✓ Advanced collection complete![/bold green]")

    except ImportError as e:
        console.print(f"[red]Could not import StatsBomb module: {e}[/red]")
        console.print("Make sure statsbombpy is installed: pip install statsbombpy")
    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"StatsBomb advanced collection failed: {e}", exc_info=True)
        raise click.Abort()


@statsbomb.command('list-competitions')
def statsbomb_list_competitions():
    """
    List available StatsBomb open data competitions.

    Example:
        python cli.py statsbomb list-competitions
    """
    console.print("\n[bold blue]StatsBomb Available Competitions[/bold blue]\n")

    try:
        from scrapers.statsbomb.client import StatsBombClient

        client = StatsBombClient()

        with console.status("[bold green]Fetching competitions..."):
            competitions = client.get_competitions()

        if not competitions:
            console.print("[yellow]No competitions available[/yellow]")
            return

        table = Table(title="StatsBomb Open Data Competitions", show_header=True)
        table.add_column("Competition ID", style="cyan", justify="right")
        table.add_column("Season ID", style="cyan", justify="right")
        table.add_column("Competition", style="white")
        table.add_column("Season", style="green")
        table.add_column("Country", style="yellow")

        # Group by competition
        seen = set()
        for comp in sorted(competitions, key=lambda x: (x.get('competition_name', ''), x.get('season_name', ''))):
            key = (comp.get('competition_id'), comp.get('season_id'))
            if key in seen:
                continue
            seen.add(key)

            table.add_row(
                str(comp.get('competition_id', '')),
                str(comp.get('season_id', '')),
                comp.get('competition_name', 'Unknown'),
                comp.get('season_name', 'Unknown'),
                comp.get('country_name', 'Unknown'),
            )

        console.print(table)
        console.print(f"\n[dim]Use --competition-id and --season-id with collect-advanced[/dim]")

    except Exception as e:
        console.print(f"[red]Error fetching competitions: {e}[/red]")


# ============================================
# UNDERSTAT COMMANDS
# ============================================

@understat.command('collect-league')
@click.option(
    '--league',
    required=True,
    type=click.Choice(['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']),
    help='League to collect xG data for'
)
@click.option(
    '--season',
    default=None,
    type=int,
    help='Season start year (e.g., 2024 for 2024/25). Defaults to current.'
)
def understat_collect_league(league, season):
    """
    Collect xG metrics from Understat for a league/season.

    Extracts xA, npxG, xGChain, xGBuildup - metrics not available
    from FotMob or API-Football.

    Example:
        python cli.py understat collect-league --league premier-league
        python cli.py understat collect-league --league la-liga --season 2023
    """
    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    console.print(Panel.fit(
        f"[bold blue]Understat xG Collection[/bold blue]\n"
        f"League: {league.replace('-', ' ').title()}\n"
        f"Season: {season}/{season+1}\n"
        f"Metrics: xA, npxG, xGChain, xGBuildup",
        border_style="blue"
    ))

    try:
        from etl.understat_etl import UnderstatETL

        with UnderstatETL() as etl:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(f"Scraping Understat for {league}...", total=None)
                etl.process_league_season(league, season)
                progress.update(task, completed=True)

            stats = etl.get_statistics()

            # Display results
            table = Table(title="Understat Collection", show_header=True)
            table.add_column("Metric", style="cyan")
            table.add_column("Count", justify="right", style="green")

            table.add_row("Players Enriched", str(stats.get('players_enriched', 0)))
            table.add_row("Teams Enriched", str(stats.get('teams_enriched', 0)))
            table.add_row("New Players", str(stats.get('new_players', 0)))
            table.add_row("New Teams", str(stats.get('new_teams', 0)))

            client_stats = stats.get('client_stats', {})
            table.add_row("HTTP Requests", str(client_stats.get('total_requests', 0)))

            if stats.get('errors'):
                table.add_row("Errors", str(len(stats['errors'])), style="red")

            console.print(table)
            console.print(f"\n[bold green]✓ Understat collection complete![/bold green]")

    except ImportError as e:
        console.print(f"[red]Could not import Understat module: {e}[/red]")
        console.print("Make sure beautifulsoup4 is installed: pip install beautifulsoup4")
    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        logger.error(f"Understat collection failed: {e}", exc_info=True)
        raise click.Abort()


@understat.command('collect-all')
@click.option(
    '--season',
    default=None,
    type=int,
    help='Season start year. Defaults to current.'
)
def understat_collect_all(season):
    """
    Collect xG data from Understat for all supported leagues.

    Supports: Premier League, La Liga, Serie A, Bundesliga, Ligue 1

    Example:
        python cli.py understat collect-all
        python cli.py understat collect-all --season 2023
    """
    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    leagues = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

    console.print(Panel.fit(
        f"[bold blue]Understat All-League Collection[/bold blue]\n"
        f"Leagues: {len(leagues)} leagues\n"
        f"Season: {season}/{season+1}\n"
        f"Metrics: xA, npxG, xGChain, xGBuildup",
        border_style="blue"
    ))

    try:
        from etl.understat_etl import UnderstatETL

        with UnderstatETL() as etl:
            for league in leagues:
                console.print(f"\n[cyan]Processing {league.replace('-', ' ').title()}...[/cyan]")

                try:
                    etl.process_league_season(league, season)
                    console.print(f"  [green]✓ Complete[/green]")
                except Exception as e:
                    console.print(f"  [red]✗ Error: {e}[/red]")

            stats = etl.get_statistics()
            console.print(f"\n[bold green]✓ All-league collection complete![/bold green]")
            console.print(f"  Players enriched: {stats.get('players_enriched', 0)}")
            console.print(f"  Teams enriched: {stats.get('teams_enriched', 0)}")

    except Exception as e:
        console.print(f"\n[bold red]✗ Error: {e}[/bold red]\n")
        raise click.Abort()


@understat.command('test-connection')
def understat_test_connection():
    """
    Test Understat connectivity.

    Example:
        python cli.py understat test-connection
    """
    console.print("\n[bold blue]Testing Understat Connection...[/bold blue]\n")

    try:
        from scrapers.understat.client import UnderstatClient

        client = UnderstatClient()

        with console.status("[bold green]Fetching EPL data..."):
            players = client.get_league_players('premier-league', 2024)

        if players:
            console.print("[bold green]✓ Connection successful![/bold green]\n")

            # Show top 5 players by xG
            sorted_players = sorted(players, key=lambda x: x.get('xg', 0), reverse=True)

            table = Table(title="Top 5 Players by xG (Premier League 2024/25)")
            table.add_column("Player", style="white")
            table.add_column("Team", style="cyan")
            table.add_column("Goals", justify="right")
            table.add_column("xG", justify="right", style="green")
            table.add_column("xA", justify="right", style="yellow")
            table.add_column("npxG", justify="right", style="blue")

            for player in sorted_players[:5]:
                table.add_row(
                    player.get('name', 'Unknown'),
                    player.get('team', 'Unknown'),
                    str(player.get('goals', 0)),
                    f"{player.get('xg', 0):.2f}",
                    f"{player.get('xa', 0):.2f}",
                    f"{player.get('npxg', 0):.2f}",
                )

            console.print(table)
            console.print(f"\n[dim]Total players found: {len(players)}[/dim]")

        else:
            console.print("[yellow]⚠ Connected but no data returned[/yellow]")

    except Exception as e:
        console.print(f"[bold red]✗ Connection failed: {e}[/bold red]")
        console.print("\nTroubleshooting:")
        console.print("  1. Check your internet connection")
        console.print("  2. Understat may be temporarily unavailable")
        console.print("  3. Make sure beautifulsoup4 is installed")
        raise click.Abort()


# ============================================
# SOFASCORE COMMANDS
# ============================================

@sofascore.command('collect-league')
@click.option('--league', required=True,
              type=click.Choice(['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']))
@click.option('--season', type=int, required=True,
              help='Season start year (e.g. 2024 for 2024-25)')
def sofascore_collect_league(league, season):
    """Collect SofaScore deep stats for one league/season."""
    from etl.sofascore_etl import SofaScoreETL
    console = Console()
    console.print(f"[cyan]Collecting SofaScore: {league} {season}...[/cyan]")
    with SofaScoreETL() as etl:
        result = etl.process_league_season(league, season)
    console.print(f"[green]Done:[/green] {result}")


@sofascore.command('collect-all')
@click.option('--season', type=int, default=None,
              help='Season start year (default: current)')
def sofascore_collect_all(season):
    """Collect SofaScore deep stats for all 5 leagues."""
    from scheduler.jobs import sofascore_collection_job
    console = Console()
    if season is None:
        from datetime import datetime
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
    console.print(f"[cyan]Collecting SofaScore all leagues season {season}...[/cyan]")
    result = sofascore_collection_job(season=season)
    console.print(f"[green]Done:[/green] {result}")


# ============================================
# COMBINED COLLECTION COMMANDS
# ============================================

@cli.command('full-refresh')
@click.option(
    '--season',
    default=None,
    type=int,
    help='Season year. Defaults to current.'
)
@click.option(
    '--skip-api-football/--with-api-football',
    default=False,
    help='Skip API-Football (useful if no API key)'
)
@click.option(
    '--skip-understat/--with-understat',
    default=False,
    help='Skip Understat scraping'
)
def full_refresh(season, skip_api_football, skip_understat):
    """
    Full data refresh from all sources.

    Collects data from:
    1. FotMob (primary) - unlimited
    2. API-Football (supplementary) - 100/day limit
    3. Understat (xG enrichment) - scraped

    Example:
        python cli.py full-refresh
        python cli.py full-refresh --season 2023 --skip-api-football
    """
    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    db_season = f"{season}-{str(season+1)[-2:]}"

    console.print(Panel.fit(
        f"[bold blue]Full Data Refresh[/bold blue]\n"
        f"Season: {db_season}\n"
        f"Sources: FotMob{'' if skip_api_football else ' + API-Football'}{'' if skip_understat else ' + Understat'}",
        border_style="blue"
    ))

    # FotMob (primary)
    console.print(f"\n[bold cyan]═══ FotMob Collection ═══[/bold cyan]")
    try:
        from scheduler.jobs import fotmob_daily_collection_job
        result = fotmob_daily_collection_job(season=db_season)
        console.print(f"[green]✓ FotMob: {len(result.get('leagues_processed', []))} leagues processed[/green]")
    except Exception as e:
        console.print(f"[red]✗ FotMob failed: {e}[/red]")

    # API-Football (supplementary)
    if not skip_api_football:
        console.print(f"\n[bold cyan]═══ API-Football Supplementary ═══[/bold cyan]")
        api_key = os.getenv('API_FOOTBALL_KEY')
        if api_key:
            try:
                from scheduler.jobs import api_football_supplementary_job
                result = api_football_supplementary_job(season=season)
                console.print(f"[green]✓ API-Football: {result.get('teams_enriched', 0)} teams enriched[/green]")
            except Exception as e:
                console.print(f"[red]✗ API-Football failed: {e}[/red]")
        else:
            console.print("[yellow]⚠ API_FOOTBALL_KEY not set - skipping[/yellow]")

    # Understat (xG enrichment)
    if not skip_understat:
        console.print(f"\n[bold cyan]═══ Understat xG Collection ═══[/bold cyan]")
        try:
            from scheduler.jobs import understat_collection_job
            result = understat_collection_job(season=season)
            console.print(f"[green]✓ Understat: {result.get('players_enriched', 0)} players enriched[/green]")
        except Exception as e:
            console.print(f"[red]✗ Understat failed: {e}[/red]")

    console.print(f"\n[bold green]✓ Full refresh complete![/bold green]")


def _display_stats(stats: dict):
    """Display ETL statistics in a formatted table."""
    table = Table(title="ETL Statistics", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="green")
    
    table.add_row("Teams Inserted", str(stats['teams_inserted']))
    table.add_row("Teams Updated", str(stats['teams_updated']))
    table.add_row("Players Inserted", str(stats['players_inserted']))
    table.add_row("Players Updated", str(stats['players_updated']))
    table.add_row("Matches Inserted", str(stats['matches_inserted']))
    table.add_row("Matches Updated", str(stats['matches_updated']))
    table.add_row("Errors", str(len(stats['errors'])), style="red" if stats['errors'] else "green")
    
    console.print(table)
    
    if stats['errors']:
        console.print("\n[bold red]Errors encountered:[/bold red]")
        for i, error in enumerate(stats['errors'][:5], 1):
            console.print(f"  {i}. {error}")
        
        if len(stats['errors']) > 5:
            console.print(f"  ... and {len(stats['errors']) - 5} more")


def _display_match_stats(stats: dict):
    """Display match statistics."""
    table = Table(title="Match Statistics", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Home", justify="right", style="green")
    table.add_column("Away", justify="right", style="yellow")
    
    metrics = [
        ('Possession %', 'possession', 'possession'),
        ('Shots', 'shots', 'shots'),
        ('Shots on Target', 'shotsOnTarget', 'shotsOnTarget'),
        ('xG', 'xg', 'xg'),
        ('Passes', 'passes', 'passes'),
    ]
    
    for label, home_key, away_key in metrics:
        home_val = stats.get(f'home{home_key.capitalize()}', 'N/A')
        away_val = stats.get(f'away{away_key.capitalize()}', 'N/A')
        table.add_row(label, str(home_val), str(away_val))
    
    console.print(table)


# ============================================
# SOCCERDATA COMMANDS
# ============================================

@soccerdata.command('check-availability')
def soccerdata_check_availability():
    """
    Check if SoccerData library is available and properly configured.

    Verifies Python version, pandas version, and soccerdata installation.

    Example:
        python cli.py soccerdata check-availability
    """
    console.print("\n[bold blue]Checking SoccerData Availability...[/bold blue]\n")

    try:
        from etl.sources.soccerdata_adapter import check_soccerdata_availability

        is_available, details = check_soccerdata_availability()

        # Create status table
        table = Table(title="SoccerData Environment Status", show_header=True)
        table.add_column("Component", style="cyan")
        table.add_column("Version", style="white")
        table.add_column("Status", justify="center")

        # Python version
        py_status = "[green]✓[/green]" if details['python_ok'] else "[red]✗[/red]"
        py_req = "≥3.10" if not details['python_ok'] else ""
        table.add_row("Python", f"{details['python_version']} {py_req}", py_status)

        # Pandas version
        pd_status = "[green]✓[/green]" if details['pandas_ok'] else "[red]✗[/red]"
        pd_req = "≥2.3.0" if not details['pandas_ok'] else ""
        table.add_row("pandas", f"{details['pandas_version']} {pd_req}", pd_status)

        # SoccerData
        sd_status = "[green]✓[/green]" if details['soccerdata_available'] else "[red]✗[/red]"
        sd_ver = "1.8.8+" if details['soccerdata_available'] else "not installed"
        table.add_row("soccerdata", sd_ver, sd_status)

        console.print(table)

        if is_available:
            console.print("\n[bold green]✓ SoccerData is ready for use![/bold green]")
        else:
            console.print("\n[bold yellow]⚠ SoccerData not fully available[/bold yellow]")
            if 'upgrade_instructions' in details:
                console.print(Panel(
                    details['upgrade_instructions'],
                    title="Upgrade Instructions",
                    border_style="yellow"
                ))

    except ImportError as e:
        console.print(f"[red]✗ Could not import SoccerData adapter: {e}[/red]")
        console.print("\n[yellow]Make sure etl/sources/soccerdata_adapter.py exists[/yellow]")


@soccerdata.command('test-connection')
@click.option(
    '--source',
    type=click.Choice(['fotmob', 'fbref', 'understat', 'clubelo']),
    default='clubelo',
    help='Data source to test (default: clubelo - most reliable)'
)
def soccerdata_test_connection(source):
    """
    Test SoccerData connection by fetching sample data.

    Example:
        python cli.py soccerdata test-connection --source clubelo
    """
    console.print(f"\n[bold blue]Testing SoccerData connection ({source})...[/bold blue]\n")

    try:
        from etl.sources.soccerdata_adapter import SoccerDataAdapter

        adapter = SoccerDataAdapter()
        is_available, issues = adapter.is_available()

        if not is_available:
            console.print("[red]✗ SoccerData not available[/red]")
            for issue in issues:
                console.print(f"  - {issue}")
            return

        if source == 'clubelo':
            console.print("[cyan]Fetching team Elo ratings...[/cyan]")
            df = adapter.get_team_elo_ratings()
            if df is not None and not df.empty:
                console.print(f"[green]✓ Fetched {len(df)} team ratings[/green]")
                console.print(f"Columns: {list(df.columns)[:8]}")
            else:
                console.print("[yellow]⚠ No data returned (may be network issue)[/yellow]")

        stats = adapter.get_statistics()
        console.print(f"\n[dim]Records fetched: {stats['records_fetched']}[/dim]")
        if stats['errors']:
            console.print(f"[yellow]Errors: {len(stats['errors'])}[/yellow]")

        adapter.close()

    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/red]")


@soccerdata.command('list-sources')
def soccerdata_list_sources():
    """
    List available data sources in SoccerData.

    Example:
        python cli.py soccerdata list-sources
    """
    console.print("\n[bold blue]SoccerData Sources[/bold blue]\n")

    try:
        from etl.sources.soccerdata_config import SOCCERDATA_CONFIG

        table = Table(title="Available Sources", show_header=True)
        table.add_column("Source", style="cyan")
        table.add_column("Priority", justify="center")
        table.add_column("Rate Limit", justify="right")
        table.add_column("Status", justify="center")
        table.add_column("Key Methods")

        for source, config in sorted(SOCCERDATA_CONFIG.items(), key=lambda x: x[1].get('priority', 99)):
            status = "[green]enabled[/green]" if config.get('enabled') else "[dim]disabled[/dim]"
            methods = ', '.join(config.get('methods', [])[:3])
            if len(config.get('methods', [])) > 3:
                methods += '...'

            table.add_row(
                source,
                str(config.get('priority', '-')),
                f"{config.get('rate_limit', 2.0)}s",
                status,
                methods
            )

        console.print(table)

        console.print("\n[dim]Use --source <name> with other commands to select a source[/dim]")

    except ImportError as e:
        console.print(f"[red]✗ Could not load config: {e}[/red]")


@soccerdata.command('list-leagues')
def soccerdata_list_leagues():
    """
    List league mappings for SoccerData.

    Example:
        python cli.py soccerdata list-leagues
    """
    console.print("\n[bold blue]SoccerData League Mappings[/bold blue]\n")

    try:
        from etl.sources.soccerdata_config import LEAGUE_MAPPINGS

        table = Table(title="League Mappings", show_header=True)
        table.add_column("Internal Key", style="cyan")
        table.add_column("SoccerData ID", style="white")
        table.add_column("Country", justify="center")

        for key, (sd_league, country) in sorted(LEAGUE_MAPPINGS.items()):
            table.add_row(key, sd_league, country)

        console.print(table)

    except ImportError as e:
        console.print(f"[red]✗ Could not load config: {e}[/red]")


if __name__ == "__main__":
    cli()
