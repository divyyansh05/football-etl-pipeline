"""
Football Data ETL Scheduler
Schedule (UTC):
  Daily   05:00  - FotMob daily (all 8 leagues)
  Daily   06:00  - API-Football (top 3 leagues)
  Monday  08:00  - Understat xG refresh (after weekend matchdays)
  Thursday 08:00 - Understat xG refresh (after midweek matchdays)
  Sunday  02:00  - FotMob weekly deep
  Sunday  04:00  - Understat full refresh (all 5 leagues)
  Daily   12:00  - Priority standings
  Daily   18:00  - Season refresh
"""
import os
import sys
import signal
import logging
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from scheduler.job_scheduler import JobScheduler, get_scheduler
from scheduler.jobs import (
    fotmob_daily_collection_job,
    fotmob_weekly_deep_job,
    daily_collection_job,
    priority_collection_job,
    update_current_season_job,
    understat_collection_job,
    sofascore_collection_job,
    get_current_api_football_season,
)
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/scheduler.log')
    ]
)
logger = logging.getLogger(__name__)
console = Console()


def had_recent_matches(days_back=2):
    try:
        from database.connection import DatabaseConnection
        db = DatabaseManager()
        query = """
            SELECT COUNT(*) as cnt FROM matches m
            JOIN leagues l ON m.league_id = l.league_id
            WHERE l.league_name IN (
                'Premier League','La Liga','Serie A','Bundesliga','Ligue 1'
            )
            AND m.match_date >= CURRENT_DATE - INTERVAL ':days days'
            AND m.status = 'FT'
        """
        result = db.execute_query(query, {'days': days_back}, fetch=True)
        return (result[0]['cnt'] if result else 0) > 0
    except Exception as e:
        logger.warning(f"Match check failed: {e} — defaulting to run")
        return True


def understat_refresh_job(full=False):
    if not had_recent_matches(days_back=3):
        logger.info("No recent matches — skipping Understat refresh")
        return {'skipped': True}
    season = get_current_api_football_season()
    logger.info(f"Understat refresh (season={season}, full={full})")
    try:
        result = understat_collection_job(season=season)
        logger.info(f"Understat refresh done: {result}")
        return result
    except Exception as e:
        logger.error(f"Understat refresh failed: {e}")
        return {'error': str(e)}


def setup_jobs(scheduler, hour=6, minute=0):
    scheduler.add_daily_job(
        job_func=fotmob_daily_collection_job,
        job_id='fotmob_daily',
        hour=max(hour - 1, 0),
        minute=minute,
        kwargs={'leagues': None, 'season': None}
    )
    console.print(f"[green]✓[/green] fotmob_daily → {max(hour-1,0):02d}:{minute:02d} UTC daily")

    scheduler.add_weekly_job(
        job_func=fotmob_weekly_deep_job,
        job_id='fotmob_weekly_deep',
        day_of_week='sun', hour=2, minute=0,
        kwargs={'season': None}
    )
    console.print("[green]✓[/green] fotmob_weekly_deep → Sunday 02:00 UTC")

    scheduler.add_daily_job(
        job_func=daily_collection_job,
        job_id='api_football_daily',
        hour=hour, minute=minute,
        kwargs={
            'leagues': ['premier-league', 'la-liga', 'serie-a'],
            'season': get_current_api_football_season(),
            'max_requests': 80
        }
    )
    console.print(f"[green]✓[/green] api_football_daily → {hour:02d}:{minute:02d} UTC daily")

    scheduler.add_weekly_job(
        job_func=understat_refresh_job,
        job_id='understat_monday',
        day_of_week='mon', hour=hour + 2, minute=0,
        kwargs={'full': False}
    )
    console.print(f"[green]✓[/green] understat_monday → Monday {hour+2:02d}:00 UTC")

    scheduler.add_weekly_job(
        job_func=understat_refresh_job,
        job_id='understat_thursday',
        day_of_week='thu', hour=hour + 2, minute=0,
        kwargs={'full': False}
    )
    console.print(f"[green]✓[/green] understat_thursday → Thursday {hour+2:02d}:00 UTC")

    scheduler.add_weekly_job(
        job_func=understat_refresh_job,
        job_id='understat_weekly_full',
        day_of_week='sun', hour=4, minute=0,
        kwargs={'full': True}
    )
    console.print("[green]✓[/green] understat_weekly_full → Sunday 04:00 UTC")

    # SofaScore Tuesday refresh (after weekend + Monday night matches)
    scheduler.add_weekly_job(
        job_func=sofascore_collection_job,
        job_id='sofascore_tuesday',
        day_of_week='tue',
        hour=hour + 2,
        minute=0,
        kwargs={'leagues': None, 'season': None}
    )
    console.print(f"[green]✓[/green] sofascore_tuesday → Tuesday {hour+2:02d}:00 UTC")

    # SofaScore Sunday full refresh
    scheduler.add_weekly_job(
        job_func=sofascore_collection_job,
        job_id='sofascore_weekly_full',
        day_of_week='sun',
        hour=6,
        minute=0,
        kwargs={'leagues': None, 'season': None}
    )
    console.print("[green]✓[/green] sofascore_weekly_full → Sunday 06:00 UTC")

    scheduler.add_daily_job(
        job_func=priority_collection_job,
        job_id='priority_standings',
        hour=12, minute=0,
        kwargs={'priority_leagues': ['premier-league'], 'collect_players': False}
    )
    console.print("[green]✓[/green] priority_standings → 12:00 UTC daily")

    scheduler.add_daily_job(
        job_func=update_current_season_job,
        job_id='current_season_update',
        hour=18, minute=0
    )
    console.print("[green]✓[/green] current_season_update → 18:00 UTC daily")


def display_jobs(scheduler):
    table = Table(title="Scheduled Jobs", show_header=True)
    table.add_column("Job ID", style="cyan")
    table.add_column("Next Run", style="green")
    table.add_column("Trigger", style="yellow")
    for job in scheduler.get_jobs():
        next_run = job['next_run_time'].strftime('%Y-%m-%d %H:%M') if job['next_run_time'] else 'N/A'
        table.add_row(job['id'], next_run, job['trigger'])
    console.print(table)


def signal_handler(signum, frame):
    console.print("\n[yellow]Shutting down...[/yellow]")
    get_scheduler().shutdown(wait=True)
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description='Football Data ETL Scheduler')
    parser.add_argument('--hour', type=int, default=6)
    parser.add_argument('--minute', type=int, default=0)
    parser.add_argument('--run-now', action='store_true')
    parser.add_argument('--list-jobs', action='store_true')
    parser.add_argument('--daemon', action='store_true')
    parser.add_argument('--timezone', default='UTC')
    args = parser.parse_args()

    if not os.getenv('API_FOOTBALL_KEY'):
        console.print("[yellow]WARNING: API_FOOTBALL_KEY not set — API-Football jobs skipped[/yellow]")

    console.print(Panel.fit(
        "[bold blue]Football Data ETL Scheduler[/bold blue]\n"
        f"Timezone: {args.timezone} | Base hour: {args.hour:02d}:00 UTC",
        border_style="blue"
    ))

    scheduler = get_scheduler(blocking=not args.daemon)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_jobs(scheduler, hour=args.hour, minute=args.minute)
    display_jobs(scheduler)

    if args.list_jobs:
        return

    if args.run_now:
        console.print("\n[cyan]Running immediate FotMob collection...[/cyan]")
        try:
            fotmob_daily_collection_job(leagues=['premier-league'], season=None)
            console.print("[green]✓[/green] Done")
        except Exception as e:
            console.print(f"[red]✗[/red] {e}")

    console.print("[bold green]Scheduler running. Ctrl+C to stop.[/bold green]\n")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == '__main__':
    main()
