#!/usr/bin/env python3
"""
Football ETL Scheduler — entry point.

Weekly cadence (UTC):
  Monday    05:00  — SofaScore refresh      (canonical creator)
  Tuesday   05:00  — Understat xG           (enrichment)
  Wednesday 05:00  — ClubElo ELO            (independent)
  Thursday  05:00  — Catch-up run           (fix gaps from Mon/Tue)
  Friday    05:00  — FotMob squads          (fotmob_id + injury coverage)
  Saturday  06:00  — Analytics scores       (per-90 + performance scoring)

Usage:
    python run_scheduler.py              # start blocking scheduler
    python run_scheduler.py --list-jobs  # print schedule and exit
    python run_scheduler.py --run-now sofascore   # run one job immediately
    python run_scheduler.py --run-now understat
    python run_scheduler.py --run-now clubelo
    python run_scheduler.py --run-now catchup
    python run_scheduler.py --run-now fotmob
    python run_scheduler.py --run-now scores
"""
import argparse
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/scheduler.log"),
    ],
)
logger = logging.getLogger(__name__)
console = Console()

from scheduler.job_scheduler import get_scheduler
from scheduler.jobs import (
    sofascore_weekly_job,
    understat_weekly_job,
    clubelo_weekly_job,
    catchup_weekly_job,
    fotmob_squads_weekly_job,
    compute_scores_job,
)

JOB_MAP = {
    "sofascore": sofascore_weekly_job,
    "understat":  understat_weekly_job,
    "clubelo":    clubelo_weekly_job,
    "catchup":    catchup_weekly_job,
    "fotmob":     fotmob_squads_weekly_job,
    "scores":     compute_scores_job,
}


def setup_jobs(scheduler) -> None:
    # Monday 05:00 — SofaScore
    scheduler.add_weekly_job(
        job_func=sofascore_weekly_job,
        job_id="sofascore_weekly",
        day_of_week="mon", hour=5, minute=0,
    )
    console.print("[green]✓[/green] sofascore_weekly → Monday 05:00 UTC")

    # Tuesday 05:00 — Understat
    scheduler.add_weekly_job(
        job_func=understat_weekly_job,
        job_id="understat_weekly",
        day_of_week="tue", hour=5, minute=0,
    )
    console.print("[green]✓[/green] understat_weekly → Tuesday 05:00 UTC")

    # Wednesday 05:00 — ClubElo
    scheduler.add_weekly_job(
        job_func=clubelo_weekly_job,
        job_id="clubelo_weekly",
        day_of_week="wed", hour=5, minute=0,
    )
    console.print("[green]✓[/green] clubelo_weekly → Wednesday 05:00 UTC")

    # Thursday 05:00 — Catch-up
    scheduler.add_weekly_job(
        job_func=catchup_weekly_job,
        job_id="catchup_weekly",
        day_of_week="thu", hour=5, minute=0,
    )
    console.print("[green]✓[/green] catchup_weekly → Thursday 05:00 UTC")

    # Friday 05:00 — FotMob squads
    scheduler.add_weekly_job(
        job_func=fotmob_squads_weekly_job,
        job_id="fotmob_squads_weekly",
        day_of_week="fri", hour=5, minute=0,
    )
    console.print("[green]✓[/green] fotmob_squads_weekly → Friday 05:00 UTC")

    # Saturday 06:00 — Analytics scores
    scheduler.add_weekly_job(
        job_func=compute_scores_job,
        job_id="compute_scores_weekly",
        day_of_week="sat", hour=6, minute=0,
    )
    console.print("[green]✓[/green] compute_scores_weekly → Saturday 06:00 UTC")


def display_jobs(scheduler) -> None:
    table = Table(title="Scheduled Jobs", show_lines=True)
    table.add_column("Job ID", style="cyan")
    table.add_column("Next Run UTC", style="green")
    table.add_column("Trigger", style="yellow")
    for job in scheduler.get_jobs():
        next_run = (
            job["next_run_time"].strftime("%Y-%m-%d %H:%M")
            if job.get("next_run_time") else "N/A"
        )
        table.add_row(job["id"], next_run, str(job.get("trigger", "")))
    console.print(table)


def signal_handler(signum, frame):
    console.print("\n[yellow]Signal received — shutting down scheduler…[/yellow]")
    try:
        get_scheduler().shutdown(wait=True)
    except Exception:
        pass
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Football ETL Scheduler")
    parser.add_argument(
        "--list-jobs", action="store_true",
        help="Print scheduled jobs and exit"
    )
    parser.add_argument(
        "--run-now", metavar="JOB",
        choices=list(JOB_MAP.keys()),
        help=f"Run a job immediately. Options: {list(JOB_MAP.keys())}"
    )
    args = parser.parse_args()

    console.print(Panel.fit(
        "[bold blue]Football ETL Scheduler[/bold blue]\n"
        "Sources: SofaScore · Understat · ClubElo · FotMob\n"
        "Cadence: Mon SS / Tue US / Wed ELO / Thu Catchup / Fri FotMob / Sat Scores",
        border_style="blue",
    ))

    scheduler = get_scheduler(blocking=True)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_jobs(scheduler)

    if args.list_jobs:
        display_jobs(scheduler)
        return

    if args.run_now:
        job_fn = JOB_MAP[args.run_now]
        console.print(f"\n[cyan]Running {args.run_now} immediately…[/cyan]")
        try:
            result = job_fn()
            console.print(f"[green]✓[/green] Done: {result}")
        except Exception as exc:
            console.print(f"[red]✗[/red] {exc}")
            logger.exception(f"--run-now {args.run_now} failed")
        return

    display_jobs(scheduler)
    console.print("[bold green]Scheduler running. Ctrl+C to stop.[/bold green]\n")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=True)


if __name__ == "__main__":
    main()
