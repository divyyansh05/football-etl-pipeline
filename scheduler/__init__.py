"""
Scheduler module for automated data collection jobs.

Active sources: SofaScore, Understat, ClubElo.
Dead sources removed: FotMob, API-Football, StatsBomb.
"""

from .job_scheduler import JobScheduler, get_scheduler
from .jobs import (
    sofascore_weekly_job,
    understat_weekly_job,
    clubelo_weekly_job,
    catchup_weekly_job,
)

__all__ = [
    'JobScheduler',
    'get_scheduler',
    'sofascore_weekly_job',
    'understat_weekly_job',
    'clubelo_weekly_job',
    'catchup_weekly_job',
]
