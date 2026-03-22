"""
Scheduler module for automated data collection jobs.

This module provides APScheduler-based job scheduling for the ETL pipeline,
enabling automatic daily data collection within API rate limits.
"""

from .job_scheduler import JobScheduler, get_scheduler
from .jobs import (
    daily_collection_job,
    priority_collection_job,
    update_current_season_job,
)

__all__ = [
    'JobScheduler',
    'get_scheduler',
    'daily_collection_job',
    'priority_collection_job',
    'update_current_season_job',
]
