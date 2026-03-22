"""
APScheduler-based Job Scheduler for Football Data ETL Pipeline.

Provides automated scheduling for data collection jobs with:
- Daily collection jobs at configurable times
- Request quota management (100 requests/day)
- Job persistence across restarts
- Graceful shutdown handling
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, List
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Manages scheduled ETL jobs with APScheduler.

    Features:
    - Persistent job storage in SQLite
    - Configurable daily run times
    - Request quota awareness
    - Job execution logging
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        """Singleton pattern for scheduler."""
        if not cls._instance:
            cls._instance = super(JobScheduler, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        db_path: Optional[str] = None,
        blocking: bool = False,
        timezone: str = 'UTC'
    ):
        """
        Initialize the job scheduler.

        Args:
            db_path: Path to SQLite database for job persistence
            blocking: If True, use BlockingScheduler (for standalone mode)
            timezone: Timezone for job scheduling
        """
        if hasattr(self, '_initialized') and self._initialized:
            return

        self._initialized = True
        self.timezone = timezone

        # Setup job store path
        if db_path is None:
            data_dir = Path(__file__).parent.parent / 'data'
            data_dir.mkdir(exist_ok=True)
            db_path = str(data_dir / 'scheduler_jobs.db')

        # Configure job stores
        jobstores = {
            'default': SQLAlchemyJobStore(url=f'sqlite:///{db_path}')
        }

        # Configure executors
        executors = {
            'default': ThreadPoolExecutor(max_workers=3)
        }

        # Job defaults
        job_defaults = {
            'coalesce': True,  # Combine missed executions into one
            'max_instances': 1,  # Only one instance of each job
            'misfire_grace_time': 3600  # 1 hour grace for missed jobs
        }

        # Create scheduler
        SchedulerClass = BlockingScheduler if blocking else BackgroundScheduler
        self.scheduler = SchedulerClass(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=timezone
        )

        # Add event listeners
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED
        )
        self.scheduler.add_listener(
            self._job_error_listener,
            EVENT_JOB_ERROR
        )
        self.scheduler.add_listener(
            self._job_missed_listener,
            EVENT_JOB_MISSED
        )

        logger.info(f"JobScheduler initialized with db: {db_path}")

    def _job_executed_listener(self, event):
        """Log successful job execution."""
        logger.info(f"Job {event.job_id} executed successfully at {datetime.now()}")

    def _job_error_listener(self, event):
        """Log job execution errors."""
        logger.error(
            f"Job {event.job_id} failed with exception: {event.exception}",
            exc_info=True
        )

    def _job_missed_listener(self, event):
        """Log missed job executions."""
        logger.warning(f"Job {event.job_id} missed its scheduled run time")

    def add_daily_job(
        self,
        job_func: Callable,
        job_id: str,
        hour: int = 6,
        minute: int = 0,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Add a job that runs daily at specified time.

        Args:
            job_func: Function to execute
            job_id: Unique identifier for the job
            hour: Hour to run (0-23, default 6 AM)
            minute: Minute to run (0-59)
            args: Positional arguments for job function
            kwargs: Keyword arguments for job function
            replace_existing: Replace job if it exists

        Returns:
            Job ID
        """
        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            timezone=self.timezone
        )

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            args=args or [],
            kwargs=kwargs or {},
            replace_existing=replace_existing
        )

        logger.info(f"Added daily job '{job_id}' scheduled for {hour:02d}:{minute:02d} {self.timezone}")
        return job.id

    def add_weekly_job(
        self,
        job_func: Callable,
        job_id: str,
        day_of_week: str = 'sun',
        hour: int = 2,
        minute: int = 0,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Add a job that runs weekly on a specified day.

        Args:
            job_func: Function to execute
            job_id: Unique identifier for the job
            day_of_week: Day to run ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')
            hour: Hour to run (0-23, default 2 AM)
            minute: Minute to run (0-59)
            args: Positional arguments for job function
            kwargs: Keyword arguments for job function
            replace_existing: Replace job if it exists

        Returns:
            Job ID
        """
        trigger = CronTrigger(
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            timezone=self.timezone
        )

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            args=args or [],
            kwargs=kwargs or {},
            replace_existing=replace_existing
        )

        day_names = {'mon': 'Monday', 'tue': 'Tuesday', 'wed': 'Wednesday',
                     'thu': 'Thursday', 'fri': 'Friday', 'sat': 'Saturday', 'sun': 'Sunday'}
        day_name = day_names.get(day_of_week.lower(), day_of_week)
        logger.info(f"Added weekly job '{job_id}' scheduled for {day_name} at {hour:02d}:{minute:02d} {self.timezone}")
        return job.id

    def add_interval_job(
        self,
        job_func: Callable,
        job_id: str,
        hours: int = 24,
        minutes: int = 0,
        start_date: Optional[datetime] = None,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Add a job that runs at fixed intervals.

        Args:
            job_func: Function to execute
            job_id: Unique identifier for the job
            hours: Hours between runs
            minutes: Additional minutes between runs
            start_date: When to start (default: now + interval)
            args: Positional arguments for job function
            kwargs: Keyword arguments for job function
            replace_existing: Replace job if it exists

        Returns:
            Job ID
        """
        if start_date is None:
            start_date = datetime.now() + timedelta(hours=hours, minutes=minutes)

        trigger = IntervalTrigger(
            hours=hours,
            minutes=minutes,
            start_date=start_date,
            timezone=self.timezone
        )

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            args=args or [],
            kwargs=kwargs or {},
            replace_existing=replace_existing
        )

        logger.info(f"Added interval job '{job_id}' running every {hours}h {minutes}m")
        return job.id

    def add_one_time_job(
        self,
        job_func: Callable,
        job_id: str,
        run_date: datetime,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Add a job that runs once at a specific time.

        Args:
            job_func: Function to execute
            job_id: Unique identifier for the job
            run_date: When to run the job
            args: Positional arguments for job function
            kwargs: Keyword arguments for job function
            replace_existing: Replace job if it exists

        Returns:
            Job ID
        """
        job = self.scheduler.add_job(
            job_func,
            trigger='date',
            run_date=run_date,
            id=job_id,
            args=args or [],
            kwargs=kwargs or {},
            replace_existing=replace_existing
        )

        logger.info(f"Added one-time job '{job_id}' scheduled for {run_date}")
        return job.id

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a scheduled job.

        Args:
            job_id: Job identifier to remove

        Returns:
            True if job was removed, False if not found
        """
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed job '{job_id}'")
            return True
        except Exception as e:
            logger.warning(f"Could not remove job '{job_id}': {e}")
            return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job."""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused job '{job_id}'")
            return True
        except Exception as e:
            logger.warning(f"Could not pause job '{job_id}': {e}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed job '{job_id}'")
            return True
        except Exception as e:
            logger.warning(f"Could not resume job '{job_id}': {e}")
            return False

    def get_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all scheduled jobs with their details.

        Returns:
            List of job information dictionaries
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': getattr(job, 'next_run_time', None),
                'trigger': str(job.trigger),
                'pending': job.pending
            })
        return jobs

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get details for a specific job."""
        job = self.scheduler.get_job(job_id)
        if job:
            return {
                'id': job.id,
                'name': job.name,
                'next_run_time': getattr(job, 'next_run_time', None),
                'trigger': str(job.trigger),
                'pending': job.pending
            }
        return None

    def run_job_now(self, job_id: str) -> bool:
        """
        Immediately execute a scheduled job.

        Args:
            job_id: Job identifier to run

        Returns:
            True if job was triggered, False otherwise
        """
        job = self.scheduler.get_job(job_id)
        if job:
            job.modify(next_run_time=datetime.now())
            logger.info(f"Triggered immediate execution of job '{job_id}'")
            return True
        logger.warning(f"Job '{job_id}' not found")
        return False

    def start(self, paused: bool = False):
        """
        Start the scheduler.

        Args:
            paused: If True, start in paused state
        """
        if not self.scheduler.running:
            self.scheduler.start(paused=paused)
            logger.info("Scheduler started")
        else:
            logger.warning("Scheduler is already running")

    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler.

        Args:
            wait: If True, wait for running jobs to complete
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("Scheduler shutdown complete")

    @property
    def running(self) -> bool:
        """Check if scheduler is running."""
        return self.scheduler.running


# Global scheduler instance
_scheduler_instance = None


def get_scheduler(blocking: bool = False) -> JobScheduler:
    """
    Get or create the global scheduler instance.

    Args:
        blocking: If True, use BlockingScheduler

    Returns:
        JobScheduler instance
    """
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = JobScheduler(blocking=blocking)
    return _scheduler_instance
