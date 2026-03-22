#!/usr/bin/env python3
"""
Autopilot Data Collector - Robust unattended collection script.

Features:
- Automatic error recovery with exponential backoff
- Network failure resilience
- Database connection retry
- Progress logging to file
- Graceful shutdown on SIGTERM/SIGINT
- Rate limiting built-in
- Collects all 8 leagues for current season

Usage:
    python scripts/autopilot_collector.py

    # Check status:
    tail -f logs/autopilot.log
"""

import os
import sys
import time
import signal
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

# Setup logging to file
log_dir = Path(__file__).parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'autopilot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Graceful shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"Received signal {signum}, requesting graceful shutdown...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def wait_for_db(max_retries=30, delay=10):
    """Wait for database to be available."""
    from database.connection import get_db

    for attempt in range(max_retries):
        if shutdown_requested:
            return False
        try:
            db = get_db()
            result = db.execute_query("SELECT 1", fetch=True)
            if result:
                logger.info("Database connection successful")
                return True
        except Exception as e:
            logger.warning(f"DB connection attempt {attempt+1}/{max_retries} failed: {e}")
            time.sleep(delay)

    logger.error("Failed to connect to database after max retries")
    return False


def collect_with_retry(func, *args, max_retries=3, base_delay=60, **kwargs):
    """Execute collection function with exponential backoff retry."""
    for attempt in range(max_retries):
        if shutdown_requested:
            return None
        try:
            return func(*args, **kwargs)
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            logger.error(f"Attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    return None


def run_fotmob_collection():
    """Run FotMob collection for all leagues."""
    from etl.fotmob_etl import FotMobETL
    from scrapers.fotmob.constants import LEAGUE_IDS

    leagues = list(LEAGUE_IDS.keys())
    logger.info(f"Starting FotMob collection for {len(leagues)} leagues")

    stats = {
        'leagues_processed': 0,
        'teams': 0,
        'players': 0,
        'player_stats': 0,
        'errors': 0,
    }

    # Phase 1: Basic collection (standings, teams)
    # Create fresh ETL instance for each league to avoid session issues
    for league in leagues:
        if shutdown_requested:
            break

        logger.info(f"[Phase 1] Collecting {league} standings and teams...")
        try:
            with FotMobETL() as etl:
                result = etl.process_league_season(league)
                stats['teams'] += result.get('teams_inserted', 0)
                stats['leagues_processed'] += 1
                logger.info(f"  -> {league}: {result.get('teams_inserted', 0)} teams")
        except Exception as e:
            logger.error(f"  -> {league} failed: {e}")
            stats['errors'] += 1

        time.sleep(3)  # Rate limit between leagues

    # Phase 2: Deep player collection - one league at a time with fresh session
    if not shutdown_requested:
        logger.info("Starting Phase 2: Deep player collection...")
        for league in leagues:
            if shutdown_requested:
                break

            logger.info(f"[Phase 2] Collecting {league} player stats...")
            try:
                with FotMobETL() as etl:
                    result = etl.process_league_players_deep(league)
                    stats['players'] += result.get('players_processed', 0)
                    stats['player_stats'] += result.get('player_season_stats', 0)
                    logger.info(f"  -> {league}: {result.get('players_processed', 0)} players, {result.get('player_season_stats', 0)} stats")
            except Exception as e:
                logger.error(f"  -> {league} failed: {e}")
                stats['errors'] += 1

            # Longer delay between deep collections
            time.sleep(5)

    return stats


def log_db_status():
    """Log current database counts."""
    try:
        from database.connection import get_db
        db = get_db()

        counts = {}
        for table in ['players', 'teams', 'player_season_stats', 'matches']:
            result = db.execute_query(f"SELECT COUNT(*) FROM {table}", fetch=True)
            counts[table] = result[0][0] if result else 0

        # Valid xG count
        result = db.execute_query(
            "SELECT COUNT(*) FROM player_season_stats WHERE xg > 0 AND xg < 10",
            fetch=True
        )
        counts['valid_xg'] = result[0][0] if result else 0

        logger.info(f"DB Status: Players={counts['players']}, Teams={counts['teams']}, "
                   f"PlayerStats={counts['player_season_stats']}, ValidXG={counts['valid_xg']}, "
                   f"Matches={counts['matches']}")
        return counts
    except Exception as e:
        logger.error(f"Failed to get DB status: {e}")
        return {}


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("AUTOPILOT COLLECTOR STARTED")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 60)

    # Wait for database
    if not wait_for_db():
        logger.error("Cannot proceed without database connection")
        sys.exit(1)

    # Log initial status
    logger.info("Initial database status:")
    initial_counts = log_db_status()

    # Run collection
    stats = collect_with_retry(run_fotmob_collection, max_retries=3, base_delay=120)

    # Log final status
    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("AUTOPILOT COLLECTOR FINISHED")
    logger.info(f"Duration: {duration}")
    if stats:
        logger.info(f"Results: {stats}")
    logger.info("Final database status:")
    final_counts = log_db_status()

    # Calculate deltas
    if initial_counts and final_counts:
        logger.info("Changes:")
        for key in final_counts:
            delta = final_counts.get(key, 0) - initial_counts.get(key, 0)
            if delta != 0:
                logger.info(f"  {key}: +{delta}")

    logger.info("=" * 60)

    if shutdown_requested:
        logger.info("Shutdown was requested - collection may be incomplete")
        sys.exit(0)


if __name__ == '__main__':
    main()
