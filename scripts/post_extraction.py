#!/usr/bin/env python3
"""
Post-extraction tasks: backfill positions, populate competitions, recompute scores.
Run after player extraction completes.

Usage:
  python3 scripts/post_extraction.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

from database.connection import get_conn, query


def populate_competitions():
    """Insert any new competition names from match data."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO competitions (name)
                SELECT DISTINCT competition_name
                FROM player_match_stats
                WHERE competition_name IS NOT NULL
                  AND competition_name NOT IN (
                      SELECT name FROM competitions)
            """)
            count = cur.rowcount
        conn.commit()
    logger.info(f'Added {count} new competitions')


def backfill_positions():
    from scripts.backfill_positions import backfill
    backfill()


def recompute_scores():
    from analytics.compute_scores import compute
    compute(min_minutes=450)


def summary():
    rows = query("""
        SELECT
            (SELECT COUNT(*) FROM players) as players,
            (SELECT COUNT(*) FROM player_match_stats) as match_rows,
            (SELECT COUNT(*) FROM team_match_stats) as team_rows,
            (SELECT COUNT(*) FROM player_scores) as scores,
            (SELECT COUNT(DISTINCT competition_name) FROM player_match_stats) as competitions
    """)
    p, m, t, s, c = rows[0]
    logger.info(f'SUMMARY: {p} players, {m} match rows, {t} team rows, '
                f'{s} scores, {c} competitions')


if __name__ == '__main__':
    logger.info('=== Post-extraction tasks ===')
    populate_competitions()
    backfill_positions()
    recompute_scores()
    summary()
    logger.info('=== Done ===')
