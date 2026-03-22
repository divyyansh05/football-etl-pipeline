#!/usr/bin/env python3
"""
Optimized Multi-Season Data Collection

Strategy:
1. FAST: Collect team standings + xG for ALL 4 seasons (quick - ~1 min per league)
2. DEEP: Collect individual player stats ONLY for current season (2025-26)
   - Player xG/xA data only available from current season endpoint

This gives us:
- Historical team xG data for 4 seasons
- Current season player xG/xA data
- Complete squad rosters

Usage:
    python scripts/collect_optimized.py
    python scripts/collect_optimized.py --skip-players  # Fast mode - team data only
"""

import sys
import os
import time
import argparse
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.fotmob_etl import FotMobETL
from scrapers.fotmob.constants import (
    LEAGUE_IDS,
    LEAGUE_NAMES,
    ALL_LEAGUE_KEYS,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/optimized_collection.log')
    ]
)
logger = logging.getLogger(__name__)

# All target seasons
SEASONS = ['2022-23', '2023-24', '2024-25', '2025-26']

# All 8 leagues
LEAGUES = ALL_LEAGUE_KEYS


def collect_team_data_all_seasons(etl: FotMobETL):
    """Collect team standings and xG for all seasons - FAST."""
    logger.info("=" * 70)
    logger.info("PHASE 1: Team Data Collection (All Seasons)")
    logger.info("=" * 70)

    total_stats = {'teams': 0, 'team_stats': 0, 'xg_updates': 0}

    for season in SEASONS:
        logger.info(f"\n--- Season: {season} ---")

        for league_key in LEAGUES:
            league_name = LEAGUE_NAMES.get(league_key, league_key)

            try:
                stats = etl.process_league_season(league_key, season)
                total_stats['teams'] += stats.get('teams', 0)
                total_stats['team_stats'] += stats.get('team_season_stats', 0)

                logger.info(f"  {league_name}: {stats.get('team_season_stats', 0)} teams")

            except Exception as e:
                logger.error(f"  {league_name}: Error - {e}")

            time.sleep(1)  # Rate limiting

        logger.info(f"Season {season} complete")
        time.sleep(2)

    logger.info(f"\nPhase 1 Complete: {total_stats['teams']} teams, {total_stats['team_stats']} season stats")
    return total_stats


def collect_squad_rosters(etl: FotMobETL):
    """Collect squad rosters for all teams - MEDIUM speed."""
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 2: Squad Rosters (Current Season)")
    logger.info("=" * 70)

    total_stats = {'teams': 0, 'players': 0}

    for league_key in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"\n{league_name}:")

        try:
            stats = etl.process_league_teams_deep(league_key)
            total_stats['teams'] += stats.get('teams_processed', 0)
            total_stats['players'] += stats.get('players_inserted', 0) + stats.get('players_updated', 0)

            logger.info(f"  -> {stats.get('teams_processed', 0)} teams, {stats.get('players_inserted', 0) + stats.get('players_updated', 0)} players")

        except Exception as e:
            logger.error(f"  Error: {e}")

        time.sleep(3)

    logger.info(f"\nPhase 2 Complete: {total_stats['teams']} teams, {total_stats['players']} players")
    return total_stats


def collect_player_stats_current(etl: FotMobETL):
    """Collect individual player stats (xG, xA) for current season - SLOW."""
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 3: Player Stats (xG/xA) - Current Season Only")
    logger.info("This takes ~15 minutes per league...")
    logger.info("=" * 70)

    total_stats = {'players': 0, 'stats': 0}

    for league_key in LEAGUES:
        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"\n{league_name}:")

        try:
            stats = etl.process_league_players_deep(league_key)
            total_stats['players'] += stats.get('players_processed', 0)
            total_stats['stats'] += stats.get('player_season_stats', 0)

            logger.info(f"  -> {stats.get('players_processed', 0)} players, {stats.get('player_season_stats', 0)} stats")

        except Exception as e:
            logger.error(f"  Error: {e}")

        time.sleep(5)

    logger.info(f"\nPhase 3 Complete: {total_stats['players']} players, {total_stats['stats']} season stats")
    return total_stats


def main():
    parser = argparse.ArgumentParser(description='Optimized data collection')
    parser.add_argument('--skip-players', action='store_true',
                       help='Skip player stats (fast mode - team data only)')
    parser.add_argument('--skip-squads', action='store_true',
                       help='Skip squad collection')
    args = parser.parse_args()

    logger.info("*" * 70)
    logger.info("OPTIMIZED DATA COLLECTION")
    logger.info("*" * 70)
    logger.info(f"Seasons: {SEASONS}")
    logger.info(f"Leagues: {len(LEAGUES)}")
    logger.info(f"Skip Players: {args.skip_players}")
    logger.info(f"Skip Squads: {args.skip_squads}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("*" * 70)

    all_stats = {}

    with FotMobETL() as etl:
        # Phase 1: Team data for all seasons (fast)
        all_stats['team_data'] = collect_team_data_all_seasons(etl)

        # Phase 2: Squad rosters (medium)
        if not args.skip_squads:
            all_stats['squads'] = collect_squad_rosters(etl)

        # Phase 3: Player stats - current season only (slow)
        if not args.skip_players:
            all_stats['player_stats'] = collect_player_stats_current(etl)

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("COLLECTION SUMMARY")
    logger.info("=" * 70)
    for phase, stats in all_stats.items():
        logger.info(f"{phase}: {stats}")
    logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
