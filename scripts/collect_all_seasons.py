#!/usr/bin/env python3
"""
Comprehensive Multi-Season Data Collection Script

Collects full-depth data for 4 seasons across all 8 leagues:
- Season 2022-23
- Season 2023-24
- Season 2024-25
- Season 2025-26 (current)

Data collected per league per season:
1. Standings (team stats: points, goals, xG)
2. Squad rosters (all players with fotmob_id)
3. Individual player stats (xG, xA, npxG, etc.)
4. Match details (when available)

Usage:
    python scripts/collect_all_seasons.py
    python scripts/collect_all_seasons.py --season 2023-24
    python scripts/collect_all_seasons.py --league premier-league
"""

import sys
import os
import time
import argparse
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.fotmob_etl import FotMobETL
from scrapers.fotmob.constants import (
    LEAGUE_IDS,
    LEAGUE_NAMES,
    ALL_LEAGUE_KEYS,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/multi_season_collection.log')
    ]
)
logger = logging.getLogger(__name__)

# Season mappings (DB format -> FotMob format)
SEASONS = {
    '2022-23': '2022/2023',
    '2023-24': '2023/2024',
    '2024-25': '2024/2025',
    '2025-26': '2025/2026',
}

# All 8 leagues
LEAGUES = ALL_LEAGUE_KEYS


def collect_league_season(etl: FotMobETL, league_key: str, season_db: str, season_fotmob: str):
    """Collect full depth data for a single league and season."""
    league_name = LEAGUE_NAMES.get(league_key, league_key)

    logger.info(f"=" * 60)
    logger.info(f"Collecting: {league_name} - {season_db}")
    logger.info(f"=" * 60)

    stats = {
        'league': league_name,
        'season': season_db,
        'standings': 0,
        'teams': 0,
        'players': 0,
        'player_stats': 0,
        'errors': 0,
    }

    try:
        # Step 1: Basic league data (standings + xG)
        logger.info(f"[1/3] Collecting standings and xG data...")
        league_stats = etl.process_league_season(league_key, season_db)
        stats['standings'] = league_stats.get('team_season_stats', 0)
        stats['teams'] = league_stats.get('teams', 0)
        logger.info(f"  -> {stats['standings']} team season stats")

        # Step 2: Deep team data (squads)
        logger.info(f"[2/3] Collecting team squads...")
        team_stats = etl.process_league_teams_deep(league_key)
        stats['players'] = team_stats.get('players_inserted', 0) + team_stats.get('players_updated', 0)
        logger.info(f"  -> {stats['players']} players processed")

        # Step 3: Deep player stats (xG, xA, etc.)
        logger.info(f"[3/3] Collecting individual player stats (xG/xA)...")
        player_stats = etl.process_league_players_deep(league_key)
        stats['player_stats'] = player_stats.get('player_season_stats', 0)
        logger.info(f"  -> {stats['player_stats']} player season stats")

    except Exception as e:
        logger.error(f"Error collecting {league_name} {season_db}: {e}")
        stats['errors'] += 1

    return stats


def collect_season(etl: FotMobETL, season_db: str, leagues: list = None):
    """Collect all leagues for a single season."""
    season_fotmob = SEASONS.get(season_db)
    if not season_fotmob:
        logger.error(f"Unknown season: {season_db}")
        return []

    leagues = leagues or LEAGUES
    season_stats = []

    logger.info(f"\n{'#' * 70}")
    logger.info(f"# SEASON: {season_db} ({season_fotmob})")
    logger.info(f"# Leagues: {len(leagues)}")
    logger.info(f"{'#' * 70}\n")

    for i, league_key in enumerate(leagues, 1):
        logger.info(f"\n[{i}/{len(leagues)}] Processing {LEAGUE_NAMES.get(league_key, league_key)}...")

        stats = collect_league_season(etl, league_key, season_db, season_fotmob)
        season_stats.append(stats)

        # Rate limiting between leagues
        if i < len(leagues):
            logger.info("Waiting 5 seconds before next league...")
            time.sleep(5)

    return season_stats


def print_summary(all_stats: dict):
    """Print collection summary."""
    logger.info(f"\n{'=' * 70}")
    logger.info("COLLECTION SUMMARY")
    logger.info(f"{'=' * 70}\n")

    total_teams = 0
    total_players = 0
    total_player_stats = 0
    total_errors = 0

    for season, stats_list in all_stats.items():
        logger.info(f"\n{season}:")
        logger.info(f"-" * 40)

        season_teams = sum(s['standings'] for s in stats_list)
        season_players = sum(s['players'] for s in stats_list)
        season_stats = sum(s['player_stats'] for s in stats_list)
        season_errors = sum(s['errors'] for s in stats_list)

        for s in stats_list:
            logger.info(f"  {s['league']}: {s['standings']} teams, {s['player_stats']} player stats")

        logger.info(f"  TOTAL: {season_teams} teams, {season_players} players, {season_stats} player stats")

        total_teams += season_teams
        total_players += season_players
        total_player_stats += season_stats
        total_errors += season_errors

    logger.info(f"\n{'=' * 70}")
    logger.info(f"GRAND TOTAL:")
    logger.info(f"  Teams: {total_teams}")
    logger.info(f"  Players: {total_players}")
    logger.info(f"  Player Season Stats: {total_player_stats}")
    logger.info(f"  Errors: {total_errors}")
    logger.info(f"{'=' * 70}\n")


def main():
    parser = argparse.ArgumentParser(description='Multi-season data collection')
    parser.add_argument('--season', type=str, help='Specific season (e.g., 2023-24)')
    parser.add_argument('--league', type=str, help='Specific league (e.g., premier-league)')
    parser.add_argument('--skip-current', action='store_true', help='Skip 2025-26 season')
    args = parser.parse_args()

    # Determine seasons to collect
    seasons_to_collect = list(SEASONS.keys())
    if args.season:
        if args.season in SEASONS:
            seasons_to_collect = [args.season]
        else:
            logger.error(f"Invalid season: {args.season}")
            logger.error(f"Valid seasons: {list(SEASONS.keys())}")
            return 1

    if args.skip_current and '2025-26' in seasons_to_collect:
        seasons_to_collect.remove('2025-26')

    # Determine leagues to collect
    leagues_to_collect = LEAGUES
    if args.league:
        if args.league in LEAGUES:
            leagues_to_collect = [args.league]
        else:
            logger.error(f"Invalid league: {args.league}")
            logger.error(f"Valid leagues: {LEAGUES}")
            return 1

    logger.info(f"\n{'*' * 70}")
    logger.info(f"MULTI-SEASON DATA COLLECTION")
    logger.info(f"{'*' * 70}")
    logger.info(f"Seasons: {seasons_to_collect}")
    logger.info(f"Leagues: {len(leagues_to_collect)} leagues")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'*' * 70}\n")

    all_stats = {}

    with FotMobETL() as etl:
        for season_db in seasons_to_collect:
            season_stats = collect_season(etl, season_db, leagues_to_collect)
            all_stats[season_db] = season_stats

            # Longer wait between seasons
            if season_db != seasons_to_collect[-1]:
                logger.info("\nWaiting 10 seconds before next season...\n")
                time.sleep(10)

    print_summary(all_stats)

    logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
