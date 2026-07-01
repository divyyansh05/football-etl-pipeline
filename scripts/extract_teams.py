#!/usr/bin/env python3
"""
Team stats extraction script.
Discovers teams via GraphQL, downloads JSON stats, loads into DB.

Usage:
  python3 scripts/extract_teams.py
  python3 scripts/extract_teams.py --competition "Premier League"
  python3 scripts/extract_teams.py --dry-run
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/extract_teams.log')
    ]
)
logger = logging.getLogger(__name__)

from scrapers.wyscout.client import (
    COMPETITION_IDS, get_season_id,
    get_all_teams_for_season, get_team_stats_json
)
from etl.team_loader import TeamLoader

BRONZE_DIR = Path('data/raw/wyscout/teams')


def run(args):
    loader = TeamLoader()

    competitions = {
        name: cid for name, cid in COMPETITION_IDS.items()
        if not args.competition or name == args.competition
    }

    total_teams = 0
    total_downloaded = 0
    total_loaded = 0
    seen_teams = set()

    for comp_name, comp_id in competitions.items():
        logger.info(f'Processing: {comp_name} (id={comp_id})')

        try:
            season_ids = get_season_id(comp_id)
        except Exception as e:
            logger.error(f'Failed seasons for {comp_name}: {e}')
            continue

        for season_id in season_ids:
            logger.info(f'  Season {season_id}')

            teams = get_all_teams_for_season(comp_id, season_id)
            logger.info(f'  {len(teams)} teams found')

            for team in teams:
                team_id = team.get('teamId')
                team_name = team.get('name', f'team_{team_id}')

                if not team_id or team_id in seen_teams:
                    continue
                seen_teams.add(team_id)
                total_teams += 1

                if args.dry_run:
                    logger.info(f'  DRY RUN: {team_name} ({team_id})')
                    continue

                json_path = BRONZE_DIR / f'{team_id}_{team_name.replace("/","_").replace(" ","_")[:40]}.json'

                try:
                    # Download if not exists
                    if json_path.exists() and json_path.stat().st_size > 100:
                        logger.info(f'  SKIP {json_path.name} — exists')
                        data = json.loads(json_path.read_text())
                    else:
                        json_path.parent.mkdir(parents=True, exist_ok=True)
                        data = get_team_stats_json(team_id)
                        if data and data.get('matches'):
                            json_path.write_text(json.dumps(data))
                            logger.info(f'  SAVED {team_name}: '
                                        f'{len(data["matches"])} matches')
                        else:
                            logger.warning(f'  No data for {team_name}')
                            continue

                    total_downloaded += 1

                    # Load into DB
                    rows = loader.load_team_json(
                        json_path, team_id, team_name)
                    total_loaded += rows

                except Exception as e:
                    logger.error(f'Error processing {team_name}: {e}')
                    continue

    logger.info(f'COMPLETE: {total_teams} teams, '
                f'{total_downloaded} downloaded, '
                f'{total_loaded} match rows loaded')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--competition', type=str)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    run(args)
