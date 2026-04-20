#!/usr/bin/env python3
"""
Main Wyscout extraction script.
Discovers all players via GraphQL.
Downloads xlsx per player via REST.
Loads into football_platform DB.

Usage:
  python3 scripts/extract.py
  python3 scripts/extract.py --competition "Premier League"
  python3 scripts/extract.py --dry-run
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

# Ensure logs dir exists
Path('logs').mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/extract.log')
    ]
)
logger = logging.getLogger(__name__)

from scrapers.wyscout.client import (
    COMPETITION_IDS, get_season_id,
    get_all_players_for_season, download_player_xlsx
)
from etl.wyscout_loader import WyscoutLoader

BRONZE_DIR = Path('data/raw/wyscout/players')
DISCOVERY_FILE = Path('data/raw/wyscout/discovery/players.json')


def _position_group(pos: str) -> str:
    """Map Wyscout position code to position group."""
    if not pos:
        return None
    pos = pos.upper()
    if pos == 'GK':
        return 'GK'
    if any(x in pos for x in ['CB', 'LB', 'RB', 'LWB', 'RWB']):
        return 'DEF'
    if any(x in pos for x in ['MF', 'DMF', 'AMF', 'CMF']):
        return 'MID'
    if any(x in pos for x in ['CF', 'WF', 'LW', 'RW', 'SS', 'FW']):
        return 'FWD'
    return 'MID'


def load_discovery() -> dict:
    if DISCOVERY_FILE.exists():
        return json.loads(DISCOVERY_FILE.read_text())
    return {}


def save_discovery(data: dict):
    DISCOVERY_FILE.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERY_FILE.write_text(json.dumps(data, indent=2))


def run(args):
    loader = WyscoutLoader()
    discovery = load_discovery()

    competitions = {
        name: cid for name, cid in COMPETITION_IDS.items()
        if not args.competition or name == args.competition
    }

    total_players = 0
    total_downloaded = 0
    total_loaded = 0

    for comp_name, comp_id in competitions.items():
        logger.info(f'Processing: {comp_name} (id={comp_id})')

        # Get current and previous seasons
        try:
            curr_season, prev_season = get_season_id(comp_id)
            season_ids = [curr_season, prev_season]
        except Exception as e:
            logger.error(f'Failed to get seasons for {comp_name}: {e}')
            continue

        for season_id in season_ids:
            logger.info(f'  Season {season_id}')

            # Get all players via GraphQL
            cache_key = f'{comp_id}_{season_id}'
            if cache_key in discovery:
                players = discovery[cache_key]
                logger.info(f'  Using cached {len(players)} players')
            else:
                players = get_all_players_for_season(
                    comp_id, season_id, limit=1000)
                discovery[cache_key] = players
                save_discovery(discovery)

            logger.info(f'  {len(players)} players to process')
            total_players += len(players)

            for player in players:
                player_id = player.get('playerId')
                player_name = player.get('name', f'player_{player_id}')

                if not player_id:
                    continue

                # Download xlsx
                safe_name = (player_name.replace('/', '_')
                             .replace(' ', '_')[:50])
                xlsx_path = BRONZE_DIR / f'{player_id}_{safe_name}.xlsx'

                if args.dry_run:
                    logger.info(f'  DRY RUN: would download '
                                f'{player_name} ({player_id})')
                    continue

                try:
                    success = download_player_xlsx(player_id, xlsx_path)
                    if success:
                        total_downloaded += 1
                        position = player.get('primaryPosition', '')
                        pos_group = _position_group(position)
                        rows = loader.load_player_xlsx(
                            xlsx_path, player_id, player_name,
                            primary_position=position,
                            position_group=pos_group)
                        total_loaded += rows
                except Exception as e:
                    logger.error(f'Error processing {player_name} '
                                 f'({player_id}): {e}')
                    continue

    logger.info(f'COMPLETE: {total_players} players discovered, '
                f'{total_downloaded} downloaded, '
                f'{total_loaded} match rows loaded')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--competition', type=str)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    run(args)
