#!/usr/bin/env python3
"""
Backfill player positions from discovery cache.
Useful after extraction runs where positions weren't set.

Usage:
  python3 scripts/backfill_positions.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from database.connection import get_conn

DISCOVERY_FILE = Path('data/raw/wyscout/discovery/players.json')


def _position_group(pos: str) -> str:
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
    return None


def backfill():
    if not DISCOVERY_FILE.exists():
        print('No discovery file found')
        return

    data = json.load(open(DISCOVERY_FILE))

    # Build wyscout_id -> position map
    pos_map = {}
    for players in data.values():
        for p in players:
            pid = p.get('playerId')
            pos = p.get('primaryPosition', '')
            if pid and pos:
                pos_map[pid] = pos

    print(f'Discovery has positions for {len(pos_map)} unique IDs')

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT player_id, wyscout_id FROM players '
                'WHERE position_group IS NULL')
            nulls = cur.fetchall()
            print(f'{len(nulls)} players with NULL position_group')

            updated = 0
            for player_id, wyscout_id in nulls:
                pos = pos_map.get(wyscout_id)
                if pos:
                    grp = _position_group(pos)
                    cur.execute(
                        'UPDATE players SET primary_position=%s, '
                        'position_group=%s WHERE player_id=%s',
                        (pos, grp, player_id))
                    updated += 1
        conn.commit()

    print(f'Updated {updated} players')


if __name__ == '__main__':
    backfill()
