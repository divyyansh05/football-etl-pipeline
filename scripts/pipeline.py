#!/usr/bin/env python3
"""
Resilient auto-pipeline runner for football-data-platform.
Runs the full extraction → load → analytics pipeline.

Design:
  - Runs as background daemon (nohup-safe)
  - State persisted to JSON: resume from any step on restart
  - Tiered error handling: skip bad records, abort on systemic failures
  - All output to logs/pipeline.log
  - PID file for single-instance enforcement

Usage:
  # Start pipeline in background (survives terminal close):
  nohup python3 scripts/pipeline.py &

  # Start with specific step:
  python3 scripts/pipeline.py --start-from teams

  # Check status:
  python3 scripts/pipeline.py --status

  # Force restart discovery:
  python3 scripts/pipeline.py --force-discovery
"""
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

# ─── Paths ───────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
STATE_FILE = PROJECT_ROOT / 'data' / 'pipeline_state.json'
PID_FILE = PROJECT_ROOT / 'logs' / 'pipeline.pid'
LOG_FILE = PROJECT_ROOT / 'logs' / 'pipeline.log'

# Ensure dirs
(PROJECT_ROOT / 'logs').mkdir(exist_ok=True)
(PROJECT_ROOT / 'data').mkdir(exist_ok=True)

# ─── Logging ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE)),
    ]
)
logger = logging.getLogger('pipeline')

# ─── Pipeline Steps ─────────────────────────────────
STEPS = [
    'discover_players',
    'extract_players',
    'discover_teams',
    'extract_teams',
    'post_extraction',
]


# ─── State Management ───────────────────────────────
def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        'current_step': None,
        'completed_steps': [],
        'started_at': None,
        'last_updated': None,
        'stats': {},
        'errors': [],
    }


def _save_state(state: dict):
    state['last_updated'] = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _print_status():
    state = _load_state()
    print('═' * 60)
    print('  football-data-platform Pipeline Status')
    print('═' * 60)
    print(f'  Started:    {state.get("started_at", "never")}')
    print(f'  Updated:    {state.get("last_updated", "never")}')
    print(f'  Current:    {state.get("current_step", "idle")}')
    print(f'  Completed:  {", ".join(state.get("completed_steps", [])) or "none"}')
    print()
    stats = state.get('stats', {})
    for k, v in stats.items():
        print(f'  {k}: {v}')
    errors = state.get('errors', [])
    if errors:
        print(f'\n  Recent errors ({len(errors)}):')
        for e in errors[-5:]:
            print(f'    - {e}')
    print('═' * 60)

    # Check PID
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 0)
            print(f'  ▶ Pipeline RUNNING (PID {pid})')
        except OSError:
            print(f'  ■ Pipeline NOT running (stale PID {pid})')
    else:
        print('  ■ Pipeline NOT running')


# ─── PID Management ─────────────────────────────────
def _write_pid():
    PID_FILE.write_text(str(os.getpid()))


def _clear_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()


def _check_single_instance():
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 0)
            logger.error(f'Pipeline already running (PID {pid}). Exiting.')
            sys.exit(1)
        except OSError:
            logger.warning(f'Stale PID file found ({pid}). Cleaning up.')
            PID_FILE.unlink()


# ─── Signal Handling ─────────────────────────────────
def _signal_handler(signum, frame):
    logger.info(f'Received signal {signum}. Saving state and exiting...')
    _clear_pid()
    sys.exit(0)


# ─── Pipeline Step Implementations ──────────────────

def step_discover_players(state: dict, force: bool = False):
    """Discover all player IDs via GraphQL for all competitions."""
    from scrapers.wyscout.client import COMPETITION_IDS, get_season_id, get_all_players_for_season

    discovery_file = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'discovery' / 'players.json'

    if discovery_file.exists() and not force:
        data = json.loads(discovery_file.read_text())
        total = sum(len(v) for v in data.values())
        logger.info(f'Using cached discovery: {len(data)} keys, {total} players')
        state['stats']['discovered_players'] = total
        return

    discovery_file.parent.mkdir(parents=True, exist_ok=True)
    data = {}

    for comp_name, comp_id in COMPETITION_IDS.items():
        logger.info(f'Discovering players for {comp_name}...')
        try:
            season_ids = get_season_id(comp_id)
        except Exception as e:
            logger.error(f'Season discovery failed for {comp_name}: {e}')
            state['errors'].append(f'{comp_name}: season discovery failed')
            continue

        for season_id in season_ids:
            cache_key = f'{comp_id}_{season_id}'
            if cache_key in data:
                continue
            try:
                players = get_all_players_for_season(comp_id, season_id, limit=1000)
                data[cache_key] = players
                logger.info(f'  {comp_name} season {season_id}: {len(players)} players')
            except Exception as e:
                logger.error(f'  Failed to fetch players for {comp_name} season {season_id}: {e}')
                state['errors'].append(f'{comp_name} ({season_id}): players failed')

    discovery_file.write_text(json.dumps(data, indent=2))
    total = sum(len(v) for v in data.values())
    state['stats']['discovered_players'] = total
    logger.info(f'Discovery complete: {total} player entries across {len(data)} competition-seasons')


def step_extract_players(state: dict):
    """Download and load all player xlsx files."""
    from scrapers.wyscout.client import download_player_xlsx
    from etl.wyscout_loader import WyscoutLoader

    discovery_file = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'discovery' / 'players.json'
    bronze_dir = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'players'
    bronze_dir.mkdir(parents=True, exist_ok=True)

    data = json.loads(discovery_file.read_text())
    loader = WyscoutLoader()

    # Build unique player set (same player can appear in multiple seasons)
    seen_ids = set()
    all_players = []
    for players in data.values():
        for p in players:
            pid = p.get('playerId')
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_players.append(p)

    total = len(all_players)
    downloaded = 0
    loaded = 0
    skipped = 0
    failed = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 10

    logger.info(f'Starting player extraction: {total} unique players')
    state['stats']['total_unique_players'] = total

    for i, player in enumerate(all_players, 1):
        player_id = player.get('playerId')
        player_name = player.get('name', f'player_{player_id}')
        position = player.get('primaryPosition', '')

        safe_name = player_name.replace('/', '_').replace(' ', '_')[:50]
        xlsx_path = bronze_dir / f'{player_id}_{safe_name}.xlsx'

        # Progress logging every 50 players
        if i % 50 == 0:
            logger.info(f'Progress: {i}/{total} '
                        f'(downloaded={downloaded}, loaded={loaded}, '
                        f'skipped={skipped}, failed={failed})')
            state['stats'].update({
                'players_processed': i,
                'players_downloaded': downloaded,
                'players_loaded': loaded,
                'players_skipped': skipped,
                'players_failed': failed,
            })
            _save_state(state)

        try:
            success = download_player_xlsx(player_id, xlsx_path)
            if success:
                consecutive_failures = 0
                downloaded += 1

                pos_group = _position_group(position)
                rows = loader.load_player_xlsx(
                    xlsx_path, player_id, player_name,
                    primary_position=position,
                    position_group=pos_group)
                loaded += rows
            else:
                failed += 1
                consecutive_failures += 1

        except Exception as e:
            failed += 1
            consecutive_failures += 1
            logger.error(f'Error processing {player_name} ({player_id}): {e}')

        # Tier 2: systemic failure detection
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.critical(
                f'TIER 2 ABORT: {MAX_CONSECUTIVE_FAILURES} consecutive failures. '
                f'Stopping player extraction. Last player: {player_name}')
            state['errors'].append(
                f'Tier 2 abort at player {player_name} after '
                f'{MAX_CONSECUTIVE_FAILURES} consecutive failures')
            break

    state['stats'].update({
        'players_processed': total,
        'players_downloaded': downloaded,
        'match_rows_loaded': loaded,
        'players_skipped': skipped,
        'players_failed': failed,
    })
    logger.info(f'Player extraction complete: {downloaded} downloaded, '
                f'{loaded} match rows loaded, {failed} failed')


def step_discover_teams(state: dict):
    """Discover all team IDs via GraphQL."""
    from scrapers.wyscout.client import COMPETITION_IDS, get_season_id, get_all_teams_for_season

    seen = set()
    all_teams = []

    for comp_name, comp_id in COMPETITION_IDS.items():
        logger.info(f'Discovering teams for {comp_name}...')
        try:
            season_ids = get_season_id(comp_id)
        except Exception as e:
            logger.error(f'Season fail for {comp_name}: {e}')
            continue

        for season_id in season_ids:
            teams = get_all_teams_for_season(comp_id, season_id)
            for t in teams:
                tid = t.get('teamId')
                if tid and tid not in seen:
                    seen.add(tid)
                    all_teams.append(t)

    # Save team discovery
    disc_file = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'discovery' / 'teams.json'
    disc_file.parent.mkdir(parents=True, exist_ok=True)
    disc_file.write_text(json.dumps(all_teams, indent=2))

    state['stats']['discovered_teams'] = len(all_teams)
    logger.info(f'Team discovery: {len(all_teams)} unique teams')


def step_extract_teams(state: dict):
    """Download and load all team stats JSON."""
    from scrapers.wyscout.client import get_team_stats_json
    from etl.team_loader import TeamLoader

    disc_file = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'discovery' / 'teams.json'
    bronze_dir = PROJECT_ROOT / 'data' / 'raw' / 'wyscout' / 'teams'
    bronze_dir.mkdir(parents=True, exist_ok=True)

    teams = json.loads(disc_file.read_text())
    loader = TeamLoader()

    downloaded = 0
    loaded = 0
    failed = 0
    consecutive_failures = 0

    logger.info(f'Starting team extraction: {len(teams)} teams')

    for i, team in enumerate(teams, 1):
        team_id = team.get('teamId')
        team_name = team.get('name', f'team_{team_id}')

        safe_name = team_name.replace('/', '_').replace(' ', '_')[:40]
        json_path = bronze_dir / f'{team_id}_{safe_name}.json'

        try:
            if json_path.exists() and json_path.stat().st_size > 100:
                data = json.loads(json_path.read_text())
            else:
                data = get_team_stats_json(team_id)
                if data and data.get('matches'):
                    json_path.write_text(json.dumps(data))
                else:
                    logger.warning(f'No data for {team_name}')
                    continue

            downloaded += 1
            consecutive_failures = 0
            rows = loader.load_team_json(json_path, team_id, team_name)
            loaded += rows

        except Exception as e:
            failed += 1
            consecutive_failures += 1
            logger.error(f'Team error {team_name}: {e}')

        if consecutive_failures >= 10:
            logger.critical('TIER 2 ABORT: 10 consecutive team failures')
            state['errors'].append('Tier 2 abort on team extraction')
            break

        if i % 20 == 0:
            logger.info(f'Teams progress: {i}/{len(teams)}')
            _save_state(state)

    state['stats'].update({
        'teams_downloaded': downloaded,
        'team_match_rows_loaded': loaded,
        'teams_failed': failed,
    })
    logger.info(f'Team extraction: {downloaded} downloaded, {loaded} rows, {failed} failed')


def step_post_extraction(state: dict):
    """Run competitions backfill, position backfill, score computation."""
    from database.connection import get_conn, query as db_query

    # 1. Populate competitions table
    logger.info('Populating competitions...')
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

    # 2. Backfill positions
    logger.info('Backfilling positions...')
    from scripts.backfill_positions import backfill
    backfill()

    # 3. Compute scores
    logger.info('Computing performance scores...')
    from analytics.compute_scores import compute
    compute(min_minutes=450)

    # 4. Summary
    rows = db_query("""
        SELECT
            (SELECT COUNT(*) FROM players),
            (SELECT COUNT(*) FROM player_match_stats),
            (SELECT COUNT(*) FROM team_match_stats),
            (SELECT COUNT(*) FROM player_scores),
            (SELECT COUNT(DISTINCT competition_name) FROM player_match_stats)
    """)
    p, m, t, s, c = rows[0]
    state['stats'].update({
        'final_players': p,
        'final_match_rows': m,
        'final_team_rows': t,
        'final_scores': s,
        'final_competitions': c,
    })
    logger.info(f'FINAL: {p} players, {m} match rows, {t} team rows, '
                f'{s} scores, {c} competitions')


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
    return None


# ─── Main Pipeline Runner ───────────────────────────

def run_pipeline(start_from: str = None, force_discovery: bool = False):
    _check_single_instance()
    _write_pid()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    state = _load_state()
    state['started_at'] = datetime.now().isoformat()

    # Determine which steps to run
    if start_from and start_from in STEPS:
        start_idx = STEPS.index(start_from)
    else:
        # Resume from last completed step
        completed = state.get('completed_steps', [])
        if completed:
            last = completed[-1]
            start_idx = STEPS.index(last) + 1 if last in STEPS else 0
        else:
            start_idx = 0

    remaining = STEPS[start_idx:]
    logger.info(f'Pipeline starting. Steps: {remaining}')
    _save_state(state)

    step_funcs = {
        'discover_players': lambda s: step_discover_players(s, force=force_discovery),
        'extract_players': step_extract_players,
        'discover_teams': step_discover_teams,
        'extract_teams': step_extract_teams,
        'post_extraction': step_post_extraction,
    }

    for step_name in remaining:
        state['current_step'] = step_name
        _save_state(state)
        logger.info(f'═══ STEP: {step_name} ═══')

        try:
            start_time = time.time()
            step_funcs[step_name](state)
            elapsed = time.time() - start_time

            state['completed_steps'].append(step_name)
            state['stats'][f'{step_name}_time_sec'] = round(elapsed, 1)
            _save_state(state)
            logger.info(f'✓ {step_name} completed in {elapsed:.0f}s')

        except Exception as e:
            logger.critical(f'FATAL: {step_name} failed: {e}', exc_info=True)
            state['errors'].append(f'{step_name}: {str(e)}')
            state['current_step'] = f'FAILED:{step_name}'
            _save_state(state)
            break

    state['current_step'] = 'COMPLETE'
    state['completed_at'] = datetime.now().isoformat()
    _save_state(state)

    _clear_pid()
    logger.info('═══ PIPELINE COMPLETE ═══')
    _print_status()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='football-data-platform pipeline')
    parser.add_argument('--status', action='store_true',
                        help='Show pipeline status')
    parser.add_argument('--start-from',
                        choices=STEPS,
                        help='Start from specific step')
    parser.add_argument('--force-discovery', action='store_true',
                        help='Force re-discovery of all players/teams')
    args = parser.parse_args()

    if args.status:
        _print_status()
    else:
        run_pipeline(
            start_from=args.start_from,
            force_discovery=args.force_discovery)
