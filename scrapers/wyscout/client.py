"""
Wyscout API client.
All confirmed endpoints from browser inspection + GraphQL exploration.
Auto-refreshes token on 401/403.
"""
import os
import time
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv
from scrapers.wyscout.token_manager import get_token, refresh_token

load_dotenv()
logger = logging.getLogger(__name__)

BASE_SEARCH = 'https://searchapi.wyscout.com'
RATE_LIMIT = 1.5  # seconds between requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/146.0.0.0 Safari/537.36',
    'Origin': 'https://wyscout.hudl.com',
    'Referer': 'https://wyscout.hudl.com/',
}

# Confirmed competition IDs from GraphQL exploration
COMPETITION_IDS = {
    'Premier League':    8,
    'La Liga':           7,
    'Serie A':           13,
    'Bundesliga':        9,
    'Ligue 1':           16,
    'Champions League':  10,
}

# Confirmed team IDs for PL (from GraphQL exploration)
PL_TEAM_IDS = {
    'Arsenal': 660, 'Chelsea': 661, 'Manchester United': 662,
    'Liverpool': 663, 'Newcastle United': 664, 'Aston Villa': 665,
    'Fulham': 667, 'Everton': 674, 'Tottenham Hotspur': 675,
    'Manchester City': 676, 'Crystal Palace': 679,
    'Wolverhampton Wanderers': 680, 'Leeds United': 681,
    'Sunderland': 683, 'West Ham United': 684,
    'Nottingham Forest': 694, 'Burnley': 698,
    'Brighton': 703, 'Bournemouth': 711, 'Brentford': 722,
}


def _params(extra=None):
    load_dotenv(override=True)
    p = {
        'token': get_token(),
        'groupId': os.getenv('WYSCOUT_GROUP_ID', '1059060'),
        'subgroupId': os.getenv('WYSCOUT_SUBGROUP_ID', '93476'),
        'lang': 'en',
    }
    if extra:
        p.update(extra)
    return p


def _get(path, extra_params=None, retried=False):
    time.sleep(RATE_LIMIT)
    r = requests.get(
        f'{BASE_SEARCH}{path}',
        params=_params(extra_params),
        headers=HEADERS, timeout=30
    )
    if r.status_code in (401, 403) and not retried:
        logger.warning('Token expired — refreshing...')
        refresh_token()
        return _get(path, extra_params, retried=True)
    return r


def _post(path, params=None, retried=False):
    time.sleep(RATE_LIMIT)
    load_dotenv(override=True)
    token = get_token()
    r = requests.post(
        f'{BASE_SEARCH}{path}',
        params=params or {'token': token,
                          'groupId': os.getenv('WYSCOUT_GROUP_ID'),
                          'subgroupId': os.getenv('WYSCOUT_SUBGROUP_ID')},
        headers=HEADERS, timeout=60
    )
    if r.status_code in (401, 403) and not retried:
        logger.warning('Token expired — refreshing...')
        refresh_token()
        return _post(path, params, retried=True)
    return r


def _gql(query: str, variables: dict = None, retried=False):
    time.sleep(RATE_LIMIT)
    load_dotenv(override=True)
    token = get_token()
    r = requests.post(
        f'{BASE_SEARCH}/graphql',
        params={'token': token,
                'groupId': os.getenv('WYSCOUT_GROUP_ID', '1059060'),
                'subgroupId': os.getenv('WYSCOUT_SUBGROUP_ID', '93476')},
        json={'query': query, 'variables': variables or {}},
        headers={**HEADERS, 'Content-Type': 'application/json'},
        timeout=30
    )
    if r.status_code in (401, 403) and not retried:
        refresh_token()
        return _gql(query, variables, retried=True)
    return r


# ─────────────────────────────────────────────────
# DISCOVERY METHODS
# ─────────────────────────────────────────────────

def get_season_id(competition_id: int) -> tuple:
    """Returns (current_season_id, previous_season_id)."""
    r = _gql('''
        query($compId: ID!) {
            competitions(id: [$compId]) {
                currentSeason { id name }
                previousSeason { id name }
            }
        }
    ''', {'compId': competition_id})
    data = r.json()['data']['competitions'][0]
    curr = data['currentSeason']['id']
    prev = data['previousSeason']['id']
    logger.info(f'Competition {competition_id}: '
                f'current={curr}, previous={prev}')
    return curr, prev


def get_all_players_for_season(competition_id: int,
                                season_id: int,
                                limit: int = 1000) -> list:
    """
    Get all players in a competition season via GraphQL.
    Returns list of {playerId, name, position, team, minutes}.
    """
    r = _gql('''
        query($seasonIds: [ID!]!, $limit: Int!) {
            playersLeaderboard(
                param: minutes_on_field
                seasonIds: $seasonIds
                limit: $limit
            ) {
                playerId
                name
                primaryPosition
                minutesOnField
                goals
                assists
                age
                teams { id name }
            }
        }
    ''', {'seasonIds': [season_id], 'limit': limit})

    data = r.json()
    if 'errors' in data:
        logger.error(f'GraphQL error: {data["errors"]}')
        return []

    players = data.get('data', {}).get('playersLeaderboard', [])
    logger.info(f'Competition {competition_id} season {season_id}: '
                f'{len(players)} players found')
    return players


def get_all_teams_for_season(competition_id: int,
                              season_id: int) -> list:
    """Get all teams in a competition season."""
    r = _gql('''
        query($seasonIds: [ID!]!) {
            teamsLeaderboard(
                param: goal
                seasonIds: $seasonIds
                limit: 100
            ) {
                teamId
                name
            }
        }
    ''', {'seasonIds': [season_id]})
    data = r.json()
    return data.get('data', {}).get('teamsLeaderboard', [])


# ─────────────────────────────────────────────────
# DOWNLOAD METHODS
# ─────────────────────────────────────────────────

def download_player_xlsx(wyscout_player_id: int,
                         save_path: Path,
                         max_retries: int = 3) -> bool:
    """
    Download player match stats xlsx.
    Returns True if successful.
    Skips if file already exists and is valid.
    Retries on timeout/network errors.
    """
    if save_path.exists() and save_path.stat().st_size > 2000:
        logger.info(f'SKIP {save_path.name} — exists')
        return True

    save_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            load_dotenv(override=True)
            token = get_token()

            r = _post(
                f'/api/v1/match_stats/players/{wyscout_player_id}.xlsx',
                params={
                    'token': token,
                    'groupId': os.getenv('WYSCOUT_GROUP_ID', '1059060'),
                    'subgroupId': os.getenv('WYSCOUT_SUBGROUP_ID', '93476'),
                }
            )

            if r.status_code != 200 or len(r.content) < 2000:
                logger.error(f'Failed player {wyscout_player_id}: '
                             f'HTTP {r.status_code}, size {len(r.content)}')
                return False

            save_path.write_bytes(r.content)
            logger.info(f'SAVED player {wyscout_player_id} → '
                        f'{save_path.name} ({len(r.content)//1024}KB)')
            return True

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            logger.warning(f'Attempt {attempt}/{max_retries} failed for '
                           f'{wyscout_player_id}: {e}')
            if attempt < max_retries:
                time.sleep(5 * attempt)
            else:
                logger.error(f'Gave up on player {wyscout_player_id} '
                             f'after {max_retries} attempts')
                return False


def get_team_stats_json(wyscout_team_id: int,
                        date_from: str = '2019-01-01',
                        date_to: str = '2026-12-31') -> dict:
    """
    Get team match stats as JSON.
    (XLSX endpoint returns 500 — JSON works fine)
    """
    r = _get(
        f'/api/v1/team_stats/teams/{wyscout_team_id}/stats',
        extra_params={
            'from': date_from,
            'to': date_to,
            'score': 'winning,draw,losing',
            'venue': 'home,away',
            'columns': ('name,team,schemes,goal,xgShot,shot,'
                        'shotSuccess,pass,passSuccess,possession,'
                        'loss,lossLow,lossMedium,lossHigh,'
                        'recovery,recoveryLow,recoveryMedium,'
                        'recoveryHigh,duel,duelSuccess,'
                        'interception,clearance,ppda,'
                        'progressivePass,deepPass,'
                        'touchInBox,offensiveDuel'),
        }
    )
    if r.status_code == 200:
        return r.json()
    logger.error(f'Team stats failed: {r.status_code}')
    return {}
