"""
Scheduled Job Definitions for Football Data ETL Pipeline.

Contains all job functions that can be scheduled:
- FotMob collection jobs (primary, no API limit)
- API-Football collection jobs (secondary, 100 req/day limit)
- Priority-based collection
- Current season updates
"""

import os
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from utils.season_utils import SeasonUtils

logger = logging.getLogger(__name__)


def get_current_api_football_season() -> int:
    """Get current season year for API-Football (uses single year format)."""
    return SeasonUtils.to_single_year(SeasonUtils.get_current_season())


# ============================================
# FOTMOB JOBS (Primary Source - No API Limit)
# ============================================

def fotmob_daily_collection_job(
    leagues: Optional[List[str]] = None,
    season: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Daily FotMob collection - standings and teams for all leagues.

    No API key or daily limit. Safe to run frequently.

    Args:
        leagues: League keys to collect (default: all 8 leagues)
        season: Season in DB format (e.g., '2024-25'). Defaults to current.

    Returns:
        Collection statistics
    """
    from etl.fotmob_etl import FotMobETL
    from scrapers.fotmob.constants import ALL_LEAGUE_KEYS

    logger.info(f"Starting FotMob daily collection at {datetime.now()}")

    if leagues is None:
        leagues = ALL_LEAGUE_KEYS

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'fotmob_daily',
        'leagues_processed': [],
        'total_teams': 0,
        'total_team_season_stats': 0,
        'errors': []
    }

    try:
        with FotMobETL() as etl:
            for league_key in leagues:
                try:
                    logger.info(f"FotMob: Processing {league_key}")
                    stats = etl.process_league_season(league_key, season)

                    results['leagues_processed'].append(league_key)
                    results['total_teams'] += stats.get('teams', 0)
                    results['total_team_season_stats'] += stats.get('team_season_stats', 0)

                    logger.info(f"FotMob: Completed {league_key}: {stats}")

                except Exception as e:
                    logger.error(f"FotMob: Error processing {league_key}: {e}")
                    results['errors'].append({
                        'league': league_key,
                        'error': str(e)
                    })

            # Include client stats
            final = etl.get_statistics()
            results['api_requests'] = final.get('api_requests', 0)

    except Exception as e:
        logger.error(f"FotMob daily collection failed: {e}")
        results['errors'].append({'error': str(e)})

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"FotMob daily collection completed: {results}")
    return results


def fotmob_deep_collection_job(
    league_key: str,
    season: Optional[str] = None,
    max_matches: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Deep FotMob collection for a single league.

    Collects standings + team squads + match details + player stats.
    Takes longer due to individual API calls per entity.

    Args:
        league_key: League to collect (e.g., 'premier-league')
        season: Season in DB format. Defaults to current.
        max_matches: Limit matches processed (for testing)

    Returns:
        Collection statistics
    """
    from etl.fotmob_etl import FotMobETL

    logger.info(f"Starting FotMob deep collection for {league_key} at {datetime.now()}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'fotmob_deep',
        'league': league_key,
        'season': season or 'current',
    }

    try:
        with FotMobETL() as etl:
            # 1. Basic league data
            league_stats = etl.process_league_season(league_key, season)
            results['standings'] = league_stats

            # 2. Deep team squads
            team_stats = etl.process_league_teams_deep(league_key)
            results['squads'] = team_stats

            # 3. Deep match data
            match_stats = etl.process_league_matches_deep(
                league_key, season, max_matches
            )
            results['matches'] = match_stats

            # Final stats
            final = etl.get_statistics()
            results['total_api_requests'] = final.get('api_requests', 0)
            results['total_errors'] = final.get('errors', 0)

    except Exception as e:
        logger.error(f"FotMob deep collection failed for {league_key}: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"FotMob deep collection completed for {league_key}: {results}")
    return results


def fotmob_weekly_deep_job(
    season: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Weekly deep collection for all leagues.

    Runs deep collection across all 8 leagues. Best scheduled
    on weekends when most matchdays are complete.

    Args:
        season: Season in DB format. Defaults to current.

    Returns:
        Aggregated statistics
    """
    from scrapers.fotmob.constants import ALL_LEAGUE_KEYS

    logger.info(f"Starting FotMob weekly deep collection at {datetime.now()}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'fotmob_weekly_deep',
        'leagues': {},
        'errors': []
    }

    for league_key in ALL_LEAGUE_KEYS:
        try:
            league_result = fotmob_deep_collection_job(league_key, season)
            results['leagues'][league_key] = {
                'status': 'success',
                'api_requests': league_result.get('total_api_requests', 0),
            }
        except Exception as e:
            logger.error(f"Weekly deep: Error with {league_key}: {e}")
            results['leagues'][league_key] = {'status': 'error', 'error': str(e)}
            results['errors'].append({'league': league_key, 'error': str(e)})

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"FotMob weekly deep collection completed")
    return results


# ============================================
# API-FOOTBALL JOBS (Secondary Source - 100 req/day)
# ============================================


def daily_collection_job(
    leagues: Optional[List[str]] = None,
    season: Optional[int] = None,
    max_requests: int = 90
) -> Dict[str, Any]:
    """
    Daily data collection job - collects data within API limits.

    This job is designed to run once per day and collect data
    for prioritized leagues while staying within the 100 req/day limit.

    Args:
        leagues: List of league keys to collect (default: all 5 major leagues)
        season: Season year (default: current season 2024)
        max_requests: Maximum requests to use (default: 90, leaving buffer)

    Returns:
        Collection statistics
    """
    from database.connection import get_db
    from etl.api_football_etl import APIFootballETL
    from scrapers.api_football.client import LEAGUE_IDS
    from utils.api_tracker import get_tracker

    logger.info(f"Starting daily collection job at {datetime.now()}")

    # Get API key
    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        logger.error("API_FOOTBALL_KEY not set")
        return {'error': 'API key not configured'}

    # Initialize tracker
    tracker = get_tracker()

    # Hard quota guard — never exceed 85 requests/day (leaves 15 buffer)
    DAILY_HARD_LIMIT = 85
    used_today = tracker.get_requests_today() if tracker else 0
    if used_today >= DAILY_HARD_LIMIT:
        logger.warning(f"Hard quota limit reached: {used_today}/{DAILY_HARD_LIMIT} used today. Skipping collection.")
        return {'skipped': True, 'reason': 'quota_limit', 'used': used_today, 'limit': DAILY_HARD_LIMIT}

    # Check if we have quota remaining
    remaining = tracker.get_remaining_requests()
    if remaining < 10:
        logger.warning(f"Insufficient quota remaining: {remaining}")
        return {'error': 'Daily quota exhausted', 'remaining': remaining}

    # Default leagues in priority order
    if leagues is None:
        leagues = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

    # Default to current season (dynamically calculated)
    if season is None:
        season = get_current_api_football_season()

    # Initialize ETL
    db = get_db()
    etl = APIFootballETL(api_key=api_key, db=db)

    results = {
        'job_start': datetime.now().isoformat(),
        'leagues_processed': [],
        'total_teams': 0,
        'total_matches': 0,
        'total_players': 0,
        'requests_used': 0,
        'errors': []
    }

    # Calculate requests per league (distribute evenly, prioritize first leagues)
    requests_per_league = max_requests // len(leagues)

    for league_key in leagues:
        # Check remaining quota before each league
        remaining = tracker.get_remaining_requests()
        if remaining < 5:
            logger.warning("Stopping collection - low quota")
            break

        try:
            logger.info(f"Processing {league_key} for season {season}")

            # Estimate cost: 1 (teams) + 1 (standings) + 1 (fixtures) = 3 base
            # Player collection is expensive, so we'll do it selectively

            stats = etl.process_league_season(league_key, season)

            results['leagues_processed'].append(league_key)
            results['total_teams'] += stats.get('teams', 0)
            results['total_matches'] += stats.get('matches', 0)
            results['total_players'] += stats.get('player_season_stats', 0)

            logger.info(f"Completed {league_key}: {stats}")

        except Exception as e:
            logger.error(f"Error processing {league_key}: {e}")
            results['errors'].append({
                'league': league_key,
                'error': str(e)
            })

    results['job_end'] = datetime.now().isoformat()
    results['requests_used'] = tracker.get_requests_today()

    logger.info(f"Daily collection completed: {results}")
    return results


def priority_collection_job(
    priority_leagues: Optional[List[str]] = None,
    collect_players: bool = False
) -> Dict[str, Any]:
    """
    Priority-based collection - focuses on most important data first.

    Designed for limited quota scenarios. Collects:
    1. Standings (league tables) - 1 request each
    2. Recent fixtures - 1 request each
    3. Players (optional) - expensive

    Args:
        priority_leagues: Leagues to collect (default: Premier League only)
        collect_players: Whether to collect player data (expensive)

    Returns:
        Collection statistics
    """
    from database.connection import get_db
    from scrapers.api_football.client import APIFootballClient, LEAGUE_IDS
    from utils.api_tracker import get_tracker

    logger.info(f"Starting priority collection job at {datetime.now()}")

    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        return {'error': 'API key not configured'}

    tracker = get_tracker()

    # Hard quota guard — never exceed 85 requests/day (leaves 15 buffer)
    DAILY_HARD_LIMIT = 85
    used_today = tracker.get_requests_today() if tracker else 0
    if used_today >= DAILY_HARD_LIMIT:
        logger.warning(f"Hard quota limit reached: {used_today}/{DAILY_HARD_LIMIT} used today. Skipping collection.")
        return {'skipped': True, 'reason': 'quota_limit', 'used': used_today, 'limit': DAILY_HARD_LIMIT}

    client = APIFootballClient(api_key=api_key)

    if priority_leagues is None:
        priority_leagues = ['premier-league']  # Default to PL only

    results = {
        'job_start': datetime.now().isoformat(),
        'standings_collected': [],
        'fixtures_collected': [],
        'requests_used': 0
    }

    season = get_current_api_football_season()  # Dynamically calculated

    for league_key in priority_leagues:
        if tracker.get_remaining_requests() < 3:
            logger.warning("Low quota - stopping")
            break

        api_league_id = LEAGUE_IDS.get(league_key)
        if not api_league_id:
            continue

        try:
            # Get standings (1 request)
            standings = client.get_standings(api_league_id, season)
            tracker.record_request('standings', {'league': league_key})
            if standings:
                results['standings_collected'].append(league_key)
                logger.info(f"Collected standings for {league_key}")

            # Get fixtures (1 request)
            fixtures = client.get_fixtures(api_league_id, season)
            tracker.record_request('fixtures', {'league': league_key})
            if fixtures:
                results['fixtures_collected'].append(league_key)
                logger.info(f"Collected fixtures for {league_key}")

        except Exception as e:
            logger.error(f"Error in priority collection for {league_key}: {e}")

    results['job_end'] = datetime.now().isoformat()
    results['requests_used'] = tracker.get_requests_today()

    return results


def update_current_season_job() -> Dict[str, Any]:
    """
    Quick update job for current season data only.

    Minimal request usage - just updates standings and recent matches.
    Designed to run multiple times per day if needed.

    Returns:
        Update statistics
    """
    from database.connection import get_db
    from scrapers.api_football.client import APIFootballClient, LEAGUE_IDS
    from database.batch_loader import BatchLoader
    from utils.api_tracker import get_tracker

    logger.info(f"Starting current season update at {datetime.now()}")

    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        return {'error': 'API key not configured'}

    tracker = get_tracker()

    # Hard quota guard — never exceed 85 requests/day (leaves 15 buffer)
    DAILY_HARD_LIMIT = 85
    used_today = tracker.get_requests_today() if tracker else 0
    if used_today >= DAILY_HARD_LIMIT:
        logger.warning(f"Hard quota limit reached: {used_today}/{DAILY_HARD_LIMIT} used today. Skipping collection.")
        return {'skipped': True, 'reason': 'quota_limit', 'used': used_today, 'limit': DAILY_HARD_LIMIT}

    # Only proceed if we have enough quota
    if tracker.get_remaining_requests() < 5:
        return {'error': 'Insufficient quota', 'remaining': tracker.get_remaining_requests()}

    client = APIFootballClient(api_key=api_key)
    db = get_db()
    batch_loader = BatchLoader(db=db)

    results = {
        'job_start': datetime.now().isoformat(),
        'leagues_updated': [],
        'requests_used': 0
    }

    # Update Premier League standings only (most important)
    current_season = get_current_api_football_season()
    try:
        standings = client.get_standings(LEAGUE_IDS['premier-league'], current_season)
        tracker.record_request('standings', {'league': 'premier-league'})

        if standings and len(standings) > 0:
            league_standings = standings[0].get('league', {}).get('standings', [[]])[0]
            logger.info(f"Retrieved {len(league_standings)} team standings")
            results['leagues_updated'].append('premier-league')

    except Exception as e:
        logger.error(f"Error updating current season: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    results['requests_used'] = tracker.get_requests_today()

    return results


def full_league_collection_job(
    league_key: str,
    season: int = 2024,
    include_players: bool = True
) -> Dict[str, Any]:
    """
    Complete data collection for a single league.

    Use this for initial data population or full refresh.
    WARNING: This is expensive (40+ requests with players).

    Args:
        league_key: League to collect (e.g., 'premier-league')
        season: Season year
        include_players: Whether to collect all player data

    Returns:
        Collection statistics
    """
    from database.connection import get_db
    from etl.api_football_etl import APIFootballETL
    from utils.api_tracker import get_tracker

    logger.info(f"Starting full collection for {league_key} season {season}")

    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        return {'error': 'API key not configured'}

    tracker = get_tracker()

    # Hard quota guard — never exceed 85 requests/day (leaves 15 buffer)
    DAILY_HARD_LIMIT = 85
    used_today = tracker.get_requests_today() if tracker else 0
    if used_today >= DAILY_HARD_LIMIT:
        logger.warning(f"Hard quota limit reached: {used_today}/{DAILY_HARD_LIMIT} used today. Skipping collection.")
        return {'skipped': True, 'reason': 'quota_limit', 'used': used_today, 'limit': DAILY_HARD_LIMIT}

    remaining = tracker.get_remaining_requests()

    # Estimate: 3 base + 40 for players = ~43 requests
    estimated_cost = 43 if include_players else 3
    if remaining < estimated_cost:
        return {
            'error': f'Insufficient quota. Need ~{estimated_cost}, have {remaining}',
            'remaining': remaining
        }

    db = get_db()
    etl = APIFootballETL(api_key=api_key, db=db)

    try:
        stats = etl.process_league_season(league_key, season)

        return {
            'job_start': datetime.now().isoformat(),
            'league': league_key,
            'season': season,
            'stats': stats,
            'requests_used': tracker.get_requests_today(),
            'job_end': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Full collection failed: {e}")
        return {'error': str(e)}


def api_football_supplementary_job(
    leagues: Optional[List[str]] = None,
    season: int = 2024,
) -> Dict[str, Any]:
    """
    Supplementary API-Football collection for data not in FotMob.

    Collects:
    - Penalty stats (won, committed, scored, missed, saved)
    - Substitution stats (in, out, bench)
    - Player weight and birth details
    - Stadium/venue information (capacity, surface)
    - Team founded year
    - Form string (WWLDW)

    This job enriches existing data from FotMob with additional fields.
    Designed to use ~50-80 requests, staying within 100/day limit.

    Args:
        leagues: Leagues to process (default: top 5 European)
        season: Season year

    Returns:
        Enrichment statistics
    """
    from database.connection import get_db
    from scrapers.api_football.client import APIFootballClient, LEAGUE_IDS
    from utils.api_tracker import get_tracker

    logger.info(f"Starting API-Football supplementary job at {datetime.now()}")

    api_key = os.getenv('API_FOOTBALL_KEY')
    if not api_key:
        logger.warning("API_FOOTBALL_KEY not set - skipping supplementary collection")
        return {'error': 'API key not configured'}

    tracker = get_tracker()

    # Hard quota guard — never exceed 85 requests/day (leaves 15 buffer)
    DAILY_HARD_LIMIT = 85
    used_today = tracker.get_requests_today() if tracker else 0
    if used_today >= DAILY_HARD_LIMIT:
        logger.warning(f"Hard quota limit reached: {used_today}/{DAILY_HARD_LIMIT} used today. Skipping collection.")
        return {'skipped': True, 'reason': 'quota_limit', 'used': used_today, 'limit': DAILY_HARD_LIMIT}

    remaining = tracker.get_remaining_requests()

    if remaining < 20:
        logger.warning(f"Low quota ({remaining}) - skipping supplementary job")
        return {'error': 'Insufficient quota', 'remaining': remaining}

    if leagues is None:
        leagues = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

    client = APIFootballClient(api_key=api_key)
    db = get_db()

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'api_football_supplementary',
        'teams_enriched': 0,
        'players_enriched': 0,
        'venues_enriched': 0,
        'errors': []
    }

    for league_key in leagues:
        if tracker.get_remaining_requests() < 5:
            logger.warning("Low quota - stopping supplementary collection")
            break

        api_league_id = LEAGUE_IDS.get(league_key)
        if not api_league_id:
            continue

        try:
            # Get teams with venue info (1 request)
            teams = client.get_teams(api_league_id, season)
            tracker.record_request('teams', {'league': league_key})

            if teams:
                for team_data in teams:
                    team_info = team_data.get('team', {})
                    venue_info = team_data.get('venue', {})

                    team_name = team_info.get('name')
                    if not team_name:
                        continue

                    # Update team with supplementary data
                    query = """
                        UPDATE teams SET
                            team_code = COALESCE(:code, team_code),
                            logo_url = COALESCE(:logo, logo_url),
                            founded_year = COALESCE(:founded, founded_year),
                            stadium = COALESCE(:stadium, stadium),
                            stadium_capacity = COALESCE(:capacity, stadium_capacity),
                            stadium_surface = COALESCE(:surface, stadium_surface),
                            stadium_city = COALESCE(:city, stadium_city),
                            api_football_id = COALESCE(:api_id, api_football_id),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE team_name = :name
                    """
                    db.execute_query(query, {
                        'name': team_name,
                        'code': team_info.get('code'),
                        'logo': team_info.get('logo'),
                        'founded': team_info.get('founded'),
                        'stadium': venue_info.get('name'),
                        'capacity': venue_info.get('capacity'),
                        'surface': venue_info.get('surface'),
                        'city': venue_info.get('city'),
                        'api_id': team_info.get('id'),
                    }, fetch=False)

                    results['teams_enriched'] += 1
                    results['venues_enriched'] += 1

            # Get standings with form string (1 request)
            standings = client.get_standings(api_league_id, season)
            tracker.record_request('standings', {'league': league_key})

            if standings and len(standings) > 0:
                league_standings = standings[0].get('league', {}).get('standings', [[]])[0]

                for team_standing in league_standings:
                    team_name = team_standing.get('team', {}).get('name')
                    form = team_standing.get('form')  # e.g., "WWDLW"

                    if team_name and form:
                        # Store form in team_season_stats or a new column
                        # For now, log it - could add form column to schema
                        logger.debug(f"Form for {team_name}: {form}")

            logger.info(f"Enriched teams for {league_key}")

        except Exception as e:
            logger.error(f"Error in supplementary collection for {league_key}: {e}")
            results['errors'].append({'league': league_key, 'error': str(e)})

    results['job_end'] = datetime.now().isoformat()
    results['requests_used'] = tracker.get_requests_today()

    logger.info(f"API-Football supplementary job completed: {results}")
    return results


# ============================================
# STATSBOMB JOBS (Historical Progressive Data)
# ============================================

def statsbomb_collection_job(
    competition_id: int,
    season_id: int,
) -> Dict[str, Any]:
    """
    StatsBomb advanced stats collection.

    Extracts progressive actions, pressing stats, and SCA/GCA from
    StatsBomb open data. Use for historical analysis and model training.

    Note: StatsBomb open data covers specific competitions only
    (e.g., La Liga, FA WSL, select international tournaments).

    Args:
        competition_id: StatsBomb competition ID
        season_id: StatsBomb season ID

    Returns:
        Collection statistics
    """
    from etl.statsbomb_advanced_etl import StatsBombAdvancedETL

    logger.info(f"Starting StatsBomb collection for comp {competition_id}, season {season_id}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'statsbomb_advanced',
        'competition_id': competition_id,
        'season_id': season_id,
    }

    try:
        with StatsBombAdvancedETL() as etl:
            stats = etl.process_competition(competition_id, season_id)
            results.update(stats)

    except Exception as e:
        logger.error(f"StatsBomb collection failed: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"StatsBomb collection completed: {results}")
    return results


def statsbomb_list_competitions_job() -> List[Dict]:
    """
    List available StatsBomb competitions.

    Returns list of available competitions with IDs.
    """
    from etl.statsbomb_advanced_etl import StatsBombAdvancedETL

    logger.info("Fetching StatsBomb available competitions")

    try:
        with StatsBombAdvancedETL() as etl:
            return etl.get_available_competitions()
    except Exception as e:
        logger.error(f"Failed to get StatsBomb competitions: {e}")
        return []


# ============================================
# UNDERSTAT JOBS (xA, npxG, xGChain)
# ============================================

def understat_collection_job(
    leagues: Optional[List[str]] = None,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Understat xG metrics collection.

    Extracts xA, npxG, xGChain, xGBuildup from understat.com.
    These metrics are critical for modern scouting but not available
    from FotMob or API-Football.

    Supported leagues: premier-league, la-liga, serie-a, bundesliga, ligue-1

    Args:
        leagues: Leagues to collect (default: all 5)
        season: Season start year (default: current)

    Returns:
        Collection statistics
    """
    from etl.understat_etl import UnderstatETL

    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    if leagues is None:
        leagues = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

    logger.info(f"Starting Understat collection for {leagues} season {season}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'understat',
        'season': season,
        'leagues_processed': [],
    }

    try:
        with UnderstatETL() as etl:
            for league in leagues:
                try:
                    etl.process_league_season(league, season)
                    results['leagues_processed'].append(league)
                    logger.info(f"Completed Understat collection for {league}")
                except Exception as e:
                    logger.error(f"Error processing {league}: {e}")
                    results.setdefault('errors', []).append({
                        'league': league,
                        'error': str(e)
                    })

            stats = etl.get_statistics()
            results.update(stats)

    except Exception as e:
        logger.error(f"Understat collection failed: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"Understat collection completed: {results}")
    return results


def understat_enrich_players_job(
    league: str,
    season: int,
) -> Dict[str, Any]:
    """
    Enrich player match records with Understat per-match xG data.

    This is more detailed than the season summary - provides xG/xA
    for each individual match a player participated in.

    Args:
        league: League key
        season: Season start year

    Returns:
        Enrichment statistics
    """
    from etl.understat_etl import UnderstatETL
    from scrapers.understat.client import UnderstatClient

    logger.info(f"Starting Understat player enrichment for {league} {season}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'understat_enrich',
        'league': league,
        'season': season,
        'players_enriched': 0,
        'matches_enriched': 0,
    }

    try:
        client = UnderstatClient()
        players = client.get_league_players(league, season)

        with UnderstatETL() as etl:
            for player in players[:50]:  # Limit to avoid too many requests
                understat_id = player.get('understat_id')
                player_name = player.get('name')

                if not understat_id:
                    continue

                # Find player in our DB
                query = "SELECT player_id FROM players WHERE understat_id = :uid OR player_name = :name"
                result = etl.db.execute_query(query, {'uid': understat_id, 'name': player_name}, fetch=True)

                if result:
                    db_player_id = result[0][0]
                    matches = etl.enrich_player_matches(db_player_id, understat_id)
                    results['players_enriched'] += 1
                    results['matches_enriched'] += matches

    except Exception as e:
        logger.error(f"Understat enrichment failed: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"Understat enrichment completed: {results}")
    return results


# ============================================
# SOFASCORE JOBS (Deep Stats - 112 fields)
# ============================================

def sofascore_collection_job(
    leagues: Optional[List[str]] = None,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    """
    SofaScore deep stats collection job.

    Enriches player_season_stats with 112 fields including aerial duels,
    ground duels, ball recovery, tackles won, and more.

    Supported leagues: premier-league, la-liga, serie-a, bundesliga, ligue-1
    """
    from etl.sofascore_etl import SofaScoreETL

    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    if leagues is None:
        leagues = ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1']

    logger.info(f"Starting SofaScore collection for {leagues} season {season}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'sofascore',
        'season': season,
        'leagues_processed': [],
        'errors': [],
    }

    try:
        with SofaScoreETL() as etl:
            for league in leagues:
                try:
                    result = etl.process_league_season(league, season)
                    results['leagues_processed'].append(league)
                    logger.info(f"SofaScore {league}: {result}")
                except Exception as e:
                    logger.error(f"SofaScore error for {league}: {e}")
                    results['errors'].append({'league': league, 'error': str(e)})
            results.update(etl.get_statistics())
    except Exception as e:
        logger.error(f"SofaScore collection failed: {e}")
        results['error'] = str(e)

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"SofaScore collection complete: {results}")
    return results


# ============================================
# COMBINED/ORCHESTRATION JOBS
# ============================================

def full_data_refresh_job(
    season: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Full data refresh from all sources.

    Orchestrates collection from:
    1. FotMob (primary) - unlimited
    2. API-Football (supplementary) - 100/day
    3. Understat (xG enrichment) - scraped

    Best run weekly or for initial data population.

    Args:
        season: Season year (default: current)

    Returns:
        Combined statistics from all sources
    """
    if season is None:
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

    logger.info(f"Starting full data refresh for season {season}")

    results = {
        'job_start': datetime.now().isoformat(),
        'job_type': 'full_refresh',
        'season': season,
        'sources': {}
    }

    # 1. FotMob (primary)
    try:
        fotmob_result = fotmob_weekly_deep_job(f"{season}-{str(season+1)[-2:]}")
        results['sources']['fotmob'] = fotmob_result
    except Exception as e:
        logger.error(f"FotMob collection failed: {e}")
        results['sources']['fotmob'] = {'error': str(e)}

    # 2. API-Football supplementary
    try:
        api_result = api_football_supplementary_job(season=season)
        results['sources']['api_football'] = api_result
    except Exception as e:
        logger.error(f"API-Football supplementary failed: {e}")
        results['sources']['api_football'] = {'error': str(e)}

    # 3. Understat xG data
    try:
        understat_result = understat_collection_job(season=season)
        results['sources']['understat'] = understat_result
    except Exception as e:
        logger.error(f"Understat collection failed: {e}")
        results['sources']['understat'] = {'error': str(e)}

    results['job_end'] = datetime.now().isoformat()
    logger.info(f"Full data refresh completed")
    return results
