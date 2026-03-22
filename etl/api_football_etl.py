"""
ETL pipeline for API-Football data.

Optimized for 100 requests/day limit with:
- API-Football ID storage to avoid redundant lookups
- Request tracking integration
- Efficient batch operations
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

from database.connection import get_db
from database.batch_loader import BatchLoader
from scrapers.api_football.client import APIFootballClient, LEAGUE_IDS

logger = logging.getLogger(__name__)


# Mapping from league key to display name
LEAGUE_NAMES = {
    'premier-league': 'Premier League',
    'la-liga': 'La Liga',
    'serie-a': 'Serie A',
    'bundesliga': 'Bundesliga',
    'ligue-1': 'Ligue 1'
}

# Reverse mapping from DB league_id to league key
DB_LEAGUE_ID_TO_KEY = {
    1: 'premier-league',
    2: 'la-liga',
    3: 'serie-a',
    4: 'bundesliga',
    5: 'ligue-1'
}


class APIFootballETL:
    """
    ETL pipeline for API-Football data.

    Features:
    - Stores API-Football IDs to avoid redundant API calls
    - Tracks API usage to respect 100/day limit
    - Efficient batch upsert operations
    - Caches team API IDs in memory during session
    """

    def __init__(self, api_key: Optional[str] = None, db=None):
        """
        Initialize API-Football ETL.

        Args:
            api_key: API-Football API key
            db: Database connection
        """
        self.client = APIFootballClient(api_key=api_key)
        self.db = db or get_db()
        self.batch_loader = BatchLoader(db=self.db)
        self.source_id = self._get_or_create_source()

        # In-memory cache for API IDs (reduces DB lookups)
        self._team_api_id_cache: Dict[int, int] = {}  # db_team_id -> api_team_id
        self._player_api_id_cache: Dict[int, int] = {}  # db_player_id -> api_player_id

    def _get_or_create_source(self) -> int:
        """Get or create API-Football data source."""
        result = self.db.execute_query(
            "SELECT source_id FROM data_sources WHERE source_name = 'api_football'",
            fetch=True
        )

        if result:
            return result[0][0]

        result = self.db.execute_query(
            """
            INSERT INTO data_sources (source_name, base_url, reliability_score)
            VALUES ('api_football', 'https://api-football-v1.p.rapidapi.com', 98)
            RETURNING source_id
            """,
            fetch=True
        )
        return result[0][0]

    def _track_request(self, endpoint: str, league_key: str = None):
        """Track API request for quota management."""
        try:
            from utils.api_tracker import get_tracker
            tracker = get_tracker()
            params = {'league': league_key} if league_key else None
            tracker.record_request(endpoint, params)
        except ImportError:
            # Tracker not available, continue without tracking
            pass

    def process_league_season(
        self,
        league_key: str,
        season: int,
        include_players: bool = True
    ) -> Dict[str, int]:
        """
        Process complete data for a league/season.

        Args:
            league_key: League key (e.g., 'premier-league')
            season: Season year (e.g., 2024)
            include_players: Whether to fetch player data (expensive: ~40 requests)

        Returns:
            Statistics dictionary
        """
        logger.info(f"Processing {league_key} - {season} (include_players={include_players})")

        stats = {
            'teams': 0,
            'matches': 0,
            'players': 0,
            'team_season_stats': 0,
            'player_season_stats': 0,
            'requests_used': 0
        }

        if not isinstance(LEAGUE_IDS, dict):
            logger.error(f"LEAGUE_IDS is not a dict: {type(LEAGUE_IDS)}")
            return stats

        league_id_api = LEAGUE_IDS.get(league_key)
        if not league_id_api:
            logger.error(f"Unknown league: {league_key} (Available: {list(LEAGUE_IDS.keys())})")
            return stats

        # Get database league and season IDs
        league_id_db = self._get_league_id(league_key)
        season_id_db = self._get_season_id(f"{season}-{season+1-2000}")  # e.g., "2024-25"

        if not league_id_db or not season_id_db:
            logger.error(f"League or season not found in database")
            return stats

        # Store the API league ID in database if not already stored
        self._store_league_api_id(league_id_db, league_id_api)

        # 1. Process teams (with API ID storage)
        logger.info("Fetching teams...")
        teams_data = self.client.get_teams(league_id_api, season)
        self._track_request('teams', league_key)
        stats['requests_used'] += 1

        if teams_data:
            teams_count = self._process_teams_with_api_ids(teams_data, league_id_db)
            stats['teams'] = teams_count

        # 2. Process standings
        logger.info("Fetching standings...")
        standings_data = self.client.get_standings(league_id_api, season)
        self._track_request('standings', league_key)
        stats['requests_used'] += 1

        if standings_data and len(standings_data) > 0:
            standings = standings_data[0].get('league', {}).get('standings', [[]])[0]
            team_stats_count = self._process_standings(standings, league_id_db, season_id_db)
            stats['team_season_stats'] = team_stats_count

        # 3. Process fixtures/matches (with API fixture ID storage)
        logger.info("Fetching fixtures...")
        fixtures = self.client.get_fixtures(league_id_api, season)
        self._track_request('fixtures', league_key)
        stats['requests_used'] += 1

        if fixtures:
            matches_count = self._process_fixtures_with_api_ids(fixtures, league_id_db, season_id_db)
            stats['matches'] = matches_count

        # 4. Process players (expensive - uses stored API IDs)
        if include_players:
            logger.info("Fetching players using stored API IDs...")
            player_stats_count, player_requests = self._process_players_optimized(
                league_id_db, season_id_db, season
            )
            stats['player_season_stats'] = player_stats_count
            stats['requests_used'] += player_requests

        logger.info(f"Completed {league_key} - {season}: {stats}")
        return stats

    def _store_league_api_id(self, league_id_db: int, api_id: int):
        """Store API-Football league ID in database."""
        try:
            self.db.execute_query(
                """
                UPDATE leagues SET api_football_id = :api_id
                WHERE league_id = :db_id AND (api_football_id IS NULL OR api_football_id != :api_id)
                """,
                params={'api_id': api_id, 'db_id': league_id_db},
                fetch=False
            )
        except Exception as e:
            logger.warning(f"Could not store league API ID: {e}")

    def _process_teams_with_api_ids(
        self,
        teams_response: List[Dict],
        league_id: int
    ) -> int:
        """Process teams data and store API-Football IDs."""
        teams_data = []

        for item in teams_response:
            team_info = item.get('team', {})
            venue_info = item.get('venue', {})
            api_team_id = team_info.get('id')

            teams_data.append({
                'team_name': team_info.get('name'),
                'league_id': league_id,
                'stadium': venue_info.get('name'),
                'api_football_id': api_team_id,  # Store API ID
            })

        # Batch upsert teams with API ID
        teams_count = self.batch_loader.batch_upsert(
            'teams',
            teams_data,
            conflict_columns=['team_name', 'league_id'],
            update_columns=['stadium', 'api_football_id']
        )

        # Refresh team API ID cache
        self._refresh_team_api_id_cache(league_id)

        return teams_count

    def _refresh_team_api_id_cache(self, league_id: int):
        """Load team API IDs into memory cache."""
        result = self.db.execute_query(
            """
            SELECT team_id, api_football_id
            FROM teams
            WHERE league_id = :lid AND api_football_id IS NOT NULL
            """,
            params={'lid': league_id},
            fetch=True
        )

        for row in result:
            self._team_api_id_cache[row[0]] = row[1]

        logger.debug(f"Cached {len(result)} team API IDs for league {league_id}")

    def _process_standings(
        self,
        standings: List[Dict],
        league_id: int,
        season_id: int
    ) -> int:
        """Process league standings."""
        team_stats_data = []

        for standing in standings:
            team_info = standing.get('team', {})
            team_name = team_info.get('name')
            api_team_id = team_info.get('id')

            # Get team ID - try by API ID first, then by name
            team_id = self._get_team_id_by_api_id(api_team_id, league_id)
            if not team_id:
                team_id = self._get_team_id(team_name, league_id)

            if not team_id:
                logger.warning(f"Team not found: {team_name}")
                continue

            # Prepare team season stats
            all_stats = standing.get('all', {})
            stats_record = {
                'team_id': team_id,
                'season_id': season_id,
                'league_id': league_id,
                'matches_played': all_stats.get('played', 0),
                'wins': all_stats.get('win', 0),
                'draws': all_stats.get('draw', 0),
                'losses': all_stats.get('lose', 0),
                'goals_for': all_stats.get('goals', {}).get('for', 0),
                'goals_against': all_stats.get('goals', {}).get('against', 0),
                'goal_difference': standing.get('goalsDiff', 0),
                'points': standing.get('points', 0),
                'league_position': standing.get('rank', 0),
                'data_source_id': self.source_id,
            }
            team_stats_data.append(stats_record)

        stats_count = self.batch_loader.batch_upsert(
            'team_season_stats',
            team_stats_data,
            conflict_columns=['team_id', 'season_id', 'league_id']
        )

        return stats_count

    def _process_fixtures_with_api_ids(
        self,
        fixtures: List[Dict],
        league_id: int,
        season_id: int
    ) -> int:
        """Process fixtures/matches with API fixture ID storage."""
        matches_data = []

        for fixture in fixtures:
            fixture_info = fixture.get('fixture', {})
            teams = fixture.get('teams', {})
            goals = fixture.get('goals', {})

            api_fixture_id = fixture_info.get('id')
            home_api_id = teams.get('home', {}).get('id')
            away_api_id = teams.get('away', {}).get('id')

            # Get team IDs using API IDs (fast lookup)
            home_team_id = self._get_team_id_by_api_id(home_api_id, league_id)
            away_team_id = self._get_team_id_by_api_id(away_api_id, league_id)

            # Fallback to name lookup
            if not home_team_id:
                home_team_id = self._get_team_id(teams.get('home', {}).get('name'), league_id)
            if not away_team_id:
                away_team_id = self._get_team_id(teams.get('away', {}).get('name'), league_id)

            if not home_team_id or not away_team_id:
                continue

            # Parse match date
            match_date_str = fixture_info.get('date')
            match_date = None
            if match_date_str:
                try:
                    match_date = datetime.fromisoformat(match_date_str.replace('Z', '+00:00')).date()
                except:
                    pass

            match_record = {
                'league_id': league_id,
                'season_id': season_id,
                'match_date': match_date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'home_score': goals.get('home'),
                'away_score': goals.get('away'),
                'venue': fixture_info.get('venue', {}).get('name'),
                'referee': fixture_info.get('referee'),
                'match_status': 'completed' if fixture_info.get('status', {}).get('short') == 'FT' else 'scheduled',
                'api_football_fixture_id': api_fixture_id,  # Store API fixture ID
                'data_source_id': self.source_id
            }

            matches_data.append(match_record)

        # Batch upsert matches
        matches_count = self.batch_loader.batch_upsert(
            'matches',
            matches_data,
            conflict_columns=['league_id', 'season_id', 'home_team_id', 'away_team_id', 'match_date'],
            update_columns=['home_score', 'away_score', 'match_status', 'api_football_fixture_id']
        )

        return matches_count

    def _process_players_optimized(
        self,
        league_id: int,
        season_id: int,
        season_year: int
    ) -> tuple:
        """
        Process players using stored API team IDs (no extra API call for team IDs).

        Returns:
            Tuple of (players_processed, requests_used)
        """
        total_players = 0
        requests_used = 0

        # Get teams with their stored API IDs (no API call needed!)
        teams = self.db.execute_query(
            """
            SELECT team_id, team_name, api_football_id
            FROM teams
            WHERE league_id = :lid AND api_football_id IS NOT NULL
            """,
            params={'lid': league_id},
            fetch=True
        )

        if not teams:
            logger.warning(f"No teams with API IDs found for league {league_id}")
            # Fallback: need to fetch from API (1 request)
            league_key = DB_LEAGUE_ID_TO_KEY.get(league_id)
            if league_key:
                api_league_id = LEAGUE_IDS.get(league_key)
                teams_data = self.client.get_teams(api_league_id, season_year) or []
                self._track_request('teams', league_key)
                requests_used += 1

                # Process and store
                self._process_teams_with_api_ids(teams_data, league_id)

                # Re-query
                teams = self.db.execute_query(
                    """
                    SELECT team_id, team_name, api_football_id
                    FROM teams
                    WHERE league_id = :lid AND api_football_id IS NOT NULL
                    """,
                    params={'lid': league_id},
                    fetch=True
                )

        # Now fetch players for each team using stored API IDs
        for team_row in teams:
            db_team_id, team_name, api_team_id = team_row

            logger.info(f"Fetching players for {team_name} (API ID: {api_team_id})...")

            page = 1
            while True:
                resp = self.client.get_players(season=season_year, team_id=api_team_id, page=page)
                self._track_request('players', team_name)
                requests_used += 1

                if not resp:
                    break

                players_list = resp.get('response', [])
                if not players_list:
                    break

                self._process_player_batch_with_api_ids(players_list, league_id, season_id)
                total_players += len(players_list)

                paging = resp.get('paging', {})
                if paging.get('current', 1) >= paging.get('total', 1):
                    break
                page += 1

        return total_players, requests_used

    def _process_player_batch_with_api_ids(
        self,
        players_list: List[Dict],
        league_id: int,
        season_id: int
    ):
        """Process a batch of players with API ID storage."""
        player_stats_data = []

        for item in players_list:
            player_info = item.get('player', {})
            statistics = item.get('statistics', [{}])[0]
            team_info = statistics.get('team', {})

            player_name = player_info.get('name')
            api_player_id = player_info.get('id')
            api_team_id = team_info.get('id')

            if not player_name:
                continue

            # Get or create player with API ID
            player_id = self._get_or_create_player_with_api_id(
                player_name=player_name,
                api_player_id=api_player_id,
                nationality=player_info.get('nationality'),
                position=statistics.get('games', {}).get('position'),
                dob=player_info.get('birth', {}).get('date')
            )

            # Get team ID using API ID
            team_id = self._get_team_id_by_api_id(api_team_id, league_id)
            if not team_id:
                team_id = self._get_team_id(team_info.get('name'), league_id)

            if not team_id:
                continue

            games = statistics.get('games', {})
            goals = statistics.get('goals', {})
            shots = statistics.get('shots', {})
            passes = statistics.get('passes', {})
            cards = statistics.get('cards', {})
            tackles = statistics.get('tackles', {})
            duels = statistics.get('duels', {})
            dribbles = statistics.get('dribbles', {})
            fouls = statistics.get('fouls', {})

            stats_record = {
                'player_id': player_id,
                'team_id': team_id,
                'season_id': season_id,
                'league_id': league_id,
                'matches_played': games.get('appearences') or 0,
                'starts': games.get('lineups') or 0,
                'minutes': games.get('minutes') or 0,
                'goals': goals.get('total') or 0,
                'assists': goals.get('assists') or 0,
                'shots': shots.get('total') or 0,
                'shots_on_target': shots.get('on') or 0,
                'passes_completed': passes.get('total') or 0,
                'passes_attempted': passes.get('total') or 0,  # API doesn't separate
                'tackles': tackles.get('total') or 0,
                'interceptions': tackles.get('interceptions') or 0,
                'dribbles_completed': dribbles.get('success') or 0,
                'dribbles_attempted': dribbles.get('attempts') or 0,
                'fouls_committed': fouls.get('committed') or 0,
                'fouls_won': fouls.get('drawn') or 0,
                'yellow_cards': cards.get('yellow') or 0,
                'red_cards': cards.get('red') or 0,
                'data_source_id': self.source_id
            }
            player_stats_data.append(stats_record)

        if player_stats_data:
            self.batch_loader.batch_upsert(
                'player_season_stats',
                player_stats_data,
                conflict_columns=['player_id', 'team_id', 'season_id', 'league_id']
            )

    def _get_or_create_player_with_api_id(
        self,
        player_name: str,
        api_player_id: int,
        nationality: str = None,
        position: str = None,
        dob: str = None
    ) -> Optional[int]:
        """Get or create player, storing API ID."""

        # First try to find by API ID (fastest)
        if api_player_id:
            result = self.db.execute_query(
                "SELECT player_id FROM players WHERE api_football_id = :api_id",
                params={'api_id': api_player_id},
                fetch=True
            )
            if result:
                return result[0][0]

        # Then try by name
        result = self.db.execute_query(
            "SELECT player_id FROM players WHERE player_name = :name",
            params={'name': player_name},
            fetch=True
        )

        if result:
            player_id = result[0][0]
            # Update API ID if not set
            if api_player_id:
                self.db.execute_query(
                    """
                    UPDATE players SET api_football_id = :api_id
                    WHERE player_id = :pid AND api_football_id IS NULL
                    """,
                    params={'api_id': api_player_id, 'pid': player_id},
                    fetch=False
                )
            return player_id

        # Create new player
        result = self.db.execute_query(
            """
            INSERT INTO players (player_name, api_football_id, nationality, position, date_of_birth)
            VALUES (:name, :api_id, :nat, :pos, :dob)
            RETURNING player_id
            """,
            params={
                'name': player_name,
                'api_id': api_player_id,
                'nat': nationality,
                'pos': position,
                'dob': dob
            },
            fetch=True
        )
        return result[0][0] if result else None

    def _get_team_id_by_api_id(self, api_team_id: int, league_id: int) -> Optional[int]:
        """Get team ID using API-Football ID (cached)."""
        if not api_team_id:
            return None

        # Check cache first
        for db_id, api_id in self._team_api_id_cache.items():
            if api_id == api_team_id:
                return db_id

        # Query database
        result = self.db.execute_query(
            """
            SELECT team_id FROM teams
            WHERE api_football_id = :api_id AND league_id = :league_id
            """,
            params={'api_id': api_team_id, 'league_id': league_id},
            fetch=True
        )

        if result:
            team_id = result[0][0]
            self._team_api_id_cache[team_id] = api_team_id
            return team_id

        return None

    def _get_league_id(self, league_key: str) -> Optional[int]:
        """Get league ID from database."""
        league_name = LEAGUE_NAMES.get(league_key)
        if not league_name:
            return None

        result = self.db.execute_query(
            "SELECT league_id FROM leagues WHERE league_name = :name",
            params={'name': league_name},
            fetch=True
        )

        return result[0][0] if result else None

    def _get_season_id(self, season_name: str) -> Optional[int]:
        """Get season ID from database."""
        result = self.db.execute_query(
            "SELECT season_id FROM seasons WHERE season_name = :name",
            params={'name': season_name},
            fetch=True
        )

        return result[0][0] if result else None

    def _get_team_id(self, team_name: str, league_id: int) -> Optional[int]:
        """Get team ID by name and league."""
        if not team_name:
            return None

        result = self.db.execute_query(
            "SELECT team_id FROM teams WHERE team_name = :name AND league_id = :league_id",
            params={'name': team_name, 'league_id': league_id},
            fetch=True
        )

        return result[0][0] if result else None
