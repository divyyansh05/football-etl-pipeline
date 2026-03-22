"""
ETL pipeline for FotMob data.

Primary data source - no API key or request limit.
Covers 8 leagues with team, player, and match-level data.

Features:
- FotMob ID storage for efficient upserts
- In-memory caching to reduce DB lookups
- Batch operations via BatchLoader
- Deep collection: team squads + match details
- Multi-season support
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import time

from database.connection import get_db
from database.batch_loader import BatchLoader
from scrapers.fotmob.client import FotMobClient
from scrapers.fotmob.data_parser import FotMobDataParser
from scrapers.fotmob.constants import (
    LEAGUE_IDS, LEAGUE_NAMES, LEAGUE_COUNTRIES,
    LEAGUE_ID_TO_KEY, API_FOOTBALL_LEAGUE_IDS
)

logger = logging.getLogger(__name__)


class FotMobETL:
    """
    ETL pipeline for FotMob data.

    Features:
    - No API key or daily limit (public JSON API)
    - Stores FotMob IDs to avoid redundant lookups
    - Efficient batch upsert operations
    - Caches team/player IDs in memory during session
    - Supports basic (standings + matches) and deep (squads + match details) collection
    """

    def __init__(self, db=None):
        """
        Initialize FotMob ETL.

        Args:
            db: Database connection (uses global if not provided)
        """
        self.client = FotMobClient()
        self.parser = FotMobDataParser()
        self.db = db or get_db()
        self.batch_loader = BatchLoader(db=self.db)
        self.source_id = self._get_or_create_source()

        # In-memory caches (reduce DB lookups within session)
        self._team_fotmob_cache: Dict[int, int] = {}     # fotmob_id -> db_team_id
        self._player_fotmob_cache: Dict[int, int] = {}   # fotmob_id -> db_player_id
        self._league_id_cache: Dict[str, int] = {}        # league_key -> db_league_id
        self._season_id_cache: Dict[str, int] = {}        # season_name -> db_season_id

        # Statistics tracking
        self.stats = {
            'teams_inserted': 0,
            'teams_updated': 0,
            'players_inserted': 0,
            'players_updated': 0,
            'matches_inserted': 0,
            'matches_updated': 0,
            'team_season_stats': 0,
            'team_match_stats': 0,
            'player_match_stats': 0,
            'player_season_stats': 0,
            'api_requests': 0,
            'errors': 0,
        }

    # =========================================
    # DATA SOURCE MANAGEMENT
    # =========================================

    def _get_or_create_source(self) -> int:
        """Get or create FotMob data source entry."""
        result = self.db.execute_query(
            "SELECT source_id FROM data_sources WHERE source_name = 'fotmob'",
            fetch=True
        )

        if result:
            return result[0][0]

        result = self.db.execute_query(
            """
            INSERT INTO data_sources (source_name, base_url, reliability_score)
            VALUES ('fotmob', 'https://www.fotmob.com/api/', 92)
            RETURNING source_id
            """,
            fetch=True
        )
        return result[0][0]

    # =========================================
    # LEAGUE-LEVEL WORKFLOWS
    # =========================================

    def process_league_season(
        self,
        league_key: str,
        season_name: str = None,
    ) -> Dict[str, int]:
        """
        Process league data: standings + matches (basic collection).

        Steps:
        1. Fetch league data from FotMob
        2. Upsert teams from standings
        3. Insert team_season_stats from standings
        4. Extract match IDs from league data

        Args:
            league_key: League key (e.g., 'premier-league')
            season_name: Season in DB format (e.g., '2024-25'). Defaults to current.

        Returns:
            Statistics dictionary
        """
        result_stats = {
            'teams': 0,
            'team_season_stats': 0,
            'matches_found': 0,
        }

        league_id_fotmob = LEAGUE_IDS.get(league_key)
        if not league_id_fotmob:
            logger.error(f"Unknown league: {league_key}")
            return result_stats

        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"Processing {league_name} (FotMob ID: {league_id_fotmob})")

        # Get DB IDs
        league_id_db = self._get_league_id(league_key)
        if not league_id_db:
            logger.error(f"League '{league_key}' not found in database")
            return result_stats

        # Fetch league data from FotMob
        if season_name:
            # Convert DB format to FotMob format: '2024-25' -> '2024/2025'
            fotmob_season = self._db_season_to_fotmob(season_name)
            raw_data = self.client.get_league_season(league_id_fotmob, fotmob_season)
        else:
            raw_data = self.client.get_league(league_id_fotmob)

        self.stats['api_requests'] += 1

        if not raw_data:
            logger.error(f"Failed to fetch league data for {league_name}")
            self.stats['errors'] += 1
            return result_stats

        # Determine season_id from response or parameter
        if not season_name:
            details = self.parser.parse_league_details(raw_data)
            if details:
                fotmob_season_str = details.get('selected_season') or details.get('latest_season')
                if fotmob_season_str:
                    season_name = self.parser.format_season_name(str(fotmob_season_str))

        if not season_name:
            season_name = '2024-25'  # Fallback to current
            logger.warning(f"Could not determine season, using default: {season_name}")

        season_id_db = self._get_or_create_season(season_name)
        if not season_id_db:
            logger.error(f"Could not get/create season: {season_name}")
            return result_stats

        # 1. Process teams from standings
        standings = self.parser.parse_league_standings(raw_data)
        if standings:
            teams_count = self._process_teams_from_standings(standings, league_id_db)
            result_stats['teams'] = teams_count
            logger.info(f"Processed {teams_count} teams from standings")

            # 2. Process team season stats
            stats_count = self._process_team_season_stats(
                standings, league_id_db, season_id_db
            )
            result_stats['team_season_stats'] = stats_count
            self.stats['team_season_stats'] += stats_count
            logger.info(f"Processed {stats_count} team season stats")

        # 3. Extract match IDs from league data
        matches = self.parser.parse_league_matches(raw_data)
        result_stats['matches_found'] = len(matches)
        logger.info(f"Found {len(matches)} matches in league data")

        # Process xG data if available
        xg_data = self.parser.parse_xg_table(raw_data)
        if xg_data:
            self._update_team_xg(xg_data, league_id_db, season_id_db)
            logger.info(f"Updated xG data for {len(xg_data)} teams")

        logger.info(f"Completed basic collection for {league_name}: {result_stats}")
        return result_stats

    def process_league_teams_deep(
        self,
        league_key: str,
    ) -> Dict[str, int]:
        """
        Deep team collection: fetch each team page for full squad data.

        For each team in the league:
        1. Fetch /teams endpoint for squad
        2. Upsert each player from squad

        Args:
            league_key: League key (e.g., 'premier-league')

        Returns:
            Statistics dictionary
        """
        result_stats = {
            'teams_processed': 0,
            'players_inserted': 0,
            'players_updated': 0,
        }

        league_id_db = self._get_league_id(league_key)
        if not league_id_db:
            logger.error(f"League '{league_key}' not found in database")
            return result_stats

        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"Deep team collection for {league_name}")

        # Get all teams with fotmob_ids for this league
        teams = self.db.execute_query(
            """
            SELECT team_id, team_name, fotmob_id
            FROM teams
            WHERE league_id = :lid AND fotmob_id IS NOT NULL
            """,
            params={'lid': league_id_db},
            fetch=True
        )

        if not teams:
            logger.warning(f"No teams with FotMob IDs found for {league_name}")
            return result_stats

        for team_row in teams:
            db_team_id, team_name, fotmob_team_id = team_row
            logger.info(f"Fetching squad for {team_name} (FotMob ID: {fotmob_team_id})")

            try:
                team_data = self.client.get_team(fotmob_team_id)
                self.stats['api_requests'] += 1

                if not team_data:
                    logger.warning(f"Failed to fetch team data for {team_name}")
                    self.stats['errors'] += 1
                    continue

                # Parse squad
                squad = self.parser.parse_team_squad(team_data)

                for player_info in squad:
                    player_fotmob_id = player_info.get('fotmob_id')
                    player_name = player_info.get('name')

                    if not player_name:
                        continue

                    # Upsert player (basic info from squad)
                    player_id, was_insert = self._upsert_player({
                        'fotmob_id': player_fotmob_id,
                        'player_name': player_name,
                        'nationality': player_info.get('nationality'),
                    })

                    if player_id:
                        if was_insert:
                            result_stats['players_inserted'] += 1
                            self.stats['players_inserted'] += 1
                        else:
                            result_stats['players_updated'] += 1
                            self.stats['players_updated'] += 1

                result_stats['teams_processed'] += 1
                logger.info(f"Processed squad for {team_name}: {len(squad)} players")

            except Exception as e:
                logger.error(f"Error processing team {team_name}: {e}")
                self.stats['errors'] += 1

        logger.info(f"Deep team collection complete for {league_name}: {result_stats}")
        return result_stats

    def process_league_players_deep(
        self,
        league_key: str,
    ) -> Dict[str, int]:
        """
        Deep player collection: fetch individual player pages for full stats.

        For each player with a fotmob_id:
        1. Fetch /playerData endpoint
        2. Update player bio (height, foot, market value)
        3. Extract current season stats

        Args:
            league_key: League key (e.g., 'premier-league')

        Returns:
            Statistics dictionary
        """
        result_stats = {
            'players_processed': 0,
            'player_season_stats': 0,
        }

        league_id_db = self._get_league_id(league_key)
        if not league_id_db:
            logger.error(f"League '{league_key}' not found in database")
            return result_stats

        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"Deep player collection for {league_name}")

        # Get all teams for this league
        teams = self.db.execute_query(
            """
            SELECT team_id, team_name, fotmob_id
            FROM teams
            WHERE league_id = :lid AND fotmob_id IS NOT NULL
            """,
            params={'lid': league_id_db},
            fetch=True
        )

        if not teams:
            logger.warning(f"No teams found for {league_name}")
            return result_stats

        for team_row in teams:
            db_team_id, team_name, fotmob_team_id = team_row
            logger.info(f"Fetching players for {team_name}")

            # Get team page for squad IDs
            team_data = self.client.get_team(fotmob_team_id)
            self.stats['api_requests'] += 1

            if not team_data:
                continue

            squad = self.parser.parse_team_squad(team_data)

            for player_info in squad:
                player_fotmob_id = player_info.get('fotmob_id')
                if not player_fotmob_id:
                    continue

                try:
                    player_data = self.client.get_player(player_fotmob_id)
                    self.stats['api_requests'] += 1

                    if not player_data:
                        continue

                    # Parse and update player bio
                    parsed = self.parser.parse_player(player_data)
                    if parsed:
                        self._update_player_bio(parsed, db_team_id)
                        result_stats['players_processed'] += 1

                    # Parse contract info
                    contract_info = self.parser.parse_player_contract(player_data)
                    if contract_info:
                        self._update_player_contract(player_fotmob_id, contract_info)

                    # Parse season stats (basic)
                    season_stats = self.parser.parse_player_season_stats(player_data)

                    # Parse deep stats (xG, xA, progressive, etc.)
                    deep_stats = self.parser.parse_player_deep_stats(player_data)

                    if season_stats or deep_stats:
                        # Merge basic and deep stats
                        merged_stats = {**(season_stats or {}), **(deep_stats or {})}
                        self._insert_player_season_stats(
                            player_fotmob_id, merged_stats,
                            league_id_db, db_team_id
                        )
                        result_stats['player_season_stats'] += 1
                        self.stats['player_season_stats'] += 1

                except Exception as e:
                    logger.error(f"Error processing player {player_info.get('name')}: {e}")
                    self.stats['errors'] += 1

        logger.info(f"Deep player collection complete for {league_name}: {result_stats}")
        return result_stats

    def process_league_matches_deep(
        self,
        league_key: str,
        season_name: str = None,
        max_matches: int = None,
    ) -> Dict[str, int]:
        """
        Deep match collection: fetch individual match details.

        For each match in the league:
        1. Fetch /matchDetails endpoint
        2. Insert match record
        3. Insert team_match_stats (home + away)
        4. Insert player_match_stats

        Args:
            league_key: League key
            season_name: Season in DB format (e.g., '2024-25')
            max_matches: Limit number of matches to process (for testing)

        Returns:
            Statistics dictionary
        """
        result_stats = {
            'matches_processed': 0,
            'team_match_stats': 0,
            'player_match_stats': 0,
            'match_events': 0,
        }

        league_id_fotmob = LEAGUE_IDS.get(league_key)
        league_id_db = self._get_league_id(league_key)
        if not league_id_fotmob or not league_id_db:
            logger.error(f"League '{league_key}' not found")
            return result_stats

        league_name = LEAGUE_NAMES.get(league_key, league_key)
        logger.info(f"Deep match collection for {league_name}")

        # Get league data to find match IDs
        if season_name:
            fotmob_season = self._db_season_to_fotmob(season_name)
            raw_data = self.client.get_league_season(league_id_fotmob, fotmob_season)
        else:
            raw_data = self.client.get_league(league_id_fotmob)

        self.stats['api_requests'] += 1

        if not raw_data:
            logger.error(f"Failed to fetch league data for {league_name}")
            return result_stats

        # Determine season
        if not season_name:
            details = self.parser.parse_league_details(raw_data)
            if details:
                fotmob_season_str = details.get('selected_season') or details.get('latest_season')
                if fotmob_season_str:
                    season_name = self.parser.format_season_name(str(fotmob_season_str))

        if not season_name:
            season_name = '2024-25'

        season_id_db = self._get_or_create_season(season_name)
        if not season_id_db:
            return result_stats

        # Extract match IDs from league data
        matches = self.parser.parse_league_matches(raw_data)
        if not matches:
            logger.warning(f"No matches found for {league_name}")
            return result_stats

        if max_matches:
            matches = matches[:max_matches]

        logger.info(f"Processing {len(matches)} matches for {league_name}")

        for i, match_info in enumerate(matches):
            match_fotmob_id = match_info.get('fotmob_match_id')
            if not match_fotmob_id:
                continue

            try:
                logger.info(f"Fetching match {i+1}/{len(matches)}: {match_fotmob_id}")
                match_data = self.client.get_match(match_fotmob_id)
                self.stats['api_requests'] += 1

                if not match_data:
                    continue

                # Parse match details
                parsed_match = self.parser.parse_match(match_data)
                if not parsed_match or not parsed_match.get('finished'):
                    continue  # Skip unfinished matches

                # Upsert match
                match_id = self._upsert_match(parsed_match, league_id_db, season_id_db)
                if not match_id:
                    continue

                result_stats['matches_processed'] += 1
                self.stats['matches_inserted'] += 1

                # Parse and insert team match stats
                match_stats = self.parser.parse_match_stats(match_data)
                if match_stats:
                    tms_count = self._insert_team_match_stats(
                        match_id, parsed_match, match_stats, league_id_db
                    )
                    result_stats['team_match_stats'] += tms_count
                    self.stats['team_match_stats'] += tms_count

                # Parse and insert player match stats
                player_stats = self.parser.parse_match_player_stats(match_data)
                if player_stats:
                    pms_count = self._insert_player_match_stats_batch(
                        match_id, player_stats, league_id_db
                    )
                    result_stats['player_match_stats'] += pms_count
                    self.stats['player_match_stats'] += pms_count

                # Parse and insert match events (goals, cards, subs)
                match_events = self.parser.parse_match_events(match_data)
                if match_events:
                    events_count = self._insert_match_events(
                        match_id, match_events, league_id_db
                    )
                    result_stats['match_events'] = result_stats.get('match_events', 0) + events_count
                    self.stats['match_events'] = self.stats.get('match_events', 0) + events_count

            except Exception as e:
                logger.error(f"Error processing match {match_fotmob_id}: {e}")
                self.stats['errors'] += 1

        logger.info(f"Deep match collection complete for {league_name}: {result_stats}")
        return result_stats

    # =========================================
    # ENTITY UPSERTS
    # =========================================

    def _process_teams_from_standings(
        self,
        standings: List[Dict],
        league_id: int
    ) -> int:
        """Upsert teams from standings data."""
        teams_data = []

        for entry in standings:
            fotmob_team_id = entry.get('fotmob_team_id')
            team_name = entry.get('team_name')

            if not team_name:
                continue

            teams_data.append({
                'team_name': team_name,
                'team_short_name': entry.get('short_name'),
                'league_id': league_id,
                'fotmob_id': fotmob_team_id,
            })

            # Update cache
            if fotmob_team_id:
                self._team_fotmob_cache[fotmob_team_id] = None  # Will be filled on lookup

        if not teams_data:
            return 0

        count = self.batch_loader.batch_upsert(
            'teams',
            teams_data,
            conflict_columns=['team_name', 'league_id'],
            update_columns=['team_short_name', 'fotmob_id']
        )

        # Refresh cache after upsert
        self._refresh_team_cache(league_id)

        return count

    def _process_team_season_stats(
        self,
        standings: List[Dict],
        league_id: int,
        season_id: int
    ) -> int:
        """Insert/update team season stats from standings."""
        stats_data = []

        for entry in standings:
            fotmob_team_id = entry.get('fotmob_team_id')
            team_id = self._get_team_id_by_fotmob_id(fotmob_team_id, league_id)

            if not team_id:
                # Fallback to name lookup
                team_id = self._get_team_id_by_name(entry.get('team_name'), league_id)

            if not team_id:
                logger.warning(f"Team not found: {entry.get('team_name')}")
                continue

            stats_data.append({
                'team_id': team_id,
                'season_id': season_id,
                'league_id': league_id,
                'matches_played': entry.get('played', 0),
                'wins': entry.get('wins', 0),
                'draws': entry.get('draws', 0),
                'losses': entry.get('losses', 0),
                'goals_for': entry.get('goals_for', 0),
                'goals_against': entry.get('goals_against', 0),
                'goal_difference': entry.get('goal_difference', 0),
                'points': entry.get('points', 0),
                'league_position': entry.get('position'),
                'data_source_id': self.source_id,
            })

        if not stats_data:
            return 0

        return self.batch_loader.batch_upsert(
            'team_season_stats',
            stats_data,
            conflict_columns=['team_id', 'season_id', 'league_id'],
            update_columns=[
                'matches_played', 'wins', 'draws', 'losses',
                'goals_for', 'goals_against', 'goal_difference',
                'points', 'league_position', 'data_source_id'
            ]
        )

    def _update_team_xg(
        self,
        xg_data: List[Dict],
        league_id: int,
        season_id: int
    ):
        """Update xG columns in team_season_stats."""
        for entry in xg_data:
            fotmob_team_id = entry.get('fotmob_team_id')
            team_id = self._get_team_id_by_fotmob_id(fotmob_team_id, league_id)

            if not team_id:
                continue

            try:
                self.db.execute_query(
                    """
                    UPDATE team_season_stats
                    SET xg_for = :xg_for, xg_against = :xg_against
                    WHERE team_id = :tid AND season_id = :sid AND league_id = :lid
                    """,
                    params={
                        'xg_for': entry.get('xg'),
                        'xg_against': entry.get('xg_conceded'),
                        'tid': team_id,
                        'sid': season_id,
                        'lid': league_id,
                    },
                    fetch=False
                )
            except Exception as e:
                logger.warning(f"Error updating xG for team {team_id}: {e}")

    def _upsert_player(self, player_data: Dict) -> Tuple[Optional[int], bool]:
        """
        Upsert player by fotmob_id or name.

        Returns:
            Tuple of (player_id, was_insert)
        """
        fotmob_id = player_data.get('fotmob_id')
        player_name = player_data.get('player_name')

        # 1. Try by fotmob_id (fastest)
        if fotmob_id:
            if fotmob_id in self._player_fotmob_cache:
                return (self._player_fotmob_cache[fotmob_id], False)

            result = self.db.execute_query(
                "SELECT player_id FROM players WHERE fotmob_id = :fid",
                params={'fid': fotmob_id},
                fetch=True
            )
            if result:
                player_id = result[0][0]
                self._player_fotmob_cache[fotmob_id] = player_id
                # Update fields if needed
                self.db.execute_query(
                    """
                    UPDATE players SET
                        player_name = COALESCE(:name, player_name),
                        nationality = COALESCE(:nat, nationality),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE player_id = :pid
                    """,
                    params={
                        'name': player_name,
                        'nat': player_data.get('nationality'),
                        'pid': player_id,
                    },
                    fetch=False
                )
                return (player_id, False)

        # 2. Try by name
        if player_name:
            result = self.db.execute_query(
                "SELECT player_id FROM players WHERE player_name = :name",
                params={'name': player_name},
                fetch=True
            )
            if result:
                player_id = result[0][0]
                # Update fotmob_id if not set
                if fotmob_id:
                    self.db.execute_query(
                        """
                        UPDATE players SET fotmob_id = :fid, updated_at = CURRENT_TIMESTAMP
                        WHERE player_id = :pid AND fotmob_id IS NULL
                        """,
                        params={'fid': fotmob_id, 'pid': player_id},
                        fetch=False
                    )
                    self._player_fotmob_cache[fotmob_id] = player_id
                return (player_id, False)

        # 3. Create new player
        if not player_name:
            return (None, False)

        result = self.db.execute_query(
            """
            INSERT INTO players (player_name, fotmob_id, nationality)
            VALUES (:name, :fid, :nat)
            RETURNING player_id
            """,
            params={
                'name': player_name,
                'fid': fotmob_id,
                'nat': player_data.get('nationality'),
            },
            fetch=True
        )

        if result:
            player_id = result[0][0]
            if fotmob_id:
                self._player_fotmob_cache[fotmob_id] = player_id
            return (player_id, True)

        return (None, False)

    def _update_player_bio(self, parsed_player: Dict, team_id: int = None):
        """Update player bio data from deep /playerData response."""
        fotmob_id = parsed_player.get('fotmob_id')
        if not fotmob_id:
            return

        player_id = self._get_player_id_by_fotmob_id(fotmob_id)
        if not player_id:
            # Create if not exists
            player_id, _ = self._upsert_player({
                'fotmob_id': fotmob_id,
                'player_name': parsed_player.get('name'),
                'nationality': parsed_player.get('nationality'),
            })

        if not player_id:
            return

        try:
            self.db.execute_query(
                """
                UPDATE players SET
                    position = COALESCE(:pos, position),
                    height_cm = COALESCE(:height, height_cm),
                    preferred_foot = COALESCE(:foot, preferred_foot),
                    date_of_birth = COALESCE(:dob, date_of_birth),
                    nationality = COALESCE(:nat, nationality),
                    updated_at = CURRENT_TIMESTAMP
                WHERE player_id = :pid
                """,
                params={
                    'pos': parsed_player.get('position'),
                    'height': parsed_player.get('height_cm'),
                    'foot': parsed_player.get('preferred_foot'),
                    'dob': parsed_player.get('date_of_birth'),
                    'nat': parsed_player.get('nationality'),
                    'pid': player_id,
                },
                fetch=False
            )
        except Exception as e:
            logger.warning(f"Error updating player bio for {fotmob_id}: {e}")

    def _update_player_contract(self, fotmob_id: int, contract_info: Dict):
        """Update player contract and injury information."""
        player_id = self._get_player_id_by_fotmob_id(fotmob_id)
        if not player_id:
            return

        try:
            self.db.execute_query(
                """
                UPDATE players SET
                    contract_end_date = COALESCE(:contract_end, contract_end_date),
                    contract_until = COALESCE(:contract_end, contract_until),
                    is_injured = :is_injured,
                    updated_at = CURRENT_TIMESTAMP
                WHERE player_id = :pid
                """,
                params={
                    'contract_end': contract_info.get('contract_end_date'),
                    'is_injured': contract_info.get('is_injured', False),
                    'pid': player_id,
                },
                fetch=False
            )
        except Exception as e:
            logger.warning(f"Error updating player contract for {fotmob_id}: {e}")

    def _insert_player_season_stats(
        self,
        player_fotmob_id: int,
        season_stats: Dict,
        league_id: int,
        team_id: int,
    ):
        """Insert player season stats from /playerData response."""
        player_id = self._get_player_id_by_fotmob_id(player_fotmob_id)
        if not player_id:
            return

        # Get season_id from the stats response
        season_str = season_stats.get('season')
        if season_str:
            season_name = self.parser.format_season_name(str(season_str))
        else:
            season_name = '2024-25'

        season_id = self._get_or_create_season(season_name)
        if not season_id:
            return

        stats_record = {
            'player_id': player_id,
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            # Basic stats
            'matches_played': season_stats.get('matches', 0),
            'starts': season_stats.get('started', 0),
            'goals': season_stats.get('goals', 0),
            'assists': season_stats.get('assists', 0),
            'minutes': season_stats.get('minutes_played', 0),
            'yellow_cards': season_stats.get('yellow_cards', 0),
            'red_cards': season_stats.get('red_cards', 0),
            # xG and shooting
            'xg': season_stats.get('xg'),
            'npxg': season_stats.get('npxg'),
            'xag': season_stats.get('xa'),  # xA maps to xag column
            'shots': season_stats.get('shots'),
            'shots_on_target': season_stats.get('shots_on_target'),
            'penalty_goals': season_stats.get('penalty_goals'),
            # Passing
            'passes_completed': season_stats.get('accurate_passes'),
            'key_passes': season_stats.get('key_passes'),
            # Defending
            'tackles': season_stats.get('tackles'),
            'interceptions': season_stats.get('interceptions'),
            'clearances': season_stats.get('clearances'),
            'blocks': season_stats.get('blocks'),
            # Possession
            'touches': season_stats.get('touches'),
            'dribbles_completed': season_stats.get('successful_dribbles'),
            # Discipline
            'fouls_committed': season_stats.get('fouls_committed'),
            'fouls_won': season_stats.get('fouls_won'),
            # Per 90 stats
            'xg_per_90': season_stats.get('xg_per_90'),
            'xag_per_90': season_stats.get('xa_per_90'),
            'shots_per_90': season_stats.get('shots_per_90'),
            # Source
            'data_source_id': self.source_id,
        }

        # Remove None values to avoid overwriting existing data
        stats_record = {k: v for k, v in stats_record.items() if v is not None}

        try:
            self.batch_loader.batch_upsert(
                'player_season_stats',
                [stats_record],
                conflict_columns=['player_id', 'team_id', 'season_id', 'league_id'],
                update_columns=[
                    'matches_played', 'starts', 'goals', 'assists', 'minutes',
                    'yellow_cards', 'red_cards', 'xg', 'npxg', 'xag',
                    'shots', 'shots_on_target', 'penalty_goals',
                    'passes_completed', 'key_passes',
                    'tackles', 'interceptions', 'clearances', 'blocks',
                    'touches', 'dribbles_completed',
                    'fouls_committed', 'fouls_won',
                    'xg_per_90', 'xag_per_90', 'shots_per_90',
                    'data_source_id'
                ]
            )
        except Exception as e:
            logger.warning(f"Error inserting player season stats: {e}")

    def _upsert_match(
        self,
        parsed_match: Dict,
        league_id: int,
        season_id: int
    ) -> Optional[int]:
        """Upsert a match record using fotmob_match_id."""
        fotmob_match_id = parsed_match.get('fotmob_match_id')
        match_date = parsed_match.get('match_date')

        if not match_date:
            return None

        # Get team IDs
        home_fotmob_id = parsed_match.get('home_fotmob_id')
        away_fotmob_id = parsed_match.get('away_fotmob_id')

        home_team_id = self._get_team_id_by_fotmob_id(home_fotmob_id, league_id)
        away_team_id = self._get_team_id_by_fotmob_id(away_fotmob_id, league_id)

        # Fallback to name lookup
        if not home_team_id:
            home_team_id = self._get_team_id_by_name(
                parsed_match.get('home_team_name'), league_id
            )
        if not away_team_id:
            away_team_id = self._get_team_id_by_name(
                parsed_match.get('away_team_name'), league_id
            )

        if not home_team_id or not away_team_id:
            logger.warning(
                f"Could not find teams for match: "
                f"{parsed_match.get('home_team_name')} vs {parsed_match.get('away_team_name')}"
            )
            return None

        # Check if match exists by fotmob_match_id
        if fotmob_match_id:
            result = self.db.execute_query(
                "SELECT match_id FROM matches WHERE fotmob_match_id = :fid",
                params={'fid': fotmob_match_id},
                fetch=True
            )
            if result:
                match_id = result[0][0]
                # Update existing match
                self.db.execute_query(
                    """
                    UPDATE matches SET
                        home_score = :hs, away_score = :as_,
                        venue = COALESCE(:venue, venue),
                        referee = COALESCE(:ref, referee),
                        attendance = COALESCE(:att, attendance),
                        match_status = :status,
                        data_source_id = :src
                    WHERE match_id = :mid
                    """,
                    params={
                        'hs': parsed_match.get('home_score'),
                        'as_': parsed_match.get('away_score'),
                        'venue': parsed_match.get('venue'),
                        'ref': parsed_match.get('referee'),
                        'att': parsed_match.get('attendance'),
                        'status': 'completed' if parsed_match.get('finished') else 'scheduled',
                        'src': self.source_id,
                        'mid': match_id,
                    },
                    fetch=False
                )
                return match_id

        # Insert new match
        try:
            result = self.db.execute_query(
                """
                INSERT INTO matches (
                    league_id, season_id, match_date,
                    home_team_id, away_team_id,
                    home_score, away_score,
                    venue, referee, attendance,
                    match_status, fotmob_match_id, data_source_id
                ) VALUES (
                    :lid, :sid, :mdate,
                    :htid, :atid,
                    :hs, :as_,
                    :venue, :ref, :att,
                    :status, :fmid, :src
                )
                ON CONFLICT (league_id, season_id, home_team_id, away_team_id, match_date)
                DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    venue = COALESCE(EXCLUDED.venue, matches.venue),
                    referee = COALESCE(EXCLUDED.referee, matches.referee),
                    attendance = COALESCE(EXCLUDED.attendance, matches.attendance),
                    match_status = EXCLUDED.match_status,
                    fotmob_match_id = EXCLUDED.fotmob_match_id,
                    data_source_id = EXCLUDED.data_source_id
                RETURNING match_id
                """,
                params={
                    'lid': league_id,
                    'sid': season_id,
                    'mdate': match_date,
                    'htid': home_team_id,
                    'atid': away_team_id,
                    'hs': parsed_match.get('home_score'),
                    'as_': parsed_match.get('away_score'),
                    'venue': parsed_match.get('venue'),
                    'ref': parsed_match.get('referee'),
                    'att': parsed_match.get('attendance'),
                    'status': 'completed' if parsed_match.get('finished') else 'scheduled',
                    'fmid': fotmob_match_id,
                    'src': self.source_id,
                },
                fetch=True
            )
            return result[0][0] if result else None

        except Exception as e:
            logger.error(f"Error upserting match {fotmob_match_id}: {e}")
            self.stats['errors'] += 1
            return None

    def _insert_team_match_stats(
        self,
        match_id: int,
        parsed_match: Dict,
        match_stats: Dict,
        league_id: int,
    ) -> int:
        """Insert team match stats (home + away)."""
        count = 0

        for side, is_home in [('home', True), ('away', False)]:
            stats = match_stats.get(side, {})
            if not stats:
                continue

            fotmob_team_id = parsed_match.get(f'{side}_fotmob_id')
            team_id = self._get_team_id_by_fotmob_id(fotmob_team_id, league_id)

            if not team_id:
                team_name = parsed_match.get(f'{side}_team_name')
                team_id = self._get_team_id_by_name(team_name, league_id)

            if not team_id:
                continue

            record = {
                'match_id': match_id,
                'team_id': team_id,
                'is_home': is_home,
                'goals': parsed_match.get(f'{side}_score', 0),
                'shots': stats.get('shots'),
                'shots_on_target': stats.get('shots_on_target'),
                'possession': stats.get('possession'),
                'passes_completed': stats.get('passes_completed'),
                'passes_attempted': stats.get('passes_attempted'),
                'pass_accuracy': stats.get('pass_accuracy'),
                'fouls_committed': stats.get('fouls_committed'),
                'yellow_cards': stats.get('yellow_cards', 0),
                'red_cards': stats.get('red_cards', 0),
                'corners': stats.get('corners'),
                'offsides': stats.get('offsides'),
                'xg': stats.get('xg'),
                'data_source_id': self.source_id,
            }

            try:
                self.batch_loader.batch_upsert(
                    'team_match_stats',
                    [record],
                    conflict_columns=['match_id', 'team_id'],
                    update_columns=[
                        'goals', 'shots', 'shots_on_target', 'possession',
                        'passes_completed', 'passes_attempted', 'pass_accuracy',
                        'fouls_committed', 'yellow_cards', 'red_cards',
                        'corners', 'offsides', 'xg', 'data_source_id'
                    ]
                )
                count += 1
            except Exception as e:
                logger.warning(f"Error inserting team match stats for {side}: {e}")

        return count

    def _insert_player_match_stats_batch(
        self,
        match_id: int,
        player_stats: List[Dict],
        league_id: int,
    ) -> int:
        """Insert player match stats from parsed match data."""
        records = []

        for ps in player_stats:
            fotmob_player_id = ps.get('fotmob_player_id')
            player_name = ps.get('name')

            # Get or create player
            player_id, _ = self._upsert_player({
                'fotmob_id': fotmob_player_id,
                'player_name': player_name,
            })

            if not player_id:
                continue

            # Get team_id from player stats
            fotmob_team_id = ps.get('team_id')
            team_id = self._get_team_id_by_fotmob_id(fotmob_team_id, league_id)

            if not team_id:
                team_id = self._get_team_id_by_name(ps.get('team_name'), league_id)

            if not team_id:
                continue

            records.append({
                'match_id': match_id,
                'player_id': player_id,
                'team_id': team_id,
                'minutes_played': ps.get('minutes_played', 0),
                'goals': ps.get('goals', 0),
                'assists': ps.get('assists', 0),
                'shots': ps.get('total_shots', 0),
                'shots_on_target': ps.get('shots_on_target', 0),
                'key_passes': ps.get('key_passes', 0),
                'passes_completed': ps.get('accurate_passes'),
                'passes_attempted': ps.get('total_passes'),
                'tackles': ps.get('tackles_won', 0),
                'interceptions': ps.get('interceptions', 0),
                'clearances': ps.get('clearances', 0),
                'aerials_won': ps.get('aerial_duels_won', 0),
                'dribbles_completed': ps.get('dribbles', 0),
                'touches': ps.get('touches', 0),
                'data_source_id': self.source_id,
            })

        if not records:
            return 0

        try:
            return self.batch_loader.batch_upsert(
                'player_match_stats',
                records,
                conflict_columns=['match_id', 'player_id'],
                update_columns=[
                    'team_id', 'minutes_played', 'goals', 'assists',
                    'shots', 'shots_on_target', 'key_passes',
                    'passes_completed', 'passes_attempted',
                    'tackles', 'interceptions', 'clearances',
                    'aerials_won', 'dribbles_completed', 'touches',
                    'data_source_id'
                ]
            )
        except Exception as e:
            logger.error(f"Error inserting player match stats: {e}")
            return 0

    # =========================================
    # LOOKUP / CACHE HELPERS
    # =========================================

    def _get_league_id(self, league_key: str) -> Optional[int]:
        """Get league DB ID from league key."""
        if league_key in self._league_id_cache:
            return self._league_id_cache[league_key]

        league_name = LEAGUE_NAMES.get(league_key)
        if not league_name:
            return None

        result = self.db.execute_query(
            "SELECT league_id FROM leagues WHERE league_name = :name",
            params={'name': league_name},
            fetch=True
        )

        if result:
            self._league_id_cache[league_key] = result[0][0]
            return result[0][0]

        return None

    def _get_or_create_season(self, season_name: str) -> Optional[int]:
        """Get or create season by name (e.g., '2024-25')."""
        if season_name in self._season_id_cache:
            return self._season_id_cache[season_name]

        result = self.db.execute_query(
            "SELECT season_id FROM seasons WHERE season_name = :name",
            params={'name': season_name},
            fetch=True
        )

        if result:
            self._season_id_cache[season_name] = result[0][0]
            return result[0][0]

        # Parse years from season name
        start_year, end_year = self._parse_season_years(season_name)
        if not start_year:
            return None

        # Determine if this is the current season
        current_year = datetime.now().year
        is_current = (start_year == current_year) or (
            start_year == current_year - 1 and end_year == current_year
        )

        try:
            result = self.db.execute_query(
                """
                INSERT INTO seasons (season_name, start_year, end_year, is_current)
                VALUES (:name, :start, :end, :current)
                ON CONFLICT (season_name) DO NOTHING
                RETURNING season_id
                """,
                params={
                    'name': season_name,
                    'start': start_year,
                    'end': end_year,
                    'current': is_current,
                },
                fetch=True
            )
            if result:
                self._season_id_cache[season_name] = result[0][0]
                return result[0][0]

            # If ON CONFLICT hit, fetch existing
            result = self.db.execute_query(
                "SELECT season_id FROM seasons WHERE season_name = :name",
                params={'name': season_name},
                fetch=True
            )
            if result:
                self._season_id_cache[season_name] = result[0][0]
                return result[0][0]

        except Exception as e:
            logger.error(f"Error creating season {season_name}: {e}")

        return None

    def _get_team_id_by_fotmob_id(
        self, fotmob_id: int, league_id: int = None
    ) -> Optional[int]:
        """Get team DB ID by FotMob ID (cached)."""
        if not fotmob_id:
            return None

        if fotmob_id in self._team_fotmob_cache:
            cached = self._team_fotmob_cache[fotmob_id]
            if cached is not None:
                return cached

        # Query database
        if league_id:
            result = self.db.execute_query(
                """
                SELECT team_id FROM teams
                WHERE fotmob_id = :fid AND league_id = :lid
                """,
                params={'fid': fotmob_id, 'lid': league_id},
                fetch=True
            )
        else:
            result = self.db.execute_query(
                "SELECT team_id FROM teams WHERE fotmob_id = :fid",
                params={'fid': fotmob_id},
                fetch=True
            )

        if result:
            team_id = result[0][0]
            self._team_fotmob_cache[fotmob_id] = team_id
            return team_id

        return None

    def _get_team_id_by_name(
        self, team_name: str, league_id: int
    ) -> Optional[int]:
        """Get team DB ID by name and league."""
        if not team_name:
            return None

        result = self.db.execute_query(
            "SELECT team_id FROM teams WHERE team_name = :name AND league_id = :lid",
            params={'name': team_name, 'lid': league_id},
            fetch=True
        )

        return result[0][0] if result else None

    def _get_player_id_by_fotmob_id(self, fotmob_id: int) -> Optional[int]:
        """Get player DB ID by FotMob ID (cached)."""
        if not fotmob_id:
            return None

        if fotmob_id in self._player_fotmob_cache:
            return self._player_fotmob_cache[fotmob_id]

        result = self.db.execute_query(
            "SELECT player_id FROM players WHERE fotmob_id = :fid",
            params={'fid': fotmob_id},
            fetch=True
        )

        if result:
            player_id = result[0][0]
            self._player_fotmob_cache[fotmob_id] = player_id
            return player_id

        return None

    def _refresh_team_cache(self, league_id: int):
        """Reload team fotmob_id -> db_id cache for a league."""
        result = self.db.execute_query(
            """
            SELECT team_id, fotmob_id
            FROM teams
            WHERE league_id = :lid AND fotmob_id IS NOT NULL
            """,
            params={'lid': league_id},
            fetch=True
        )

        for row in result:
            self._team_fotmob_cache[row[1]] = row[0]

        logger.debug(f"Cached {len(result)} team FotMob IDs for league {league_id}")

    # =========================================
    # SEASON FORMAT HELPERS
    # =========================================

    def _db_season_to_fotmob(self, season_name: str) -> str:
        """
        Convert DB season format to FotMob format.

        '2024-25' -> '2024/2025'
        '2024' -> '2024' (single-year seasons)
        """
        if '-' in season_name:
            parts = season_name.split('-')
            if len(parts) == 2:
                start = parts[0]
                end_short = parts[1]
                # Reconstruct full end year
                end = start[:2] + end_short
                return f"{start}/{end}"

        return season_name

    def _parse_season_years(self, season_name: str) -> Tuple[int, int]:
        """
        Parse season name to (start_year, end_year).

        '2024-25' -> (2024, 2025)
        '2024' -> (2024, 2024)
        """
        if '-' in season_name:
            parts = season_name.split('-')
            if len(parts) == 2:
                try:
                    start = int(parts[0])
                    end_short = parts[1]
                    end = int(parts[0][:2] + end_short)
                    return (start, end)
                except ValueError:
                    return (0, 0)

        try:
            year = int(season_name)
            return (year, year)
        except ValueError:
            return (0, 0)

    # =========================================
    # MATCH EVENTS
    # =========================================

    def _insert_match_events(
        self,
        match_id: int,
        events: List[Dict],
        league_id: int,
    ) -> int:
        """
        Insert match events (goals, cards, substitutions).

        Uses match_events table from schema_match_events.sql.

        Args:
            match_id: Database match ID
            events: Parsed events from parse_match_events
            league_id: Database league ID

        Returns:
            Number of events inserted
        """
        if not events:
            return 0

        count = 0
        for event in events:
            event_type = event.get('type')
            if not event_type:
                continue

            # Map event types to standard format
            if event_type == 'Goal':
                event_type_db = 'goal'
            elif event_type == 'Card':
                card_type = event.get('card_type', '').lower()
                if 'red' in card_type:
                    event_type_db = 'red_card'
                else:
                    event_type_db = 'yellow_card'
            elif event_type == 'Substitution':
                event_type_db = 'substitution'
            else:
                event_type_db = event_type.lower().replace(' ', '_')

            # Get player ID
            player_id = None
            player_fotmob_id = event.get('player_id')
            if player_fotmob_id:
                player_id = self._get_player_id_by_fotmob_id(player_fotmob_id)

            # For substitutions, get both players
            player_on_id = None
            player_off_id = None
            if event_type == 'Substitution':
                if event.get('player_on_id'):
                    player_on_id = self._get_player_id_by_fotmob_id(event.get('player_on_id'))
                if event.get('player_off_id'):
                    player_off_id = self._get_player_id_by_fotmob_id(event.get('player_off_id'))
                player_id = player_on_id  # Use player_on as the main player

            record = {
                'match_id': match_id,
                'event_type': event_type_db,
                'event_time': event.get('time'),
                'player_id': player_id,
                'player_name': event.get('player_name'),
                'additional_info': event.get('time_str'),
                'data_source_id': self.source_id,
            }

            try:
                self.db.execute_query(
                    """
                    INSERT INTO match_events (
                        match_id, event_type, event_time, player_id, player_name,
                        additional_info, data_source_id
                    )
                    VALUES (
                        :match_id, :event_type, :event_time, :player_id, :player_name,
                        :additional_info, :data_source_id
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    params=record,
                    fetch=False
                )
                count += 1
            except Exception as e:
                logger.warning(f"Error inserting match event: {e}")

        return count

    # =========================================
    # STATISTICS
    # =========================================

    def get_statistics(self) -> Dict[str, Any]:
        """Get ETL run statistics."""
        client_stats = self.client.get_stats()
        return {
            **self.stats,
            'client': client_stats,
        }

    def reset_statistics(self):
        """Reset statistics counters."""
        for key in self.stats:
            self.stats[key] = 0

    def close(self):
        """Cleanup resources."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
