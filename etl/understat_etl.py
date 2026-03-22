"""
Understat ETL Integration

Extracts advanced xG metrics from understat.com:
- xA (Expected Assists)
- npxG (Non-penalty xG)
- xGChain
- xGBuildup

These metrics fill critical gaps not available from FotMob or API-Football.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from database.connection import get_db
from scrapers.understat.client import UnderstatClient, LEAGUE_MAPPINGS

logger = logging.getLogger(__name__)


class UnderstatETL:
    """
    ETL pipeline for Understat data.

    Enriches existing player/team records with advanced xG metrics.
    """

    # Map our league keys to Understat's
    SUPPORTED_LEAGUES = list(LEAGUE_MAPPINGS.keys())

    def __init__(self, db=None):
        self.client = UnderstatClient(rate_limit_delay=2.0)
        self.db = db or get_db()
        self.source_id = self._get_or_create_source()

        # Caches for ID mapping
        self._player_cache = {}  # understat_id -> db_id
        self._team_cache = {}  # (name, league_id) -> db_id
        self._league_cache = {}  # league_key -> db_id

        # Statistics
        self.stats = {
            'players_enriched': 0,
            'teams_enriched': 0,
            'new_players': 0,
            'new_teams': 0,
            'errors': []
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _get_or_create_source(self) -> int:
        """Get or create Understat data source ID."""
        query = """
            INSERT INTO data_sources (source_name, base_url, reliability_score)
            VALUES ('understat', 'https://understat.com', 90)
            ON CONFLICT (source_name)
            DO UPDATE SET last_successful_scrape = CURRENT_TIMESTAMP
            RETURNING source_id
        """
        result = self.db.execute_query(query, fetch=True)
        return result[0][0]

    def process_league_season(self, league: str, season: int) -> Dict:
        """
        Process all players and teams for a league/season.

        Args:
            league: League key (e.g., 'premier-league')
            season: Season start year (e.g., 2024 for 2024/25)

        Returns:
            Statistics dict
        """
        if league not in self.SUPPORTED_LEAGUES:
            logger.error(f"League {league} not supported by Understat")
            return self.stats

        logger.info(f"Processing Understat data for {league} {season}")

        # Get league ID from our database
        league_id = self._get_league_id(league)
        if not league_id:
            logger.error(f"League {league} not found in database")
            return self.stats

        # Get season ID
        season_id = self._get_season_id(season)
        if not season_id:
            logger.error(f"Season {season} not found in database")
            return self.stats

        # Process teams
        try:
            teams = self.client.get_league_teams(league, season)
            for team_data in teams:
                self._process_team(team_data, league_id, season_id)
        except Exception as e:
            logger.error(f"Error processing teams: {e}")
            self.stats['errors'].append(f"Teams: {str(e)}")

        # Process players
        try:
            players = self.client.get_league_players(league, season)
            for player_data in players:
                self._process_player(player_data, league_id, season_id)
        except Exception as e:
            logger.error(f"Error processing players: {e}")
            self.stats['errors'].append(f"Players: {str(e)}")

        return self.stats

    def _process_team(self, team_data: Dict, league_id: int, season_id: int):
        """Process and save team xG data."""
        team_name = team_data.get('name', '')
        understat_id = team_data.get('understat_id')

        if not team_name:
            return

        # Find or create team
        team_id = self._find_or_create_team(team_name, league_id, understat_id)

        # Update team_season_stats with xG data
        query = """
            INSERT INTO team_season_stats (
                team_id, season_id, league_id,
                matches_played, goals_for, goals_against,
                xg_for, xg_against, npxg_for, npxg_against,
                ppda, deep_completions, deep_completions_allowed,
                data_source_id, last_updated
            )
            VALUES (
                :team_id, :season_id, :league_id,
                :matches, :gf, :ga,
                :xg, :xga, :npxg, :npxga,
                :ppda, :deep, :deep_allowed,
                :src, CURRENT_TIMESTAMP
            )
            ON CONFLICT (team_id, season_id, league_id) DO UPDATE SET
                xg_for = COALESCE(EXCLUDED.xg_for, team_season_stats.xg_for),
                xg_against = COALESCE(EXCLUDED.xg_against, team_season_stats.xg_against),
                npxg_for = COALESCE(EXCLUDED.npxg_for, team_season_stats.npxg_for),
                npxg_against = COALESCE(EXCLUDED.npxg_against, team_season_stats.npxg_against),
                ppda = COALESCE(EXCLUDED.ppda, team_season_stats.ppda),
                deep_completions = COALESCE(EXCLUDED.deep_completions, team_season_stats.deep_completions),
                deep_completions_allowed = COALESCE(EXCLUDED.deep_completions_allowed, team_season_stats.deep_completions_allowed),
                last_updated = CURRENT_TIMESTAMP
        """

        params = {
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            'matches': team_data.get('matches', 0),
            'gf': team_data.get('goals_for', 0),
            'ga': team_data.get('goals_against', 0),
            'xg': team_data.get('xg'),
            'xga': team_data.get('xga'),
            'npxg': team_data.get('npxg'),
            'npxga': team_data.get('npxga'),
            'ppda': team_data.get('ppda'),
            'deep': team_data.get('deep'),
            'deep_allowed': team_data.get('deep_allowed'),
            'src': self.source_id
        }

        try:
            self.db.execute_query(query, params, fetch=False)
            self.stats['teams_enriched'] += 1
        except Exception as e:
            logger.error(f"Error saving team stats for {team_name}: {e}")

    def _process_player(self, player_data: Dict, league_id: int, season_id: int):
        """Process and save player xG data."""
        player_name = player_data.get('name', '')
        understat_id = player_data.get('understat_id')
        team_name = player_data.get('team', '')

        if not player_name:
            return

        # Find or create player
        player_id = self._find_or_create_player(player_name, understat_id)

        # Find team
        team_id = self._find_team_by_name(team_name, league_id)

        # Update player_season_stats with xG data
        query = """
            INSERT INTO player_season_stats (
                player_id, team_id, season_id, league_id,
                matches_played, minutes, goals, assists,
                shots, key_passes,
                xg, xa, npxg, xg_chain, xg_buildup,
                yellow_cards, red_cards,
                data_source_id, last_updated
            )
            VALUES (
                :player_id, :team_id, :season_id, :league_id,
                :matches, :minutes, :goals, :assists,
                :shots, :key_passes,
                :xg, :xa, :npxg, :xg_chain, :xg_buildup,
                :yellow, :red,
                :src, CURRENT_TIMESTAMP
            )
            ON CONFLICT (player_id, team_id, season_id, league_id) DO UPDATE SET
                xg = COALESCE(EXCLUDED.xg, player_season_stats.xg),
                xa = COALESCE(EXCLUDED.xa, player_season_stats.xa),
                npxg = COALESCE(EXCLUDED.npxg, player_season_stats.npxg),
                xg_chain = COALESCE(EXCLUDED.xg_chain, player_season_stats.xg_chain),
                xg_buildup = COALESCE(EXCLUDED.xg_buildup, player_season_stats.xg_buildup),
                last_updated = CURRENT_TIMESTAMP
        """

        params = {
            'player_id': player_id,
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            'matches': player_data.get('games', 0),
            'minutes': player_data.get('minutes', 0),
            'goals': player_data.get('goals', 0),
            'assists': player_data.get('assists', 0),
            'shots': player_data.get('shots', 0),
            'key_passes': player_data.get('key_passes', 0),
            'xg': player_data.get('xg'),
            'xa': player_data.get('xa'),
            'npxg': player_data.get('npxg'),
            'xg_chain': player_data.get('xg_chain'),
            'xg_buildup': player_data.get('xg_buildup'),
            'yellow': player_data.get('yellow_cards', 0),
            'red': player_data.get('red_cards', 0),
            'src': self.source_id
        }

        try:
            self.db.execute_query(query, params, fetch=False)
            self.stats['players_enriched'] += 1
        except Exception as e:
            logger.error(f"Error saving player stats for {player_name}: {e}")

    def _get_league_id(self, league_key: str) -> Optional[int]:
        """Get database league ID from league key."""
        if league_key in self._league_cache:
            return self._league_cache[league_key]

        # Map league keys to database names
        league_name_map = {
            'premier-league': 'Premier League',
            'la-liga': 'La Liga',
            'serie-a': 'Serie A',
            'bundesliga': 'Bundesliga',
            'ligue-1': 'Ligue 1',
        }

        db_name = league_name_map.get(league_key)
        if not db_name:
            return None

        query = "SELECT league_id FROM leagues WHERE league_name = :name"
        result = self.db.execute_query(query, {'name': db_name}, fetch=True)

        if result:
            league_id = result[0][0]
            self._league_cache[league_key] = league_id
            return league_id

        return None

    def _get_season_id(self, start_year: int) -> Optional[int]:
        """Get database season ID from start year."""
        # Convert 2024 -> "2024-25"
        end_year = start_year + 1
        season_name = f"{start_year}-{str(end_year)[-2:]}"

        query = "SELECT season_id FROM seasons WHERE season_name = :name"
        result = self.db.execute_query(query, {'name': season_name}, fetch=True)

        return result[0][0] if result else None

    def _find_or_create_team(self, name: str, league_id: int, understat_id: int) -> int:
        """Find existing team or create new one."""
        cache_key = (name, league_id)
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]

        # Try to find by name and league
        query = """
            SELECT team_id FROM teams
            WHERE team_name = :name AND league_id = :lid
        """
        result = self.db.execute_query(query, {'name': name, 'lid': league_id}, fetch=True)

        if result:
            team_id = result[0][0]
            # Update Understat ID if not set
            self.db.execute_query(
                "UPDATE teams SET understat_id = :uid WHERE team_id = :tid AND understat_id IS NULL",
                {'uid': understat_id, 'tid': team_id},
                fetch=False
            )
        else:
            # Create new team
            query = """
                INSERT INTO teams (team_name, league_id, understat_id)
                VALUES (:name, :lid, :uid)
                RETURNING team_id
            """
            result = self.db.execute_query(
                query,
                {'name': name, 'lid': league_id, 'uid': understat_id},
                fetch=True
            )
            team_id = result[0][0]
            self.stats['new_teams'] += 1

        self._team_cache[cache_key] = team_id
        return team_id

    def _find_team_by_name(self, name: str, league_id: int) -> Optional[int]:
        """Find team by name in league."""
        cache_key = (name, league_id)
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]

        query = """
            SELECT team_id FROM teams
            WHERE team_name = :name AND league_id = :lid
        """
        result = self.db.execute_query(query, {'name': name, 'lid': league_id}, fetch=True)

        if result:
            team_id = result[0][0]
            self._team_cache[cache_key] = team_id
            return team_id

        return None

    def _find_or_create_player(self, name: str, understat_id: int) -> int:
        """Find existing player or create new one."""
        if understat_id in self._player_cache:
            return self._player_cache[understat_id]

        # Try to find by Understat ID first
        query = "SELECT player_id FROM players WHERE understat_id = :uid"
        result = self.db.execute_query(query, {'uid': understat_id}, fetch=True)

        if result:
            player_id = result[0][0]
        else:
            # Try by name
            query = "SELECT player_id FROM players WHERE player_name = :name"
            result = self.db.execute_query(query, {'name': name}, fetch=True)

            if result:
                player_id = result[0][0]
                # Update Understat ID
                self.db.execute_query(
                    "UPDATE players SET understat_id = :uid WHERE player_id = :pid",
                    {'uid': understat_id, 'pid': player_id},
                    fetch=False
                )
            else:
                # Create new player
                query = """
                    INSERT INTO players (player_name, understat_id)
                    VALUES (:name, :uid)
                    RETURNING player_id
                """
                result = self.db.execute_query(
                    query,
                    {'name': name, 'uid': understat_id},
                    fetch=True
                )
                player_id = result[0][0]
                self.stats['new_players'] += 1

        self._player_cache[understat_id] = player_id
        return player_id

    def enrich_player_matches(self, player_id: int, understat_player_id: int) -> int:
        """
        Enrich player match records with Understat xG data.

        Args:
            player_id: Database player ID
            understat_player_id: Understat player ID

        Returns:
            Number of matches enriched
        """
        matches = self.client.get_player_matches(understat_player_id)
        enriched = 0

        for match in matches:
            # Try to find matching match in our database by date and teams
            # This is approximate matching - could be improved with better ID mapping

            # Update player_match_stats if we find a match
            query = """
                UPDATE player_match_stats
                SET xg = :xg,
                    xa = :xa,
                    npxg = :npxg,
                    xg_chain = :xg_chain,
                    xg_buildup = :xg_buildup
                WHERE player_id = :pid
                  AND match_id IN (
                      SELECT m.match_id FROM matches m
                      WHERE m.match_date = :date
                  )
            """

            try:
                match_date = match.get('date')
                if match_date:
                    self.db.execute_query(query, {
                        'pid': player_id,
                        'date': match_date,
                        'xg': match.get('xg'),
                        'xa': match.get('xa'),
                        'npxg': match.get('npxg'),
                        'xg_chain': match.get('xg_chain'),
                        'xg_buildup': match.get('xg_buildup'),
                    }, fetch=False)
                    enriched += 1
            except Exception as e:
                logger.debug(f"Could not enrich match: {e}")

        return enriched

    def process_all_leagues(self, season: int) -> Dict:
        """
        Process all supported leagues for a season.

        Args:
            season: Season start year

        Returns:
            Combined statistics
        """
        for league in self.SUPPORTED_LEAGUES:
            try:
                self.process_league_season(league, season)
            except Exception as e:
                logger.error(f"Error processing {league}: {e}")
                self.stats['errors'].append(f"{league}: {str(e)}")

        return self.stats

    def get_statistics(self) -> Dict:
        """Return processing statistics."""
        return {
            **self.stats,
            'client_stats': self.client.get_statistics()
        }

    def reset_statistics(self):
        """Reset processing statistics."""
        self.stats = {
            'players_enriched': 0,
            'teams_enriched': 0,
            'new_players': 0,
            'new_teams': 0,
            'errors': []
        }
