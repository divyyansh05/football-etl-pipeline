"""
StatsBomb Advanced ETL Integration

Extracts progressive actions, pressing stats, and shot-creating actions
from StatsBomb open data for historical analysis and model training.

Key stats extracted:
- Progressive passes/carries
- Pressures and pressure regains
- Shot-creating actions (SCA)
- Goal-creating actions (GCA)
- Carries into final third / penalty area
- xA (from pass end location + shot outcome)
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import pandas as pd

from database.connection import get_db
from scrapers.statsbomb.client import StatsBombClient

logger = logging.getLogger(__name__)


class StatsBombAdvancedETL:
    """
    Advanced ETL pipeline for StatsBomb Open Data.

    Extracts progressive actions and pressing stats that are not
    available from FotMob or API-Football.
    """

    # StatsBomb event type IDs
    EVENT_TYPES = {
        'pass': 30,
        'carry': 43,
        'pressure': 17,
        'shot': 16,
        'dribble': 14,
        'interception': 10,
        'clearance': 9,
        'block': 6,
        'foul_committed': 22,
        'foul_won': 21,
        'duel': 4,
        'ball_recovery': 2,
        'dispossessed': 3,
        'miscontrol': 38,
    }

    # Pitch dimensions (StatsBomb uses 120x80)
    PITCH_LENGTH = 120
    PITCH_WIDTH = 80
    FINAL_THIRD_X = 80  # Last 40 yards = 80+ on 120-yard pitch
    PENALTY_AREA_X = 102  # ~18 yards from goal line
    PENALTY_AREA_Y_MIN = 18
    PENALTY_AREA_Y_MAX = 62

    def __init__(self, db=None):
        self.client = StatsBombClient()
        self.db = db or get_db()
        self.source_id = self._get_or_create_source()

        # Caches
        self._team_cache = {}  # name -> db_id
        self._player_cache = {}  # statsbomb_id -> db_id
        self._league_cache = {}  # (name, country) -> db_id
        self._season_cache = {}  # name -> db_id

        # Statistics
        self.stats = {
            'matches_processed': 0,
            'players_processed': 0,
            'teams_processed': 0,
            'events_analyzed': 0,
            'errors': []
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _get_or_create_source(self) -> int:
        """Get or create StatsBomb data source ID."""
        query = """
            INSERT INTO data_sources (source_name, base_url, reliability_score)
            VALUES ('statsbomb_open', 'https://github.com/statsbomb/open-data', 95)
            ON CONFLICT (source_name)
            DO UPDATE SET last_successful_scrape = CURRENT_TIMESTAMP
            RETURNING source_id
        """
        result = self.db.execute_query(query, fetch=True)
        return result[0][0]

    def get_available_competitions(self) -> List[Dict]:
        """Get list of available competitions from StatsBomb."""
        return self.client.get_competitions()

    def process_competition(self, competition_id: int, season_id: int) -> Dict:
        """
        Process all matches for a competition/season.

        Args:
            competition_id: StatsBomb competition ID
            season_id: StatsBomb season ID

        Returns:
            Statistics dictionary
        """
        logger.info(f"Processing StatsBomb competition {competition_id}, season {season_id}")

        matches = self.client.get_matches(competition_id, season_id)
        logger.info(f"Found {len(matches)} matches")

        for match in matches:
            try:
                self._process_match(match)
                self.stats['matches_processed'] += 1
            except Exception as e:
                logger.error(f"Error processing match {match.get('match_id')}: {e}")
                self.stats['errors'].append(f"Match {match.get('match_id')}: {str(e)}")

        return self.stats

    def _process_match(self, match_data: Dict):
        """Process a single match with advanced stats extraction."""
        match_id = match_data.get('match_id')
        if not match_id:
            return

        # Get competition/league info
        league_name = match_data.get('competition_name', 'Unknown')
        country = match_data.get('country_name', 'International')
        league_id = self._ensure_league(league_name, country)

        # Get season
        season_name = match_data.get('season_name', '2020/2021')
        season_id = self._ensure_season(season_name)

        # Get teams
        home_name = match_data.get('home_team', 'Unknown')
        away_name = match_data.get('away_team', 'Unknown')
        home_team_id = self._ensure_team(home_name, league_id)
        away_team_id = self._ensure_team(away_name, league_id)

        # Upsert match
        db_match_id = self._upsert_match(
            match_data, league_id, season_id,
            home_team_id, away_team_id
        )

        # Get events and calculate advanced stats
        events = self.client.get_match_events(match_id)
        if not events:
            logger.warning(f"No events found for match {match_id}")
            return

        self.stats['events_analyzed'] += len(events)

        # Process lineups to get player info
        lineups = self.client.get_lineups(match_id)
        for team_name, players in lineups.items():
            team_id = home_team_id if team_name == home_name else away_team_id
            for player in players:
                self._ensure_player(player, team_id)

        # Calculate and save advanced stats
        player_stats = self._calculate_player_advanced_stats(events, home_name, away_name)
        team_stats = self._calculate_team_advanced_stats(events, home_name, away_name)

        # Save team match stats
        self._save_team_match_stats(db_match_id, home_team_id, team_stats.get(home_name, {}), is_home=True)
        self._save_team_match_stats(db_match_id, away_team_id, team_stats.get(away_name, {}), is_home=False)

        # Save player match stats
        for player_name, stats in player_stats.items():
            player_id = self._get_player_id_by_name(player_name)
            if player_id:
                team_id = home_team_id if stats.get('team') == home_name else away_team_id
                self._save_player_match_stats(db_match_id, player_id, team_id, stats)

    def _calculate_player_advanced_stats(self, events: List[Dict], home_team: str, away_team: str) -> Dict[str, Dict]:
        """
        Calculate advanced per-player stats from event stream.

        Returns dict: player_name -> {stat: value}
        """
        player_stats = defaultdict(lambda: defaultdict(int))

        # Track previous events for SCA/GCA calculation
        recent_events = []  # Last N events for chain analysis

        for event in events:
            player = event.get('player')
            if not player:
                continue

            team = event.get('team')
            event_type = event.get('type', {})
            if isinstance(event_type, dict):
                event_type_name = event_type.get('name', '')
            else:
                event_type_name = str(event_type)

            player_stats[player]['team'] = team

            # Location data
            location = event.get('location', [0, 0])
            end_location = event.get('end_location') or event.get('pass', {}).get('end_location') or event.get('carry', {}).get('end_location')

            if isinstance(location, list) and len(location) >= 2:
                start_x, start_y = location[0], location[1]
            else:
                start_x, start_y = 0, 0

            if isinstance(end_location, list) and len(end_location) >= 2:
                end_x, end_y = end_location[0], end_location[1]
            else:
                end_x, end_y = start_x, start_y

            # === PASSES ===
            if event_type_name == 'Pass':
                pass_data = event.get('pass', {})
                outcome = pass_data.get('outcome', {})
                is_complete = outcome.get('name') != 'Incomplete' if outcome else True

                player_stats[player]['passes_attempted'] += 1
                if is_complete:
                    player_stats[player]['passes_completed'] += 1

                # Progressive pass: moves ball >=10m towards goal
                if is_complete and self._is_progressive(start_x, end_x):
                    player_stats[player]['progressive_passes'] += 1

                # Pass into final third
                if is_complete and start_x < self.FINAL_THIRD_X <= end_x:
                    player_stats[player]['passes_into_final_third'] += 1

                # Pass into penalty area
                if is_complete and self._is_in_penalty_area(end_x, end_y) and not self._is_in_penalty_area(start_x, start_y):
                    player_stats[player]['passes_into_penalty_area'] += 1

                # Long pass (>30m)
                if end_location:
                    distance = ((end_x - start_x)**2 + (end_y - start_y)**2)**0.5
                    if distance > 30:
                        player_stats[player]['long_passes_attempted'] += 1
                        if is_complete:
                            player_stats[player]['long_passes_completed'] += 1

                # Through ball
                if pass_data.get('through_ball'):
                    player_stats[player]['through_balls'] += 1

                # Cross
                if pass_data.get('cross'):
                    player_stats[player]['crosses_attempted'] += 1
                    if is_complete:
                        player_stats[player]['crosses_completed'] += 1

                # Switch
                if pass_data.get('switch'):
                    player_stats[player]['switches'] += 1

                # Key pass (shot assist)
                if pass_data.get('shot_assist') or pass_data.get('goal_assist'):
                    player_stats[player]['key_passes'] += 1

                # Assist
                if pass_data.get('goal_assist'):
                    player_stats[player]['assists'] += 1

            # === CARRIES ===
            elif event_type_name == 'Carry':
                # Progressive carry
                if self._is_progressive(start_x, end_x):
                    player_stats[player]['progressive_carries'] += 1

                # Carry into final third
                if start_x < self.FINAL_THIRD_X <= end_x:
                    player_stats[player]['carries_into_final_third'] += 1

                # Carry into penalty area
                if self._is_in_penalty_area(end_x, end_y) and not self._is_in_penalty_area(start_x, start_y):
                    player_stats[player]['carries_into_penalty_area'] += 1

            # === PRESSURES ===
            elif event_type_name == 'Pressure':
                player_stats[player]['pressures'] += 1

                # Check if pressure led to turnover (regain)
                pressure_data = event.get('pressure', {}) or {}
                if pressure_data.get('regained'):
                    player_stats[player]['pressure_regains'] += 1

                # Counterpressure (within 5 seconds of losing ball)
                if event.get('counterpress'):
                    player_stats[player]['counterpressures'] += 1

            # === SHOTS ===
            elif event_type_name == 'Shot':
                shot_data = event.get('shot', {})
                player_stats[player]['shots'] += 1

                # xG
                xg = shot_data.get('statsbomb_xg', 0)
                player_stats[player]['xg'] = player_stats[player].get('xg', 0) + (xg or 0)

                # Outcome
                outcome = shot_data.get('outcome', {}).get('name', '')
                if outcome == 'Goal':
                    player_stats[player]['goals'] += 1
                if outcome in ['Goal', 'Saved', 'Saved to Post']:
                    player_stats[player]['shots_on_target'] += 1

            # === DEFENSIVE ACTIONS ===
            elif event_type_name == 'Interception':
                player_stats[player]['interceptions'] += 1

            elif event_type_name == 'Clearance':
                player_stats[player]['clearances'] += 1

            elif event_type_name == 'Block':
                player_stats[player]['blocks'] += 1

            elif event_type_name == 'Ball Recovery':
                player_stats[player]['ball_recoveries'] += 1

            # === DUELS ===
            elif event_type_name == 'Duel':
                duel_data = event.get('duel', {})
                duel_type = duel_data.get('type', {}).get('name', '')
                outcome = duel_data.get('outcome', {}).get('name', '')

                won = outcome in ['Won', 'Success', 'Success In Play', 'Success Out']

                if duel_type == 'Aerial Lost' or 'Aerial' in duel_type:
                    if won:
                        player_stats[player]['aerial_duels_won'] += 1
                    else:
                        player_stats[player]['aerial_duels_lost'] += 1
                else:
                    if won:
                        player_stats[player]['ground_duels_won'] += 1
                    else:
                        player_stats[player]['ground_duels_lost'] += 1

            # === DRIBBLES ===
            elif event_type_name == 'Dribble':
                dribble_data = event.get('dribble', {})
                outcome = dribble_data.get('outcome', {}).get('name', '')

                player_stats[player]['dribbles_attempted'] += 1
                if outcome == 'Complete':
                    player_stats[player]['dribbles_completed'] += 1

            # === FOULS ===
            elif event_type_name == 'Foul Committed':
                player_stats[player]['fouls_committed'] += 1

                # Check for card
                card = event.get('foul_committed', {}).get('card', {})
                if card:
                    card_name = card.get('name', '')
                    if 'Yellow' in card_name:
                        player_stats[player]['yellow_cards'] += 1
                    if 'Red' in card_name:
                        player_stats[player]['red_cards'] += 1

            elif event_type_name == 'Foul Won':
                player_stats[player]['fouls_won'] += 1

            # === GOALKEEPER ===
            elif event_type_name == 'Goal Keeper':
                gk_data = event.get('goalkeeper', {})
                gk_type = gk_data.get('type', {}).get('name', '')

                if 'Save' in gk_type:
                    player_stats[player]['saves'] += 1
                if 'Punch' in gk_type:
                    player_stats[player]['punches'] += 1
                if 'Collected' in gk_type and 'Cross' in gk_type:
                    player_stats[player]['crosses_stopped'] += 1
                if 'Sweeper' in gk_type:
                    player_stats[player]['sweeper_actions'] += 1

            # Track for SCA/GCA calculation
            recent_events.append({
                'player': player,
                'team': team,
                'type': event_type_name,
                'timestamp': event.get('timestamp'),
                'index': event.get('index')
            })

            # Keep only last 10 events
            if len(recent_events) > 10:
                recent_events.pop(0)

        # Calculate SCA/GCA by looking at events leading to shots/goals
        self._calculate_sca_gca(events, player_stats)

        return dict(player_stats)

    def _calculate_sca_gca(self, events: List[Dict], player_stats: Dict):
        """Calculate Shot-Creating and Goal-Creating Actions."""
        # Find all shots and goals, then trace back 2 actions
        for i, event in enumerate(events):
            event_type = event.get('type', {})
            if isinstance(event_type, dict):
                event_type_name = event_type.get('name', '')
            else:
                event_type_name = str(event_type)

            if event_type_name != 'Shot':
                continue

            shot_data = event.get('shot', {})
            is_goal = shot_data.get('outcome', {}).get('name') == 'Goal'
            shooting_team = event.get('team')

            # Look at previous 2 events by the same team
            actions_before = []
            for j in range(i-1, max(i-6, -1), -1):  # Look back up to 5 events
                prev_event = events[j]
                if prev_event.get('team') == shooting_team:
                    prev_type = prev_event.get('type', {})
                    if isinstance(prev_type, dict):
                        prev_type_name = prev_type.get('name', '')
                    else:
                        prev_type_name = str(prev_type)

                    # SCA-eligible actions
                    if prev_type_name in ['Pass', 'Dribble', 'Foul Won', 'Shot']:
                        prev_player = prev_event.get('player')
                        if prev_player and prev_player != event.get('player'):
                            actions_before.append(prev_player)

                        if len(actions_before) >= 2:
                            break

            # Credit SCA/GCA to players in the chain
            for player in actions_before[:2]:
                if player in player_stats:
                    player_stats[player]['shot_creating_actions'] += 1
                    if is_goal:
                        player_stats[player]['goal_creating_actions'] += 1

    def _calculate_team_advanced_stats(self, events: List[Dict], home_team: str, away_team: str) -> Dict[str, Dict]:
        """Calculate advanced team-level stats."""
        team_stats = {
            home_team: defaultdict(int),
            away_team: defaultdict(int)
        }

        for event in events:
            team = event.get('team')
            if team not in team_stats:
                continue

            event_type = event.get('type', {})
            if isinstance(event_type, dict):
                event_type_name = event_type.get('name', '')
            else:
                event_type_name = str(event_type)

            location = event.get('location', [0, 0])
            if isinstance(location, list) and len(location) >= 2:
                x = location[0]
            else:
                x = 0

            # Pressures
            if event_type_name == 'Pressure':
                team_stats[team]['pressures'] += 1
                if event.get('pressure', {}).get('regained'):
                    team_stats[team]['pressure_regains'] += 1

            # Progressive passes
            if event_type_name == 'Pass':
                end_loc = event.get('pass', {}).get('end_location', [0, 0])
                if isinstance(end_loc, list) and len(end_loc) >= 2:
                    end_x = end_loc[0]
                    if self._is_progressive(x, end_x):
                        team_stats[team]['progressive_passes'] += 1

            # Progressive carries
            if event_type_name == 'Carry':
                end_loc = event.get('carry', {}).get('end_location', [0, 0])
                if isinstance(end_loc, list) and len(end_loc) >= 2:
                    end_x = end_loc[0]
                    if self._is_progressive(x, end_x):
                        team_stats[team]['progressive_carries'] += 1

            # Shots
            if event_type_name == 'Shot':
                team_stats[team]['shots'] += 1
                if self._is_in_penalty_area(x, location[1] if len(location) > 1 else 40):
                    team_stats[team]['shots_inside_box'] += 1
                else:
                    team_stats[team]['shots_outside_box'] += 1

            # Defensive actions
            if event_type_name == 'Interception':
                team_stats[team]['interceptions'] += 1
            if event_type_name == 'Clearance':
                team_stats[team]['clearances'] += 1
            if event_type_name == 'Block':
                team_stats[team]['blocks'] += 1
            if event_type_name == 'Tackle':
                team_stats[team]['tackles'] += 1

        # Calculate PPDA (Passes per Defensive Action)
        # This needs opponent's passes in attacking third
        for team in [home_team, away_team]:
            opponent = away_team if team == home_team else home_team
            opp_passes_att_third = team_stats[opponent].get('passes_attacking_third', 0)
            def_actions = (team_stats[team]['pressures'] +
                          team_stats[team]['tackles'] +
                          team_stats[team]['interceptions'])
            if def_actions > 0:
                team_stats[team]['ppda'] = opp_passes_att_third / def_actions

        return {k: dict(v) for k, v in team_stats.items()}

    def _is_progressive(self, start_x: float, end_x: float, threshold: float = 10) -> bool:
        """Check if a pass/carry is progressive (moves ball towards goal)."""
        # Ball moves at least 10m (on 120m pitch) towards opponent goal
        return (end_x - start_x) >= threshold

    def _is_in_penalty_area(self, x: float, y: float) -> bool:
        """Check if coordinates are in the penalty area."""
        return (x >= self.PENALTY_AREA_X and
                self.PENALTY_AREA_Y_MIN <= y <= self.PENALTY_AREA_Y_MAX)

    def _ensure_league(self, name: str, country: str) -> int:
        """Ensure league exists and return ID."""
        cache_key = (name, country)
        if cache_key in self._league_cache:
            return self._league_cache[cache_key]

        query = """
            INSERT INTO leagues (league_name, country)
            VALUES (:name, :country)
            ON CONFLICT (league_name, country) DO UPDATE SET tier=1
            RETURNING league_id
        """
        result = self.db.execute_query(query, {'name': name, 'country': country}, fetch=True)
        league_id = result[0][0]
        self._league_cache[cache_key] = league_id
        return league_id

    def _ensure_season(self, name: str) -> int:
        """Ensure season exists and return ID."""
        if name in self._season_cache:
            return self._season_cache[name]

        # Parse season name
        try:
            parts = str(name).split('/')
            start = int(parts[0])
            end = int(parts[1]) if len(parts) > 1 else start
        except:
            start, end = 2020, 2021

        # Convert to our format: "2020/2021" -> "2020-21"
        db_name = f"{start}-{str(end)[-2:]}"

        query = """
            INSERT INTO seasons (season_name, start_year, end_year)
            VALUES (:name, :start, :end)
            ON CONFLICT (season_name) DO UPDATE SET start_year=EXCLUDED.start_year
            RETURNING season_id
        """
        result = self.db.execute_query(query, {'name': db_name, 'start': start, 'end': end}, fetch=True)
        season_id = result[0][0]
        self._season_cache[name] = season_id
        return season_id

    def _ensure_team(self, name: str, league_id: int) -> int:
        """Ensure team exists and return ID."""
        cache_key = (name, league_id)
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]

        query = """
            INSERT INTO teams (team_name, league_id)
            VALUES (:name, :lid)
            ON CONFLICT (team_name, league_id) DO UPDATE SET updated_at=CURRENT_TIMESTAMP
            RETURNING team_id
        """
        result = self.db.execute_query(query, {'name': name, 'lid': league_id}, fetch=True)
        team_id = result[0][0]
        self._team_cache[cache_key] = team_id
        self.stats['teams_processed'] += 1
        return team_id

    def _ensure_player(self, player_data: Dict, team_id: int) -> int:
        """Ensure player exists and return ID."""
        sb_id = player_data.get('player_id')
        name = player_data.get('player_name', 'Unknown')

        # Handle NaN, None, or invalid names
        if name is None or (isinstance(name, float) and pd.isna(name)):
            name = 'Unknown'
        else:
            name = str(name)
            if name.lower() in ('nan', 'none', ''):
                name = 'Unknown'

        if sb_id in self._player_cache:
            return self._player_cache[sb_id]

        # Check if exists by StatsBomb ID
        query = "SELECT player_id FROM players WHERE statsbomb_id = :sb_id"
        result = self.db.execute_query(query, {'sb_id': sb_id}, fetch=True)

        if result:
            player_id = result[0][0]
        else:
            # Try by name (only if we have a valid name)
            if name != 'Unknown':
                query = "SELECT player_id FROM players WHERE player_name = :name"
                result = self.db.execute_query(query, {'name': name}, fetch=True)
            else:
                result = None

            if result:
                player_id = result[0][0]
                # Update StatsBomb ID
                self.db.execute_query(
                    "UPDATE players SET statsbomb_id = :sb_id WHERE player_id = :pid",
                    {'sb_id': sb_id, 'pid': player_id},
                    fetch=False
                )
            else:
                # Insert new player
                position = player_data.get('position', {})
                if isinstance(position, dict):
                    position = position.get('name')

                query = """
                    INSERT INTO players (player_name, position, statsbomb_id)
                    VALUES (:name, :pos, :sb_id)
                    RETURNING player_id
                """
                result = self.db.execute_query(
                    query,
                    {'name': name, 'pos': position, 'sb_id': sb_id},
                    fetch=True
                )
                player_id = result[0][0]
                self.stats['players_processed'] += 1

        self._player_cache[sb_id] = player_id
        return player_id

    def _get_player_id_by_name(self, name: str) -> Optional[int]:
        """Get player ID by name from cache or DB."""
        # Handle NaN, None, or empty names
        if name is None or (isinstance(name, float) and pd.isna(name)) or name == '':
            return None

        # Convert to string in case it's not
        name = str(name)
        if name.lower() in ('nan', 'none', 'unknown', ''):
            return None

        # Check cache
        for sb_id, pid in self._player_cache.items():
            # This is inefficient, but works for demo
            pass

        query = "SELECT player_id FROM players WHERE player_name = :name"
        result = self.db.execute_query(query, {'name': name}, fetch=True)
        return result[0][0] if result else None

    def _upsert_match(self, match_data: Dict, league_id: int, season_id: int,
                      home_id: int, away_id: int) -> int:
        """Upsert match and return ID."""
        query = """
            INSERT INTO matches (
                league_id, season_id, match_date,
                home_team_id, away_team_id,
                home_score, away_score, venue,
                data_source_id
            )
            VALUES (
                :lid, :sid, :date,
                :hid, :aid,
                :hs, :as, :venue,
                :src
            )
            ON CONFLICT (league_id, season_id, home_team_id, away_team_id, match_date)
            DO UPDATE SET home_score=EXCLUDED.home_score, away_score=EXCLUDED.away_score
            RETURNING match_id
        """
        params = {
            'lid': league_id,
            'sid': season_id,
            'date': match_data.get('match_date'),
            'hid': home_id,
            'aid': away_id,
            'hs': match_data.get('home_score', 0),
            'as': match_data.get('away_score', 0),
            'venue': match_data.get('stadium_name'),
            'src': self.source_id
        }
        result = self.db.execute_query(query, params, fetch=True)
        return result[0][0]

    def _save_team_match_stats(self, match_id: int, team_id: int, stats: Dict, is_home: bool):
        """Save team match stats."""
        query = """
            INSERT INTO team_match_stats (
                match_id, team_id, is_home,
                pressures, pressure_regains, ppda,
                progressive_passes, progressive_carries,
                shots_inside_box, shots_outside_box,
                tackles, interceptions, clearances, blocks,
                data_source_id
            )
            VALUES (
                :mid, :tid, :home,
                :pressures, :pressure_regains, :ppda,
                :prog_pass, :prog_carry,
                :shots_in, :shots_out,
                :tackles, :interceptions, :clearances, :blocks,
                :src
            )
            ON CONFLICT (match_id, team_id) DO UPDATE SET
                pressures = EXCLUDED.pressures,
                pressure_regains = EXCLUDED.pressure_regains,
                ppda = EXCLUDED.ppda,
                progressive_passes = EXCLUDED.progressive_passes,
                progressive_carries = EXCLUDED.progressive_carries
        """
        params = {
            'mid': match_id,
            'tid': team_id,
            'home': is_home,
            'pressures': stats.get('pressures', 0),
            'pressure_regains': stats.get('pressure_regains', 0),
            'ppda': stats.get('ppda'),
            'prog_pass': stats.get('progressive_passes', 0),
            'prog_carry': stats.get('progressive_carries', 0),
            'shots_in': stats.get('shots_inside_box', 0),
            'shots_out': stats.get('shots_outside_box', 0),
            'tackles': stats.get('tackles', 0),
            'interceptions': stats.get('interceptions', 0),
            'clearances': stats.get('clearances', 0),
            'blocks': stats.get('blocks', 0),
            'src': self.source_id
        }
        self.db.execute_query(query, params, fetch=False)

    def _save_player_match_stats(self, match_id: int, player_id: int, team_id: int, stats: Dict):
        """Save player match stats with advanced metrics."""
        query = """
            INSERT INTO player_match_stats (
                match_id, player_id, team_id,
                goals, assists, shots, shots_on_target,
                xg, xa, key_passes,
                passes_completed, passes_attempted,
                progressive_passes, progressive_carries, progressive_passes_received,
                carries_into_final_third, carries_into_penalty_area,
                passes_into_final_third, passes_into_penalty_area,
                pressures, pressure_regains, counterpressures,
                shot_creating_actions, goal_creating_actions,
                long_passes_completed, long_passes_attempted,
                through_balls, crosses_completed, crosses_attempted, switches,
                tackles, interceptions, blocks, clearances,
                ground_duels_won, ground_duels_lost,
                aerial_duels_won, aerial_duels_lost,
                dribbles_completed, dribbles_attempted,
                fouls_committed, fouls_won,
                yellow_cards, red_cards,
                saves, punches, crosses_stopped, sweeper_actions,
                data_source_id
            )
            VALUES (
                :mid, :pid, :tid,
                :goals, :assists, :shots, :sot,
                :xg, :xa, :key_passes,
                :pass_comp, :pass_att,
                :prog_pass, :prog_carry, :prog_recv,
                :carry_ft, :carry_pa,
                :pass_ft, :pass_pa,
                :pressures, :press_reg, :counter,
                :sca, :gca,
                :long_comp, :long_att,
                :through, :cross_comp, :cross_att, :switches,
                :tackles, :interceptions, :blocks, :clearances,
                :ground_won, :ground_lost,
                :aerial_won, :aerial_lost,
                :drib_comp, :drib_att,
                :fouls_comm, :fouls_won,
                :yellow, :red,
                :saves, :punches, :crosses_stopped, :sweeper,
                :src
            )
            ON CONFLICT (match_id, player_id) DO UPDATE SET
                progressive_passes = EXCLUDED.progressive_passes,
                progressive_carries = EXCLUDED.progressive_carries,
                pressures = EXCLUDED.pressures,
                shot_creating_actions = EXCLUDED.shot_creating_actions,
                goal_creating_actions = EXCLUDED.goal_creating_actions,
                xg = EXCLUDED.xg
        """
        params = {
            'mid': match_id,
            'pid': player_id,
            'tid': team_id,
            'goals': stats.get('goals', 0),
            'assists': stats.get('assists', 0),
            'shots': stats.get('shots', 0),
            'sot': stats.get('shots_on_target', 0),
            'xg': stats.get('xg'),
            'xa': stats.get('xa'),
            'key_passes': stats.get('key_passes', 0),
            'pass_comp': stats.get('passes_completed', 0),
            'pass_att': stats.get('passes_attempted', 0),
            'prog_pass': stats.get('progressive_passes', 0),
            'prog_carry': stats.get('progressive_carries', 0),
            'prog_recv': stats.get('progressive_passes_received', 0),
            'carry_ft': stats.get('carries_into_final_third', 0),
            'carry_pa': stats.get('carries_into_penalty_area', 0),
            'pass_ft': stats.get('passes_into_final_third', 0),
            'pass_pa': stats.get('passes_into_penalty_area', 0),
            'pressures': stats.get('pressures', 0),
            'press_reg': stats.get('pressure_regains', 0),
            'counter': stats.get('counterpressures', 0),
            'sca': stats.get('shot_creating_actions', 0),
            'gca': stats.get('goal_creating_actions', 0),
            'long_comp': stats.get('long_passes_completed', 0),
            'long_att': stats.get('long_passes_attempted', 0),
            'through': stats.get('through_balls', 0),
            'cross_comp': stats.get('crosses_completed', 0),
            'cross_att': stats.get('crosses_attempted', 0),
            'switches': stats.get('switches', 0),
            'tackles': stats.get('tackles', 0),
            'interceptions': stats.get('interceptions', 0),
            'blocks': stats.get('blocks', 0),
            'clearances': stats.get('clearances', 0),
            'ground_won': stats.get('ground_duels_won', 0),
            'ground_lost': stats.get('ground_duels_lost', 0),
            'aerial_won': stats.get('aerial_duels_won', 0),
            'aerial_lost': stats.get('aerial_duels_lost', 0),
            'drib_comp': stats.get('dribbles_completed', 0),
            'drib_att': stats.get('dribbles_attempted', 0),
            'fouls_comm': stats.get('fouls_committed', 0),
            'fouls_won': stats.get('fouls_won', 0),
            'yellow': stats.get('yellow_cards', 0),
            'red': stats.get('red_cards', 0),
            'saves': stats.get('saves', 0),
            'punches': stats.get('punches', 0),
            'crosses_stopped': stats.get('crosses_stopped', 0),
            'sweeper': stats.get('sweeper_actions', 0),
            'src': self.source_id
        }
        self.db.execute_query(query, params, fetch=False)

    def get_statistics(self) -> Dict:
        """Return processing statistics."""
        return self.stats

    def reset_statistics(self):
        """Reset processing statistics."""
        self.stats = {
            'matches_processed': 0,
            'players_processed': 0,
            'teams_processed': 0,
            'events_analyzed': 0,
            'errors': []
        }
