"""
FotMob API response parser.

Normalizes FotMob JSON responses into database-ready dictionaries.
All methods are static for stateless parsing.

Response structure reference:
- Leagues: table.data[0].table.all[i] for standings
- Teams: details.sportsTeamJSONLD.athlete for squad
- Players: playerInformation for bio, mainLeague.stats for current stats
- Matches: header.teams for scores, stats.Periods.All for stats
"""

import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime, date

logger = logging.getLogger(__name__)


class FotMobDataParser:
    """
    Static parser for FotMob API responses.

    Converts raw API JSON into normalized dictionaries ready for database insertion.
    """

    # =========================================
    # LEAGUE PARSING
    # =========================================

    @staticmethod
    def parse_league_details(data: Dict) -> Optional[Dict]:
        """
        Parse league metadata from /leagues response.

        Returns: {fotmob_id, name, short_name, country, selected_season, latest_season}
        """
        details = data.get('details')
        if not details:
            return None

        return {
            'fotmob_id': details.get('id'),
            'name': details.get('name'),
            'short_name': details.get('shortName'),
            'country': details.get('country'),
            'selected_season': details.get('selectedSeason'),
            'latest_season': details.get('latestSeason'),
        }

    @staticmethod
    def parse_league_standings(data) -> List[Dict]:
        """
        Parse standings from /leagues response.

        Path: table.data[0].table.all[i]

        Returns list of: {
            fotmob_team_id, team_name, short_name, position,
            played, wins, draws, losses, goals_for, goals_against,
            goal_difference, points
        }
        """
        standings = []
        try:
            # Handle None or non-dict data
            if data is None or not isinstance(data, dict):
                return standings

            # Handle both old format (dict with 'data' key) and new format (direct list)
            table = data.get('table', {})
            if isinstance(table, list):
                # New format: table is directly a list
                table_data = table
            else:
                # Old format: table is dict with 'data' key
                table_data = table.get('data', [])

            if not table_data:
                return standings

            # Get the first table entry
            first_table = table_data[0] if table_data else {}

            # Handle nested structure - new format: data.table.all, old format: table.all
            if 'data' in first_table and isinstance(first_table.get('data'), dict):
                # New format: table[0].data.table.all
                all_table = first_table.get('data', {}).get('table', {}).get('all', [])
            elif 'table' in first_table:
                all_table = first_table.get('table', {}).get('all', [])
            else:
                all_table = first_table.get('all', [])

            for team in all_table:
                # Parse scoresStr "42-22" into goals_for and goals_against
                scores_str = team.get('scoresStr', '0-0')
                goals = scores_str.split('-')
                goals_for = int(goals[0]) if len(goals) >= 2 else 0
                goals_against = int(goals[1]) if len(goals) >= 2 else 0

                standings.append({
                    'fotmob_team_id': team.get('id'),
                    'team_name': team.get('name'),
                    'short_name': team.get('shortName'),
                    'position': team.get('idx'),
                    'played': team.get('played', 0),
                    'wins': team.get('wins', 0),
                    'draws': team.get('draws', 0),
                    'losses': team.get('losses', 0),
                    'goals_for': goals_for,
                    'goals_against': goals_against,
                    'goal_difference': team.get('goalConDiff', 0),
                    'points': team.get('pts', 0),
                })

        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Error parsing standings: {e}")

        return standings

    @staticmethod
    def parse_league_matches(data) -> List[Dict]:
        """
        Parse matches from /leagues response (via team form data).

        Returns list of match info extracted from teamForm tooltips.
        Note: For full match data, use get_match() endpoint per match.
        """
        matches = []
        try:
            # Handle None or non-dict data
            if data is None or not isinstance(data, dict):
                return matches

            team_form = data.get('teamForm', {})
            seen_matches = set()

            for team_id, form_entries in team_form.items():
                if not isinstance(form_entries, list):
                    continue
                for entry in form_entries:
                    tooltip = entry.get('tooltipText', {})
                    if not tooltip:
                        continue

                    # Extract match link for ID
                    link = entry.get('linkToMatch', '')
                    match_id = None
                    if link:
                        # Extract match ID from URL like "/match/4193490/..."
                        parts = link.split('/')
                        for i, part in enumerate(parts):
                            if part == 'match' and i + 1 < len(parts):
                                try:
                                    match_id = int(parts[i + 1])
                                except ValueError:
                                    pass

                    if match_id and match_id not in seen_matches:
                        seen_matches.add(match_id)
                        matches.append({
                            'fotmob_match_id': match_id,
                            'home_team_name': tooltip.get('homeTeam'),
                            'away_team_name': tooltip.get('awayTeam'),
                            'home_fotmob_id': tooltip.get('homeTeamId'),
                            'away_fotmob_id': tooltip.get('awayTeamId'),
                            'home_score': tooltip.get('homeScore'),
                            'away_score': tooltip.get('awayScore'),
                            'match_date': tooltip.get('utcTime'),
                            'score_str': entry.get('score'),
                        })

        except (KeyError, TypeError) as e:
            logger.warning(f"Error parsing league matches: {e}")

        return matches

    @staticmethod
    def parse_available_seasons(data: Dict) -> List[Dict]:
        """
        Parse available seasons from /leagues response.

        Path: allAvailableSeasons

        Returns list of: {season_id, season_name, is_current}
        """
        seasons = []
        try:
            all_seasons = data.get('allAvailableSeasons', [])
            details = data.get('details', {})
            current_season = details.get('selectedSeason') or details.get('latestSeason')

            for season in all_seasons:
                if isinstance(season, str):
                    seasons.append({
                        'season_id': season,
                        'season_name': season,
                        'is_current': season == current_season,
                    })
                elif isinstance(season, dict):
                    name = season.get('name') or season.get('id', '')
                    seasons.append({
                        'season_id': str(season.get('id', name)),
                        'season_name': name,
                        'is_current': name == current_season,
                    })

        except (KeyError, TypeError) as e:
            logger.warning(f"Error parsing seasons: {e}")

        return seasons

    @staticmethod
    def parse_xg_table(data) -> List[Dict]:
        """
        Parse xG table from /leagues response.

        Path: table.data[0].table.xg[i]

        Returns list of: {fotmob_team_id, team_name, xg, xg_conceded, x_points, x_position}
        """
        xg_data = []
        try:
            # Handle case where data itself is None or not a dict
            if data is None:
                return xg_data
            if isinstance(data, list):
                # API returned a list directly - log and skip
                logger.debug("parse_xg_table received list instead of dict, skipping")
                return xg_data
            if not isinstance(data, dict):
                return xg_data

            # Handle both old format (dict with 'data' key) and new format (direct list)
            table = data.get('table', {})
            if isinstance(table, list):
                table_data = table
            else:
                table_data = table.get('data', [])

            if not table_data:
                return xg_data

            first_table = table_data[0] if table_data else {}

            # Handle nested structure - new format: data.table.xg, old format: table.xg
            if 'data' in first_table and isinstance(first_table.get('data'), dict):
                xg_table = first_table.get('data', {}).get('table', {}).get('xg', [])
            elif 'table' in first_table:
                xg_table = first_table.get('table', {}).get('xg', [])
            else:
                xg_table = first_table.get('xg', [])

            for team in xg_table:
                xg_data.append({
                    'fotmob_team_id': team.get('id'),
                    'team_name': team.get('name'),
                    'xg': team.get('xg'),
                    'xg_conceded': team.get('xgConceded'),
                    'x_points': team.get('xPoints'),
                    'x_position': team.get('xPosition'),
                })

        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Error parsing xG table: {e}")

        return xg_data

    # =========================================
    # TEAM PARSING
    # =========================================

    @staticmethod
    def parse_team(data: Dict) -> Optional[Dict]:
        """
        Parse team info from /teams response.

        Returns: {fotmob_id, name, short_name, country, primary_league_id, primary_league_name}
        """
        details = data.get('details')
        if not details:
            return None

        return {
            'fotmob_id': details.get('id'),
            'name': details.get('name'),
            'short_name': details.get('shortName'),
            'country': details.get('country'),
            'primary_league_id': details.get('primaryLeagueId'),
            'primary_league_name': details.get('primaryLeagueName'),
        }

    @staticmethod
    def parse_team_squad(data: Dict) -> List[Dict]:
        """
        Parse squad from /teams response.

        New path: squad.squad[].members[]
        Old path: details.sportsTeamJSONLD.athlete (fallback)

        Returns list of: {
            name, fotmob_id, position, nationality, age, height_cm
        }
        """
        squad = []
        try:
            # New format: squad.squad[{title, members}]
            squad_container = data.get('squad', {})
            if isinstance(squad_container, dict):
                position_groups = squad_container.get('squad', [])

                for group in position_groups:
                    position_title = group.get('title', '')
                    members = group.get('members', [])

                    for player in members:
                        # Skip coaches
                        role = player.get('role', {})
                        if isinstance(role, dict) and role.get('key') == 'coach':
                            continue

                        squad.append({
                            'name': player.get('name'),
                            'fotmob_id': player.get('id'),
                            'position': position_title,
                            'nationality': player.get('cname'),
                            'nationality_code': player.get('ccode'),
                            'age': player.get('age'),
                            'height_cm': player.get('height'),
                            'date_of_birth': player.get('dateOfBirth'),
                        })

            # Fallback to old format
            if not squad:
                json_ld = data.get('details', {}).get('sportsTeamJSONLD', {})
                athletes = json_ld.get('athlete', [])

                for athlete in athletes:
                    url = athlete.get('url', '')
                    player_id = None
                    if url:
                        parts = url.strip('/').split('/')
                        for i, part in enumerate(parts):
                            if part == 'players' and i + 1 < len(parts):
                                try:
                                    player_id = int(parts[i + 1])
                                except ValueError:
                                    pass

                    squad.append({
                        'name': athlete.get('name'),
                        'fotmob_id': player_id,
                        'nationality': athlete.get('nationality', {}).get('name') if isinstance(athlete.get('nationality'), dict) else athlete.get('nationality'),
                    })

        except (KeyError, TypeError) as e:
            logger.warning(f"Error parsing team squad: {e}")

        return squad

    # =========================================
    # PLAYER PARSING
    # =========================================

    @staticmethod
    def parse_player(data: Dict) -> Optional[Dict]:
        """
        Parse player data from /playerData response.

        Returns: {
            fotmob_id, name, date_of_birth, nationality, position,
            height_cm, preferred_foot, market_value, shirt_number,
            current_team_id, current_team_name, is_captain
        }
        """
        if not data or 'id' not in data:
            return None

        # Parse birth date
        dob = None
        birth_date = data.get('birthDate', {})
        if isinstance(birth_date, dict):
            utc_time = birth_date.get('utcTime')
            if utc_time:
                try:
                    dob = datetime.fromisoformat(utc_time.replace('Z', '+00:00')).date()
                except (ValueError, TypeError):
                    pass

        # Parse position
        position = None
        pos_desc = data.get('positionDescription', {})
        if pos_desc:
            primary_pos = pos_desc.get('primaryPosition', {})
            position = primary_pos.get('label')

        # Parse playerInformation array for bio details
        height_cm = None
        preferred_foot = None
        market_value = None
        shirt_number = None
        nationality = None

        for info in data.get('playerInformation', []):
            title = info.get('title', '').lower()
            value = info.get('value', '')

            if 'height' in title:
                # Parse "178 cm" or "178"
                match = re.search(r'(\d+)', str(value))
                if match:
                    height_cm = int(match.group(1))

            elif 'foot' in title:
                # Handle dict format: {'key': 'right', 'fallback': 'Right'}
                if isinstance(value, dict):
                    preferred_foot = value.get('fallback') or value.get('key', '')
                elif hasattr(value, 'get'):
                    # Handle dict-like objects
                    preferred_foot = value.get('fallback') or value.get('key', '')
                else:
                    val_str = str(value)
                    # Ensure we don't store a stringified dict
                    if val_str.startswith('{') and 'fallback' in val_str:
                        # This is a stringified dict - skip it
                        preferred_foot = None
                    else:
                        preferred_foot = val_str

            elif 'market' in title and 'value' in title:
                # Handle dict format: {'key': None, 'fallback': '€25M'}
                if isinstance(value, dict):
                    market_value = value.get('fallback') or value.get('key', '')
                else:
                    market_value = str(value)

            elif 'shirt' in title:
                try:
                    if isinstance(value, dict):
                        value = value.get('fallback') or value.get('key', 0)
                    shirt_number = int(value)
                except (ValueError, TypeError):
                    pass

            elif 'country' in title:
                # Handle dict format: {'key': None, 'fallback': 'Spain'}
                if isinstance(value, dict):
                    nationality = value.get('fallback') or value.get('key', '')
                elif hasattr(value, 'get'):
                    # Handle dict-like objects
                    nationality = value.get('fallback') or value.get('key', '')
                else:
                    val_str = str(value)
                    # Ensure we don't store a stringified dict
                    if val_str.startswith('{') and 'fallback' in val_str:
                        nationality = None
                    else:
                        nationality = val_str

        # Parse primary team
        primary_team = data.get('primaryTeam', {})

        return {
            'fotmob_id': data.get('id'),
            'name': data.get('name'),
            'date_of_birth': dob,
            'nationality': nationality,
            'position': position,
            'height_cm': height_cm,
            'preferred_foot': preferred_foot,
            'market_value': market_value,
            'shirt_number': shirt_number,
            'current_team_id': primary_team.get('teamId'),
            'current_team_name': primary_team.get('teamName'),
            'is_captain': data.get('isCaptain', False),
        }

    @staticmethod
    def parse_player_season_stats(data: Dict) -> Optional[Dict]:
        """
        Parse current season stats from /playerData response.

        Path: mainLeague.stats

        Returns: {
            league_id, league_name, season,
            goals, assists, matches, started, minutes_played,
            rating, yellow_cards, red_cards
        }
        """
        main_league = data.get('mainLeague')
        if not main_league:
            return None

        stats = {}
        for stat in main_league.get('stats', []):
            title = stat.get('title', '').lower()
            value = stat.get('value')

            if 'goal' in title and 'assist' not in title:
                stats['goals'] = value
            elif 'assist' in title:
                stats['assists'] = value
            elif 'started' in title:
                stats['started'] = value
            elif 'match' in title:
                stats['matches'] = value
            elif 'minute' in title:
                stats['minutes_played'] = value
            elif 'rating' in title:
                stats['rating'] = value
            elif 'yellow' in title:
                stats['yellow_cards'] = value
            elif 'red' in title:
                stats['red_cards'] = value

        return {
            'league_id': main_league.get('leagueId'),
            'league_name': main_league.get('leagueName'),
            'season': main_league.get('season'),
            **stats,
        }

    @staticmethod
    def parse_player_deep_stats(data: Dict) -> Optional[Dict]:
        """
        Parse deep player statistics from /playerData response.

        Path: firstSeasonStats.statsSection.items

        Returns comprehensive stats dict with:
        - Shooting: goals, xG, xGOT, npxG, shots, shots_on_target
        - Passing: assists, xA, passes, key_passes, progressive_passes
        - Possession: touches, dribbles, carries, progressive_carries
        - Defending: tackles, interceptions, clearances, blocks, pressures
        - Plus percentile rankings for all stats
        """
        fss = data.get('firstSeasonStats', {})
        stats_section = fss.get('statsSection', {})

        if not stats_section:
            return None

        items = stats_section.get('items', [])
        if not items:
            return None

        result = {
            # Shooting
            'goals': None, 'xg': None, 'xgot': None, 'npxg': None,
            'penalty_goals': None, 'shots': None, 'shots_on_target': None,
            'headed_shots': None,
            # Passing
            'assists': None, 'xa': None, 'accurate_passes': None,
            'pass_accuracy': None, 'key_passes': None, 'big_chances_created': None,
            'accurate_long_balls': None, 'accurate_crosses': None,
            # Possession
            'touches': None, 'touches_in_box': None,
            'successful_dribbles': None, 'dribble_success_rate': None,
            # Defending
            'tackles': None, 'tackle_success_rate': None,
            'interceptions': None, 'clearances': None, 'blocks': None,
            'headed_clearances': None, 'recoveries': None,
            # Duels
            'duels_won': None, 'duels_won_pct': None,
            'ground_duels_won': None, 'aerial_duels_won': None,
            # Discipline
            'fouls_committed': None, 'fouls_won': None,
            # Percentiles (per 90)
            'xg_percentile': None, 'xa_percentile': None,
            'npxg_percentile': None, 'shots_percentile': None,
            'passes_percentile': None, 'tackles_percentile': None,
            # Per 90 stats
            'goals_per_90': None, 'xg_per_90': None, 'xa_per_90': None,
            'shots_per_90': None, 'tackles_per_90': None,
        }

        # Stat title to field mapping
        STAT_MAP = {
            # Shooting
            'goals': 'goals', 'xg': 'xg', 'xgot': 'xgot',
            'xg excl. penalty': 'npxg', 'non_penalty_xg': 'npxg',
            'penalty goals': 'penalty_goals', 'shots': 'shots',
            'shots on target': 'shots_on_target', 'headed shots': 'headed_shots',
            # Passing
            'assists': 'assists', 'xa': 'xa', 'expected_assists': 'xa',
            'accurate passes': 'accurate_passes', 'successful_passes': 'accurate_passes',
            'pass accuracy': 'pass_accuracy', 'successful_passes_accuracy': 'pass_accuracy',
            'key passes': 'key_passes', 'big chances created': 'big_chances_created',
            'accurate long balls': 'accurate_long_balls', 'long_balls_accurate': 'accurate_long_balls',
            'accurate crosses': 'accurate_crosses', 'crosses_accurate': 'accurate_crosses',
            # Possession
            'touches': 'touches', 'touches in opposition box': 'touches_in_box',
            'successful dribbles': 'successful_dribbles', 'dribbles_successful': 'successful_dribbles',
            'dribble success': 'dribble_success_rate',
            # Defending
            'tackles': 'tackles', 'tackles won': 'tackles',
            'tackle success': 'tackle_success_rate',
            'interceptions': 'interceptions', 'clearances': 'clearances',
            'blocks': 'blocks', 'blocked shots': 'blocks',
            'headed clearances': 'headed_clearances', 'recoveries': 'recoveries',
            # Duels
            'duels won': 'duels_won', 'ground duels won': 'ground_duels_won',
            'aerial duels won': 'aerial_duels_won',
            # Discipline
            'fouls committed': 'fouls_committed', 'fouls': 'fouls_committed',
            'fouls won': 'fouls_won', 'was fouled': 'fouls_won',
        }

        # Stats that need EXACT matching (not substring) to avoid confusion
        # e.g., "xG" vs "xG against while on pitch"
        EXACT_MATCH_STATS = {
            'goals': 'goals',
            'xg': 'xg',
            'xgot': 'xgot',
            'xg excl. penalty': 'npxg',
            'assists': 'assists',
            'xa': 'xa',
            'shots': 'shots',
            'shots on target': 'shots_on_target',
        }

        for group in items:
            if group.get('display') != 'stats-group':
                continue

            group_title = group.get('title', '').lower()
            group_items = group.get('items', [])

            for stat in group_items:
                title = stat.get('title', '').lower()
                stat_value = stat.get('statValue')
                per_90 = stat.get('per90')
                percentile = stat.get('percentileRankPer90')

                # Map title to field - use exact match for critical stats
                field = None

                # First try exact match
                if title in EXACT_MATCH_STATS:
                    field = EXACT_MATCH_STATS[title]
                else:
                    # Fall back to substring match for non-critical stats
                    for key, mapped_field in STAT_MAP.items():
                        # Skip exact-match stats in substring search
                        if key in EXACT_MATCH_STATS:
                            continue
                        if key in title:
                            field = mapped_field
                            break

                if field and stat_value is not None:
                    # Parse value - could be string "4" or "5.58"
                    try:
                        if isinstance(stat_value, str):
                            if '.' in stat_value:
                                result[field] = float(stat_value)
                            else:
                                result[field] = int(stat_value)
                        else:
                            result[field] = stat_value
                    except (ValueError, TypeError):
                        result[field] = stat_value

                    # Store per 90 and percentile for key stats
                    if per_90 is not None:
                        per_90_field = f"{field}_per_90"
                        if per_90_field in result:
                            result[per_90_field] = per_90

                    if percentile is not None:
                        percentile_field = f"{field}_percentile"
                        if percentile_field in result:
                            result[percentile_field] = percentile

        return result

    @staticmethod
    def parse_player_contract(data: Dict) -> Optional[Dict]:
        """
        Parse contract information from /playerData response.

        Returns: {contract_end_date, is_on_loan, injury_status}
        """
        result = {
            'contract_end_date': None,
            'is_injured': False,
            'injury_type': None,
        }

        # Contract end date
        contract_end = data.get('contractEnd', {})
        if contract_end:
            utc_time = contract_end.get('utcTime')
            if utc_time:
                try:
                    from datetime import datetime
                    result['contract_end_date'] = datetime.fromisoformat(
                        utc_time.replace('Z', '+00:00')
                    ).date()
                except (ValueError, TypeError):
                    pass

        # Injury information
        injury_info = data.get('injuryInformation')
        if injury_info:
            result['is_injured'] = True
            result['injury_type'] = injury_info.get('injuryType')

        return result

    @staticmethod
    def parse_player_career(data: Dict) -> List[Dict]:
        """
        Parse career history from /playerData response.

        Path: careerHistory.senior.seasonEntries

        Returns list of: {
            season_name, appearances, goals, assists, rating,
            tournament_stats: [{league_id, league_name, appearances, goals, assists, rating}]
        }
        """
        career = []
        try:
            senior = data.get('careerHistory', {}).get('senior', {})
            entries = senior.get('seasonEntries', [])

            for entry in entries:
                season_data = {
                    'season_name': entry.get('seasonName'),
                    'appearances': entry.get('appearances'),
                    'goals': entry.get('goals'),
                    'assists': entry.get('assists'),
                    'rating': entry.get('rating'),
                    'tournament_stats': [],
                }

                for tourney in entry.get('tournamentStats', []):
                    season_data['tournament_stats'].append({
                        'league_id': tourney.get('leagueId'),
                        'league_name': tourney.get('leagueName'),
                        'appearances': tourney.get('appearances'),
                        'goals': tourney.get('goals'),
                        'assists': tourney.get('assists'),
                        'rating': tourney.get('rating'),
                    })

                career.append(season_data)

        except (KeyError, TypeError) as e:
            logger.warning(f"Error parsing player career: {e}")

        return career

    # =========================================
    # MATCH PARSING
    # =========================================

    @staticmethod
    def parse_match(data: Dict) -> Optional[Dict]:
        """
        Parse match details from /matchDetails response.

        Returns: {
            fotmob_match_id, match_date, venue, attendance, referee,
            home_team_name, away_team_name, home_fotmob_id, away_fotmob_id,
            home_score, away_score, status, finished
        }
        """
        if not data:
            return None

        header = data.get('header', {})
        teams = header.get('teams', [])
        status = header.get('status', {})

        if len(teams) < 2:
            return None

        home_team = teams[0]
        away_team = teams[1]

        # Parse match date
        match_date = None
        utc_time = status.get('utcTime')
        if utc_time:
            try:
                match_date = datetime.fromisoformat(utc_time.replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                pass

        # Parse general info
        general = data.get('general', {})

        return {
            'fotmob_match_id': general.get('matchId') or data.get('general', {}).get('matchId'),
            'match_date': match_date,
            'venue': general.get('matchVenue'),
            'attendance': general.get('matchAttendance'),
            'referee': general.get('matchReferee'),
            'home_team_name': home_team.get('name'),
            'away_team_name': away_team.get('name'),
            'home_fotmob_id': home_team.get('id'),
            'away_fotmob_id': away_team.get('id'),
            'home_score': home_team.get('score'),
            'away_score': away_team.get('score'),
            'status': status.get('reason', {}).get('short', 'Unknown'),
            'finished': status.get('finished', False),
        }

    @staticmethod
    def parse_match_stats(data: Dict) -> Optional[Dict]:
        """
        Parse team match statistics from /matchDetails response.

        Path: stats.Periods.All[i].stats[j]
        Convention: stats[0] = home, stats[1] = away

        Returns: {
            home: {shots, shots_on_target, possession, passes, xg, ...},
            away: {shots, shots_on_target, possession, passes, xg, ...}
        }
        """
        stats_data = data.get('stats', {})
        periods = stats_data.get('Periods', {})
        all_stats = periods.get('All', [])

        if not all_stats:
            return None

        home_stats = {}
        away_stats = {}

        # Key mapping from FotMob stat keys to our schema
        key_mapping = {
            'expected_goals': 'xg',
            'possession': 'possession',
            'total_shots': 'shots',
            'shots_on_target': 'shots_on_target',
            'accurate_passes': 'passes_completed',
            'total_passes': 'passes_attempted',
            'fouls': 'fouls_committed',
            'corners': 'corners',
            'offsides': 'offsides',
            'yellow_cards': 'yellow_cards',
            'red_cards': 'red_cards',
            'shots_off_target': 'shots_off_target',
            'blocked_shots': 'blocked_shots',
            'big_chances': 'big_chances',
            'big_chances_missed': 'big_chances_missed',
            'tackles': 'tackles',
            'interceptions': 'interceptions',
            'clearances': 'clearances',
            'saves': 'saves',
        }

        for section in all_stats:
            for stat in section.get('stats', []):
                key = stat.get('key', '')
                values = stat.get('stats', [])

                if len(values) < 2:
                    continue

                mapped_key = key_mapping.get(key, key)
                home_val = values[0]
                away_val = values[1]

                # Parse percentage values like "58%"
                if isinstance(home_val, str) and '%' in home_val:
                    try:
                        home_val = float(home_val.replace('%', ''))
                    except ValueError:
                        pass
                if isinstance(away_val, str) and '%' in away_val:
                    try:
                        away_val = float(away_val.replace('%', ''))
                    except ValueError:
                        pass

                # Parse "X/Y" fraction values (e.g., passes "385/452")
                if isinstance(home_val, str) and '/' in home_val:
                    parts = home_val.split('/')
                    try:
                        home_val = int(parts[0])
                    except ValueError:
                        pass
                if isinstance(away_val, str) and '/' in away_val:
                    parts = away_val.split('/')
                    try:
                        away_val = int(parts[0])
                    except ValueError:
                        pass

                home_stats[mapped_key] = home_val
                away_stats[mapped_key] = away_val

        # Calculate pass accuracy if we have the data
        for stats in [home_stats, away_stats]:
            completed = stats.get('passes_completed')
            attempted = stats.get('passes_attempted')
            if completed and attempted and isinstance(completed, (int, float)) and isinstance(attempted, (int, float)) and attempted > 0:
                stats['pass_accuracy'] = round((completed / attempted) * 100, 1)

        return {
            'home': home_stats,
            'away': away_stats,
        }

    @staticmethod
    def parse_match_player_stats(data: Dict) -> List[Dict]:
        """
        Parse per-player stats from /matchDetails response.

        Path: playerStats[playerId]

        Returns list of: {
            fotmob_player_id, name, team_id, team_name, shirt_number,
            is_goalkeeper, position_id,
            goals, assists, shots, shots_on_target, minutes_played,
            rating, key_passes, tackles, interceptions, touches,
            passes_completed, passes_attempted, dribbles, aerials_won
        }
        """
        players = []
        player_stats = data.get('playerStats', {})

        if not player_stats:
            return players

        for player_id_str, player_data in player_stats.items():
            if not isinstance(player_data, dict):
                continue

            # Flatten stat sections
            flat_stats = {}
            for section in player_data.get('stats', []):
                for stat_name, stat_data in section.get('stats', {}).items():
                    if isinstance(stat_data, dict):
                        stat_obj = stat_data.get('stat', {})
                        if isinstance(stat_obj, dict):
                            flat_stats[stat_data.get('key', stat_name.lower())] = stat_obj.get('value')

            # Extract rating
            rating = None
            rating_data = player_data.get('rating')
            if isinstance(rating_data, dict):
                rating = rating_data.get('num')
                if rating:
                    try:
                        rating = float(rating)
                    except (ValueError, TypeError):
                        rating = None

            players.append({
                'fotmob_player_id': player_data.get('id'),
                'name': player_data.get('name'),
                'team_id': player_data.get('teamId'),
                'team_name': player_data.get('teamName'),
                'shirt_number': player_data.get('shirtNumber'),
                'is_goalkeeper': player_data.get('isGoalkeeper', False),
                'position_id': player_data.get('positionId'),
                'rating': rating,
                'goals': flat_stats.get('goals', 0),
                'assists': flat_stats.get('assists', 0),
                'total_shots': flat_stats.get('total_shots', 0),
                'shots_on_target': flat_stats.get('shots_on_target', 0),
                'key_passes': flat_stats.get('chances_created', 0),
                'touches': flat_stats.get('touches', 0),
                'tackles_won': flat_stats.get('tackles_won', 0),
                'interceptions': flat_stats.get('interceptions', 0),
                'clearances': flat_stats.get('clearances', 0),
                'duels_won': flat_stats.get('duels_won', 0),
                'ground_duels_won': flat_stats.get('ground_duels_won', 0),
                'aerial_duels_won': flat_stats.get('aerial_duels_won', 0),
                'minutes_played': flat_stats.get('minutes_played'),
                'accurate_passes': flat_stats.get('accurate_passes'),
                'total_passes': flat_stats.get('total_passes'),
                'dribbles': flat_stats.get('successful_dribbles', 0),
            })

        return players

    @staticmethod
    def parse_match_events(data: Dict) -> List[Dict]:
        """
        Parse match events (goals, cards, subs) from /matchDetails response.

        Path: content.events.events

        Returns list of: {type, time, player_name, player_id, card_type, ...}
        """
        events = []
        try:
            content_events = data.get('content', {}).get('events', {})
            event_list = content_events.get('events', [])

            for event in event_list:
                event_type = event.get('type')
                parsed = {
                    'type': event_type,
                    'time': event.get('time'),
                    'time_str': event.get('timeStr'),
                }

                if event_type == 'Goal':
                    player = event.get('player', {})
                    parsed['player_id'] = player.get('id')
                    parsed['player_name'] = player.get('name')

                elif event_type == 'Card':
                    player = event.get('player', {})
                    parsed['player_id'] = player.get('id')
                    parsed['player_name'] = player.get('name')
                    parsed['card_type'] = event.get('card')

                elif event_type == 'Substitution':
                    swap = event.get('swap', [])
                    if len(swap) >= 2:
                        parsed['player_off'] = swap[0].get('name')
                        parsed['player_off_id'] = swap[0].get('id')
                        parsed['player_on'] = swap[1].get('name')
                        parsed['player_on_id'] = swap[1].get('id')

                events.append(parsed)

        except (KeyError, TypeError) as e:
            logger.warning(f"Error parsing match events: {e}")

        return events

    # =========================================
    # UTILITY METHODS
    # =========================================

    @staticmethod
    def format_season_name(fotmob_season: str) -> str:
        """
        Convert FotMob season format to database format.

        '2024/2025' -> '2024-25'
        '2023/2024' -> '2023-24'
        '2024' -> '2024'  (for single-year seasons like Brazil/Argentina)
        """
        if not fotmob_season:
            return ''

        if '/' in fotmob_season:
            parts = fotmob_season.split('/')
            if len(parts) == 2:
                start = parts[0].strip()
                end = parts[1].strip()
                return f"{start}-{end[-2:]}"

        return fotmob_season

    @staticmethod
    def parse_fotmob_season_to_years(fotmob_season: str) -> tuple:
        """
        Parse FotMob season string to (start_year, end_year).

        '2024/2025' -> (2024, 2025)
        '2024' -> (2024, 2024)
        """
        if not fotmob_season:
            return (0, 0)

        if '/' in fotmob_season:
            parts = fotmob_season.split('/')
            try:
                return (int(parts[0].strip()), int(parts[1].strip()))
            except ValueError:
                return (0, 0)

        try:
            year = int(fotmob_season.strip())
            return (year, year)
        except ValueError:
            return (0, 0)
