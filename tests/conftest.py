"""
Pytest fixtures for Football Data ETL Pipeline tests.
"""

import pytest
from datetime import date, datetime
from unittest.mock import MagicMock


# ============================================
# MOCK DATA FIXTURES
# ============================================

@pytest.fixture
def sample_player_data():
    """Sample player data from FotMob API."""
    return {
        'id': 12345,
        'name': 'Bruno Fernandes',
        'birthDate': {'utcTime': '1994-09-08T00:00:00Z'},
        'positionDescription': {
            'primaryPosition': {'label': 'Midfielder'}
        },
        'playerInformation': [
            {'title': 'Height', 'value': '179 cm'},
            {'title': 'Preferred foot', 'value': {'key': 'right', 'fallback': 'Right'}},
            {'title': 'Country', 'value': {'key': None, 'fallback': 'Portugal'}},
            {'title': 'Shirt', 'value': 8},
        ],
        'primaryTeam': {
            'teamId': 10260,
            'teamName': 'Manchester United',
        },
        'isCaptain': False,
        'mainLeague': {
            'leagueId': 47,
            'leagueName': 'Premier League',
            'season': '2024/2025',
            'stats': [
                {'title': 'Goals', 'value': 5},
                {'title': 'Assists', 'value': 4},
                {'title': 'Matches', 'value': 20},
                {'title': 'Minutes played', 'value': 1650},
            ],
        },
        'firstSeasonStats': {
            'statsSection': {
                'items': [
                    {
                        'display': 'stats-group',
                        'title': 'Attacking',
                        'items': [
                            {'title': 'Goals', 'statValue': '5', 'per90': 0.27, 'percentileRankPer90': 75},
                            {'title': 'xG', 'statValue': '4.2', 'per90': 0.23},
                            {'title': 'Shots', 'statValue': '45', 'per90': 2.45},
                        ],
                    },
                    {
                        'display': 'stats-group',
                        'title': 'Passing',
                        'items': [
                            {'title': 'Assists', 'statValue': '4', 'per90': 0.22},
                            {'title': 'xA', 'statValue': '3.5', 'per90': 0.19},
                            {'title': 'Key passes', 'statValue': '35', 'per90': 1.91},
                        ],
                    },
                ],
            },
        },
    }


@pytest.fixture
def sample_standings_data():
    """Sample league standings from FotMob API."""
    return {
        'table': {
            'data': [
                {
                    'table': {
                        'all': [
                            {
                                'id': 9825,
                                'name': 'Liverpool',
                                'shortName': 'LIV',
                                'idx': 1,
                                'played': 20,
                                'wins': 14,
                                'draws': 4,
                                'losses': 2,
                                'scoresStr': '45-18',
                                'goalConDiff': 27,
                                'pts': 46,
                            },
                            {
                                'id': 9825,
                                'name': 'Arsenal',
                                'shortName': 'ARS',
                                'idx': 2,
                                'played': 20,
                                'wins': 12,
                                'draws': 5,
                                'losses': 3,
                                'scoresStr': '40-20',
                                'goalConDiff': 20,
                                'pts': 41,
                            },
                        ],
                        'xg': [
                            {'id': 9825, 'name': 'Liverpool', 'xg': 42.5, 'xgConceded': 20.1, 'xPoints': 44.2},
                            {'id': 9826, 'name': 'Arsenal', 'xg': 38.0, 'xgConceded': 22.5, 'xPoints': 38.5},
                        ],
                    },
                },
            ],
        },
        'details': {
            'id': 47,
            'name': 'Premier League',
            'country': 'England',
            'selectedSeason': '2024/2025',
            'latestSeason': '2024/2025',
        },
    }


@pytest.fixture
def sample_match_data():
    """Sample match details from FotMob API."""
    return {
        'general': {'matchId': 4193490},
        'header': {
            'teams': [
                {'id': 9825, 'name': 'Liverpool', 'score': 2},
                {'id': 10260, 'name': 'Manchester United', 'score': 1},
            ],
            'status': {
                'finished': True,
                'reason': {'short': 'FT'},
                'utcTime': '2025-01-15T15:00:00Z',
            },
        },
        'stats': {
            'Periods': {
                'All': [
                    {
                        'stats': [
                            {'key': 'expected_goals', 'stats': ['1.8', '1.2']},
                            {'key': 'possession', 'stats': ['55%', '45%']},
                            {'key': 'total_shots', 'stats': [15, 10]},
                            {'key': 'shots_on_target', 'stats': [6, 4]},
                        ],
                    },
                ],
            },
        },
        'content': {
            'events': {
                'events': [
                    {'type': 'Goal', 'time': 25, 'timeStr': '25\'', 'player': {'id': 12345, 'name': 'Salah'}},
                    {'type': 'Card', 'time': 40, 'card': 'Yellow', 'player': {'id': 12346, 'name': 'Bruno Fernandes'}},
                    {'type': 'Goal', 'time': 60, 'timeStr': '60\'', 'player': {'id': 12347, 'name': 'Diaz'}},
                ],
            },
        },
    }


@pytest.fixture
def sample_player_season_stats():
    """Sample player season stats record."""
    return {
        'player_id': 100,
        'team_id': 1,
        'season_id': 1,
        'league_id': 1,
        'matches_played': 20,
        'starts': 18,
        'minutes': 1650,
        'goals': 5,
        'assists': 4,
        'xg': 4.2,
        'xag': 3.5,
        'shots': 45,
        'shots_on_target': 20,
        'key_passes': 35,
        'tackles': 25,
        'interceptions': 15,
        'yellow_cards': 3,
        'red_cards': 0,
    }


@pytest.fixture
def sample_team_season_stats():
    """Sample team season stats record."""
    return {
        'team_id': 1,
        'season_id': 1,
        'league_id': 1,
        'matches_played': 20,
        'wins': 14,
        'draws': 4,
        'losses': 2,
        'goals_for': 45,
        'goals_against': 18,
        'goal_difference': 27,
        'points': 46,
        'xg_for': 42.5,
        'xg_against': 20.1,
        'league_position': 1,
    }


# ============================================
# MOCK DATABASE
# ============================================

@pytest.fixture
def mock_db():
    """Mock database connection."""
    db = MagicMock()

    # Default execute_query returns empty
    db.execute_query.return_value = []

    return db


# ============================================
# EDGE CASE DATA
# ============================================

@pytest.fixture
def edge_case_player_dict_foot():
    """Player with dict-format preferred_foot (ISSUE-019)."""
    return {
        'id': 99999,
        'name': 'Test Player',
        'playerInformation': [
            {'title': 'Preferred foot', 'value': {'key': 'right', 'fallback': 'Right'}},
            {'title': 'Country', 'value': {'key': None, 'fallback': 'Spain'}},
        ],
    }


@pytest.fixture
def edge_case_player_xg_against():
    """Player data with 'xG against' stat that shouldn't match 'xG' (ISSUE-015)."""
    return {
        'id': 88888,
        'name': 'Defender Test',
        'firstSeasonStats': {
            'statsSection': {
                'items': [
                    {
                        'display': 'stats-group',
                        'title': 'Defensive',
                        'items': [
                            {'title': 'xG against while on pitch', 'statValue': '15.5'},
                            {'title': 'xG', 'statValue': '0.8'},
                        ],
                    },
                ],
            },
        },
    }


@pytest.fixture
def anomaly_data():
    """Data with anomalous values for quality testing."""
    return [
        {'player_name': 'Normal Player', 'xg': 5.0, 'goals': 5},
        {'player_name': 'High xG', 'xg': 35.0, 'goals': 10},  # Anomaly: xG > 30
        {'player_name': 'Negative Goals', 'xg': 3.0, 'goals': -5},  # Anomaly: negative
        {'player_name': 'Missing Name', 'xg': 2.0, 'goals': 2},
        {'player_name': None, 'xg': 1.0, 'goals': 1},  # Missing required field
    ]
