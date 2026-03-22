"""
SoccerData Configuration

Maps our internal league keys to soccerdata league/season identifiers.
Defines source priorities and cache settings.
"""

from typing import Dict, List, Any

# ============================================
# LEAGUE MAPPINGS
# ============================================

# Map our internal league keys to soccerdata league identifiers
LEAGUE_MAPPINGS = {
    # Internal key -> (soccerdata_league, country_code)
    'premier-league': ('ENG-Premier League', 'ENG'),
    'la-liga': ('ESP-La Liga', 'ESP'),
    'serie-a': ('ITA-Serie A', 'ITA'),
    'bundesliga': ('GER-Bundesliga', 'GER'),
    'ligue-1': ('FRA-Ligue 1', 'FRA'),
    'eredivisie': ('NED-Eredivisie', 'NED'),
    'brasileiro-serie-a': ('BRA-Serie A', 'BRA'),
    'argentina-primera': ('ARG-Liga Profesional', 'ARG'),
}

# Reverse mapping for lookups
SOCCERDATA_TO_INTERNAL = {v[0]: k for k, v in LEAGUE_MAPPINGS.items()}

# Season format mapping (our format -> soccerdata format)
# Our format: "2024-25", soccerdata format varies by source
SEASON_MAPPINGS = {
    '2024-25': {'fotmob': '2024/2025', 'fbref': '2024-2025', 'whoscored': '2024/2025'},
    '2023-24': {'fotmob': '2023/2024', 'fbref': '2023-2024', 'whoscored': '2023/2024'},
    '2022-23': {'fotmob': '2022/2023', 'fbref': '2022-2023', 'whoscored': '2022/2023'},
    '2021-22': {'fotmob': '2021/2022', 'fbref': '2021-2022', 'whoscored': '2021/2022'},
    '2020-21': {'fotmob': '2020/2021', 'fbref': '2020-2021', 'whoscored': '2020/2021'},
}


# ============================================
# SOURCE CONFIGURATION
# ============================================

SOCCERDATA_CONFIG = {
    # FotMob - Primary source (no API key needed)
    'fotmob': {
        'enabled': True,
        'priority': 1,
        'rate_limit': 2.0,  # seconds between requests
        'cache_expiry': 3600,  # 1 hour for live data
        'methods': [
            'read_leagues',
            'read_seasons',
            'read_schedule',
            'read_team_match_stats',
            'read_player_match_stats',
            'read_lineup',
            'read_events',
            'read_shotmap',
            'read_player_season_stats',
        ],
        'event_tables': ['match_events', 'shot_events', 'match_lineups'],
    },

    # WhoScored - Rich event taxonomy
    'whoscored': {
        'enabled': True,
        'priority': 2,
        'rate_limit': 3.0,
        'cache_expiry': 86400,  # 24 hours
        'methods': [
            'read_schedule',
            'read_lineup',
            'read_team_match_stats',
            'read_player_match_stats',
            'read_events',
        ],
        'event_tables': ['match_events', 'match_lineups'],
        'notes': 'Requires headless browser (Selenium)',
    },

    # FBref - Historical depth
    'fbref': {
        'enabled': True,
        'priority': 3,
        'rate_limit': 5.0,  # FBref is strict on rate limiting
        'cache_expiry': 604800,  # 7 days for historical
        'methods': [
            'read_schedule',
            'read_team_season_stats',
            'read_player_season_stats',
            'read_player_match_stats',
            'read_team_match_stats',
            'read_lineup',
            'read_events',
            'read_shot_events',
        ],
        'event_tables': ['match_events', 'shot_events', 'match_lineups'],
    },

    # Understat - xG/xA metrics
    'understat': {
        'enabled': True,
        'priority': 4,
        'rate_limit': 2.0,
        'cache_expiry': 604800,  # 7 days
        'methods': [
            'read_schedule',
            'read_team_match_stats',
            'read_player_match_stats',
            'read_shot_events',
        ],
        'event_tables': ['shot_events'],
        'leagues_supported': ['premier-league', 'la-liga', 'serie-a', 'bundesliga', 'ligue-1'],
    },

    # ClubElo - Team strength ratings
    'clubelo': {
        'enabled': True,
        'priority': 5,
        'rate_limit': 1.0,
        'cache_expiry': 86400,
        'methods': [
            'read_by_date',
            'read_team_history',
        ],
        'tables': ['team_elo_ratings'],
    },

    # SoFIFA - Player ratings and market values
    'sofifa': {
        'enabled': True,
        'priority': 6,
        'rate_limit': 2.0,
        'cache_expiry': 604800,
        'methods': [
            'read_versions',
            'read_teams',
            'read_players',
            'read_team_ratings',
            'read_player_ratings',
        ],
        'tables': ['player_fifa_ratings'],
    },
}


# ============================================
# EVENT TYPE MAPPINGS
# ============================================

# Map soccerdata event types to our schema event types
EVENT_TYPE_MAPPINGS = {
    # Goals
    'Goal': 'goal',
    'OwnGoal': 'own_goal',
    'PenaltyScored': 'penalty_scored',
    'PenaltyMissed': 'penalty_missed',

    # Cards
    'YellowCard': 'yellow_card',
    'SecondYellow': 'second_yellow',
    'RedCard': 'red_card',

    # Substitutions
    'SubstitutionOn': 'substitution_on',
    'SubstitutionOff': 'substitution_off',
    'Substitution': 'substitution',

    # Shots
    'Shot': 'shot',
    'ShotOnTarget': 'shot_on_target',
    'ShotBlocked': 'shot_blocked',
    'ShotOffTarget': 'shot_off_target',

    # Passes
    'Pass': 'pass',
    'KeyPass': 'key_pass',
    'Assist': 'assist',
    'CrossCompleted': 'cross_completed',
    'CrossBlocked': 'cross_blocked',

    # Defensive
    'Tackle': 'tackle',
    'Interception': 'interception',
    'Clearance': 'clearance',
    'Block': 'block',
    'Foul': 'foul',
    'FoulWon': 'foul_won',

    # Other
    'Dribble': 'dribble',
    'Aerial': 'aerial',
    'Corner': 'corner',
    'Offside': 'offside',
    'Start': 'period_start',
    'End': 'period_end',
}


# ============================================
# COLUMN MAPPINGS
# ============================================

# Map soccerdata column names to our schema column names
COLUMN_MAPPINGS = {
    # Player columns
    'player': 'player_name',
    'player_id': 'source_player_id',
    'team': 'team_name',
    'team_id': 'source_team_id',

    # Match columns
    'match_id': 'source_match_id',
    'date': 'match_date',
    'home_team': 'home_team_name',
    'away_team': 'away_team_name',
    'home_score': 'home_goals',
    'away_score': 'away_goals',

    # Event columns
    'minute': 'event_minute',
    'second': 'event_second',
    'type': 'event_type',
    'outcome': 'event_outcome',
    'x': 'location_x',
    'y': 'location_y',
    'end_x': 'end_location_x',
    'end_y': 'end_location_y',

    # Stats columns
    'xG': 'xg',
    'xA': 'xa',
    'npxG': 'npxg',
    'xGChain': 'xg_chain',
    'xGBuildup': 'xg_buildup',
    'goals': 'goals',
    'assists': 'assists',
    'shots': 'shots',
    'shots_on_target': 'shots_on_target',
    'key_passes': 'key_passes',
    'passes_completed': 'passes_completed',
    'passes_attempted': 'passes_attempted',
    'tackles': 'tackles',
    'interceptions': 'interceptions',
    'clearances': 'clearances',
    'blocks': 'blocks',
    'aerials_won': 'aerials_won',
    'aerials_lost': 'aerials_lost',
    'fouls_committed': 'fouls_committed',
    'fouls_drawn': 'fouls_drawn',
    'yellow_cards': 'yellow_cards',
    'red_cards': 'red_cards',
    'minutes': 'minutes_played',
}


# ============================================
# DATA QUALITY THRESHOLDS
# ============================================

QUALITY_THRESHOLDS = {
    'xg_max': 50.0,  # Max reasonable xG per season
    'xg_min': 0.0,
    'xa_max': 30.0,
    'xa_min': 0.0,
    'minutes_max': 5000,  # Max minutes per season
    'minutes_min': 0,
    'goals_max': 60,  # Max goals per season
    'location_x_max': 100.0,
    'location_x_min': 0.0,
    'location_y_max': 100.0,
    'location_y_min': 0.0,
}
