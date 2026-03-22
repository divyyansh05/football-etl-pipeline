"""
FotMob API constants and league mappings.

All league IDs verified working against the FotMob public API.
"""

# FotMob League IDs (verified working)
LEAGUE_IDS = {
    'premier-league': 47,
    'la-liga': 87,
    'serie-a': 55,
    'bundesliga': 54,
    'ligue-1': 53,
    'eredivisie': 57,
    'brasileiro-serie-a': 268,
    'argentina-primera': 112,
}

# Display names for CLI output
LEAGUE_NAMES = {
    'premier-league': 'Premier League',
    'la-liga': 'La Liga',
    'serie-a': 'Serie A',
    'bundesliga': 'Bundesliga',
    'ligue-1': 'Ligue 1',
    'eredivisie': 'Eredivisie',
    'brasileiro-serie-a': 'Brasileiro Serie A',
    'argentina-primera': 'Argentina Primera',
}

# Maps FotMob league ID to country (for DB league lookup)
LEAGUE_COUNTRIES = {
    47: 'England',
    87: 'Spain',
    55: 'Italy',
    54: 'Germany',
    53: 'France',
    57: 'Netherlands',
    268: 'Brazil',
    112: 'Argentina',
}

# Maps FotMob league ID to league key
LEAGUE_ID_TO_KEY = {v: k for k, v in LEAGUE_IDS.items()}

# All available league keys
ALL_LEAGUE_KEYS = list(LEAGUE_IDS.keys())

# API-Football league IDs (for cross-referencing)
API_FOOTBALL_LEAGUE_IDS = {
    'premier-league': 39,
    'la-liga': 140,
    'serie-a': 135,
    'bundesliga': 78,
    'ligue-1': 61,
    'eredivisie': 88,
    'brasileiro-serie-a': 71,
    'argentina-primera': 128,
}
