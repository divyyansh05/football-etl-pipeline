"""
SofaScore constants — league IDs, season IDs, field mappings.
All IDs confirmed working March 2026.
"""

# ── League IDs (canonical DB name → SofaScore unique-tournament ID) ───────────

LEAGUE_IDS = {
    "Premier League": 17,
    "La Liga":        8,
    "Serie A":        23,
    "Bundesliga":     35,
    "Ligue 1":        34,
}

# ── Season IDs (canonical DB name → {start_year → SofaScore season ID}) ──────
# Season key is the START year: 2024-25 → key 2024

SEASON_IDS = {
    "Premier League": {2022: 41886, 2023: 52186, 2024: 61627, 2025: 76986},
    "La Liga":        {2022: 42409, 2023: 52376, 2024: 61643, 2025: 77559},
    "Serie A":        {2022: 42415, 2023: 52760, 2024: 63515, 2025: 76457},
    "Bundesliga":     {2022: 42268, 2023: 52608, 2024: 63516, 2025: 77333},
    "Ligue 1":        {2022: 42273, 2023: 52571, 2024: 61736, 2025: 77356},
}

# ── Season name ↔ start_year ───────────────────────────────────────────────────

SEASON_NAME_TO_YEAR = {
    "2022-23": 2022,
    "2023-24": 2023,
    "2024-25": 2024,
    "2025-26": 2025,
}

SEASON_YEAR_TO_NAME = {v: k for k, v in SEASON_NAME_TO_YEAR.items()}

# ── SofaScore position codes → canonical position_group ───────────────────────
# Confirmed from /player/{id} endpoint: single letter G/D/M/F

POSITION_MAP = {
    "G": "GK",
    "D": "DEF",
    "M": "MID",
    "F": "FWD",
}

# ── Top-player categories collected ───────────────────────────────────────────
# These are the category slugs returned by the top-players/overall endpoint.

TOP_PLAYER_CATEGORIES = [
    "rating",
    "goals",
    "assists",
    "goalAssist",
    "tackles",
    "interceptions",
    "saves",
    "cleanSheet",
]

# ── SofaScore statistics field → DB player_season_stats column ────────────────
# Only fields that exist as columns in the DB schema are mapped.
# Fields prefixed with '#' exist in API but are not in DB — ignored.

STATS_FIELD_MAP = {
    # Playing time
    "minutesPlayed":                  "minutes",
    "appearances":                    "matches_played",
    "matchesStarted":                 "matches_started",

    # Attacking
    "goals":                          "goals",
    "assists":                        "assists",
    "shots":                          "shots",
    "onTargetScoringAttempt":         "shots_on_target",
    "shotsFromInsideTheBox":          "shots_inside_box",
    "shotsFromOutsideTheBox":         "shots_outside_box",
    "keyPasses":                      "key_passes",
    "bigChancesCreated":              "big_chances_created",
    "bigChancesMissed":               "big_chances_missed",

    # Disciplinary
    "yellowCards":                    "yellow_cards",
    "redCards":                       "red_cards",

    # Defensive
    "aerialDuelsWon":                 "aerial_duels_won",
    "aerialLost":                     "aerial_duels_lost",
    "aerialDuelsWonPercentage":       "aerial_win_pct",
    "groundDuelsWon":                 "ground_duels_won",
    "groundDuelsLost":                "ground_duels_lost",
    "groundDuelsWonPercentage":       "ground_duels_won_pct",
    "totalDuelsWon":                  "duels_won",
    "totalDuelsWonPercentage":        "duels_won_pct",
    "tackles":                        "tackles",
    "tacklesWon":                     "tackles_won",
    "tacklesWonPercentage":           "tackles_won_pct",
    "interceptions":                  "interceptions",
    "clearances":                     "clearances",
    "ballRecovery":                   "recoveries",
    "dispossessed":                   "dispossessed",
    "dribbledPast":                   "dribbled_past",
    "fouls":                          "fouls_committed",
    "wasFouled":                      "fouls_won",
    "errorLeadToGoal":                "error_lead_to_goal",

    # Passing
    "accuratePassesPercentage":       "accurate_passes_pct",
    "accurateLongBalls":              "accurate_long_balls",
    "accurateFinalThirdPasses":       "accurate_final_third",
    "successfulDribbles":             "successful_dribbles",
    "touches":                        "touches",
    "possessionWonAttThird":          "possession_won_att_third",
    "offsides":                       None,  # no DB column — skip

    # Goalkeeper
    "saves":                          "saves",
    "savePercentage":                 "save_pct",
    "goalsConceded":                  "goals_conceded",
    "cleanSheet":                     "clean_sheets",
    "punches":                        "punches",
    "highClaims":                     "high_claims",

    # Rating
    "rating":                         "sofascore_rating",
}
