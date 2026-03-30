"""
per90.py — Per-90-minute normalisation of counting stats.

Formula: (stat / minutes) * 90
Only applied to players with minutes >= MIN_MINUTES.
NULL input -> NULL output. Never impute 0.
"""
import pandas as pd
from typing import Optional

MIN_MINUTES = 450

# Counting stats to normalise — produces {stat}_p90 columns
PER90_STATS = [
    'goals', 'assists', 'shots', 'shots_on_target',
    'key_passes', 'big_chances_created',
    'aerial_duels_won', 'aerial_duels_lost',
    'tackles', 'tackles_won', 'interceptions',
    'clearances', 'recoveries', 'dispossessed',
    'successful_dribbles', 'touches',
    'fouls_committed', 'fouls_won',
    'saves', 'goals_conceded', 'high_claims',
    'xg', 'xa', 'npxg', 'xg_chain', 'xg_buildup',
]

# Already-rate stats — include unchanged, no _p90 suffix
RATE_STATS = [
    'aerial_win_pct', 'ground_duels_won_pct',
    'duels_won_pct', 'tackles_won_pct',
    'accurate_passes_pct', 'sofascore_rating',
    'clean_sheets',
]


def compute_per90(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame of player_season_stats rows, return a new
    DataFrame with per-90 columns added.

    Required input columns: player_id, season_id, league_id, minutes,
    position_group, plus all stats listed in PER90_STATS and RATE_STATS
    (missing columns are silently skipped).

    Returns DataFrame with:
      - player_id, season_id, league_id, minutes, position_group (identity cols)
      - {stat}_p90 for each stat in PER90_STATS that exists in input
      - {stat} (unchanged) for each stat in RATE_STATS that exists in input
    Only rows with minutes >= MIN_MINUTES are returned.
    """
    if df.empty:
        return df

    # Filter to minimum minutes
    df = df[df['minutes'] >= MIN_MINUTES].copy()
    if df.empty:
        return df

    result_cols = ['player_id', 'season_id', 'league_id', 'minutes', 'position_group']
    result = df[result_cols].copy()

    # Per-90 normalisation — only for columns that exist in input
    for stat in PER90_STATS:
        if stat in df.columns:
            col_name = f'{stat}_p90'
            # Use where to preserve NULL (NaN) — don't compute on NaN
            result[col_name] = (df[stat] / df['minutes'] * 90).where(df[stat].notna())

    # Rate stats — pass through unchanged
    for stat in RATE_STATS:
        if stat in df.columns:
            result[stat] = df[stat]

    return result
