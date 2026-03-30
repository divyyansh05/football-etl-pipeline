"""
player_score.py — Position-based performance scoring (0-100).

Methodology:
  1. Per-90 metrics (from per90.py)
  2. Z-score normalise within (position_group, season_id, league_id) cohort
  3. Weighted sum using SCORE_WEIGHTS — weights applied to available metrics only
  4. Min-max scale to 0-100 within cohort
  5. Percentile rank within cohort
"""
import pandas as pd
import numpy as np
from typing import Optional

SCORE_WEIGHTS = {
    'FWD': {
        'goals_p90': 0.25,
        'xg_p90': 0.20,
        'shots_p90': 0.10,
        'assists_p90': 0.12,
        'xa_p90': 0.10,
        'key_passes_p90': 0.08,
        'successful_dribbles_p90': 0.08,
        'big_chances_created_p90': 0.07,
    },
    'MID': {
        'key_passes_p90': 0.18,
        'xa_p90': 0.15,
        'xg_p90': 0.12,
        'assists_p90': 0.10,
        'tackles_p90': 0.10,
        'interceptions_p90': 0.10,
        'successful_dribbles_p90': 0.10,
        'recoveries_p90': 0.08,
        'goals_p90': 0.07,
    },
    'DEF': {
        'aerial_duels_won_p90': 0.22,   # NOTE: column is aerial_duels_won_p90 from per90
        'tackles_p90': 0.20,
        'interceptions_p90': 0.18,
        'clearances_p90': 0.15,
        'recoveries_p90': 0.12,
        'aerial_win_pct': 0.08,
        'duels_won_pct': 0.05,
    },
    'GK': {
        'saves_p90': 0.35,
        'clean_sheets': 0.25,
        'high_claims_p90': 0.20,
        'goals_conceded_p90': -0.20,  # negative weight: fewer goals conceded = better
    },
}

MIN_METRICS_REQUIRED = 3  # min non-null weighted metrics to compute a score


def compute_scores(per90_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute performance scores from per-90 DataFrame.

    Input: output of per90.compute_per90() — must have
           player_id, season_id, league_id, minutes, position_group,
           plus per-90 and rate stat columns.

    Returns DataFrame with:
      player_id, season_id, league_id, position_group, minutes,
      performance_score (0-100 or NULL), percentile_rank (0-100 or NULL),
      plus all per-90 columns from input (for storage in player_scores).
    """
    if per90_df.empty:
        return per90_df

    results = []

    # Process each cohort: position_group x season_id x league_id
    group_cols = ['position_group', 'season_id', 'league_id']
    for group_keys, cohort in per90_df.groupby(group_cols):
        pos, season_id, league_id = group_keys
        weights = SCORE_WEIGHTS.get(pos, {})

        if not weights or len(cohort) < 2:
            # Cannot normalise a cohort of 1; leave scores NULL
            cohort = cohort.copy()
            cohort['performance_score'] = None
            cohort['percentile_rank'] = None
            results.append(cohort)
            continue

        cohort = cohort.copy()

        # For each player: compute weighted z-score sum
        weighted_scores = []
        for idx, row in cohort.iterrows():
            available = {}
            for metric, weight in weights.items():
                val = row.get(metric)
                if pd.notna(val):
                    available[metric] = (val, weight)

            if len(available) < MIN_METRICS_REQUIRED:
                weighted_scores.append(None)
                continue

            # Z-score each metric within cohort, apply weight
            score = 0.0
            total_weight = 0.0
            for metric, (val, weight) in available.items():
                col_vals = cohort[metric].dropna()
                if len(col_vals) < 2:
                    continue
                mean = col_vals.mean()
                std = col_vals.std()
                if std == 0 or pd.isna(std):
                    z = 0.0
                else:
                    z = (val - mean) / std
                score += z * weight
                total_weight += abs(weight)

            if total_weight == 0:
                weighted_scores.append(None)
            else:
                # Normalise by total absolute weight applied
                weighted_scores.append(score / total_weight)

        cohort['_raw_score'] = weighted_scores

        # Min-max scale to 0-100 within cohort
        valid = cohort['_raw_score'].dropna()
        if len(valid) >= 2:
            s_min, s_max = valid.min(), valid.max()
            if s_max > s_min:
                cohort['performance_score'] = (
                    (cohort['_raw_score'] - s_min) / (s_max - s_min) * 100
                ).round(2)
            else:
                cohort['performance_score'] = 50.0  # all equal
        else:
            cohort['performance_score'] = None

        # Percentile rank within cohort (higher score = higher rank)
        scored = cohort[cohort['performance_score'].notna()].copy()
        if not scored.empty:
            cohort['percentile_rank'] = None
            ranks = scored['performance_score'].rank(pct=True) * 100
            cohort.loc[ranks.index, 'percentile_rank'] = ranks.round(2)
        else:
            cohort['percentile_rank'] = None

        cohort = cohort.drop(columns=['_raw_score'])
        results.append(cohort)

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)
