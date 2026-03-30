"""Unit tests for analytics/player_score.py"""
import pandas as pd
import numpy as np
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.player_score import compute_scores, MIN_METRICS_REQUIRED


def make_cohort(n=10, pos='FWD', season_id=1, league_id=1):
    """
    Build a synthetic cohort DataFrame with incrementally increasing stats
    so player_id == n is unambiguously the best player.
    """
    rows = []
    for i in range(n):
        rows.append({
            'player_id': i + 1,
            'season_id': season_id,
            'league_id': league_id,
            'position_group': pos,
            'minutes': 900,
            'goals_p90': 0.1 * (i + 1),
            'xg_p90': 0.09 * (i + 1),
            'shots_p90': 1.0 + 0.2 * i,
            'assists_p90': 0.05 * i,
            'xa_p90': 0.04 * i,
            'key_passes_p90': 0.5 + 0.1 * i,
            'successful_dribbles_p90': 0.3 + 0.05 * i,
            'big_chances_created_p90': 0.02 * i,
        })
    return pd.DataFrame(rows)


def test_score_range():
    """All performance_score values must be in [0, 100]."""
    df = make_cohort(n=15)
    result = compute_scores(df)
    scored = result[result['performance_score'].notna()]
    assert len(scored) > 0
    assert (scored['performance_score'] >= 0).all()
    assert (scored['performance_score'] <= 100).all()


def test_score_ordering():
    """Player with highest stats (player_id=10) should have the highest score."""
    df = make_cohort(n=10)
    result = compute_scores(df)
    top = result.nlargest(1, 'performance_score').iloc[0]
    assert top['player_id'] == 10, (
        f"Expected player_id=10 to be top scorer, got player_id={top['player_id']}"
    )


def test_min_score_is_zero_max_is_100():
    """Min-max scaling within cohort must produce exactly 0.0 at bottom and 100.0 at top."""
    df = make_cohort(n=10)
    result = compute_scores(df)
    scored = result[result['performance_score'].notna()]
    assert abs(scored['performance_score'].min()) < 0.01, (
        f"Min score should be 0, got {scored['performance_score'].min()}"
    )
    assert abs(scored['performance_score'].max() - 100.0) < 0.01, (
        f"Max score should be 100, got {scored['performance_score'].max()}"
    )


def test_insufficient_metrics_gives_null():
    """Players with fewer than MIN_METRICS_REQUIRED non-null metrics get NULL score."""
    rows = [
        {
            'player_id': i,
            'season_id': 1,
            'league_id': 1,
            'position_group': 'FWD',
            'minutes': 900,
            'goals_p90': 0.3 if i == 0 else None,  # only 1 non-null metric
            'xg_p90': None,
            'shots_p90': None,
            'assists_p90': None,
            'xa_p90': None,
            'key_passes_p90': None,
            'successful_dribbles_p90': None,
            'big_chances_created_p90': None,
        }
        for i in range(5)
    ]
    df = pd.DataFrame(rows)
    result = compute_scores(df)
    assert result['performance_score'].isna().all(), (
        "All players with < MIN_METRICS_REQUIRED non-null metrics should get NULL score"
    )


def test_percentile_rank_range():
    """Percentile ranks must be in [0, 100]."""
    df = make_cohort(n=10)
    result = compute_scores(df)
    scored = result[result['percentile_rank'].notna()]
    assert (scored['percentile_rank'] >= 0).all()
    assert (scored['percentile_rank'] <= 100).all()


def test_single_player_cohort_gives_null():
    """A cohort of exactly 1 player cannot be normalised — score must be NULL."""
    df = make_cohort(n=1)
    result = compute_scores(df)
    assert result.iloc[0]['performance_score'] is None or pd.isna(
        result.iloc[0]['performance_score']
    )


def test_empty_input_returns_empty():
    """Empty DataFrame input should return empty DataFrame, not raise."""
    result = compute_scores(pd.DataFrame())
    assert result.empty


def test_multiple_cohorts_independent():
    """Two cohorts (different league_id) must be scored independently — each has its own 0-100 range."""
    cohort_a = make_cohort(n=10, league_id=1)
    cohort_b = make_cohort(n=10, league_id=2)
    df = pd.concat([cohort_a, cohort_b], ignore_index=True)
    result = compute_scores(df)

    for lid in [1, 2]:
        subset = result[(result['league_id'] == lid) & result['performance_score'].notna()]
        assert abs(subset['performance_score'].min()) < 0.01
        assert abs(subset['performance_score'].max() - 100.0) < 0.01


def test_gk_negative_weight():
    """
    GK scoring: goals_conceded_p90 has a negative weight.
    GK with fewer goals conceded should score higher than one who concedes more.
    """
    rows = []
    for i in range(10):
        rows.append({
            'player_id': i + 1,
            'season_id': 1,
            'league_id': 1,
            'position_group': 'GK',
            'minutes': 900,
            'saves_p90': 3.0 + 0.1 * i,
            'clean_sheets': 5 + i,
            'high_claims_p90': 1.0 + 0.05 * i,
            # player 1 concedes most (worst), player 10 concedes fewest (best)
            'goals_conceded_p90': 1.5 - 0.1 * i,
        })
    df = pd.DataFrame(rows)
    result = compute_scores(df)
    top = result.nlargest(1, 'performance_score').iloc[0]
    assert top['player_id'] == 10, (
        f"GK with fewest goals conceded + most saves should top score; "
        f"got player_id={top['player_id']}"
    )
