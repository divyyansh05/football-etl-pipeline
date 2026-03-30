"""Unit tests for analytics/per90.py"""
import pandas as pd
import numpy as np
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.per90 import compute_per90, MIN_MINUTES, PER90_STATS


def make_row(player_id=1, season_id=1, league_id=1, minutes=900,
             position_group='FWD', **stats):
    """Build a minimal player_season_stats row dict."""
    base = {
        'player_id': player_id, 'season_id': season_id,
        'league_id': league_id, 'minutes': minutes,
        'position_group': position_group,
    }
    base.update(stats)
    return base


def test_basic_per90():
    """9 goals in 900 minutes should produce exactly 0.9 goals/90."""
    df = pd.DataFrame([make_row(goals=9, minutes=900)])
    result = compute_per90(df)
    assert len(result) == 1
    assert abs(result.iloc[0]['goals_p90'] - 0.9) < 0.001


def test_null_preserved():
    """NULL stat value should produce NULL (NaN) per-90 column, not 0."""
    df = pd.DataFrame([make_row(goals=None, minutes=900)])
    result = compute_per90(df)
    assert pd.isna(result.iloc[0]['goals_p90'])


def test_min_minutes_filter():
    """Players below MIN_MINUTES threshold should be excluded from output."""
    df = pd.DataFrame([
        make_row(player_id=1, minutes=400, goals=5),
        make_row(player_id=2, minutes=900, goals=9),
    ])
    result = compute_per90(df)
    assert len(result) == 1
    assert result.iloc[0]['player_id'] == 2


def test_haaland_plausibility():
    """Haaland PL 2022-23: 36 goals in ~2776 mins gives ~1.17 goals/90."""
    df = pd.DataFrame([make_row(goals=36, minutes=2776)])
    result = compute_per90(df)
    g90 = result.iloc[0]['goals_p90']
    assert g90 > 1.0, f"Expected >1.0 goals/90 for Haaland-level output, got {g90}"


def test_rate_stats_unchanged():
    """Rate stats (aerial_win_pct etc.) must pass through with original values, no _p90 suffix."""
    df = pd.DataFrame([make_row(minutes=900, aerial_win_pct=65.0)])
    result = compute_per90(df)
    assert 'aerial_win_pct' in result.columns
    assert result.iloc[0]['aerial_win_pct'] == 65.0
    assert 'aerial_win_pct_p90' not in result.columns


def test_empty_dataframe_returned_empty():
    """Empty input DataFrame should return an empty DataFrame, not raise."""
    df = pd.DataFrame()
    result = compute_per90(df)
    assert result.empty


def test_all_below_min_minutes_returns_empty():
    """All players below MIN_MINUTES should produce empty output."""
    df = pd.DataFrame([
        make_row(player_id=1, minutes=MIN_MINUTES - 1, goals=3),
        make_row(player_id=2, minutes=MIN_MINUTES - 100, goals=2),
    ])
    result = compute_per90(df)
    assert result.empty


def test_exactly_min_minutes_included():
    """A player with exactly MIN_MINUTES should be included."""
    df = pd.DataFrame([make_row(minutes=MIN_MINUTES, goals=5)])
    result = compute_per90(df)
    assert len(result) == 1


def test_identity_cols_preserved():
    """player_id, season_id, league_id, minutes, position_group must all survive."""
    df = pd.DataFrame([make_row(player_id=42, season_id=3, league_id=7,
                                minutes=1800, position_group='MID', goals=4)])
    result = compute_per90(df)
    row = result.iloc[0]
    assert row['player_id'] == 42
    assert row['season_id'] == 3
    assert row['league_id'] == 7
    assert row['minutes'] == 1800
    assert row['position_group'] == 'MID'
