import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
import numpy as np

from scrapers.wyscout.parser import parse_player_xlsx, _safe_int, _safe_float

def test_safe_int_formatting():
    """Test that numeric extraction handles string formatting properly."""
    assert _safe_int("10") == 10
    assert _safe_int("10.0") == 10
    assert _safe_int(10.5) == 10
    assert _safe_int("10.5") == 10
    assert _safe_int("abc") is None
    assert _safe_int("") is None

def test_safe_float_formatting():
    """Test that float extraction handles string formatting properly."""
    assert _safe_float("10.5") == 10.5
    assert _safe_float(10) == 10.0
    assert _safe_float("abc") is None
    assert _safe_float("") is None

@patch("pandas.read_excel")
def test_parse_null_logic(mock_read_excel):
    """Test that NULL logic returns None instead of 0 when data is missing."""
    data = {
        'match_id': [1],
        'match_name': ['A vs B'],
        'playerStats_minutes_on_field': [np.nan],  # Missing data
        'playerStats_goal': [np.nan],              # Missing data
        'playerStats_xg_shot': [np.nan],           # Missing float
        'playerStats_positions': [np.nan]          # Missing string
    }
    mock_read_excel.return_value = pd.DataFrame(data)

    results = parse_player_xlsx(Path("dummy.xlsx"))
    
    assert len(results) == 1
    assert results[0].get('minutes_played') is None
    assert results[0].get('goals') is None
    assert results[0].get('xg') is None
    assert results[0].get('position_played') is None

@patch("pandas.read_excel")
def test_parse_position_tracking(mock_read_excel):
    """Test position tracking and GK columns."""
    data = {
        'match_id': [1, 2],
        'match_name': ['GK Match', 'FWD Match'],
        'playerStats_positions': ['GK', 'CF'],
        'playerStats_save': [5, np.nan],
        'playerStats_goal': [0, 2]
    }
    mock_read_excel.return_value = pd.DataFrame(data)

    results = parse_player_xlsx(Path("dummy.xlsx"))
    
    assert len(results) == 2
    # Check string stripping/extraction for positions
    assert results[0]['position_played'] == 'GK'
    assert results[1]['position_played'] == 'CF'
    
    # Check GK stats extracted vs missing (NaN -> None)
    assert results[0]['gk_saves'] == 5
    assert results[1].get('gk_saves') is None
    
