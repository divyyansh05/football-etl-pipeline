import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, ANY
from etl.wyscout_loader import WyscoutLoader

@pytest.fixture
def loader():
    return WyscoutLoader()

@patch("etl.wyscout_loader.get_conn")
def test_file_loaded(mock_get_conn, loader):
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    
    # Test case: file is loaded
    mock_cur.fetchone.return_value = (1,)
    assert loader.file_loaded("test.xlsx") is True
    
    # Test case: file is not loaded
    mock_cur.fetchone.return_value = None
    assert loader.file_loaded("test.xlsx") is False

@patch("etl.wyscout_loader.get_conn")
@patch("etl.wyscout_loader.parse_player_xlsx")
def test_load_player_xlsx_idempotency(mock_parse, mock_get_conn, loader):
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    
    # Mock parse_player_xlsx to return one record
    mock_parse.return_value = [{
        'match_date': '2024-01-01',
        'competition_name': 'PL',
        'match_label': 'A vs B',
        'minutes_played': 90
    }]
    
    # Mock upsert_player to return an ID
    with patch.object(loader, 'upsert_player', return_value=1):
        # Mock file_loaded to return False first time
        with patch.object(loader, 'file_loaded', return_value=False):
            with patch.object(loader, 'mark_loaded') as mock_mark:
                count = loader.load_player_xlsx(Path("test.xlsx"), 123, "Player Name")
                assert count == 1
                assert mock_cur.execute.call_count >= 3 # SAVEPOINT, INSERT, RELEASE
                mock_mark.assert_called_once()
        
        # Mock file_loaded to return True second time
        with patch.object(loader, 'file_loaded', return_value=True):
            count = loader.load_player_xlsx(Path("test.xlsx"), 123, "Player Name")
            assert count == 0

@patch("etl.wyscout_loader.get_conn")
@patch("etl.wyscout_loader.parse_player_xlsx")
def test_load_player_xlsx_savepoint_failure(mock_parse, mock_get_conn, loader):
    mock_conn = MagicMock()
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    
    # One good record, one breaking record
    mock_parse.return_value = [
        {'match_date': '2024-01-01', 'competition_name': 'PL'},
        {'match_date': '2024-01-02', 'competition_name': 'PL'}
    ]
    
    # Make the second execute fail
    def side_effect(sql, *args):
        if "INSERT INTO player_match_stats" in sql and "2024-01-02" in str(args):
            raise Exception("DB Error")
        return MagicMock()
    
    mock_cur.execute.side_effect = side_effect
    
    with patch.object(loader, 'upsert_player', return_value=1):
        with patch.object(loader, 'file_loaded', return_value=False):
            with patch.object(loader, 'mark_loaded'):
                count = loader.load_player_xlsx(Path("test.xlsx"), 123, "Player Name")
                assert count == 1 # Only one should have succeeded
                
                # Check rollbacks
                mock_cur.execute.assert_any_call('ROLLBACK TO SAVEPOINT row_sp')
