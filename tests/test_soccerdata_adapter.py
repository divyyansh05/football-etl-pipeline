"""
Tests for SoccerData Adapter

Tests the adapter's functionality including:
- Environment validation (graceful degradation when deps not met)
- Configuration mappings
- Column normalization
- Event type normalization
- Data validation
"""

import pytest
import sys
from unittest.mock import MagicMock, patch

# Import the adapter and config
from etl.sources.soccerdata_adapter import (
    SoccerDataAdapter,
    AdapterStats,
    check_soccerdata_availability,
    PYTHON_VERSION_OK,
    PANDAS_VERSION_OK,
    SOCCERDATA_AVAILABLE,
)
from etl.sources.soccerdata_config import (
    LEAGUE_MAPPINGS,
    SEASON_MAPPINGS,
    SOCCERDATA_CONFIG,
    EVENT_TYPE_MAPPINGS,
    COLUMN_MAPPINGS,
    QUALITY_THRESHOLDS,
)


class TestSoccerDataConfig:
    """Test configuration mappings."""

    def test_league_mappings_complete(self):
        """Verify all 8 leagues are mapped."""
        expected_leagues = [
            'premier-league',
            'la-liga',
            'serie-a',
            'bundesliga',
            'ligue-1',
            'eredivisie',
            'brasileiro-serie-a',
            'argentina-primera',
        ]
        for league in expected_leagues:
            assert league in LEAGUE_MAPPINGS, f"Missing league: {league}"
            assert len(LEAGUE_MAPPINGS[league]) == 2, f"Invalid mapping for {league}"

    def test_season_mappings_format(self):
        """Verify season format mappings."""
        assert '2024-25' in SEASON_MAPPINGS
        assert '2023-24' in SEASON_MAPPINGS

        # Check format conversions
        assert SEASON_MAPPINGS['2024-25']['fotmob'] == '2024/2025'
        assert SEASON_MAPPINGS['2024-25']['fbref'] == '2024-2025'

    def test_source_config_structure(self):
        """Verify source configuration structure."""
        for source, config in SOCCERDATA_CONFIG.items():
            assert 'enabled' in config, f"Missing 'enabled' for {source}"
            assert 'priority' in config, f"Missing 'priority' for {source}"
            assert 'rate_limit' in config, f"Missing 'rate_limit' for {source}"
            assert 'methods' in config, f"Missing 'methods' for {source}"

    def test_fotmob_is_primary_source(self):
        """Verify FotMob has highest priority."""
        assert SOCCERDATA_CONFIG['fotmob']['priority'] == 1
        assert SOCCERDATA_CONFIG['fotmob']['enabled'] is True

    def test_event_type_mappings(self):
        """Verify event type mappings are lowercase normalized."""
        for source_type, normalized in EVENT_TYPE_MAPPINGS.items():
            assert normalized == normalized.lower(), f"Not lowercase: {normalized}"

    def test_column_mappings_present(self):
        """Verify essential columns are mapped."""
        essential_columns = ['xG', 'xA', 'player', 'team', 'minute', 'x', 'y']
        for col in essential_columns:
            assert col in COLUMN_MAPPINGS, f"Missing column mapping: {col}"

    def test_quality_thresholds_valid(self):
        """Verify quality thresholds are sensible."""
        assert QUALITY_THRESHOLDS['xg_min'] >= 0
        assert QUALITY_THRESHOLDS['xg_max'] <= 100
        assert QUALITY_THRESHOLDS['location_x_min'] >= 0
        assert QUALITY_THRESHOLDS['location_x_max'] <= 100
        assert QUALITY_THRESHOLDS['location_y_min'] >= 0
        assert QUALITY_THRESHOLDS['location_y_max'] <= 100


class TestAdapterStats:
    """Test AdapterStats dataclass."""

    def test_default_values(self):
        """Verify default statistics values."""
        stats = AdapterStats()
        assert stats.records_fetched == 0
        assert stats.records_processed == 0
        assert stats.records_inserted == 0
        assert stats.records_updated == 0
        assert stats.records_skipped == 0
        assert stats.records_failed == 0
        assert stats.errors == []

    def test_error_list_isolation(self):
        """Verify error lists are independent between instances."""
        stats1 = AdapterStats()
        stats2 = AdapterStats()
        stats1.errors.append("error1")
        assert "error1" not in stats2.errors


class TestSoccerDataAdapter:
    """Test SoccerDataAdapter class."""

    def test_adapter_initialization(self):
        """Verify adapter initializes without error."""
        adapter = SoccerDataAdapter()
        assert adapter is not None
        assert adapter.db is None
        assert adapter.stats is not None

    def test_adapter_with_db(self):
        """Verify adapter accepts database connection."""
        mock_db = MagicMock()
        adapter = SoccerDataAdapter(db=mock_db)
        assert adapter.db == mock_db

    def test_environment_validation(self):
        """Verify environment validation runs."""
        adapter = SoccerDataAdapter()
        is_available, issues = adapter.is_available()

        # Should return boolean and list
        assert isinstance(is_available, bool)
        assert isinstance(issues, list)

    def test_availability_check_function(self):
        """Test the convenience availability check function."""
        is_available, details = check_soccerdata_availability()

        assert isinstance(is_available, bool)
        assert 'python_version' in details
        assert 'python_ok' in details
        assert 'pandas_version' in details
        assert 'pandas_ok' in details
        assert 'soccerdata_available' in details

    def test_league_conversion(self):
        """Test league key conversion."""
        adapter = SoccerDataAdapter()

        # Should convert without error
        sd_league = adapter._convert_league('premier-league')
        assert sd_league == 'ENG-Premier League'

    def test_league_conversion_invalid(self):
        """Test invalid league raises error."""
        adapter = SoccerDataAdapter()

        with pytest.raises(ValueError, match="Unknown league"):
            adapter._convert_league('invalid-league')

    def test_season_conversion(self):
        """Test season format conversion."""
        adapter = SoccerDataAdapter()

        sd_season = adapter._convert_season('2024-25', 'fotmob')
        assert sd_season == '2024/2025'

        sd_season = adapter._convert_season('2024-25', 'fbref')
        assert sd_season == '2024-2025'

    def test_season_conversion_passthrough(self):
        """Test unknown season passes through unchanged."""
        adapter = SoccerDataAdapter()

        sd_season = adapter._convert_season('2019-20', 'fotmob')
        assert sd_season == '2019-20'  # Not in mappings, unchanged

    def test_statistics_tracking(self):
        """Test statistics getter and reset."""
        adapter = SoccerDataAdapter()

        stats = adapter.get_statistics()
        assert 'records_fetched' in stats
        assert 'environment_ok' in stats
        assert 'environment_issues' in stats

        # Modify stats
        adapter.stats.records_fetched = 100
        assert adapter.get_statistics()['records_fetched'] == 100

        # Reset
        adapter.reset_statistics()
        assert adapter.get_statistics()['records_fetched'] == 0

    def test_close_clears_sources(self):
        """Test close method clears source cache."""
        adapter = SoccerDataAdapter()
        adapter._sources['test'] = MagicMock()

        adapter.close()
        assert len(adapter._sources) == 0


class TestAdapterGracefulDegradation:
    """Test adapter behavior when dependencies are not available."""

    def test_source_access_without_soccerdata(self):
        """Test that getting source raises error when soccerdata unavailable."""
        adapter = SoccerDataAdapter()

        if not SOCCERDATA_AVAILABLE:
            with pytest.raises(RuntimeError, match="soccerdata not available"):
                adapter._get_source('fotmob')

    def test_disabled_source(self):
        """Test that disabled sources raise error."""
        adapter = SoccerDataAdapter()

        # Mock soccerdata as available but source disabled
        if SOCCERDATA_AVAILABLE:
            # Temporarily disable a source
            original = SOCCERDATA_CONFIG['fotmob']['enabled']
            SOCCERDATA_CONFIG['fotmob']['enabled'] = False

            with pytest.raises(ValueError, match="not enabled"):
                adapter._get_source('fotmob')

            SOCCERDATA_CONFIG['fotmob']['enabled'] = original

    def test_unknown_source(self):
        """Test that unknown source raises error."""
        adapter = SoccerDataAdapter()

        if SOCCERDATA_AVAILABLE:
            with pytest.raises(ValueError, match="(Unknown source|not enabled)"):
                adapter._get_source('unknown_source')

    def test_data_methods_return_none_on_error(self):
        """Test that data methods return None gracefully on error."""
        adapter = SoccerDataAdapter()

        # These should return None and log error, not raise
        result = adapter.get_schedule('premier-league', '2024-25')
        # Will be None if soccerdata not available
        if not SOCCERDATA_AVAILABLE:
            assert result is None
            assert len(adapter.stats.errors) > 0


class TestColumnNormalization:
    """Test column normalization with mock DataFrames."""

    @pytest.fixture
    def mock_pandas(self):
        """Create mock pandas module if not available."""
        try:
            import pandas as pd
            return pd
        except ImportError:
            pytest.skip("pandas not available")

    def test_normalize_columns(self, mock_pandas):
        """Test column renaming."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'xG': [0.5, 0.3],
            'xA': [0.2, 0.1],
            'player': ['Player A', 'Player B'],
        })

        result = adapter._normalize_columns(df)

        assert 'xg' in result.columns
        assert 'xa' in result.columns
        assert 'player_name' in result.columns

    def test_normalize_empty_dataframe(self, mock_pandas):
        """Test normalization handles empty DataFrame."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame()
        result = adapter._normalize_columns(df)

        assert result.empty

    def test_normalize_none_dataframe(self):
        """Test normalization handles None."""
        adapter = SoccerDataAdapter()

        result = adapter._normalize_columns(None)
        assert result is None


class TestEventTypeNormalization:
    """Test event type normalization."""

    @pytest.fixture
    def mock_pandas(self):
        """Create mock pandas module if not available."""
        try:
            import pandas as pd
            return pd
        except ImportError:
            pytest.skip("pandas not available")

    def test_normalize_event_types(self, mock_pandas):
        """Test event type mapping."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'event_type': ['Goal', 'YellowCard', 'Shot'],
        })

        result = adapter._normalize_event_types(df)

        assert result['event_type'].tolist() == ['goal', 'yellow_card', 'shot']

    def test_normalize_unknown_event_type(self, mock_pandas):
        """Test unknown event types get lowercased."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'event_type': ['UnknownEvent'],
        })

        result = adapter._normalize_event_types(df)

        assert result['event_type'].tolist() == ['unknownevent']


class TestDataValidation:
    """Test data quality validation."""

    @pytest.fixture
    def mock_pandas(self):
        """Create mock pandas module if not available."""
        try:
            import pandas as pd
            return pd
        except ImportError:
            pytest.skip("pandas not available")

    def test_validate_xg_bounds(self, mock_pandas):
        """Test xG values outside bounds are filtered."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'xg': [0.5, 100.0, -5.0, 1.0],  # 100 and -5 should be filtered
        })

        result = adapter._validate_data(df, 'player_stats')

        # Only 0.5 and 1.0 should remain
        assert len(result) == 2
        assert 100.0 not in result['xg'].values
        assert -5.0 not in result['xg'].values

    def test_validate_location_bounds(self, mock_pandas):
        """Test location values outside bounds are filtered."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'location_x': [50.0, 150.0, -10.0, 75.0],
        })

        result = adapter._validate_data(df, 'events')

        assert len(result) == 2
        assert 150.0 not in result['location_x'].values
        assert -10.0 not in result['location_x'].values

    def test_validate_allows_null_values(self, mock_pandas):
        """Test that null values are not filtered."""
        adapter = SoccerDataAdapter()

        df = mock_pandas.DataFrame({
            'xg': [0.5, None, 1.0],
        })

        result = adapter._validate_data(df, 'player_stats')

        # All 3 should remain (None is allowed)
        assert len(result) == 3


class TestProcessLeagueEvents:
    """Test league event processing."""

    def test_process_requires_db(self):
        """Test process_league_events requires database."""
        adapter = SoccerDataAdapter(db=None)

        with pytest.raises(ValueError, match="Database connection required"):
            adapter.process_league_events('premier-league', '2024-25')


class TestVersionChecks:
    """Test version compatibility checking."""

    def test_python_version_check(self):
        """Test Python version check logic."""
        # This is a runtime check
        assert isinstance(PYTHON_VERSION_OK, bool)

        # Current environment check
        if sys.version_info >= (3, 10):
            assert PYTHON_VERSION_OK is True
        else:
            assert PYTHON_VERSION_OK is False

    def test_pandas_version_check(self):
        """Test pandas version check logic."""
        assert isinstance(PANDAS_VERSION_OK, bool)

    def test_soccerdata_check(self):
        """Test soccerdata availability check."""
        assert isinstance(SOCCERDATA_AVAILABLE, bool)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
