"""
Tests for data quality validation.

Tests the DataQualityValidator and AnomalyDetector classes.
"""

import pytest
from datetime import date
from utils.data_quality import (
    DataQualityValidator,
    AnomalyDetector,
    safe_extract_scalar,
    safe_extract_numeric,
)


class TestDataQualityValidator:
    """Tests for DataQualityValidator class."""

    def setup_method(self):
        """Set up validator for each test."""
        self.validator = DataQualityValidator()

    def test_valid_player_data(self, sample_player_season_stats):
        """Test validation passes for good data."""
        result = self.validator.validate_player_stats(sample_player_season_stats)

        assert result.is_valid
        assert len(result.errors) == 0
        assert result.quality_score >= 90

    def test_missing_required_field(self):
        """Test validation fails for missing required field."""
        data = {
            'team_id': 1,
            'season_id': 1,
            'league_id': 1,
            # Missing player_id
        }
        result = self.validator.validate_player_stats(data)

        assert not result.is_valid
        assert any(e['field'] == 'player_id' for e in result.errors)

    def test_xg_out_of_range_high(self):
        """Test validation catches xG > 50."""
        data = {
            'player_id': 1,
            'team_id': 1,
            'season_id': 1,
            'league_id': 1,
            'xg': 55.0,  # Too high
        }
        result = self.validator.validate_player_stats(data)

        # Should have a high severity error
        assert any(
            e['field'] == 'xg' and e['severity'] == 'high'
            for e in result.errors
        )

    def test_xg_in_valid_range(self):
        """Test xG validation passes for normal value."""
        data = {
            'player_id': 1,
            'team_id': 1,
            'season_id': 1,
            'league_id': 1,
            'xg': 8.5,
        }
        result = self.validator.validate_player_stats(data)

        assert not any(e['field'] == 'xg' for e in result.errors)

    def test_negative_value_validation(self):
        """Test validation catches negative goals."""
        data = {
            'player_id': 1,
            'team_id': 1,
            'season_id': 1,
            'league_id': 1,
            'goals': -5,  # Invalid
        }
        result = self.validator.validate_player_stats(data)

        # Goals rule has 'medium' severity, so issues go to warnings (not errors)
        assert any(e['field'] == 'goals' for e in result.warnings)

    def test_valid_team_stats(self, sample_team_season_stats):
        """Test team stats validation."""
        result = self.validator.validate_team_stats(sample_team_season_stats)

        assert result.is_valid
        assert result.quality_score >= 90

    def test_batch_validation(self):
        """Test batch validation separates valid and invalid records."""
        records = [
            {'player_id': 1, 'team_id': 1, 'season_id': 1, 'league_id': 1, 'xg': 5.0},
            {'player_id': 2, 'team_id': 1, 'season_id': 1, 'league_id': 1, 'xg': 60.0},  # Invalid
            {'team_id': 1, 'season_id': 1, 'league_id': 1, 'xg': 3.0},  # Missing player_id
        ]

        valid, invalid, score = self.validator.validate_batch('player_season_stats', records)

        assert len(valid) == 1
        assert len(invalid) == 2


class TestAnomalyDetector:
    """Tests for AnomalyDetector class."""

    def setup_method(self):
        """Set up detector for each test."""
        self.detector = AnomalyDetector()

    def test_detects_high_xg_anomaly(self):
        """Test detection of unusually high xG."""
        records = [
            {'player_name': 'Normal', 'xg': 5.0},
            {'player_name': 'Anomaly', 'xg': 35.0},  # Very high
        ]

        anomalies = self.detector.detect_anomalies(records)

        assert len(anomalies) == 1
        assert anomalies[0]['record_name'] == 'Anomaly'
        assert any(a['field'] == 'xg' for a in anomalies[0]['anomalies'])

    def test_normal_data_no_anomalies(self):
        """Test no anomalies detected for normal data."""
        records = [
            {'player_name': 'Player A', 'xg': 5.0, 'goals': 5, 'assists': 3},
            {'player_name': 'Player B', 'xg': 8.0, 'goals': 8, 'assists': 5},
        ]

        anomalies = self.detector.detect_anomalies(records)

        assert len(anomalies) == 0

    def test_handles_null_values(self):
        """Test graceful handling of null/missing values."""
        records = [
            {'player_name': 'Player', 'xg': None, 'goals': 5},
        ]

        # Should not raise an exception
        anomalies = self.detector.detect_anomalies(records)
        assert isinstance(anomalies, list)


class TestSafeExtractScalar:
    """Tests for safe_extract_scalar() utility."""

    def test_extracts_from_dict_fallback(self):
        """Test extraction from dict with 'fallback' key."""
        value = {'key': 'right', 'fallback': 'Right'}
        result = safe_extract_scalar(value)

        assert result == 'Right'

    def test_extracts_from_dict_key(self):
        """Test extraction from dict with only 'key'."""
        value = {'key': 'left'}
        result = safe_extract_scalar(value)

        assert result == 'left'

    def test_returns_plain_value(self):
        """Test passthrough for plain values."""
        assert safe_extract_scalar('Brazil') == 'Brazil'
        assert safe_extract_scalar(42) == 42

    def test_returns_default_for_none(self):
        """Test default return for None."""
        assert safe_extract_scalar(None, default='Unknown') == 'Unknown'

    def test_truncates_long_strings(self):
        """Test string truncation for max_length."""
        long_value = 'A' * 100
        result = safe_extract_scalar(long_value, max_length=10)

        assert len(result) == 10

    def test_handles_stringified_dict(self):
        """Test detection of stringified dicts."""
        value = "{'key': 'right', 'fallback': 'Right'}"
        result = safe_extract_scalar(value, default='Unknown')

        # Should return default, not the stringified dict
        assert result == 'Unknown'


class TestSafeExtractNumeric:
    """Tests for safe_extract_numeric() utility."""

    def test_extracts_numeric_from_string(self):
        """Test extraction from string with units."""
        assert safe_extract_numeric('179 cm') == 179.0
        assert safe_extract_numeric('75%') == 75.0

    def test_extracts_from_dict(self):
        """Test extraction from dict format."""
        value = {'key': 180, 'fallback': '180 cm'}
        result = safe_extract_numeric(value)

        assert result == 180.0

    def test_returns_default_for_invalid(self):
        """Test default return for non-numeric."""
        assert safe_extract_numeric('invalid', default=0.0) == 0.0
        assert safe_extract_numeric(None, default=0.0) == 0.0

    def test_handles_float_strings(self):
        """Test float string parsing."""
        assert safe_extract_numeric('4.25') == 4.25
