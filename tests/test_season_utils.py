"""
Tests for utils/season_utils.py — SeasonUtils and _season_key in understat_etl.
"""
import pytest
from utils.season_utils import SeasonUtils
from etl.understat_etl import _season_key


class TestSeasonUtils:
    # ── to_db_format ─────────────────────────────────────────────────────────

    def test_db_format_passthrough(self):
        assert SeasonUtils.to_db_format("2024-25") == "2024-25"

    def test_slash_full_to_db(self):
        assert SeasonUtils.to_db_format("2024/2025") == "2024-25"

    def test_slash_short_to_db(self):
        assert SeasonUtils.to_db_format("2024/25") == "2024-25"

    def test_single_year_to_db(self):
        assert SeasonUtils.to_db_format("2024") == "2024-25"

    def test_2022_season(self):
        assert SeasonUtils.to_db_format("2022-23") == "2022-23"

    def test_2025_season(self):
        assert SeasonUtils.to_db_format("2025-26") == "2025-26"

    # ── parse_years ───────────────────────────────────────────────────────────

    def test_parse_db_format(self):
        assert SeasonUtils.parse_years("2024-25") == (2024, 2025)

    def test_parse_slash_full(self):
        assert SeasonUtils.parse_years("2022/2023") == (2022, 2023)

    def test_parse_single_year(self):
        assert SeasonUtils.parse_years("2024") == (2024, 2025)

    def test_parse_unknown_raises(self):
        with pytest.raises(ValueError):
            SeasonUtils.parse_years("24-25")

    # ── to_single_year ────────────────────────────────────────────────────────

    def test_to_single_year_db_format(self):
        assert SeasonUtils.to_single_year("2024-25") == 2024

    def test_to_single_year_slash(self):
        assert SeasonUtils.to_single_year("2023/2024") == 2023

    # ── get_current_season ────────────────────────────────────────────────────

    def test_get_current_season_returns_db_format(self):
        season = SeasonUtils.get_current_season()
        # Should match e.g. '2024-25' or '2025-26'
        import re
        assert re.match(r"^\d{4}-\d{2}$", season), f"Bad format: {season}"

    def test_get_current_season_august_cutoff(self):
        """Months ≥ 8 should give the season starting this year."""
        from unittest.mock import patch
        from datetime import datetime
        with patch("utils.season_utils.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 9, 1)
            season = SeasonUtils.get_current_season()
        assert season == "2025-26"

    def test_get_current_season_january(self):
        """January should give the season that started last year."""
        from unittest.mock import patch
        from datetime import datetime
        with patch("utils.season_utils.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 15)
            season = SeasonUtils.get_current_season()
        assert season == "2025-26"

    # ── are_same_season ───────────────────────────────────────────────────────

    def test_same_season_different_formats(self):
        assert SeasonUtils.are_same_season("2024-25", "2024/2025") is True

    def test_different_seasons(self):
        assert SeasonUtils.are_same_season("2024-25", "2023-24") is False


class TestSeasonKey:
    """Test the _season_key() helper used by UnderstatETL."""

    def test_2024_25_to_2425(self):
        assert _season_key("2024-25") == "2425"

    def test_2025_26_to_2526(self):
        assert _season_key("2025-26") == "2526"

    def test_2022_23_to_2223(self):
        assert _season_key("2022-23") == "2223"

    def test_2023_24_to_2324(self):
        assert _season_key("2023-24") == "2324"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _season_key("2024")
