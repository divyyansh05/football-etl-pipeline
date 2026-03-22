"""
Unit tests for FotMob data parsers.

Tests critical parsing functions including:
- Player data parsing (bio, stats, deep stats)
- League standings parsing
- Match parsing
- Edge cases (dict-to-scalar, xG vs xG against)
"""

import pytest
from datetime import date
from scrapers.fotmob.data_parser import FotMobDataParser


class TestParsePlayer:
    """Tests for parse_player() function."""

    def test_basic_player_parsing(self, sample_player_data):
        """Test basic player data extraction."""
        result = FotMobDataParser.parse_player(sample_player_data)

        assert result is not None
        assert result['fotmob_id'] == 12345
        assert result['name'] == 'Bruno Fernandes'
        assert result['position'] == 'Midfielder'
        assert result['current_team_id'] == 10260
        assert result['current_team_name'] == 'Manchester United'

    def test_player_birth_date_parsing(self, sample_player_data):
        """Test date of birth parsing from ISO format."""
        result = FotMobDataParser.parse_player(sample_player_data)

        assert result['date_of_birth'] == date(1994, 9, 8)

    def test_player_info_extraction(self, sample_player_data):
        """Test playerInformation array parsing."""
        result = FotMobDataParser.parse_player(sample_player_data)

        assert result['height_cm'] == 179
        assert result['shirt_number'] == 8

    def test_dict_format_preferred_foot(self, edge_case_player_dict_foot):
        """Test that dict-format preferred_foot is extracted correctly (ISSUE-019)."""
        result = FotMobDataParser.parse_player(edge_case_player_dict_foot)

        # Should extract 'Right' from {'key': 'right', 'fallback': 'Right'}
        assert result['preferred_foot'] == 'Right'
        assert len(result['preferred_foot']) <= 10  # VARCHAR(10) limit

    def test_dict_format_nationality(self, edge_case_player_dict_foot):
        """Test that dict-format nationality is extracted correctly (ISSUE-019)."""
        result = FotMobDataParser.parse_player(edge_case_player_dict_foot)

        # Should extract 'Spain' from {'key': None, 'fallback': 'Spain'}
        assert result['nationality'] == 'Spain'

    def test_empty_player_data(self):
        """Test handling of empty/missing player data."""
        result = FotMobDataParser.parse_player({})
        assert result is None

        result = FotMobDataParser.parse_player(None)
        assert result is None

    def test_missing_optional_fields(self):
        """Test player with minimal data."""
        minimal_data = {
            'id': 11111,
            'name': 'Minimal Player',
        }
        result = FotMobDataParser.parse_player(minimal_data)

        assert result['fotmob_id'] == 11111
        assert result['name'] == 'Minimal Player'
        assert result['height_cm'] is None
        assert result['preferred_foot'] is None


class TestParsePlayerDeepStats:
    """Tests for parse_player_deep_stats() function."""

    def test_basic_deep_stats(self, sample_player_data):
        """Test deep stats extraction."""
        result = FotMobDataParser.parse_player_deep_stats(sample_player_data)

        assert result is not None
        assert result['goals'] == 5
        assert result['xg'] == 4.2
        assert result['shots'] == 45
        assert result['assists'] == 4
        assert result['xa'] == 3.5
        assert result['key_passes'] == 35

    def test_xg_vs_xg_against_exact_match(self, edge_case_player_xg_against):
        """Test that 'xG' doesn't match 'xG against' (ISSUE-015)."""
        result = FotMobDataParser.parse_player_deep_stats(edge_case_player_xg_against)

        # Should get 0.8 (the actual xG), not 15.5 (xG against)
        assert result is not None
        assert result['xg'] == 0.8

    def test_empty_stats_returns_none(self):
        """Test that missing stats section returns None."""
        data = {'firstSeasonStats': {}}
        result = FotMobDataParser.parse_player_deep_stats(data)
        assert result is None

        data = {'firstSeasonStats': {'statsSection': {}}}
        result = FotMobDataParser.parse_player_deep_stats(data)
        assert result is None


class TestParseLeagueStandings:
    """Tests for parse_league_standings() function."""

    def test_basic_standings_parsing(self, sample_standings_data):
        """Test standings extraction."""
        result = FotMobDataParser.parse_league_standings(sample_standings_data)

        assert len(result) == 2
        assert result[0]['team_name'] == 'Liverpool'
        assert result[0]['position'] == 1
        assert result[0]['points'] == 46
        assert result[0]['goals_for'] == 45
        assert result[0]['goals_against'] == 18

    def test_scores_str_parsing(self, sample_standings_data):
        """Test scoresStr parsing (e.g., '45-18')."""
        result = FotMobDataParser.parse_league_standings(sample_standings_data)

        assert result[0]['goals_for'] == 45
        assert result[0]['goals_against'] == 18

    def test_empty_standings_returns_empty_list(self):
        """Test that empty/missing data returns empty list."""
        assert FotMobDataParser.parse_league_standings(None) == []
        assert FotMobDataParser.parse_league_standings({}) == []
        assert FotMobDataParser.parse_league_standings({'table': {}}) == []


class TestParseXgTable:
    """Tests for parse_xg_table() function."""

    def test_xg_table_parsing(self, sample_standings_data):
        """Test xG table extraction."""
        result = FotMobDataParser.parse_xg_table(sample_standings_data)

        assert len(result) >= 1
        assert result[0]['xg'] == 42.5
        assert result[0]['xg_conceded'] == 20.1
        assert result[0]['x_points'] == 44.2

    def test_empty_xg_table_returns_empty_list(self):
        """Test that empty/missing data returns empty list."""
        assert FotMobDataParser.parse_xg_table(None) == []
        assert FotMobDataParser.parse_xg_table({}) == []
        assert FotMobDataParser.parse_xg_table([]) == []  # List instead of dict


class TestParseMatch:
    """Tests for parse_match() function."""

    def test_basic_match_parsing(self, sample_match_data):
        """Test match details extraction."""
        result = FotMobDataParser.parse_match(sample_match_data)

        assert result is not None
        assert result['fotmob_match_id'] == 4193490
        assert result['home_team_name'] == 'Liverpool'
        assert result['away_team_name'] == 'Manchester United'
        assert result['home_score'] == 2
        assert result['away_score'] == 1
        assert result['finished'] is True

    def test_match_date_parsing(self, sample_match_data):
        """Test match date extraction."""
        result = FotMobDataParser.parse_match(sample_match_data)

        assert result['match_date'] == date(2025, 1, 15)


class TestParseMatchStats:
    """Tests for parse_match_stats() function."""

    def test_match_stats_parsing(self, sample_match_data):
        """Test match stats extraction."""
        result = FotMobDataParser.parse_match_stats(sample_match_data)

        assert result is not None
        assert 'home' in result
        assert 'away' in result

        assert float(result['home']['xg']) == 1.8
        assert float(result['away']['xg']) == 1.2
        assert result['home']['possession'] == 55
        assert result['home']['shots'] == 15


class TestParseMatchEvents:
    """Tests for parse_match_events() function."""

    def test_match_events_parsing(self, sample_match_data):
        """Test match events extraction."""
        result = FotMobDataParser.parse_match_events(sample_match_data)

        assert len(result) == 3

        # Goal event
        assert result[0]['type'] == 'Goal'
        assert result[0]['time'] == 25
        assert result[0]['player_name'] == 'Salah'

        # Card event
        assert result[1]['type'] == 'Card'
        assert result[1]['card_type'] == 'Yellow'


class TestFormatSeasonName:
    """Tests for format_season_name() utility."""

    def test_slash_format_conversion(self):
        """Test '2024/2025' -> '2024-25' conversion."""
        assert FotMobDataParser.format_season_name('2024/2025') == '2024-25'
        assert FotMobDataParser.format_season_name('2023/2024') == '2023-24'

    def test_single_year_passthrough(self):
        """Test single year seasons (Brazil, Argentina)."""
        assert FotMobDataParser.format_season_name('2024') == '2024'

    def test_empty_input(self):
        """Test empty/None input."""
        assert FotMobDataParser.format_season_name('') == ''
        assert FotMobDataParser.format_season_name(None) == ''
