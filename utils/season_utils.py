"""
Season Name Formatting Utilities

Provides consistent season name conversion between different data sources:
- Database format: '2024-25' (standard format used in DB)
- FotMob format: '2024/2025' (full years with slash)
- StatsBomb format: '2020/2021' (same as FotMob)
- API-Football format: '2024' (single year)
- Understat format: '2024' (single year)

Usage:
    from utils.season_utils import SeasonUtils

    # Convert to DB format
    db_format = SeasonUtils.to_db_format('2024/2025')  # Returns '2024-25'
    db_format = SeasonUtils.to_db_format('2024')       # Returns '2024-25'

    # Convert to FotMob format
    fotmob_format = SeasonUtils.to_fotmob_format('2024-25')  # Returns '2024/2025'

    # Convert to API-Football format (single year)
    api_format = SeasonUtils.to_single_year('2024-25')  # Returns '2024'
"""

import re
import logging
from typing import Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class SeasonUtils:
    """
    Utility class for converting between season name formats.

    Supports the following formats:
    - DB format: '2024-25' (hyphen-separated, 2-digit end year)
    - Slash format: '2024/2025' (slash-separated, full years)
    - Single year: '2024' (start year only)
    """

    # Patterns for detecting season formats
    PATTERN_DB = re.compile(r'^(\d{4})-(\d{2})$')           # 2024-25
    PATTERN_SLASH_FULL = re.compile(r'^(\d{4})/(\d{4})$')   # 2024/2025
    PATTERN_SLASH_SHORT = re.compile(r'^(\d{4})/(\d{2})$')  # 2024/25
    PATTERN_SINGLE = re.compile(r'^(\d{4})$')               # 2024

    @classmethod
    def detect_format(cls, season: str) -> str:
        """
        Detect the format of a season string.

        Returns: 'db', 'slash_full', 'slash_short', 'single', or 'unknown'
        """
        season = str(season).strip()

        if cls.PATTERN_DB.match(season):
            return 'db'
        elif cls.PATTERN_SLASH_FULL.match(season):
            return 'slash_full'
        elif cls.PATTERN_SLASH_SHORT.match(season):
            return 'slash_short'
        elif cls.PATTERN_SINGLE.match(season):
            return 'single'
        else:
            return 'unknown'

    @classmethod
    def parse_years(cls, season: str) -> Tuple[int, int]:
        """
        Extract start and end years from any season format.

        Args:
            season: Season string in any format

        Returns:
            Tuple of (start_year, end_year)

        Raises:
            ValueError: If format is unrecognized
        """
        season = str(season).strip()
        fmt = cls.detect_format(season)

        if fmt == 'db':
            # 2024-25
            match = cls.PATTERN_DB.match(season)
            start = int(match.group(1))
            end_short = int(match.group(2))
            # Convert 25 to 2025
            century = start // 100 * 100
            end = century + end_short
            if end <= start:
                end += 100  # Handle century boundary (99 -> 00)
            return (start, end)

        elif fmt == 'slash_full':
            # 2024/2025
            match = cls.PATTERN_SLASH_FULL.match(season)
            return (int(match.group(1)), int(match.group(2)))

        elif fmt == 'slash_short':
            # 2024/25
            match = cls.PATTERN_SLASH_SHORT.match(season)
            start = int(match.group(1))
            end_short = int(match.group(2))
            century = start // 100 * 100
            end = century + end_short
            if end <= start:
                end += 100
            return (start, end)

        elif fmt == 'single':
            # 2024 -> 2024-25
            match = cls.PATTERN_SINGLE.match(season)
            start = int(match.group(1))
            return (start, start + 1)

        else:
            raise ValueError(f"Unrecognized season format: '{season}'")

    @classmethod
    def to_db_format(cls, season: str) -> str:
        """
        Convert any season format to database format (2024-25).

        This is the canonical format used in the database.

        Args:
            season: Season string in any format

        Returns:
            Season in DB format (e.g., '2024-25')
        """
        try:
            start, end = cls.parse_years(season)
            end_short = end % 100
            return f"{start}-{end_short:02d}"
        except ValueError as e:
            logger.warning(f"Could not convert season to DB format: {e}")
            return str(season)

    @classmethod
    def to_fotmob_format(cls, season: str) -> str:
        """
        Convert any season format to FotMob format (2024/2025).

        Args:
            season: Season string in any format

        Returns:
            Season in FotMob format (e.g., '2024/2025')
        """
        try:
            start, end = cls.parse_years(season)
            return f"{start}/{end}"
        except ValueError as e:
            logger.warning(f"Could not convert season to FotMob format: {e}")
            return str(season)

    @classmethod
    def to_statsbomb_format(cls, season: str) -> str:
        """
        Convert any season format to StatsBomb format (2020/2021).

        Same as FotMob format.

        Args:
            season: Season string in any format

        Returns:
            Season in StatsBomb format (e.g., '2020/2021')
        """
        return cls.to_fotmob_format(season)

    @classmethod
    def to_single_year(cls, season: str) -> int:
        """
        Convert any season format to single year (start year).

        Used for API-Football and Understat which use single year format.

        Args:
            season: Season string in any format

        Returns:
            Start year as integer (e.g., 2024)
        """
        try:
            start, _ = cls.parse_years(season)
            return start
        except ValueError as e:
            logger.warning(f"Could not extract year: {e}")
            return datetime.now().year

    @classmethod
    def to_api_football_format(cls, season: str) -> str:
        """
        Convert any season format to API-Football format (single year string).

        Args:
            season: Season string in any format

        Returns:
            Season as single year string (e.g., '2024')
        """
        return str(cls.to_single_year(season))

    @classmethod
    def to_understat_format(cls, season: str) -> int:
        """
        Convert any season format to Understat format (single year int).

        Args:
            season: Season string in any format

        Returns:
            Start year as integer (e.g., 2024)
        """
        return cls.to_single_year(season)

    @classmethod
    def get_current_season(cls, fmt: str = 'db') -> str:
        """
        Get the current season based on today's date.

        European football seasons typically run August-May.
        If current month is >= August, we're in season YYYY-YY+1.
        If current month is < August, we're in season YYYY-1-YY.

        Args:
            fmt: Output format ('db', 'fotmob', 'single')

        Returns:
            Current season in requested format
        """
        now = datetime.now()
        year = now.year
        month = now.month

        # Season starts in August
        if month >= 8:
            # We're in the season that started this year
            start_year = year
        else:
            # We're in the season that started last year
            start_year = year - 1

        end_year = start_year + 1

        if fmt == 'db':
            return f"{start_year}-{end_year % 100:02d}"
        elif fmt == 'fotmob':
            return f"{start_year}/{end_year}"
        elif fmt == 'single':
            return str(start_year)
        else:
            return f"{start_year}-{end_year % 100:02d}"

    @classmethod
    def normalize_season(cls, season: str) -> str:
        """
        Normalize any season format to DB format.

        Alias for to_db_format() for clarity.

        Args:
            season: Season string in any format

        Returns:
            Normalized season in DB format
        """
        return cls.to_db_format(season)

    @classmethod
    def are_same_season(cls, season1: str, season2: str) -> bool:
        """
        Check if two season strings refer to the same season.

        Args:
            season1: First season string
            season2: Second season string

        Returns:
            True if they refer to the same season
        """
        try:
            return cls.to_db_format(season1) == cls.to_db_format(season2)
        except ValueError:
            return False


# Convenience functions for direct import
def normalize_season(season: str) -> str:
    """Convert any season format to DB format (2024-25)."""
    return SeasonUtils.to_db_format(season)


def get_current_season(fmt: str = 'db') -> str:
    """Get current season in specified format."""
    return SeasonUtils.get_current_season(fmt)
