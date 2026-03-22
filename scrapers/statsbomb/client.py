"""
StatsBomb Open Data Client

Wraps statsbombpy to fetch free football data suitable for testing the pipeline.
"""

import logging
from typing import Dict, List, Any, Optional
from statsbombpy import sb
import pandas as pd

logger = logging.getLogger(__name__)

class StatsBombClient:
    """
    Client for StatsBomb Open Data.
    Accesses free data via statsbombpy.
    """
    
    def __init__(self):
        logger.info("StatsBombClient initialized")
    
    def get_competitions(self) -> List[Dict[str, Any]]:
        """Get available competitions."""
        try:
            comps = sb.competitions()
            return comps.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to fetch competitions: {e}")
            raise

    def get_matches(self, competition_id: int, season_id: int) -> List[Dict[str, Any]]:
        """Get matches for a specific competition and season."""
        try:
            matches = sb.matches(competition_id=competition_id, season_id=season_id)
            return matches.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to fetch matches for comp {competition_id}, season {season_id}: {e}")
            raise

    def get_match_events(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Get granular event data for a match (shots, passes, etc.).
        Returns raw event dictionary.
        """
        try:
            # sb.events returns a DataFrame, convert to records
            events = sb.events(match_id=match_id)
            return events.to_dict('records')
        except Exception as e:
            logger.error(f"Failed to fetch events for match {match_id}: {e}")
            # StatsBomb might return warnings for some matches, treat as empty if failure
            return []

    def get_lineups(self, match_id: int) -> Dict[str, Any]:
        """Get lineups for a match."""
        try:
            lineups = sb.lineups(match_id=match_id)
            # Lineups is a dict of team_name -> DataFrame
            result = {}
            for team, df in lineups.items():
                result[team] = df.to_dict('records')
            return result
        except Exception as e:
            logger.error(f"Failed to fetch lineups for match {match_id}: {e}")
            raise
