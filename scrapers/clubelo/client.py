"""
Club ELO API Client.
Interfaces with the completely open http://api.clubelo.com CSV endpoints.
Fetches historical ELO ratings for teams to provide opponent-strength context.
"""
import logging
import csv
from io import StringIO
from typing import List, Dict
from curl_cffi import requests

logger = logging.getLogger(__name__)

BASE_URL = 'http://api.clubelo.com'

def get_team_elo_history(team_name: str) -> List[Dict]:
    """
    Fetches the entire ELO history for a specific team.
    The team_name must be formatted without spaces (e.g., 'RealMadrid', 'ManUnited').
    
    Returns a list of dictionaries containing:
    - Rank
    - Club
    - Country
    - Level
    - Elo
    - From (Date)
    - To (Date)
    """
    # Format the name: remove spaces, remove special characters
    formatted_name = team_name.replace(" ", "").replace("-", "")
    
    url = f"{BASE_URL}/{formatted_name}"
    
    try:
        # Use curl_cffi to perfectly impersonate Chrome and bypass packet-dropping
        r = requests.get(url, impersonate='chrome120', timeout=30)
        
        # If the API returns 404 or an empty response, the team wasn't found
        if r.status_code != 200 or len(r.text) < 50:
            logger.warning(f"Could not fetch ELO for {team_name} (tried: {formatted_name})")
            return []
            
        # Parse the raw CSV response
        csv_file = StringIO(r.text)
        reader = csv.DictReader(csv_file)
        
        history = []
        for row in reader:
            history.append(row)
            
        return history
        
    except Exception as e:
        logger.error(f"Error fetching Club ELO for {team_name}: {e}")
        return []

def get_current_elo(team_name: str) -> float:
    """
    Fetches just the most recent ELO rating for a team.
    """
    history = get_team_elo_history(team_name)
    if not history:
        return None
        
    # The CSV is ordered chronologically, so the last row is the current ELO
    latest = history[-1]
    try:
        return float(latest.get('Elo', 0))
    except ValueError:
        return None
