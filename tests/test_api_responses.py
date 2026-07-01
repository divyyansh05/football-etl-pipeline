import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from api.main import app

client = TestClient(app)

def test_players_list_envelope():
    """Test validating the Pydantic JSON envelope {"data": [], "total": 0, "limit": 20}."""
    with patch("api.routers.players.query") as mock_query:
        # Mocking the two queries in `list_players`
        # First query: list of players
        mock_query.side_effect = [
            [
                {
                    "player_id": 1, "wyscout_id": 1001, "name": "Test Player",
                    "position_group": "MID", "primary_position": "CM", 
                    "nationality": "ENG", "matches": 5, "total_goals": 1, 
                    "total_assists": 2, "avg_xg": 0.1
                }
            ],
            # Second query: count total players
            [(1,)] 
        ]
        
        response = client.get("/api/v1/players?limit=20&offset=0")
        assert response.status_code == 200
        
        json_data = response.json()
        
        # Validating envelope
        assert "data" in json_data
        assert "total" in json_data
        assert "limit" in json_data
        assert "offset" in json_data
        
        assert isinstance(json_data["data"], list)
        assert len(json_data["data"]) == 1
        assert json_data["data"][0]["name"] == "Test Player"
        assert json_data["total"] == 1
        assert json_data["limit"] == 20
        assert json_data["offset"] == 0

def test_player_matches_envelope():
    """Test validating envelope for player matches."""
    with patch("api.routers.players.query") as mock_query:
        mock_query.side_effect = [
            [
                {
                    "match_label": "A vs B", "competition_name": "PL",
                    "match_date": "2024-01-01", "position_played": "CM",
                    "minutes_played": 90, "goals": 0, "assists": 0,
                    "xg": 0.0, "xa": 0.0, "shots": 0, "shots_on_target": 0,
                    "passes": 50, "passes_accurate": 45, "dribbles": 2,
                    "dribbles_successful": 1, "duels": 10, "duels_won": 5,
                    "interceptions": 2, "progressive_runs": 1, "touches_in_box": 1,
                    "yellow_cards": 0, "red_cards": 0
                }
            ],
            [(1,)]
        ]
        
        response = client.get("/api/v1/players/1/matches?limit=10&offset=0")
        assert response.status_code == 200
        
        json_data = response.json()
        assert "data" in json_data
        assert "total" in json_data
        assert "limit" in json_data
        assert "offset" in json_data
        
        assert len(json_data["data"]) == 1
        assert json_data["total"] == 1
        assert json_data["limit"] == 10
