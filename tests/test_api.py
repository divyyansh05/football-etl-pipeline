import pytest
from fastapi.testclient import TestClient
from api.main import app
from unittest.mock import patch, MagicMock

client = TestClient(app)

def test_health_endpoint():
    with patch("database.connection.query") as mock_query:
        mock_query.return_value = [(100,)]
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["player_match_rows"] == 100

def test_players_list():
    with patch("api.routers.players.query") as mock_query:
        # Mock data for list_players
        mock_query.side_effect = [
            # First call for players list
            [
                {
                    "player_id": 1, "wyscout_id": 123, "name": "Test Player",
                    "position_group": "FWD", "primary_position": "CF",
                    "matches": 10, "total_goals": 5, "total_assists": 2, "avg_xg": 0.45
                }
            ],
            # Second call for count
            [(1,)]
        ]
        
        response = client.get("/api/v1/players?limit=1&min_minutes=90")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["name"] == "Test Player"

def test_player_detail():
    with patch("api.routers.players.query") as mock_query:
        mock_query.return_value = [{
            "player_id": 1, "name": "Test Player", "wyscout_id": 123
        }]
        
        response = client.get("/api/v1/players/1")
        assert response.status_code == 200
        assert response.json()["data"]["name"] == "Test Player"

def test_player_not_found():
    with patch("api.routers.players.query") as mock_query:
        mock_query.return_value = []
        
        response = client.get("/api/v1/players/999")
        assert response.status_code == 404

def test_compare_players_validation():
    # Test invalid format (not integers)
    response = client.get("/api/v1/players/compare?ids=abc,def")
    assert response.status_code == 400
    
    # Test invalid count (less than 2)
    response = client.get("/api/v1/players/compare?ids=1")
    assert response.status_code == 400

def test_stats_overview():
    with patch("api.routers.stats.query") as mock_query:
        mock_query.return_value = [{
            "players": 100, "teams": 20, "player_match_rows": 5000
        }]
        
        response = client.get("/api/v1/stats/overview")
        assert response.status_code == 200
        assert response.json()["data"]["players"] == 100

def test_leaderboard():
    with patch("api.routers.stats.query") as mock_query:
        mock_query.return_value = [
            {"name": "Top Scorer", "total": 10}
        ]
        
        response = client.get("/api/v1/stats/leaderboard?stat=goals")
        assert response.status_code == 200
        assert response.json()["data"][0]["name"] == "Top Scorer"
