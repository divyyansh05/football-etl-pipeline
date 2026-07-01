import sys
import os
from fastapi.testclient import TestClient

# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.main import app

client = TestClient(app)

def test_health():
    response = client.get("/api/health")
    print(f"Health: {response.status_code}")
    assert response.status_code == 200
    assert "status" in response.json()

def test_players_list():
    response = client.get("/api/v1/players?limit=1")
    print(f"Players List: {response.status_code}")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "total" in data
    assert isinstance(data["data"], list)

def test_teams_list():
    response = client.get("/api/v1/teams?limit=1")
    print(f"Teams List: {response.status_code}")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert isinstance(data["data"], list)

def test_stats_overview():
    response = client.get("/api/v1/stats/overview")
    print(f"Stats Overview: {response.status_code}")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data

def test_error_format():
    response = client.get("/api/v1/players/99999999")
    print(f"Error Format (404): {response.status_code}")
    assert response.status_code == 404
    data = response.json()
    # FastAPI HTTPException logic: detail contains our dict
    assert "detail" in data
    assert "error" in data["detail"]

if __name__ == "__main__":
    try:
        test_health()
        test_players_list()
        test_teams_list()
        test_stats_overview()
        test_error_format()
        print("\n✅ All API verification tests passed!")
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        sys.exit(1)
