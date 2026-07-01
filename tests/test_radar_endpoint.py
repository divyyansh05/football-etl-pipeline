import sys
import os
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.main import app
from database.connection import query

client = TestClient(app)

def get_valid_player_id():
    """Find a player with >= 450 minutes."""
    res = query('''
        SELECT player_id 
        FROM player_match_stats 
        GROUP BY player_id 
        HAVING SUM(minutes_played) >= 450 
        LIMIT 1
    ''', as_dict=True)
    return res[0]['player_id'] if res else None

def get_low_minutes_player_id():
    """Find a player with < 450 minutes."""
    res = query('''
        SELECT p.player_id 
        FROM player_match_stats pms
        JOIN players p ON pms.player_id = p.player_id
        GROUP BY p.player_id 
        HAVING SUM(pms.minutes_played) < 450 AND SUM(pms.minutes_played) > 0
        LIMIT 1
    ''', as_dict=True)
    return res[0]['player_id'] if res else None

def test_radar_404():
    # Attempting to fetch a player ID that definitely does not exist
    response = client.get("/api/v1/players/999999999/radar")
    print(f"404 Test: {response.status_code} - {response.json()}")
    assert response.status_code == 404

def test_radar_400():
    pid = get_low_minutes_player_id()
    if pid:
        response = client.get(f"/api/v1/players/{pid}/radar")
        print(f"400 Test (PID={pid}): {response.status_code} - {response.json()}")
        assert response.status_code == 400
    else:
        print("400 Test: Skipped (no suitable player found)")

def test_radar_200():
    pid = get_valid_player_id()
    if pid:
        response = client.get(f"/api/v1/players/{pid}/radar")
        print(f"200 Test (PID={pid}): {response.status_code} - {response.json()}")
        assert response.status_code == 200
        assert "data" in response.json()
        data = response.json()["data"]
        # Radar data is a dictionary mapping string keys to float values
        assert isinstance(data, dict)
    else:
        print("200 Test: Skipped (no suitable player found)")

if __name__ == "__main__":
    try:
        print("Running Radar Endpoint Verification...")
        test_radar_404()
        test_radar_400()
        test_radar_200()
        print("\n✅ Verification Successful!")
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        sys.exit(1)
