import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from database.connection import get_db

def check_data():
    db = get_db()
    
    print("--- Database Diagnostic ---")
    
    # Check Counts
    p_count = db.execute_query("SELECT COUNT(*) FROM players", fetch=True)[0][0]
    print(f"Total Players: {p_count}")
    
    pms_count = db.execute_query("SELECT COUNT(*) FROM player_match_stats", fetch=True)[0][0]
    print(f"Total Player Match Stats: {pms_count}")
    
    t_count = db.execute_query("SELECT COUNT(*) FROM teams", fetch=True)[0][0]
    print(f"Total Teams: {t_count}")
    
    # Check Sample Players
    if p_count > 0:
        print("\n--- Sample Players ---")
        players = db.execute_query("SELECT player_id, player_name, position, nationality FROM players LIMIT 5", fetch=True)
        for p in players:
            print(f"ID: {p[0]}, Name: {p[1]}, Pos: {p[2]}, Nat: {p[3]}")
    
    # Check Linkage
    if pms_count == 0:
        print("\n[CRITICAL] No player match stats found! This explains why 'Squads' and 'Match Logs' are empty.")
        print("The StatsBomb ETL might be ingesting players but not their match events/stats.")
    else:
        print("\n--- Sample Stats ---")
        stats = db.execute_query("""
            SELECT p.player_name, t.team_name, m.match_date 
            FROM player_match_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN teams t ON pms.team_id = t.team_id
            JOIN matches m ON pms.match_id = m.match_id
            LIMIT 5
        """, fetch=True)
        for s in stats:
            print(f"{s[0]} ({s[1]}) - {s[2]}")

if __name__ == "__main__":
    check_data()
