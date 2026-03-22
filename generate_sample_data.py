"""
Generate realistic sample data for Premier League 2024-25.
Used for testing UI and pipeline when external data sources are unavailable.
"""

import sys
import random
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from database.connection import get_db
from database.batch_loader import BatchLoader

def generate_sample_data():
    """Generate realistic sample data."""
    print("=" * 60)
    print("Generating Sample Data: Premier League 2024-25")
    print("=" * 60)
    
    db = get_db()
    batch_loader = BatchLoader(db)
    
    # 1. Get or create data source
    print("\n1. Setting up data source...")
    result = db.execute_query(
        "SELECT source_id FROM data_sources WHERE source_name = 'sample_generator'",
        fetch=True
    )
    
    if result:
        source_id = result[0][0]
    else:
        result = db.execute_query(
            """
            INSERT INTO data_sources (source_name, base_url, reliability_score)
            VALUES ('sample_generator', 'local', 100)
            RETURNING source_id
            """,
            fetch=True
        )
        source_id = result[0][0]
    
    # 2. Get Premier League ID and 2024-25 Season ID
    print("2. resolving IDs...")
    result = db.execute_query("SELECT league_id FROM leagues WHERE league_name = 'Premier League'", fetch=True)
    league_id = result[0][0]
    
    result = db.execute_query("SELECT season_id FROM seasons WHERE season_name = '2024-25'", fetch=True)
    season_id = result[0][0]
    
    # 3. Create Teams
    print("3. Creating teams...")
    teams = [
        ("Arsenal", "Emirates Stadium"),
        ("Aston Villa", "Villa Park"),
        ("Bournemouth", "Vitality Stadium"),
        ("Brentford", "Gtech Community Stadium"),
        ("Brighton", "Amex Stadium"),
        ("Chelsea", "Stamford Bridge"),
        ("Crystal Palace", "Selhurst Park"),
        ("Everton", "Goodison Park"),
        ("Fulham", "Craven Cottage"),
        ("Ipswich Town", "Portman Road"),
        ("Leicester City", "King Power Stadium"),
        ("Liverpool", "Anfield"),
        ("Manchester City", "Etihad Stadium"),
        ("Manchester United", "Old Trafford"),
        ("Newcastle United", "St. James' Park"),
        ("Nottingham Forest", "City Ground"),
        ("Southampton", "St. Mary's Stadium"),
        ("Tottenham Hotspur", "Tottenham Hotspur Stadium"),
        ("West Ham United", "London Stadium"),
        ("Wolverhampton Wanderers", "Molineux Stadium")
    ]
    
    teams_data = []
    for name, stadium in teams:
        teams_data.append({
            'team_name': name,
            'league_id': league_id,
            'stadium': stadium
        })
    
    batch_loader.batch_upsert('teams', teams_data, conflict_columns=['team_name', 'league_id'])
    
    # 4. Create Team Season Stats
    print("4. Generating team stats...")
    
    # Helper to get team ID
    def get_team_id(name):
        res = db.execute_query(
            "SELECT team_id FROM teams WHERE team_name = :name AND league_id = :id",
            params={'name': name, 'id': league_id},
            fetch=True
        )
        return res[0][0]
    
    # Simulate standings (top 6 strong, bottom 3 weak)
    random.shuffle(teams)
    
    stat_records = []
    
    for i, (name, _) in enumerate(teams):
        team_id = get_team_id(name)
        played = random.randint(28, 30)
        
        # Bias results based on simulated strength (index)
        strength = (20 - i) / 20.0  # 1.0 (strong) to 0.05 (weak)
        
        wins = int(played * strength * random.uniform(0.8, 1.0))
        draws = int(played * 0.2 * random.uniform(0.5, 1.5))
        losses = played - wins - draws
        
        # Adjust if negative
        if losses < 0:
            losses = 0
            draws = played - wins
        
        goals_for = int(wins * 2.5 + draws * 1.0 + losses * 0.5)
        goals_against = int(losses * 2.0 + draws * 1.0 + wins * 0.8)
        points = (wins * 3) + draws
        
        stat_records.append({
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            'matches_played': played,
            'wins': wins,
            'draws': draws,
            'losses': losses,
            'goals_for': goals_for,
            'goals_against': goals_against,
            'goal_difference': goals_for - goals_against,
            'points': points,
            'league_position': i + 1,
            'xg_for': goals_for * random.uniform(0.9, 1.1),
            'xg_against': goals_against * random.uniform(0.9, 1.1),
            'possession_avg': 50 + (strength - 0.5) * 20,
            'data_source_id': source_id
        })
        
    batch_loader.batch_upsert('team_season_stats', stat_records, conflict_columns=['team_id', 'season_id', 'league_id'])
    
    # 5. Create Players and Player Stats (Top Scorers)
    print("5. Generating player stats...")
    
    top_players = [
        ("Erling Haaland", "Manchester City", "FW", 25),
        ("Mohamed Salah", "Liverpool", "FW", 20),
        ("Ollie Watkins", "Aston Villa", "FW", 18),
        ("Bukayo Saka", "Arsenal", "MF", 15),
        ("Cole Palmer", "Chelsea", "MF", 14),
        ("Son Heung-min", "Tottenham Hotspur", "FW", 14),
        ("Alexander Isak", "Newcastle United", "FW", 13),
        ("Phil Foden", "Manchester City", "MF", 12),
        ("Dominic Solanke", "Bournemouth", "FW", 12),
        ("Jarrod Bowen", "West Ham United", "FW", 11),
        ("Martin Ødegaard", "Arsenal", "MF", 8),
        ("Bruno Fernandes", "Manchester United", "MF", 7),
        ("Kevin De Bruyne", "Manchester City", "MF", 6),
        ("Virgil van Dijk", "Liverpool", "DF", 3),
        ("William Saliba", "Arsenal", "DF", 2)
    ]
    
    player_data = []
    player_stat_records = []
    
    # Insert players manually (no unique constraint on name)
    final_player_stats = []
    
    for p_name, t_name, pos, goals in top_players:
        team_id = get_team_id(t_name)
        
        # Check if player exists
        res = db.execute_query("SELECT player_id FROM players WHERE player_name = :name", params={'name': p_name}, fetch=True)
        
        if res:
            player_id = res[0][0]
        else:
            # Insert player
            res = db.execute_query(
                """
                INSERT INTO players (player_name, position, nationality) 
                VALUES (:name, :pos, 'Unknown') 
                RETURNING player_id
                """,
                params={'name': p_name, 'pos': pos},
                fetch=True
            )
            player_id = res[0][0]
            
        # Stats
        assists = int(goals * random.uniform(0.2, 0.6))
        matches = random.randint(25, 30)
        minutes = matches * random.randint(80, 90)
        
        final_player_stats.append({
            'player_id': player_id,
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            'matches_played': matches,
            'starts': int(matches * 0.9),
            'minutes': minutes,
            'goals': goals,
            'assists': assists,
            'xg': goals * random.uniform(0.9, 1.2),
            'xag': assists * random.uniform(0.9, 1.2),
            'shots': goals * random.uniform(4, 7),
            'shots_on_target': goals * random.uniform(2, 3),
            'data_source_id': source_id
        })
        
    batch_loader.batch_upsert('player_season_stats', final_player_stats, conflict_columns=['player_id', 'team_id', 'season_id', 'league_id'])
    
    # 6. Create Matches
    print("6. Generating matches...")
    matches_data = []
    today = datetime.now().date()
    
    # Generate some recent matches
    for _ in range(30):
        t1, t2 = random.sample(teams, 2)
        team1, _ = t1
        team2, _ = t2
        
        t1_id = get_team_id(team1)
        t2_id = get_team_id(team2)
        
        match_date = today - timedelta(days=random.randint(1, 60))
        
        score1 = random.randint(0, 4)
        score2 = random.randint(0, 4)
        
        matches_data.append({
            'league_id': league_id,
            'season_id': season_id,
            'match_date': match_date,
            'home_team_id': t1_id,
            'away_team_id': t2_id,
            'home_score': score1,
            'away_score': score2,
            'venue': f"{team1} Stadium",
            'match_status': 'completed',
            'data_source_id': source_id
        })
        
    batch_loader.batch_upsert('matches', matches_data, conflict_columns=['league_id', 'season_id', 'home_team_id', 'away_team_id', 'match_date'])
    
    print("\n✓ Sample data generated successfully!")
    return True

if __name__ == "__main__":
    generate_sample_data()
