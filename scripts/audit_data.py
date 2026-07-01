import sys
from database.connection import query

def main():
    print("========================================")
    print("      DATA QUALITY AUDIT REPORT         ")
    print("========================================")
    
    # 1. Duplicate matches (Player X playing twice on same day)
    duplicates = query("""
        SELECT p.name, pms.match_date, COUNT(*) as matches_on_day
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        GROUP BY p.name, pms.match_date
        HAVING COUNT(*) > 1
    """, as_dict=True)
    
    print(f"\\n[1] Duplicate Matches per Day:")
    if not duplicates:
        print("    PASSED - No players played twice on the same day.")
    else:
        print(f"    FAILED - Found {len(duplicates)} occurrences.")
        for d in duplicates[:5]:
            print(f"      - {d['name']} played {d['matches_on_day']} matches on {d['match_date']}")
    
    # 2. Distribution of NULL vals across key stats cols
    stat_cols = [
        "goals", "assists", "xg", "xa", "shots", "passes", "passes_accurate", 
        "duels", "duels_won", "gk_saves", "gk_conceded"
    ]
    
    null_counts = []
    print(f"\\n[2] NULL Distribution across selected columns:")
    for col in stat_cols:
        res = query(f"SELECT COUNT(*) FROM player_match_stats WHERE {col} IS NULL")
        count = res[0][0] if res else 0
        null_counts.append((col, count))
    
    for col, count in null_counts:
        print(f"    - {col}: {count} NULL values")
    
    # 3. Goalkeepers mistakenly flagged as Outfielders
    # E.g., position_group is not 'GK' but position_played is 'GK'
    gk_flag_issues = query("""
        SELECT p.name, p.position_group, pms.position_played
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE pms.position_played = 'GK' AND p.position_group != 'GK'
        GROUP BY p.name, p.position_group, pms.position_played
    """, as_dict=True)
    
    print(f"\\n[3] GK Positional Integrity:")
    if not gk_flag_issues:
        print("    PASSED - No goalkeepers mapped to outfield position groups.")
    else:
        print(f"    FAILED - Found {len(gk_flag_issues)} players with mismatched GK groups.")
        for d in gk_flag_issues[:5]:
            print(f"      - {d['name']} is {d['position_group']} but played as {d['position_played']}")
            
    print("\\n========================================")

if __name__ == "__main__":
    main()
