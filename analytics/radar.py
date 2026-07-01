import sys
import os
from pathlib import Path
from typing import Dict

# Add parent directory to path so database.connection imports correctly
sys.path.insert(0, str(Path(__file__).parent.parent))
from database.connection import query

# Position-specific metric configurations.
# Format: (Display Name, SQL Aggregation Expression, Invert Percentile (True if lower is better))
RADAR_METRICS = {
    'FWD': [
        ('Goals', 'COALESCE(SUM(goals), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('xG', 'COALESCE(SUM(xg), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Shots', 'COALESCE(SUM(shots), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Shots on Target', 'COALESCE(SUM(shots_on_target), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Touches in Box', 'COALESCE(SUM(touches_in_box), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Progressive Runs', 'COALESCE(SUM(progressive_runs), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
    ],
    'MID': [
        ('Assists', 'COALESCE(SUM(assists), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('xA', 'COALESCE(SUM(xa), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Passes', 'COALESCE(SUM(passes), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Progressive Runs', 'COALESCE(SUM(progressive_runs), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Interceptions', 'COALESCE(SUM(interceptions), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Duels Won %', 'COALESCE(SUM(duels_won), 0)::float / NULLIF(SUM(duels), 0) * 100', False),
    ],
    'DEF': [
        ('Interceptions', 'COALESCE(SUM(interceptions), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Duels Won %', 'COALESCE(SUM(duels_won), 0)::float / NULLIF(SUM(duels), 0) * 100', False),
        ('Aerial Duels', 'COALESCE(SUM(aerial_duels), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Recoveries', 'COALESCE(SUM(recoveries), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Pass Accuracy', 'COALESCE(SUM(passes_accurate), 0)::float / NULLIF(SUM(passes), 0) * 100', False),
    ],
    'GK': [
        ('Saves', 'COALESCE(SUM(gk_saves), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Goals Conceded', 'COALESCE(SUM(gk_conceded), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', True), # Inverse: fewer is better
        ('xG Faced', 'COALESCE(SUM(gk_xg_save), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Sweeps/Clearances', 'COALESCE(SUM(gk_sweeps), 0)::float / NULLIF(SUM(minutes_played), 0) * 90', False),
        ('Pass Accuracy', 'COALESCE(SUM(gk_passes_acc), 0)::float / NULLIF(SUM(gk_passes), 0) * 100', False),
    ]
}

def _percentile_rank_stat(target_value: float, all_values: list[float], invert: bool = False) -> float:
    """Computes standard percentile rank handling ties properly."""
    valid_values = [v for v in all_values if v is not None]
    if not valid_values:
        return 0.0
    n = len(valid_values)
    if n == 0:
        return 0.0
    
    if invert:
        less_than = sum(1 for v in valid_values if v > target_value)
    else:
        less_than = sum(1 for v in valid_values if v < target_value)
        
    equals = sum(1 for v in valid_values if v == target_value)
    
    pctl = (less_than + (0.5 * equals)) / n * 100
    return round(pctl, 1)

def compute_radar_data(player_id: int, min_minutes: int = 450) -> Dict[str, float]:
    """
    Computes a player's percentile rank across core metrics compared to peers.
    Peers are players in the same position group and competition with >= min_minutes.
    Returns: Dict[str, float] of { "Metric Name": 85.5 }
    """
    # 1. Get player's primary position group and competition
    player_info = query('''
        SELECT p.position_group, pms.competition_name
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE p.player_id = %s AND pms.minutes_played > 0
        GROUP BY p.position_group, pms.competition_name
        ORDER BY SUM(pms.minutes_played) DESC
        LIMIT 1
    ''', (player_id,), as_dict=True)
    
    if not player_info:
        return {} # Player not found or has 0 minutes
        
    pos_group = player_info[0]['position_group']
    competition = player_info[0]['competition_name']
    
    if not pos_group or pos_group not in RADAR_METRICS:
        return {} # Positional analytics cannot be performed on unmapped players
        
    config = RADAR_METRICS[pos_group]
    
    # 2. Build and execute peer group query natively derived from DB
    columns_sql = [f"{expr} AS m_{i}" for i, (_, expr, _) in enumerate(config)]
    
    sql = f'''
        SELECT 
            pms.player_id,
            SUM(pms.minutes_played) AS total_minutes,
            {', '.join(columns_sql)}
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE p.position_group = %s AND pms.competition_name = %s
        GROUP BY pms.player_id
        HAVING SUM(pms.minutes_played) >= %s
    '''
    
    peers = query(sql, (pos_group, competition, min_minutes), as_dict=True)
    
    if not peers:
        return {}
        
    # Extract distributions and locate the target player
    metric_distributions = {i: [] for i in range(len(config))}
    target_player_raw = None
    
    for row in peers:
        pid = row['player_id']
        is_target = (pid == player_id)
        
        for i in range(len(config)):
            val = row[f'm_{i}']
            if val is not None:
                parsed_val = float(val)
                metric_distributions[i].append(parsed_val)
                if is_target:
                    if target_player_raw is None:
                        target_player_raw = {}
                    target_player_raw[i] = parsed_val
                    
    if not target_player_raw:
        return {}
        
    # 3. Compute and format percentiles
    result = {}
    for i, (name, _, invert) in enumerate(config):
        target_val = target_player_raw[i]
        pctl = _percentile_rank_stat(target_val, metric_distributions[i], invert)
        result[name] = pctl
        
    return result

if __name__ == '__main__':
    # Auto-detect a valid player ID to print a sample for the report
    res = query('''
        SELECT p.player_id 
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        GROUP BY p.player_id
        HAVING SUM(pms.minutes_played) >= 450
        LIMIT 1
    ''', as_dict=True)
    if res:
        pid = res[0]['player_id']
        print(f"Radar data for player_id={pid}:")
        print(compute_radar_data(pid))
    else:
        print("No players found with >= 450 minutes.")
