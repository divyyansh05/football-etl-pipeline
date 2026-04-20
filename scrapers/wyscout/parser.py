"""
Parse Wyscout player stats xlsx files.
New format: 307-439 columns, machine-readable names.
Pattern: playerStats_X (total) + playerStats_X_success (accurate).
GK files have 307 cols, outfield 439 cols.
"""
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# xlsx column → DB column mapping
# Only maps columns that exist in the player_match_stats schema.
COLUMN_MAP = {
    # Match context
    'match_name': 'match_label',
    'match_competition': 'competition_name',
    'match_date': 'match_date',
    'playerStats_positions': 'position_played',
    'playerStats_minutes_on_field': 'minutes_played',

    # Volume
    'playerStats_total_actions': 'total_actions',
    'playerStats_total_actions_success': 'successful_actions',

    # Goals / xG
    'playerStats_goal': 'goals',
    'playerStats_assist': 'assists',
    'playerStats_shot': 'shots',
    'playerStats_shot_on_goal': 'shots_on_target',
    'playerStats_xg_shot': 'xg',
    'playerStats_xg_assist': 'xa',
    # npxg computed: xg minus penalty xg (derived in loader)
    # xg_on_target: not directly available

    # Chance creation
    'playerStats_shot_assist': 'shot_assists',
    'playerStats_pre_assist': 'second_assists',
    'playerStats_key_pass': 'key_passes',
    'playerStats_smart_pass': 'smart_passes',
    'playerStats_smart_pass_success': 'smart_passes_acc',
    'playerStats_through_pass': 'through_passes',
    'playerStats_through_pass_success': 'through_passes_acc',

    # Passing
    'playerStats_pass': 'passes',
    'playerStats_pass_success': 'passes_accurate',
    'playerStats_long_pass': 'long_passes',
    'playerStats_long_pass_success': 'long_passes_accurate',
    'playerStats_cross': 'crosses',
    'playerStats_cross_success': 'crosses_accurate',
    'playerStats_forward_pass': 'forward_passes',
    'playerStats_forward_pass_success': 'forward_passes_acc',
    'playerStats_back_pass': 'back_passes',
    'playerStats_back_pass_success': 'back_passes_acc',
    'playerStats_lateral_pass': 'lateral_passes',
    'playerStats_lateral_pass_success': 'lateral_passes_acc',
    'playerStats_pass_to_final_third': 'passes_final_third',
    'playerStats_pass_to_final_third_success': 'passes_final_third_acc',
    'playerStats_pass_to_penalty_area': 'passes_penalty_area',
    'playerStats_pass_to_penalty_area_success': 'passes_penalty_area_acc',

    # Dribbling / attacking
    'playerStats_dribble': 'dribbles',
    'playerStats_dribble_success': 'dribbles_successful',
    'playerStats_progressive_run': 'progressive_runs',
    'playerStats_touch_in_box': 'touches_in_box',
    'playerStats_offensive_duel': 'offensive_duels',
    'playerStats_offensive_duel_success': 'offensive_duels_won',

    # Defending
    'playerStats_defensive_duel': 'defensive_duels',
    'playerStats_defensive_duel_success': 'defensive_duels_won',
    'playerStats_aerial_duel': 'aerial_duels',
    'playerStats_aerial_duel_success': 'aerial_duels_won',
    'playerStats_duel': 'duels',
    'playerStats_duel_success': 'duels_won',
    'playerStats_loose_ball_duel': 'loose_ball_duels',
    'playerStats_loose_ball_duel_success': 'loose_ball_duels_won',
    'playerStats_interception': 'interceptions',
    'playerStats_tackle': 'sliding_tackles',
    'playerStats_tackle_success': 'sliding_tackles_succ',
    'playerStats_clearance': 'clearances',

    # Ball winning/losing
    'playerStats_own_half_loss': 'losses_own_half',
    'playerStats_opponent_half_recovery': 'recoveries_opp_half',
    'playerStats_recovery': 'recoveries',
    'playerStats_loss': 'losses',

    # Discipline
    'playerStats_foul': 'fouls_committed',
    'playerStats_foul_suffered': 'fouls_suffered',
    'playerStats_offside': 'offsides',
    'playerStats_yellow_cards': 'yellow_cards',
    # red_cards: derived from red_card_minute > 0

    # GK specific
    'playerStats_save': 'gk_saves',
    'playerStats_save_with_reflex': 'gk_saves_reflex',
    'playerStats_super_save': 'gk_super_saves',
    'playerStats_conceded_goal': 'gk_conceded',
    'playerStats_xg_save': 'gk_xg_save',
    'playerStats_goalkeeper_exit': 'gk_exits',
    'playerStats_goalkeeper_claim': 'gk_claims',
    'playerStats_goalkeeper_punch': 'gk_punches',
    'playerStats_goalkeeper_sweep': 'gk_sweeps',
    'playerStats_goalkeeper_foot_pass': 'gk_passes',
    'playerStats_goalkeeper_foot_pass_success': 'gk_passes_acc',
    'playerStats_goal_kick': 'gk_goal_kicks',
    'playerStats_goal_kick_short': 'gk_short_kicks',
    'playerStats_goal_kick_long': 'gk_long_kicks',
}

# Integer columns in DB (everything except xg/xa/gk_xg_save)
FLOAT_COLS = {'xg', 'xa', 'gk_xg_save'}


def parse_player_xlsx(filepath: Path) -> list:
    """
    Parse Wyscout player stats xlsx.
    Returns list of dicts mapped to DB column names.
    """
    try:
        df = pd.read_excel(filepath, header=0)
    except Exception as e:
        logger.error(f'Failed to read {filepath}: {e}')
        return []

    df = df.dropna(how='all')
    if df.empty:
        return []

    # Drop rows where match_id is NaN (header/footer noise)
    if 'match_id' in df.columns:
        df = df.dropna(subset=['match_id'])

    records = []
    for _, row in df.iterrows():
        record = {}

        for xlsx_col, db_col in COLUMN_MAP.items():
            if xlsx_col not in df.columns:
                continue
            val = row[xlsx_col]
            if pd.isna(val):
                record[db_col] = None
                continue
            if db_col in FLOAT_COLS:
                record[db_col] = _safe_float(val)
            elif db_col in ('match_label', 'competition_name',
                            'position_played', 'match_date'):
                record[db_col] = str(val).strip()
            else:
                record[db_col] = _safe_int(val)

        # Derive red_cards from red_card_minute
        rcm = row.get('playerStats_red_card_minute')
        if pd.notna(rcm) and _safe_int(rcm) and _safe_int(rcm) > 0:
            record['red_cards'] = 1
        else:
            record['red_cards'] = 0

        # Parse match_date string to date object
        if 'match_date' in record and record['match_date']:
            try:
                record['match_date'] = pd.to_datetime(
                    record['match_date']).date()
            except Exception:
                record['match_date'] = None

        records.append(record)

    logger.info(f'Parsed {len(records)} rows from {filepath.name}')
    return records


def _safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _safe_float(val):
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None
