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
    'match_name': 'match_label',
    'match_competition': 'competition_name',
    'match_date': 'match_date',
    'playerStats_positions': 'position_played',
    'playerStats_minutes_on_field': 'minutes_played',
    'playerStats_total_actions': 'total_actions',
    'playerStats_total_actions_success': 'successful_actions',
    'playerStats_goal': 'goals',
    'playerStats_assist': 'assists',
    'playerStats_shot': 'shots',
    'playerStats_shot_on_goal': 'shots_on_target',
    'playerStats_xg_shot': 'xg',
    'playerStats_xg_assist': 'xa',
    'playerStats_shot_assist': 'shot_assists',
    'playerStats_pre_assist': 'second_assists',
    'playerStats_key_pass': 'key_passes',
    'playerStats_smart_pass': 'smart_passes',
    'playerStats_smart_pass_success': 'smart_passes_acc',
    'playerStats_through_pass': 'through_passes',
    'playerStats_through_pass_success': 'through_passes_acc',
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
    'playerStats_dribble': 'dribbles',
    'playerStats_dribble_success': 'dribbles_successful',
    'playerStats_progressive_run': 'progressive_runs',
    'playerStats_touch_in_box': 'touches_in_box',
    'playerStats_offensive_duel': 'offensive_duels',
    'playerStats_offensive_duel_success': 'offensive_duels_won',
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
    'playerStats_own_half_loss': 'losses_own_half',
    'playerStats_opponent_half_recovery': 'recoveries_opp_half',
    'playerStats_recovery': 'recoveries',
    'playerStats_loss': 'losses',
    'playerStats_foul': 'fouls_committed',
    'playerStats_foul_suffered': 'fouls_suffered',
    'playerStats_offside': 'offsides',
    'playerStats_yellow_cards': 'yellow_cards',
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

    # === NEW EXTENDED COLUMNS ===
    'playerStats_goalkeeper_action': 'goalkeeper_action',
    'playerStats_action': 'action',
    'playerStats_received_pass': 'received_pass',
    'playerStats_long_pass_or_cross': 'long_pass_or_cross',
    'playerStats_short_medium_pass': 'short_medium_pass',
    'playerStats_buildup_pass': 'buildup_pass',
    'playerStats_goalkeeper_long_pass': 'goalkeeper_long_pass',
    'playerStats_progressive_pass': 'progressive_pass',
    'playerStats_goalkeeper_short_pass': 'goalkeeper_short_pass',
    'playerStats_pass_followed_by_teammate_loss': 'pass_followed_by_teammate_loss',
    'playerStats_vertical_pass': 'vertical_pass',
    'playerStats_pick_up': 'pick_up',
    'playerStats_long_pass_into_duel': 'long_pass_into_duel',
    'playerStats_pass_into_duel': 'pass_into_duel',
    'playerStats_loss_after_pass': 'loss_after_pass',
    'playerStats_loss_v2': 'loss_v2',
    'playerStats_shot_against': 'shot_against',
    'playerStats_touch': 'touch',
    'playerStats_hand_pass': 'hand_pass',
    'playerStats_launch': 'launch',
    'playerStats_meaningless': 'meaningless',
    'playerStats_pass_to_zone_fourteen': 'pass_to_zone_fourteen',
    'playerStats_quick_recovery': 'quick_recovery',
    'playerStats_any_goal': 'any_goal',
    'playerStats_easy_conceded_goal': 'easy_conceded_goal',
    'playerStats_far_conceded_goal': 'far_conceded_goal',
    'playerStats_long_conceded_goal': 'long_conceded_goal',
    'playerStats_long_shot_against': 'long_shot_against',
    'playerStats_near_shot_against': 'near_shot_against',
    'playerStats_run': 'run',
    'playerStats_run_to_final_third': 'run_to_final_third',
    'playerStats_any_free_kick': 'any_free_kick',
    'playerStats_back_pass_to_gk': 'back_pass_to_gk',
    'playerStats_free_kick': 'free_kick',
    'playerStats_free_kick_cross': 'free_kick_cross',
    'playerStats_goalkeeper_action_on_cross_faced': 'goalkeeper_action_on_cross_faced',
    'playerStats_goalkeeper_distribution': 'goalkeeper_distribution',
    'playerStats_goalkeeper_goal_kick': 'goalkeeper_goal_kick',
    'playerStats_indirect_free_kick': 'indirect_free_kick',
    'playerStats_pressed_sequence_loss': 'pressed_sequence_loss',
    'playerStats_pressed_sequence_recovery': 'pressed_sequence_recovery',
    'playerStats_recovery_counterpressing': 'recovery_counterpressing',
    'playerStats_through_pass_interception': 'through_pass_interception',
    'playerStats_goalkeeper_action_success': 'goalkeeper_action_success',
    'playerStats_action_success': 'action_success',
    'playerStats_short_medium_pass_success': 'short_medium_pass_success',
    'playerStats_received_pass_success': 'received_pass_success',
    'playerStats_long_pass_or_cross_success': 'long_pass_or_cross_success',
    'playerStats_buildup_pass_success': 'buildup_pass_success',
    'playerStats_goalkeeper_long_pass_success': 'goalkeeper_long_pass_success',
    'playerStats_goalkeeper_short_pass_success': 'goalkeeper_short_pass_success',
    'playerStats_pass_followed_by_teammate_loss_success': 'pass_followed_by_teammate_loss_success',
    'playerStats_progressive_pass_success': 'progressive_pass_success',
    'playerStats_recovery_success': 'recovery_success',
    'playerStats_vertical_pass_success': 'vertical_pass_success',
    'playerStats_pick_up_success': 'pick_up_success',
    'playerStats_long_pass_into_duel_success': 'long_pass_into_duel_success',
    'playerStats_pass_into_duel_success': 'pass_into_duel_success',
    'playerStats_shot_against_success': 'shot_against_success',
    'playerStats_hand_pass_success': 'hand_pass_success',
    'playerStats_any_goal_success': 'any_goal_success',
    'playerStats_conceded_goal_success': 'conceded_goal_success',
    'playerStats_easy_conceded_goal_success': 'easy_conceded_goal_success',
    'playerStats_far_conceded_goal_success': 'far_conceded_goal_success',
    'playerStats_goalkeeper_sweep_success': 'goalkeeper_sweep_success',
    'playerStats_long_conceded_goal_success': 'long_conceded_goal_success',
    'playerStats_long_shot_against_success': 'long_shot_against_success',
    'playerStats_near_shot_against_success': 'near_shot_against_success',
    'playerStats_pass_to_zone_fourteen_success': 'pass_to_zone_fourteen_success',
    'playerStats_quick_recovery_success': 'quick_recovery_success',
    'playerStats_save_success': 'save_success',
    'playerStats_any_free_kick_success': 'any_free_kick_success',
    'playerStats_back_pass_to_gk_success': 'back_pass_to_gk_success',
    'playerStats_free_kick_success': 'free_kick_success',
    'playerStats_free_kick_cross_success': 'free_kick_cross_success',
    'playerStats_goal_kick_success': 'goal_kick_success',
    'playerStats_goal_kick_long_success': 'goal_kick_long_success',
    'playerStats_goalkeeper_distribution_success': 'goalkeeper_distribution_success',
    'playerStats_goalkeeper_goal_kick_success': 'goalkeeper_goal_kick_success',
    'playerStats_indirect_free_kick_success': 'indirect_free_kick_success',
    'playerStats_interception_success': 'interception_success',
    'playerStats_launch_success': 'launch_success',
    'playerStats_run_success': 'run_success',
    'playerStats_run_to_final_third_success': 'run_to_final_third_success',
    'playerStats_save_with_reflex_success': 'save_with_reflex_success',
    'playerStats_yellow_card_minute': 'yellow_card_minute',
    'playerStats_near_conceded_goal': 'near_conceded_goal',
    'playerStats_received_long_pass': 'received_long_pass',
    'playerStats_ball_delivery_to_penalty_area': 'ball_delivery_to_penalty_area',
    'playerStats_last': 'last',
    'playerStats_pass_to_penalty_area_v2': 'pass_to_penalty_area_v2',
    'playerStats_aerial_duel_in_own_penalty_area': 'aerial_duel_in_own_penalty_area',
    'playerStats_ball_delivery_to_danger_zone': 'ball_delivery_to_danger_zone',
    'playerStats_heading': 'heading',
    'playerStats_legacy_won_duel': 'legacy_won_duel',
    'playerStats_real_won_duel': 'real_won_duel',
    'playerStats_loss_success': 'loss_success',
    'playerStats_loss_v2_success': 'loss_v2_success',
    'playerStats_near_conceded_goal_success': 'near_conceded_goal_success',
    'playerStats_received_long_pass_success': 'received_long_pass_success',
    'playerStats_loss_after_pass_success': 'loss_after_pass_success',
    'playerStats_pressed_sequence_loss_success': 'pressed_sequence_loss_success',
    'playerStats_recovery_counterpressing_success': 'recovery_counterpressing_success',
    'playerStats_aerial_duel_in_own_penalty_area_success': 'aerial_duel_in_own_penalty_area_success',
    'playerStats_goalkeeper_action_on_cross_faced_success': 'goalkeeper_action_on_cross_faced_success',
    'playerStats_heading_success': 'heading_success',
    'playerStats_legacy_won_duel_success': 'legacy_won_duel_success',
    'playerStats_pressed_sequence_recovery_success': 'pressed_sequence_recovery_success',
    'playerStats_real_won_duel_success': 'real_won_duel_success',
    'playerStats_goalkeeper_shot_buildup': 'goalkeeper_shot_buildup',
    'playerStats_goalkeeper_shot_buildup_success': 'goalkeeper_shot_buildup_success',
    'playerStats_goalkeeper_action_on_corner_faced': 'goalkeeper_action_on_corner_faced',
    'playerStats_goalkeeper_accurate_punch': 'goalkeeper_accurate_punch',
    'playerStats_dribble_against': 'dribble_against',
    'playerStats_goalkeeper_action_on_free_kick_faced': 'goalkeeper_action_on_free_kick_faced',
    'playerStats_goalkeeper_long_pass_under_pressure': 'goalkeeper_long_pass_under_pressure',
    'playerStats_goalkeeper_pass_under_pressure': 'goalkeeper_pass_under_pressure',
    'playerStats_ground_duel': 'ground_duel',
    'playerStats_stopped_progress_defensive_duel': 'stopped_progress_defensive_duel',
    'playerStats_under_pressure': 'under_pressure',
    'playerStats_dribble_against_success': 'dribble_against_success',
    'playerStats_goal_kick_short_success': 'goal_kick_short_success',
    'playerStats_ground_duel_success': 'ground_duel_success',
    'playerStats_last_success': 'last_success',
    'playerStats_stopped_progress_defensive_duel_success': 'stopped_progress_defensive_duel_success',
    'playerStats_through_pass_interception_success': 'through_pass_interception_success',
    'playerStats_action_in_counterattack': 'action_in_counterattack',
    'playerStats_head_conceded_goal': 'head_conceded_goal',
    'playerStats_pass_behind_the_back': 'pass_behind_the_back',
    'playerStats_action_in_counterattack_success': 'action_in_counterattack_success',
    'playerStats_head_conceded_goal_success': 'head_conceded_goal_success',
    'playerStats_pass_behind_the_back_success': 'pass_behind_the_back_success',
    'playerStats_dangerous_own_half_loss': 'dangerous_own_half_loss',
    'playerStats_deep_completed_pass': 'deep_completed_pass',
    'playerStats_highlight': 'highlight',
    'playerStats_loss_after_duel': 'loss_after_duel',
    'playerStats_minus': 'minus',
    'playerStats_opportunity': 'opportunity',
    'playerStats_penalty_conceded_goal': 'penalty_conceded_goal',
    'playerStats_received_cross': 'received_cross',
    'playerStats_received_dangerous_pass': 'received_dangerous_pass',
    'playerStats_received_pass_in_final_third': 'received_pass_in_final_third',
    'playerStats_received_progressive_pass': 'received_progressive_pass',
    'playerStats_touch_in_final_third': 'touch_in_final_third',
    'playerStats_touch_in_penalty_area': 'touch_in_penalty_area',
    'playerStats_very_deep_completion': 'very_deep_completion',
    'playerStats_own_half_loss_success': 'own_half_loss_success',
    'playerStats_ball_delivery_to_danger_zone_success': 'ball_delivery_to_danger_zone_success',
    'playerStats_ball_delivery_to_penalty_area_success': 'ball_delivery_to_penalty_area_success',
    'playerStats_clearance_success': 'clearance_success',
    'playerStats_dangerous_own_half_loss_success': 'dangerous_own_half_loss_success',
    'playerStats_deep_completed_pass_success': 'deep_completed_pass_success',
    'playerStats_loss_after_duel_success': 'loss_after_duel_success',
    'playerStats_pass_to_penalty_area_v2_success': 'pass_to_penalty_area_v2_success',
    'playerStats_penalty_conceded_goal_success': 'penalty_conceded_goal_success',
    'playerStats_very_deep_completion_success': 'very_deep_completion_success',
    'playerStats_shot_buildup_pass': 'shot_buildup_pass',
    'playerStats_shot_buildup_pass_success': 'shot_buildup_pass_success',
    'playerStats_goalkeeper_medium_pass_under_pressure': 'goalkeeper_medium_pass_under_pressure',
    'playerStats_goalkeeper_short_pass_under_pressure': 'goalkeeper_short_pass_under_pressure',
    'playerStats_goalkeeper_medium_pass_under_pressure_success': 'goalkeeper_medium_pass_under_pressure_success',
    'playerStats_goalkeeper_pass_under_pressure_success': 'goalkeeper_pass_under_pressure_success',
    'playerStats_under_pressure_success': 'under_pressure_success',
    'playerStats_cross_from_right': 'cross_from_right',
    'playerStats_cross_high': 'cross_high',
    'playerStats_deep_completed_cross': 'deep_completed_cross',
    'playerStats_pass_within_final_third': 'pass_within_final_third',
    'playerStats_received_pass_in_final_third_on_side': 'received_pass_in_final_third_on_side',
    'playerStats_goalkeeper_long_pass_under_pressure_success': 'goalkeeper_long_pass_under_pressure_success',
    'playerStats_defensive_one_on_one': 'defensive_one_on_one',
    'playerStats_dribble_against_with_take_on': 'dribble_against_with_take_on',
    'playerStats_dribble_with_take_on': 'dribble_with_take_on',
    'playerStats_dribbled_past_attempt': 'dribbled_past_attempt',
    'playerStats_lost_defensive_duel': 'lost_defensive_duel',
    'playerStats_one_on_one': 'one_on_one',
    'playerStats_infraction': 'infraction',
    'playerStats_penalty_foul': 'penalty_foul',
    'playerStats_regular_foul': 'regular_foul',
    'playerStats_plus': 'plus',
    'playerStats_plus_success': 'plus_success',
    'playerStats_misconduct': 'misconduct',
    'playerStats_time_lost_foul': 'time_lost_foul',
    'playerStats_defensive_duel_regain': 'defensive_duel_regain',
    'playerStats_legacy_won_defensive_duel': 'legacy_won_defensive_duel',
    'playerStats_defensive_duel_regain_success': 'defensive_duel_regain_success',
    'playerStats_legacy_won_defensive_duel_success': 'legacy_won_defensive_duel_success',
    'playerStats_goalkeeper_mistake_on_corner_faced': 'goalkeeper_mistake_on_corner_faced',
    'playerStats_cross_block': 'cross_block',
    'playerStats_ball_loss': 'ball_loss',
    'playerStats_ball_loss_in_area': 'ball_loss_in_area',
    'playerStats_dangerous_lost_defensive_duel': 'dangerous_lost_defensive_duel',
    'playerStats_offensive_shielding': 'offensive_shielding',
    'playerStats_foul_suffered_success': 'foul_suffered_success',
    'playerStats_missed_ball': 'missed_ball',
    'playerStats_penalty_save': 'penalty_save',
    'playerStats_penalty_save_success': 'penalty_save_success',
    'playerStats_minus_success': 'minus_success',
    'playerStats_head_shot': 'head_shot',
    'playerStats_shot_after_corner': 'shot_after_corner',
    'playerStats_shot_from_box': 'shot_from_box',
    'playerStats_shot_from_danger_zone': 'shot_from_danger_zone',
    'playerStats_shot_from_penalty_area': 'shot_from_penalty_area',
    'playerStats_shot_from_play': 'shot_from_play',
    'playerStats_shot_to_near_corner': 'shot_to_near_corner',
    'playerStats_shot_wide': 'shot_wide',
    'playerStats_counterattack_interception': 'counterattack_interception',
    'playerStats_opportunity_creation': 'opportunity_creation',
    'playerStats_defensive_one_on_one_success': 'defensive_one_on_one_success',
    'playerStats_dribble_against_with_take_on_success': 'dribble_against_with_take_on_success',
    'playerStats_dribble_with_take_on_success': 'dribble_with_take_on_success',
    'playerStats_dribbled_past_attempt_success': 'dribbled_past_attempt_success',
    'playerStats_one_on_one_success': 'one_on_one_success',
    'playerStats_goalkeeper_mistake_on_free_kick_faced': 'goalkeeper_mistake_on_free_kick_faced',
    'playerStats_head_pass': 'head_pass',
    'playerStats_goalkeeper_short_pass_under_pressure_success': 'goalkeeper_short_pass_under_pressure_success',
    'playerStats_head_pass_success': 'head_pass_success',
    'playerStats_free_kick_conceded_goal': 'free_kick_conceded_goal',
    'playerStats_free_kick_conceded_goal_success': 'free_kick_conceded_goal_success',
    'playerStats_card_suffered': 'card_suffered',
    'playerStats_duel_with_foul': 'duel_with_foul',
    'playerStats_legacy_won_offensive_duel': 'legacy_won_offensive_duel',
    'playerStats_offensive_duel_with_progress': 'offensive_duel_with_progress',
    'playerStats_card_suffered_success': 'card_suffered_success',
    'playerStats_duel_with_foul_success': 'duel_with_foul_success',
    'playerStats_legacy_won_offensive_duel_success': 'legacy_won_offensive_duel_success',
    'playerStats_offensive_duel_with_progress_success': 'offensive_duel_with_progress_success',
    'playerStats_offensive_shielding_success': 'offensive_shielding_success',
    'playerStats_counterattack_interception_success': 'counterattack_interception_success',
    'playerStats_key_pass_v2': 'key_pass_v2',
    'playerStats_key_pass_success': 'key_pass_success',
    'playerStats_key_pass_v2_success': 'key_pass_v2_success',
    'playerStats_goalkeeper_action_on_free_kick_faced_success': 'goalkeeper_action_on_free_kick_faced_success',
    'playerStats_goalkeeper_action_on_corner_faced_success': 'goalkeeper_action_on_corner_faced_success',
    'playerStats_dribble_with_space': 'dribble_with_space',
    'playerStats_legacy_dribble_success': 'legacy_dribble_success',
    'playerStats_dribble_with_space_success': 'dribble_with_space_success',
    'playerStats_legacy_dribble_success_success': 'legacy_dribble_success_success',
    'playerStats_protest_foul': 'protest_foul',
    'playerStats_goalkeeper_mistake_on_cross_faced': 'goalkeeper_mistake_on_cross_faced',
    'playerStats_fairplay': 'fairplay',
    'playerStats_opportunity_creation_success': 'opportunity_creation_success',
    'playerStats_super_save_success': 'super_save_success',
    'playerStats_pre_shot_assist': 'pre_shot_assist',
    'playerStats_pre_shot_assist_success': 'pre_shot_assist_success',
    'playerStats_pre_assist_success': 'pre_assist_success',
    'playerStats_out_of_play_foul': 'out_of_play_foul',
    'playerStats_shot_on_goal_assist': 'shot_on_goal_assist',
    'playerStats_shot_assist_success': 'shot_assist_success',
    'playerStats_shot_on_goal_assist_success': 'shot_on_goal_assist_success',
}


# Integer columns in DB (everything except xg/xa/gk_xg_save)
FLOAT_COLS = {'xg', 'xa', 'gk_xg_save', 'save_with_reflex_success'}


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
