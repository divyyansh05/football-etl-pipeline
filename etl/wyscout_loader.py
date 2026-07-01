"""
Wyscout ETL loader.
Parses xlsx files and loads into football_platform DB.
Idempotent — safe to re-run.
Never creates duplicate rows.
"""
import logging
from pathlib import Path
from database.connection import get_conn
from scrapers.wyscout.parser import parse_player_xlsx

logger = logging.getLogger(__name__)

# All stat columns in player_match_stats (excluding identity/meta cols).
# Order matters — must match INSERT VALUES placeholders.
STAT_COLUMNS = [
    'total_actions',
    'successful_actions',
    'goals',
    'assists',
    'shots',
    'shots_on_target',
    'xg',
    'xa',
    'shot_assists',
    'second_assists',
    'key_passes',
    'smart_passes',
    'smart_passes_acc',
    'through_passes',
    'through_passes_acc',
    'passes',
    'passes_accurate',
    'long_passes',
    'long_passes_accurate',
    'crosses',
    'crosses_accurate',
    'forward_passes',
    'forward_passes_acc',
    'back_passes',
    'back_passes_acc',
    'lateral_passes',
    'lateral_passes_acc',
    'passes_final_third',
    'passes_final_third_acc',
    'passes_penalty_area',
    'passes_penalty_area_acc',
    'dribbles',
    'dribbles_successful',
    'progressive_runs',
    'touches_in_box',
    'offensive_duels',
    'offensive_duels_won',
    'defensive_duels',
    'defensive_duels_won',
    'aerial_duels',
    'aerial_duels_won',
    'duels',
    'duels_won',
    'loose_ball_duels',
    'loose_ball_duels_won',
    'interceptions',
    'sliding_tackles',
    'sliding_tackles_succ',
    'clearances',
    'losses_own_half',
    'recoveries_opp_half',
    'recoveries',
    'losses',
    'fouls_committed',
    'fouls_suffered',
    'offsides',
    'yellow_cards',
    'gk_saves',
    'gk_saves_reflex',
    'gk_super_saves',
    'gk_conceded',
    'gk_xg_save',
    'gk_exits',
    'gk_claims',
    'gk_punches',
    'gk_sweeps',
    'gk_passes',
    'gk_passes_acc',
    'gk_goal_kicks',
    'gk_short_kicks',
    'gk_long_kicks',
    'goalkeeper_action',
    'action',
    'received_pass',
    'long_pass_or_cross',
    'short_medium_pass',
    'buildup_pass',
    'goalkeeper_long_pass',
    'progressive_pass',
    'goalkeeper_short_pass',
    'pass_followed_by_teammate_loss',
    'vertical_pass',
    'pick_up',
    'long_pass_into_duel',
    'pass_into_duel',
    'loss_after_pass',
    'loss_v2',
    'shot_against',
    'touch',
    'hand_pass',
    'launch',
    'meaningless',
    'pass_to_zone_fourteen',
    'quick_recovery',
    'any_goal',
    'easy_conceded_goal',
    'far_conceded_goal',
    'long_conceded_goal',
    'long_shot_against',
    'near_shot_against',
    'run',
    'run_to_final_third',
    'any_free_kick',
    'back_pass_to_gk',
    'free_kick',
    'free_kick_cross',
    'goalkeeper_action_on_cross_faced',
    'goalkeeper_distribution',
    'goalkeeper_goal_kick',
    'indirect_free_kick',
    'pressed_sequence_loss',
    'pressed_sequence_recovery',
    'recovery_counterpressing',
    'through_pass_interception',
    'goalkeeper_action_success',
    'action_success',
    'short_medium_pass_success',
    'received_pass_success',
    'long_pass_or_cross_success',
    'buildup_pass_success',
    'goalkeeper_long_pass_success',
    'goalkeeper_short_pass_success',
    'pass_followed_by_teammate_loss_success',
    'progressive_pass_success',
    'recovery_success',
    'vertical_pass_success',
    'pick_up_success',
    'long_pass_into_duel_success',
    'pass_into_duel_success',
    'shot_against_success',
    'hand_pass_success',
    'any_goal_success',
    'conceded_goal_success',
    'easy_conceded_goal_success',
    'far_conceded_goal_success',
    'goalkeeper_sweep_success',
    'long_conceded_goal_success',
    'long_shot_against_success',
    'near_shot_against_success',
    'pass_to_zone_fourteen_success',
    'quick_recovery_success',
    'save_success',
    'any_free_kick_success',
    'back_pass_to_gk_success',
    'free_kick_success',
    'free_kick_cross_success',
    'goal_kick_success',
    'goal_kick_long_success',
    'goalkeeper_distribution_success',
    'goalkeeper_goal_kick_success',
    'indirect_free_kick_success',
    'interception_success',
    'launch_success',
    'run_success',
    'run_to_final_third_success',
    'save_with_reflex_success',
    'yellow_card_minute',
    'near_conceded_goal',
    'received_long_pass',
    'ball_delivery_to_penalty_area',
    'last',
    'pass_to_penalty_area_v2',
    'aerial_duel_in_own_penalty_area',
    'ball_delivery_to_danger_zone',
    'heading',
    'legacy_won_duel',
    'real_won_duel',
    'loss_success',
    'loss_v2_success',
    'near_conceded_goal_success',
    'received_long_pass_success',
    'loss_after_pass_success',
    'pressed_sequence_loss_success',
    'recovery_counterpressing_success',
    'aerial_duel_in_own_penalty_area_success',
    'goalkeeper_action_on_cross_faced_success',
    'heading_success',
    'legacy_won_duel_success',
    'pressed_sequence_recovery_success',
    'real_won_duel_success',
    'goalkeeper_shot_buildup',
    'goalkeeper_shot_buildup_success',
    'goalkeeper_action_on_corner_faced',
    'goalkeeper_accurate_punch',
    'dribble_against',
    'goalkeeper_action_on_free_kick_faced',
    'goalkeeper_long_pass_under_pressure',
    'goalkeeper_pass_under_pressure',
    'ground_duel',
    'stopped_progress_defensive_duel',
    'under_pressure',
    'dribble_against_success',
    'goal_kick_short_success',
    'ground_duel_success',
    'last_success',
    'stopped_progress_defensive_duel_success',
    'through_pass_interception_success',
    'action_in_counterattack',
    'head_conceded_goal',
    'pass_behind_the_back',
    'action_in_counterattack_success',
    'head_conceded_goal_success',
    'pass_behind_the_back_success',
    'dangerous_own_half_loss',
    'deep_completed_pass',
    'highlight',
    'loss_after_duel',
    'minus',
    'opportunity',
    'penalty_conceded_goal',
    'received_cross',
    'received_dangerous_pass',
    'received_pass_in_final_third',
    'received_progressive_pass',
    'touch_in_final_third',
    'touch_in_penalty_area',
    'very_deep_completion',
    'own_half_loss_success',
    'ball_delivery_to_danger_zone_success',
    'ball_delivery_to_penalty_area_success',
    'clearance_success',
    'dangerous_own_half_loss_success',
    'deep_completed_pass_success',
    'loss_after_duel_success',
    'pass_to_penalty_area_v2_success',
    'penalty_conceded_goal_success',
    'very_deep_completion_success',
    'shot_buildup_pass',
    'shot_buildup_pass_success',
    'goalkeeper_medium_pass_under_pressure',
    'goalkeeper_short_pass_under_pressure',
    'goalkeeper_medium_pass_under_pressure_success',
    'goalkeeper_pass_under_pressure_success',
    'under_pressure_success',
    'cross_from_right',
    'cross_high',
    'deep_completed_cross',
    'pass_within_final_third',
    'received_pass_in_final_third_on_side',
    'goalkeeper_long_pass_under_pressure_success',
    'defensive_one_on_one',
    'dribble_against_with_take_on',
    'dribble_with_take_on',
    'dribbled_past_attempt',
    'lost_defensive_duel',
    'one_on_one',
    'infraction',
    'penalty_foul',
    'regular_foul',
    'plus',
    'plus_success',
    'misconduct',
    'time_lost_foul',
    'defensive_duel_regain',
    'legacy_won_defensive_duel',
    'defensive_duel_regain_success',
    'legacy_won_defensive_duel_success',
    'goalkeeper_mistake_on_corner_faced',
    'cross_block',
    'ball_loss',
    'ball_loss_in_area',
    'dangerous_lost_defensive_duel',
    'offensive_shielding',
    'foul_suffered_success',
    'missed_ball',
    'penalty_save',
    'penalty_save_success',
    'minus_success',
    'head_shot',
    'shot_after_corner',
    'shot_from_box',
    'shot_from_danger_zone',
    'shot_from_penalty_area',
    'shot_from_play',
    'shot_to_near_corner',
    'shot_wide',
    'counterattack_interception',
    'opportunity_creation',
    'defensive_one_on_one_success',
    'dribble_against_with_take_on_success',
    'dribble_with_take_on_success',
    'dribbled_past_attempt_success',
    'one_on_one_success',
    'goalkeeper_mistake_on_free_kick_faced',
    'head_pass',
    'goalkeeper_short_pass_under_pressure_success',
    'head_pass_success',
    'free_kick_conceded_goal',
    'free_kick_conceded_goal_success',
    'card_suffered',
    'duel_with_foul',
    'legacy_won_offensive_duel',
    'offensive_duel_with_progress',
    'card_suffered_success',
    'duel_with_foul_success',
    'legacy_won_offensive_duel_success',
    'offensive_duel_with_progress_success',
    'offensive_shielding_success',
    'counterattack_interception_success',
    'key_pass_v2',
    'key_pass_success',
    'key_pass_v2_success',
    'goalkeeper_action_on_free_kick_faced_success',
    'goalkeeper_action_on_corner_faced_success',
    'dribble_with_space',
    'legacy_dribble_success',
    'dribble_with_space_success',
    'legacy_dribble_success_success',
    'protest_foul',
    'goalkeeper_mistake_on_cross_faced',
    'fairplay',
    'opportunity_creation_success',
    'super_save_success',
    'pre_shot_assist',
    'pre_shot_assist_success',
    'pre_assist_success',
    'out_of_play_foul',
    'shot_on_goal_assist',
    'shot_assist_success',
    'shot_on_goal_assist_success',
]

# Build INSERT SQL once
_IDENTITY_COLS = 'player_id, wyscout_player_id, wyscout_player_name, match_label, competition_name, match_date, position_played, minutes_played'
_ALL_COLS = f'{_IDENTITY_COLS}, ' + ', '.join(STAT_COLUMNS)
_PLACEHOLDERS = ', '.join(['%s'] * (8 + len(STAT_COLUMNS)))
_UPDATE_SET = ', '.join(
    f'{c}=EXCLUDED.{c}' for c in STAT_COLUMNS
    if c not in ('match_label', 'competition_name', 'match_date',
                 'position_played', 'minutes_played')
)

INSERT_SQL = f'''
    INSERT INTO player_match_stats ({_ALL_COLS})
    VALUES ({_PLACEHOLDERS})
    ON CONFLICT (player_id, match_date, competition_name)
    DO UPDATE SET {_UPDATE_SET}, updated_at=NOW()
'''


class WyscoutLoader:

    def file_loaded(self, filename: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM loaded_files WHERE filename=%s',
                    (filename,))
                return bool(cur.fetchone())

    def mark_loaded(self, filename, file_type, entity_id, rows):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO loaded_files
                    (filename, file_type, entity_id, rows_loaded)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (filename) DO UPDATE
                    SET rows_loaded=%s, loaded_at=NOW()
                ''', (filename, file_type, entity_id, rows, rows))
            conn.commit()

    def upsert_player(self, wyscout_id: int,
                      name: str, **kwargs) -> int:
        """Insert or get player. Returns player_id."""
        norm = name.lower().replace('.', ' ').strip()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO players
                    (wyscout_id, name, normalised_name,
                     position_group, primary_position)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (wyscout_id) DO UPDATE
                    SET name=EXCLUDED.name,
                        position_group=COALESCE(EXCLUDED.position_group, players.position_group),
                        primary_position=COALESCE(EXCLUDED.primary_position, players.primary_position)
                    RETURNING player_id
                ''', (wyscout_id, name, norm,
                      kwargs.get('position_group'),
                      kwargs.get('primary_position')))
                result = cur.fetchone()
            conn.commit()
        return result[0] if result else None

    def load_player_xlsx(self, xlsx_path: Path,
                         wyscout_player_id: int,
                         player_name: str,
                         **player_kwargs) -> int:
        """
        Load one player xlsx into player_match_stats.
        Returns number of rows inserted.
        player_kwargs forwarded to upsert_player (e.g. primary_position).
        """
        filename = xlsx_path.name
        if self.file_loaded(filename):
            logger.info(f'SKIP {filename} — already loaded')
            return 0

        records = parse_player_xlsx(xlsx_path)
        if not records:
            return 0

        # Upsert player identity
        player_id = self.upsert_player(
            wyscout_player_id, player_name, **player_kwargs)

        loaded = 0
        skipped = 0
        failed = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                for rec in records:
                    match_date = rec.get('match_date')
                    competition = rec.get('competition_name', '')

                    if not match_date or not competition:
                        skipped += 1
                        continue

                    # Build values tuple: identity + context + all stat columns
                    values = (
                        player_id, wyscout_player_id, player_name,
                        rec.get('match_label'), competition, match_date,
                        rec.get('position_played'), rec.get('minutes_played'),
                    ) + tuple(rec.get(col) for col in STAT_COLUMNS)

                    try:
                        cur.execute('SAVEPOINT row_sp')
                        cur.execute(INSERT_SQL, values)
                        cur.execute('RELEASE SAVEPOINT row_sp')
                        loaded += 1
                    except Exception as e:
                        cur.execute('ROLLBACK TO SAVEPOINT row_sp')
                        failed += 1
                        logger.error(
                            f'Row error for {player_name} '
                            f'{match_date}: {e}')
                        continue

            conn.commit()

        self.mark_loaded(filename, 'player',
                         wyscout_player_id, loaded)
        logger.info(f'LOADED {player_name}: {loaded} rows '
                    f'(skipped={skipped}, failed={failed})')
        return loaded
