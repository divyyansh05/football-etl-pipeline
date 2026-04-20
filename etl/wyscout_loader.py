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
    'match_label', 'competition_name', 'match_date',
    'position_played', 'minutes_played',
    'total_actions', 'successful_actions',
    'goals', 'assists', 'shots', 'shots_on_target',
    'xg', 'xa',
    'shot_assists', 'second_assists', 'key_passes',
    'smart_passes', 'smart_passes_acc',
    'through_passes', 'through_passes_acc',
    'passes', 'passes_accurate',
    'long_passes', 'long_passes_accurate',
    'crosses', 'crosses_accurate',
    'forward_passes', 'forward_passes_acc',
    'back_passes', 'back_passes_acc',
    'lateral_passes', 'lateral_passes_acc',
    'passes_final_third', 'passes_final_third_acc',
    'passes_penalty_area', 'passes_penalty_area_acc',
    'dribbles', 'dribbles_successful',
    'progressive_runs', 'touches_in_box',
    'offensive_duels', 'offensive_duels_won',
    'defensive_duels', 'defensive_duels_won',
    'aerial_duels', 'aerial_duels_won',
    'duels', 'duels_won',
    'loose_ball_duels', 'loose_ball_duels_won',
    'interceptions', 'sliding_tackles', 'sliding_tackles_succ',
    'clearances',
    'losses_own_half', 'recoveries_opp_half',
    'recoveries', 'losses',
    'fouls_committed', 'fouls_suffered', 'offsides',
    'yellow_cards', 'red_cards',
    'gk_saves', 'gk_saves_reflex', 'gk_super_saves',
    'gk_conceded', 'gk_xg_save',
    'gk_exits', 'gk_claims', 'gk_punches', 'gk_sweeps',
    'gk_passes', 'gk_passes_acc',
    'gk_goal_kicks', 'gk_short_kicks', 'gk_long_kicks',
]

# Build INSERT SQL once
_IDENTITY_COLS = 'player_id, wyscout_player_id, wyscout_player_name'
_ALL_COLS = f'{_IDENTITY_COLS}, ' + ', '.join(STAT_COLUMNS)
_PLACEHOLDERS = ', '.join(['%s'] * (3 + len(STAT_COLUMNS)))
_UPDATE_SET = ', '.join(
    f'{c}=EXCLUDED.{c}' for c in STAT_COLUMNS
    if c not in ('match_label', 'competition_name', 'match_date',
                 'position_played', 'minutes_played')
)

INSERT_SQL = f'''
    INSERT INTO player_match_stats ({_ALL_COLS})
    VALUES ({_PLACEHOLDERS})
    ON CONFLICT (wyscout_player_name, match_date,
                 competition_name, minutes_played)
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
                    SET name=EXCLUDED.name
                    RETURNING player_id
                ''', (wyscout_id, name, norm,
                      kwargs.get('position_group'),
                      kwargs.get('primary_position')))
                result = cur.fetchone()
            conn.commit()
        return result[0] if result else None

    def load_player_xlsx(self, xlsx_path: Path,
                         wyscout_player_id: int,
                         player_name: str) -> int:
        """
        Load one player xlsx into player_match_stats.
        Returns number of rows inserted.
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
            wyscout_player_id, player_name)

        loaded = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                for rec in records:
                    match_date = rec.get('match_date')
                    competition = rec.get('competition_name', '')

                    if not match_date or not competition:
                        continue

                    # Build values tuple: identity + all stat columns
                    values = (
                        player_id, wyscout_player_id, player_name,
                    ) + tuple(rec.get(col) for col in STAT_COLUMNS)

                    try:
                        cur.execute(INSERT_SQL, values)
                        loaded += 1
                    except Exception as e:
                        logger.error(
                            f'Row error for {player_name} '
                            f'{match_date}: {e}')
                        conn.rollback()
                        continue

            conn.commit()

        self.mark_loaded(filename, 'player',
                         wyscout_player_id, loaded)
        logger.info(f'LOADED {player_name}: {loaded} rows')
        return loaded
