"""
Wyscout ETL loader.
Parses xlsx files and loads into football_platform DB.
Idempotent — safe to re-run.
Never creates duplicate rows.
"""
import logging
from pathlib import Path
from database.connection import get_conn
from scrapers.wyscout.parser import (
    parse_player_xlsx, _safe_int, _safe_float
)

logger = logging.getLogger(__name__)


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
                    minutes = rec.get('minutes_played')

                    if not match_date or not competition:
                        continue

                    try:
                        cur.execute('''
                            INSERT INTO player_match_stats (
                                player_id, wyscout_player_id,
                                wyscout_player_name,
                                match_label, competition_name,
                                match_date, position_played,
                                minutes_played,
                                goals, assists,
                                shots, shots_on_target,
                                xg, xa,
                                passes, passes_accurate,
                                long_passes, long_passes_accurate,
                                crosses, crosses_accurate,
                                dribbles, dribbles_successful,
                                duels, duels_won,
                                aerial_duels, aerial_duels_won,
                                defensive_duels, defensive_duels_won,
                                offensive_duels, offensive_duels_won,
                                interceptions, clearances,
                                losses_own_half, recoveries_opp_half,
                                fouls_committed, fouls_suffered,
                                yellow_cards, red_cards,
                                touches_in_box, progressive_runs,
                                shot_assists, total_actions,
                                successful_actions
                            ) VALUES (
                                %s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s
                            )
                            ON CONFLICT
                            (wyscout_player_name, match_date,
                             competition_name, minutes_played)
                            DO UPDATE SET
                                goals=EXCLUDED.goals,
                                assists=EXCLUDED.assists,
                                xg=EXCLUDED.xg,
                                xa=EXCLUDED.xa,
                                updated_at=NOW()
                        ''', (
                            player_id, wyscout_player_id, player_name,
                            rec.get('match_label'), competition,
                            match_date,
                            rec.get('position_played'), minutes,
                            _safe_int(rec.get('Goals', 0)),
                            _safe_int(rec.get('Assists', 0)),
                            _safe_int(rec.get('Shots / on target')),
                            _safe_int(rec.get('Shots on target')),
                            _safe_float(rec.get('xG')),
                            _safe_float(rec.get('xA')),
                            _safe_int(rec.get('Passes / accurate')),
                            _safe_int(rec.get('Passes accurate')),
                            _safe_int(rec.get('Long passes / accurate')),
                            _safe_int(rec.get('Long passes accurate')),
                            _safe_int(rec.get('Crosses / accurate')),
                            _safe_int(rec.get('Crosses accurate')),
                            _safe_int(rec.get('Dribbles / successful')),
                            _safe_int(rec.get('Dribbles successful')),
                            _safe_int(rec.get('Duels / won')),
                            _safe_int(rec.get('Duels won')),
                            _safe_int(rec.get('Aerial duels / won')),
                            _safe_int(rec.get('Aerial duels won')),
                            _safe_int(rec.get('Defensive duels / won')),
                            _safe_int(rec.get('Defensive duels won')),
                            _safe_int(rec.get('Offensive duels / won')),
                            _safe_int(rec.get('Offensive duels won')),
                            _safe_int(rec.get('Interceptions')),
                            _safe_int(rec.get('Clearances')),
                            _safe_int(rec.get('Losses / own half')),
                            _safe_int(rec.get('Recoveries / opp. half')),
                            _safe_int(rec.get('Fouls')),
                            _safe_int(rec.get('Fouls suffered')),
                            _safe_int(rec.get('Yellow card', 0)),
                            _safe_int(rec.get('Red card', 0)),
                            _safe_int(rec.get('Touches in penalty area')),
                            _safe_int(rec.get('Progressive runs')),
                            _safe_int(rec.get('Shot assists')),
                            _safe_int(rec.get('Total actions')),
                            _safe_int(rec.get('Successful actions')),
                        ))
                        loaded += 1
                    except Exception as e:
                        logger.error(
                            f'Row error for {player_name} '
                            f'{match_date}: {e}')
                        continue

            conn.commit()

        self.mark_loaded(filename, 'player',
                         wyscout_player_id, loaded)
        logger.info(f'LOADED {player_name}: {loaded} rows')
        return loaded
