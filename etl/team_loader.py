"""
Wyscout team stats ETL loader.
Parses JSON from get_team_stats_json() and loads into team_match_stats.
Idempotent — safe to re-run.
"""
import json
import logging
from pathlib import Path
from database.connection import get_conn

logger = logging.getLogger(__name__)

# JSON key → DB column mapping
STAT_MAP = {
    'goal': 'goals',
    'xgShot': 'xg',
    'shot': 'shots',
    'shotSuccess': 'shots_on_target',
    'pass': 'passes',
    'passSuccess': 'passes_accurate',
    'possession': 'possession_pct',
    'loss': 'losses',
    'lossLow': 'losses_low',
    'lossMedium': 'losses_mid',
    'lossHigh': 'losses_high',
    'recovery': 'recoveries',
    'recoveryLow': 'recoveries_low',
    'recoveryMedium': 'recoveries_mid',
    'recoveryHigh': 'recoveries_high',
    'duel': 'duels',
    'duelSuccess': 'duels_won',
    'interception': 'interceptions',
    'clearance': 'clearances',
    'ppda': 'ppda',
    'progressivePass': 'progressive_passes',
    'touchInBox': 'touches_in_box',
    'offensiveDuel': 'offensive_duels',
}

# DB columns in insert order (after identity/context cols)
STAT_COLUMNS = list(STAT_MAP.values())

_IDENTITY = ('team_id', 'wyscout_team_id', 'wyscout_team_name', 'is_home',
             'match_label', 'competition_name', 'match_date',
             'duration', 'formation', 'result')
_ALL_COLS = ', '.join(_IDENTITY) + ', ' + ', '.join(STAT_COLUMNS)
_PLACEHOLDERS = ', '.join(['%s'] * (len(_IDENTITY) + len(STAT_COLUMNS)))
_UPDATE_SET = ', '.join(f'{c}=EXCLUDED.{c}' for c in STAT_COLUMNS)

INSERT_SQL = f'''
    INSERT INTO team_match_stats ({_ALL_COLS})
    VALUES ({_PLACEHOLDERS})
    ON CONFLICT (wyscout_team_name, match_date, competition_name, is_home)
    DO UPDATE SET {_UPDATE_SET}
'''


class TeamLoader:

    def file_loaded(self, filename: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM loaded_files WHERE filename=%s',
                    (filename,))
                return bool(cur.fetchone())

    def mark_loaded(self, filename, entity_id, rows):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO loaded_files
                    (filename, file_type, entity_id, rows_loaded)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (filename) DO UPDATE
                    SET rows_loaded=%s, loaded_at=NOW()
                ''', (filename, 'team', entity_id, rows, rows))
            conn.commit()

    def upsert_team(self, wyscout_id: int, name: str) -> int:
        """Insert or get team. Returns team_id."""
        norm = name.lower().strip()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO teams (wyscout_id, name, normalised_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (wyscout_id) DO UPDATE SET name=EXCLUDED.name
                    RETURNING team_id
                ''', (wyscout_id, name, norm))
                result = cur.fetchone()
            conn.commit()
        return result[0] if result else None

    def load_team_json(self, json_path: Path,
                       wyscout_team_id: int,
                       team_name: str) -> int:
        """Load one team's match stats from saved JSON. Returns rows loaded."""
        filename = json_path.name
        if self.file_loaded(filename):
            logger.info(f'SKIP {filename} — already loaded')
            return 0

        try:
            data = json.loads(json_path.read_text())
        except Exception as e:
            logger.error(f'Failed to read {json_path}: {e}')
            return 0

        matches = data.get('matches', [])
        if not matches:
            return 0

        team_id = self.upsert_team(wyscout_team_id, team_name)
        loaded = 0

        with get_conn() as conn:
            with conn.cursor() as cur:
                for m in matches:
                    match_info = m.get('match', {})
                    team_stats = m.get('teamStats', {})

                    match_date = match_info.get('date')
                    competition = match_info.get('competition', '')
                    if not match_date or not competition:
                        continue

                    is_home = (match_info.get('teamAId') == wyscout_team_id)

                    # Primary formation (highest % usage)
                    schemes = team_stats.get('schemes', {})
                    formation = max(schemes, key=schemes.get) if schemes else None

                    # Result from score
                    score = match_info.get('score', '')
                    result = _parse_result(score, is_home)

                    # Build stat values
                    stat_values = tuple(
                        _safe_num(team_stats.get(json_key))
                        for json_key in STAT_MAP
                    )

                    values = (
                        team_id, wyscout_team_id, team_name, is_home,
                        match_info.get('name', ''),
                        competition, match_date,
                        match_info.get('duration'),
                        formation, result,
                    ) + stat_values

                    try:
                        cur.execute(INSERT_SQL, values)
                        loaded += 1
                    except Exception as e:
                        logger.error(f'Row error for {team_name} '
                                     f'{match_date}: {e}')
                        conn.rollback()
                        continue

            conn.commit()

        self.mark_loaded(filename, wyscout_team_id, loaded)
        logger.info(f'LOADED {team_name}: {loaded} match rows')
        return loaded


def _safe_num(val):
    if val is None:
        return None
    try:
        f = float(val)
        return int(f) if f == int(f) and abs(f) < 2**31 else round(f, 3)
    except (TypeError, ValueError):
        return None


def _parse_result(score: str, is_home: bool) -> str:
    """Parse '2:1' + is_home → 'W'/'D'/'L'."""
    if not score or ':' not in score:
        return None
    try:
        # Handle scores like "2:1 (P)" or "1:1 AET"
        clean = score.split('(')[0].split('AET')[0].strip()
        parts = clean.split(':')
        a, b = int(parts[0].strip()), int(parts[1].strip())
        if is_home:
            return 'W' if a > b else ('D' if a == b else 'L')
        else:
            return 'W' if b > a else ('D' if a == b else 'L')
    except (ValueError, IndexError):
        return None
