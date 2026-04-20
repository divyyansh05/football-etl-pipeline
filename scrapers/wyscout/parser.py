"""
Parse Wyscout player stats xlsx files.
439 columns, one row per match.
Handles compound values like "13/2" (total/accurate).
"""
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def parse_player_xlsx(filepath: Path) -> list:
    """
    Parse Wyscout player stats xlsx.
    Returns list of clean dicts, one per match row.
    """
    try:
        df = pd.read_excel(filepath, header=0)
    except Exception as e:
        logger.error(f'Failed to read {filepath}: {e}')
        return []

    # Drop empty rows
    df = df.dropna(how='all')

    # The first column is 'Match' — drop rows where it's NaN
    match_col = df.columns[0]
    df = df.dropna(subset=[match_col])

    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                continue
            col_clean = str(col).strip()
            record[col_clean] = val

        # Parse date
        for date_key in ['Date', 'date']:
            if date_key in record:
                try:
                    record['match_date'] = pd.to_datetime(
                        record[date_key]).date()
                except Exception:
                    pass
                break

        # Clean up key names for common fields
        record['match_label'] = record.get('Match', '')
        record['competition_name'] = record.get('Competition', '')
        record['position_played'] = record.get('Position', '')
        record['minutes_played'] = _safe_int(
            record.get('Minutes played'))

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
        return float(val)
    except (TypeError, ValueError):
        return None


def parse_compound(val):
    """
    Parse compound Wyscout values like "13/2" → (13, 2).
    Returns (total, accurate) or (None, None) if unparseable.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    s = str(val).strip()
    if '/' in s:
        parts = s.split('/')
        return _safe_int(parts[0]), _safe_int(parts[1])
    return _safe_int(s), None
