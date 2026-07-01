#!/usr/bin/env python3
"""
Compute and store stat_vector for every player in player_scores.
Enables pgvector cosine-similarity search ("find similar players").

Vectors are 20-dimensional, one per normalised per-90 / percentage metric.
Each dimension is min-max normalised (0-1) within the player's position group
so that all metrics sit on the same scale regardless of unit.

Usage:
    python3 analytics/compute_vectors.py
    python3 analytics/compute_vectors.py --min-minutes 450

Dependencies:
    pgvector extension + stat_vector column must exist first:
        docker exec -i football_db psql -U postgres -d football_platform \\
            < database/migrations/003_add_stat_vector.sql
"""
import argparse
import logging
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

from database.connection import get_conn

# ── 20 Stat Dimensions ─────────────────────────────────────────────────────────
# Order is fixed. Do NOT change order (invalidates existing vectors).
# Prefix '-' = lower is better (inverted before normalisation).
VECTOR_STATS = [
    'goals_p90',
    'xg_p90',
    'assists_p90',
    'xa_p90',
    'shots_p90',
    'key_passes_p90',
    'dribbles_succ_p90',
    'progressive_runs_p90',
    'touches_box_p90',
    'interceptions_p90',
    'clearances_p90',
    'recoveries_p90',
    'aerial_won_pct',
    'duels_won_pct',
    'passes_acc_pct',
    '-losses_p90',          # inverted: fewer losses = better
    'gk_saves_p90',
    '-gk_conceded_p90',     # inverted
    'gk_xg_save_p90',
    'gk_passes_acc_pct',
]

VECTOR_DIM = len(VECTOR_STATS)  # = 20


def _minmax_normalise(values: list[float]) -> list[float]:
    """Min-max normalise a list of floats to [0, 1]."""
    arr = np.array(values, dtype=float)
    mn, mx = arr.min(), arr.max()
    if mx == mn:
        return [0.5] * len(values)
    return ((arr - mn) / (mx - mn)).tolist()


def _fetch_raw_stats(conn, min_minutes: int) -> list[dict]:
    """Aggregate raw sums from player_match_stats. Division done in Python to avoid SQL div/0."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                pms.player_id,
                p.position_group,
                pms.competition_name,
                SUM(pms.minutes_played)                          AS minutes,
                COALESCE(SUM(pms.goals), 0)                      AS goals,
                COALESCE(SUM(pms.xg), 0)                         AS xg,
                COALESCE(SUM(pms.assists), 0)                    AS assists,
                COALESCE(SUM(pms.xa), 0)                         AS xa,
                COALESCE(SUM(pms.shots), 0)                      AS shots,
                COALESCE(SUM(pms.key_passes), 0)                 AS key_passes,
                COALESCE(SUM(pms.dribbles_successful), 0)        AS dribbles_succ,
                COALESCE(SUM(pms.progressive_runs), 0)           AS prog_runs,
                COALESCE(SUM(pms.touches_in_box), 0)             AS touches_box,
                COALESCE(SUM(pms.interceptions), 0)              AS interceptions,
                COALESCE(SUM(pms.clearances), 0)                 AS clearances,
                COALESCE(SUM(pms.recoveries), 0)                 AS recoveries,
                COALESCE(SUM(pms.aerial_duels_won), 0)           AS aerial_won,
                COALESCE(SUM(pms.aerial_duels), 0)               AS aerial_total,
                COALESCE(SUM(pms.duels_won), 0)                  AS duels_won,
                COALESCE(SUM(pms.duels), 0)                      AS duels_total,
                COALESCE(SUM(pms.passes_accurate), 0)            AS passes_acc,
                COALESCE(SUM(pms.passes), 0)                     AS passes_total,
                COALESCE(SUM(pms.losses), 0)                     AS losses,
                COALESCE(SUM(pms.gk_saves), 0)                   AS gk_saves,
                COALESCE(SUM(pms.gk_conceded), 0)                AS gk_conceded,
                COALESCE(SUM(pms.gk_xg_save), 0)                 AS gk_xg_save,
                COALESCE(SUM(pms.gk_passes_acc), 0)              AS gk_passes_acc,
                COALESCE(SUM(pms.gk_passes), 0)                  AS gk_passes_total
            FROM player_match_stats pms
            JOIN players p ON p.player_id = pms.player_id
            WHERE pms.minutes_played > 0
            GROUP BY pms.player_id, p.position_group, pms.competition_name
            HAVING SUM(pms.minutes_played) >= %s
        """, (min_minutes,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    records = []
    for row in rows:
        rec = dict(zip(cols, row))
        # Convert Decimal/None → float safely
        for k, v in rec.items():
            if v is None:
                rec[k] = 0.0
            elif not isinstance(v, (int, float, str)):
                rec[k] = float(v)

        # Compute per-90 and percentages in Python (safe division)
        mins = max(float(rec['minutes']), 1.0)
        p90 = 90.0 / mins

        rec['goals_p90']           = float(rec['goals'])        * p90
        rec['xg_p90']              = float(rec['xg'])           * p90
        rec['assists_p90']         = float(rec['assists'])       * p90
        rec['xa_p90']              = float(rec['xa'])            * p90
        rec['shots_p90']           = float(rec['shots'])         * p90
        rec['key_passes_p90']      = float(rec['key_passes'])    * p90
        rec['dribbles_succ_p90']   = float(rec['dribbles_succ']) * p90
        rec['progressive_runs_p90']= float(rec['prog_runs'])     * p90
        rec['touches_box_p90']     = float(rec['touches_box'])   * p90
        rec['interceptions_p90']   = float(rec['interceptions']) * p90
        rec['clearances_p90']      = float(rec['clearances'])    * p90
        rec['recoveries_p90']      = float(rec['recoveries'])    * p90
        rec['losses_p90']          = float(rec['losses'])        * p90
        rec['gk_saves_p90']        = float(rec['gk_saves'])      * p90
        rec['gk_conceded_p90']     = float(rec['gk_conceded'])   * p90
        rec['gk_xg_save_p90']      = float(rec['gk_xg_save'])    * p90
        rec['aerial_won_pct']      = float(rec['aerial_won'])    / max(float(rec['aerial_total']),  1) * 100
        rec['duels_won_pct']       = float(rec['duels_won'])     / max(float(rec['duels_total']),   1) * 100
        rec['passes_acc_pct']      = float(rec['passes_acc'])    / max(float(rec['passes_total']),  1) * 100
        rec['gk_passes_acc_pct']   = float(rec['gk_passes_acc']) / max(float(rec['gk_passes_total']), 1) * 100

        records.append(rec)



    logger.info(f'Fetched {len(records)} player-competition records')
    return records


def _build_raw_vector(rec: dict) -> list[float]:
    """Extract 20-dim raw stat values from a record, applying inversions."""
    vec = []
    for stat in VECTOR_STATS:
        invert = stat.startswith('-')
        key = stat.lstrip('-')
        val = float(rec.get(key, 0.0) or 0.0)
        if invert:
            val = -val  # inversion handled during normalisation
        vec.append(val)
    return vec


def compute_vectors(min_minutes: int = 450):
    with get_conn() as conn:
        records = _fetch_raw_stats(conn, min_minutes)

        # Group by position for within-position normalisation
        by_pos: dict[str, list[dict]] = defaultdict(list)
        for rec in records:
            by_pos[rec['position_group'] or 'MID'].append(rec)

        # Build raw vectors, then normalise dimension-by-dimension within position
        for pos, group in by_pos.items():
            raw_vecs = [_build_raw_vector(r) for r in group]

            # Normalise each of the 20 dimensions independently
            norm_vecs = []
            for dim_idx in range(VECTOR_DIM):
                dim_values = [v[dim_idx] for v in raw_vecs]
                dim_norm = _minmax_normalise(dim_values)
                for i, n in enumerate(dim_norm):
                    if len(norm_vecs) <= i:
                        norm_vecs.append([0.0] * VECTOR_DIM)
                    norm_vecs[i][dim_idx] = n

            for rec, vec in zip(group, norm_vecs):
                rec['_vector'] = vec

        logger.info(f'Vectors computed. Positions: '
                    + ', '.join(f'{k}={len(v)}' for k, v in by_pos.items()))

        # Write vectors to DB
        updated = 0
        skipped = 0
        with conn.cursor() as cur:
            for rec in records:
                vec = rec.get('_vector')
                if not vec:
                    skipped += 1
                    continue

                # pgvector expects '[0.1, 0.2, ...]' string format
                vec_str = '[' + ','.join(f'{v:.6f}' for v in vec) + ']'

                # Match on player_id + competition_name via competitions join
                cur.execute("""
                    UPDATE player_scores ps
                    SET stat_vector = %s::vector
                    FROM competitions c
                    WHERE ps.player_id = %s
                      AND c.competition_id = ps.competition_id
                      AND c.name = %s
                """, (vec_str, rec['player_id'], rec['competition_name']))

                if cur.rowcount == 0:
                    # Row exists with NULL competition_id (global score) — update by player_id only
                    cur.execute("""
                        UPDATE player_scores
                        SET stat_vector = %s::vector
                        WHERE player_id = %s AND competition_id IS NULL
                    """, (vec_str, rec['player_id']))

                updated += cur.rowcount

        conn.commit()
        logger.info(f'stat_vector updated for {updated} rows ({skipped} skipped)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compute pgvector stat_vector for all players in player_scores')
    parser.add_argument('--min-minutes', type=int, default=450,
                        help='Minimum minutes threshold (default: 450)')
    args = parser.parse_args()
    compute_vectors(args.min_minutes)
