#!/usr/bin/env python3
"""
Compute player performance scores and per-90 stats.
Populates the player_scores table.

Uses percentile-based scoring within position groups.

Usage:
  python3 analytics/compute_scores.py
  python3 analytics/compute_scores.py --min-minutes 450
"""
import argparse
import logging
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

from database.connection import get_conn

# Stats used for scoring, by position group.
# Each stat gets equal weight within the group.
# Prefix '-' means lower is better.
SCORE_STATS = {
    'FWD': ['goals_p90', 'xg_p90', 'assists_p90', 'xa_p90',
            'shots_p90', 'dribbles_succ_p90', 'touches_box_p90',
            'progressive_runs_p90', 'duels_won_pct'],
    'MID': ['goals_p90', 'assists_p90', 'xa_p90', 'key_passes_p90',
            'passes_acc_pct', 'progressive_runs_p90',
            'interceptions_p90', 'duels_won_pct', 'recoveries_p90'],
    'DEF': ['interceptions_p90', 'duels_won_pct', 'aerial_won_pct',
            'clearances_p90', 'passes_acc_pct', 'recoveries_p90',
            'progressive_runs_p90', '-losses_p90'],
    'GK':  ['gk_saves_p90', 'gk_xg_save_p90', '-gk_conceded_p90',
            'gk_passes_acc_pct', 'gk_sweeps_p90'],
}


def _percentile_rank(values):
    """Return percentile rank (0-100) for each value in the list."""
    n = len(values)
    if n == 0:
        return []
    indexed = sorted(enumerate(values), key=lambda x: (x[1] is None, x[1]))
    ranks = [0.0] * n
    for rank, (orig_idx, _) in enumerate(indexed):
        ranks[orig_idx] = (rank + 1) / n * 100
    return ranks


def compute(min_minutes: int = 450):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pms.player_id, p.position_group,
                    pms.competition_name,
                    COUNT(*) as matches,
                    SUM(pms.minutes_played) as minutes,
                    COALESCE(SUM(pms.goals), 0),
                    COALESCE(SUM(pms.assists), 0),
                    SUM(pms.xg), SUM(pms.xa),
                    COALESCE(SUM(pms.shots), 0),
                    COALESCE(SUM(pms.dribbles_successful), 0),
                    COALESCE(SUM(pms.touches_in_box), 0),
                    COALESCE(SUM(pms.progressive_runs), 0),
                    COALESCE(SUM(pms.key_passes), 0),
                    COALESCE(SUM(pms.interceptions), 0),
                    COALESCE(SUM(pms.clearances), 0),
                    COALESCE(SUM(pms.recoveries), 0),
                    COALESCE(SUM(pms.losses), 0),
                    COALESCE(SUM(pms.passes), 0),
                    COALESCE(SUM(pms.passes_accurate), 0),
                    COALESCE(SUM(pms.duels), 0),
                    COALESCE(SUM(pms.duels_won), 0),
                    COALESCE(SUM(pms.aerial_duels), 0),
                    COALESCE(SUM(pms.aerial_duels_won), 0),
                    COALESCE(SUM(pms.gk_saves), 0),
                    SUM(pms.gk_xg_save),
                    COALESCE(SUM(pms.gk_conceded), 0),
                    COALESCE(SUM(pms.gk_passes), 0),
                    COALESCE(SUM(pms.gk_passes_acc), 0),
                    COALESCE(SUM(pms.gk_sweeps), 0)
                FROM player_match_stats pms
                JOIN players p ON p.player_id = pms.player_id
                WHERE pms.minutes_played > 0
                GROUP BY pms.player_id, p.position_group, pms.competition_name
                HAVING SUM(pms.minutes_played) >= %s
            """, (min_minutes,))

            rows = cur.fetchall()
            logger.info(f'Processing {len(rows)} player-competition entries')

            entries = []
            for row in rows:
                (pid, pos_group, comp, matches, minutes,
                 goals, assists, xg, xa, shots, drib_succ,
                 touches_box, prog_runs, key_passes, interceptions,
                 clearances, recoveries, losses,
                 passes, passes_acc, duels, duels_won,
                 aerial, aerial_won,
                 gk_saves, gk_xg_save, gk_conceded,
                 gk_passes, gk_passes_acc, gk_sweeps) = row

                if not minutes:
                    continue

                xg = float(xg) if xg else 0
                xa = float(xa) if xa else 0
                gk_xg_save = float(gk_xg_save) if gk_xg_save else 0
                p90 = 90.0 / minutes
                pos = pos_group or 'MID'

                stats = {
                    'goals_p90': goals * p90,
                    'xg_p90': xg * p90,
                    'assists_p90': assists * p90,
                    'xa_p90': xa * p90,
                    'shots_p90': shots * p90,
                    'dribbles_succ_p90': drib_succ * p90,
                    'touches_box_p90': touches_box * p90,
                    'progressive_runs_p90': prog_runs * p90,
                    'key_passes_p90': key_passes * p90,
                    'interceptions_p90': interceptions * p90,
                    'clearances_p90': clearances * p90,
                    'recoveries_p90': recoveries * p90,
                    'losses_p90': losses * p90,
                    'passes_acc_pct': passes_acc / max(passes, 1) * 100,
                    'duels_won_pct': duels_won / max(duels, 1) * 100,
                    'aerial_won_pct': aerial_won / max(aerial, 1) * 100,
                    'gk_saves_p90': gk_saves * p90,
                    'gk_xg_save_p90': gk_xg_save * p90,
                    'gk_conceded_p90': gk_conceded * p90,
                    'gk_passes_acc_pct': gk_passes_acc / max(gk_passes, 1) * 100,
                    'gk_sweeps_p90': gk_sweeps * p90,
                }

                entries.append({
                    'player_id': pid, 'pos': pos, 'competition': comp,
                    'matches': matches, 'minutes': minutes,
                    'goals_p90': round(goals * p90, 3),
                    'assists_p90': round(assists * p90, 3),
                    'xg_p90': round(xg * p90, 3),
                    'xa_p90': round(xa * p90, 3),
                    'stats': stats,
                })

            # Group by position, compute percentile for each stat
            by_pos = defaultdict(list)
            for e in entries:
                by_pos[e['pos']].append(e)

            for pos, group in by_pos.items():
                stat_names = SCORE_STATS.get(pos, SCORE_STATS['MID'])
                n_stats = len(stat_names)

                # For each stat, compute percentile ranks
                for stat_name in stat_names:
                    invert = stat_name.startswith('-')
                    clean_name = stat_name.lstrip('-')
                    values = [e['stats'].get(clean_name, 0) for e in group]
                    if invert:
                        values = [-v for v in values]
                    pctls = _percentile_rank(values)
                    for e, pctl in zip(group, pctls):
                        e.setdefault('stat_pctls', []).append(pctl)

                # Average of percentile ranks = performance score
                for e in group:
                    pctls = e.get('stat_pctls', [])
                    e['score'] = round(sum(pctls) / max(len(pctls), 1), 2)

                # Overall percentile rank based on score
                scores = [e['score'] for e in group]
                overall_pctls = _percentile_rank(scores)
                for e, pctl in zip(group, overall_pctls):
                    e['percentile'] = round(pctl, 2)

            # Insert scores
            # Clear old scores first
            cur.execute("DELETE FROM player_scores")

            inserted = 0
            for e in entries:
                try:
                    cur.execute("""
                        INSERT INTO player_scores
                        (player_id, competition_id, season_id,
                         position_group, minutes_total, matches_total,
                         performance_score, percentile_rank,
                         goals_p90, assists_p90, xg_p90, xa_p90)
                        VALUES (%s,
                                (SELECT competition_id FROM competitions
                                 WHERE name = %s LIMIT 1),
                                NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, COALESCE(competition_id, 0))
                        DO UPDATE SET
                            performance_score=EXCLUDED.performance_score,
                            percentile_rank=EXCLUDED.percentile_rank,
                            minutes_total=EXCLUDED.minutes_total,
                            matches_total=EXCLUDED.matches_total,
                            goals_p90=EXCLUDED.goals_p90,
                            assists_p90=EXCLUDED.assists_p90,
                            xg_p90=EXCLUDED.xg_p90,
                            xa_p90=EXCLUDED.xa_p90,
                            computed_at=NOW()
                    """, (e['player_id'], e['competition'],
                          e['pos'], e['minutes'], e['matches'],
                          e['score'], e['percentile'],
                          e['goals_p90'], e['assists_p90'],
                          e['xg_p90'], e['xa_p90']))
                    inserted += 1
                except Exception as ex:
                    logger.error(f'Insert error pid={e["player_id"]}: {ex}')
                    conn.rollback()
                    continue

            conn.commit()
            logger.info(f'Inserted {inserted} scores across '
                        f'{len(by_pos)} position groups: '
                        + ', '.join(f'{k}={len(v)}' for k, v in by_pos.items()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--min-minutes', type=int, default=450)
    args = parser.parse_args()
    compute(args.min_minutes)
