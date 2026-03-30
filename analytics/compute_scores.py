#!/usr/bin/env python3
"""
compute_scores.py — Orchestrator: compute and store analytics scores.

Safe to re-run — uses INSERT ... ON CONFLICT DO UPDATE.

Usage:
  python3 analytics/compute_scores.py
  python3 analytics/compute_scores.py --season 2025-26
  python3 analytics/compute_scores.py --league "Premier League"
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from analytics.per90 import compute_per90, MIN_MINUTES
from analytics.player_score import compute_scores

DB_URL = 'postgresql://postgres:postgres@localhost:5434/football_data'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger('compute_scores')


def get_player_stats(season: str = None, league: str = None) -> pd.DataFrame:
    """
    Pull all player_season_stats rows (with minutes >= MIN_MINUTES)
    joined to player position_group, season_id, league_id.

    Returns a DataFrame with all counting and rate stats needed for
    per-90 normalisation and scoring.
    """
    conn = psycopg2.connect(DB_URL)

    where_clauses = [f'pss.minutes >= {MIN_MINUTES}', 'p.position_group IS NOT NULL']
    params = []
    if season:
        where_clauses.append('s.season_name = %s')
        params.append(season)
    if league:
        where_clauses.append('l.league_name = %s')
        params.append(league)

    where_sql = ' AND '.join(where_clauses)

    query = f"""
        SELECT
            pss.player_id,
            pss.season_id,
            pss.league_id,
            pss.team_id,
            p.position_group,
            pss.minutes,
            -- counting stats
            pss.goals, pss.assists, pss.shots, pss.shots_on_target,
            pss.key_passes, pss.big_chances_created,
            pss.aerial_duels_won, pss.aerial_duels_lost,
            pss.tackles, pss.tackles_won, pss.interceptions,
            pss.clearances, pss.recoveries, pss.dispossessed,
            pss.successful_dribbles, pss.touches,
            pss.fouls_committed, pss.fouls_won,
            pss.saves, pss.goals_conceded, pss.high_claims,
            pss.xg, pss.xa, pss.npxg, pss.xg_chain, pss.xg_buildup,
            -- rate stats
            pss.aerial_win_pct, pss.ground_duels_won_pct,
            pss.duels_won_pct, pss.tackles_won_pct,
            pss.accurate_passes_pct, pss.sofascore_rating,
            pss.clean_sheets
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        JOIN seasons s ON pss.season_id = s.season_id
        JOIN leagues l ON pss.league_id = l.league_id
        WHERE {where_sql}
        ORDER BY pss.season_id, pss.league_id, p.position_group
    """

    df = pd.read_sql(query, conn, params=params if params else None)
    conn.close()
    log.info(f"Loaded {len(df)} player-season rows (minutes >= {MIN_MINUTES})")
    return df


def upsert_scores(scores_df: pd.DataFrame) -> int:
    """
    Upsert computed scores into player_scores table.

    Uses INSERT ... ON CONFLICT DO UPDATE for idempotency.
    Returns the number of rows upserted.
    """
    if scores_df.empty:
        return 0

    rows = []
    for _, row in scores_df.iterrows():
        def val(col):
            """Return None for NaN/None, else float."""
            v = row.get(col)
            return None if pd.isna(v) else float(v)

        rows.append((
            int(row['player_id']),
            int(row['season_id']),
            int(row['league_id']),
            str(row['position_group']),
            int(row['minutes']),
            val('performance_score'),
            val('percentile_rank'),
            val('goals_p90'),
            val('assists_p90'),
            val('xg_p90'),
            val('xa_p90'),
            val('shots_p90'),
            val('key_passes_p90'),
            val('tackles_p90'),
            val('interceptions_p90'),
            val('aerial_duels_won_p90'),    # stored as aerial_won_p90 in table
            val('successful_dribbles_p90'),
            val('recoveries_p90'),
        ))

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    sql = """
        INSERT INTO player_scores (
            player_id, season_id, league_id, position_group, minutes,
            performance_score, percentile_rank,
            goals_p90, assists_p90, xg_p90, xa_p90, shots_p90,
            key_passes_p90, tackles_p90, interceptions_p90,
            aerial_won_p90, successful_dribbles_p90, recoveries_p90,
            score_computed_at
        ) VALUES %s
        ON CONFLICT (player_id, season_id, league_id) DO UPDATE SET
            position_group = EXCLUDED.position_group,
            minutes = EXCLUDED.minutes,
            performance_score = EXCLUDED.performance_score,
            percentile_rank = EXCLUDED.percentile_rank,
            goals_p90 = EXCLUDED.goals_p90,
            assists_p90 = EXCLUDED.assists_p90,
            xg_p90 = EXCLUDED.xg_p90,
            xa_p90 = EXCLUDED.xa_p90,
            shots_p90 = EXCLUDED.shots_p90,
            key_passes_p90 = EXCLUDED.key_passes_p90,
            tackles_p90 = EXCLUDED.tackles_p90,
            interceptions_p90 = EXCLUDED.interceptions_p90,
            aerial_won_p90 = EXCLUDED.aerial_won_p90,
            successful_dribbles_p90 = EXCLUDED.successful_dribbles_p90,
            recoveries_p90 = EXCLUDED.recoveries_p90,
            score_computed_at = NOW()
    """

    execute_values(
        cur,
        sql,
        rows,
        template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
        page_size=500,
    )
    conn.commit()
    conn.close()
    return len(rows)


def print_summary(scores_df: pd.DataFrame):
    """Print a breakdown table of scored players by league, season, and position."""
    if scores_df.empty:
        print("No players scored.")
        return

    print("\n=== SCORING SUMMARY ===")
    summary = (
        scores_df[scores_df['performance_score'].notna()]
        .groupby(['league_id', 'season_id', 'position_group'])
        .size()
        .reset_index(name='count')
    )

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT league_id, league_name FROM leagues")
    leagues = {r[0]: r[1] for r in cur.fetchall()}
    cur.execute("SELECT season_id, season_name FROM seasons")
    seasons = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()

    print(f"{'League':<22} {'Season':>8} {'Pos':>5} {'Players':>8}")
    for _, row in summary.sort_values(['league_id', 'season_id', 'position_group']).iterrows():
        print(
            f"{leagues.get(row['league_id'], '?'):<22} "
            f"{seasons.get(row['season_id'], '?'):>8} "
            f"{row['position_group']:>5} "
            f"{row['count']:>8}"
        )

    total = scores_df['performance_score'].notna().sum()
    null_scores = scores_df['performance_score'].isna().sum()
    print(f"\nTotal scored: {total} / {len(scores_df)} players with >= {MIN_MINUTES} mins")
    if null_scores:
        print(f"NULL scores (insufficient metrics): {null_scores}")


def main():
    """Entry point: parse args, compute per-90, score, upsert, summarise."""
    parser = argparse.ArgumentParser(description='Compute analytics performance scores')
    parser.add_argument('--season', help='Filter to season name (e.g. 2025-26)')
    parser.add_argument('--league', help='Filter to league name (e.g. Premier League)')
    args = parser.parse_args()

    log.info("Loading player stats from DB...")
    raw_df = get_player_stats(season=args.season, league=args.league)

    if raw_df.empty:
        log.warning("No player stats found matching filters.")
        return

    log.info("Computing per-90 normalisation...")
    per90_df = compute_per90(raw_df)
    log.info(f"Per-90 computed for {len(per90_df)} players")

    log.info("Computing performance scores...")
    scores_df = compute_scores(per90_df)
    log.info(f"Scores computed for {len(scores_df)} players")

    log.info("Upserting to player_scores table...")
    n = upsert_scores(scores_df)
    log.info(f"Upserted {n} rows")

    print_summary(scores_df)


if __name__ == '__main__':
    main()
