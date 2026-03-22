"""
historical_backfill.py

Creates minimal player_season_stats records for players who exist in a team
(via current_team_id) but have no stat row for a given season.

Usage:
    python etl/historical_backfill.py --season 2023-24
    python etl/historical_backfill.py --season 2023-24 --league premier-league
"""

import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DB_PARAMS = dict(
    host="localhost",
    port=5434,
    dbname="football_data",
    user="postgres",
    password="postgres",
)

# Maps CLI slug → league_name in DB
LEAGUE_SLUG_MAP = {
    "premier-league": "Premier League",
    "la-liga": "La Liga",
    "serie-a": "Serie A",
    "bundesliga": "Bundesliga",
    "ligue-1": "Ligue 1",
    "eredivisie": "Eredivisie",
    "brasileirao": "Brasileiro Serie A",
    "argentina-primera": "Argentina Primera Division",
}


def get_season_id(cur, season_name: str) -> int:
    cur.execute(
        "SELECT season_id FROM seasons WHERE season_name = %s",
        (season_name,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"Season '{season_name}' not found in seasons table. "
            f"Run: SELECT season_name FROM seasons ORDER BY start_year;"
        )
    return row[0]


def get_leagues(cur, league_slug: str | None) -> list[tuple[int, str]]:
    """Return list of (league_id, league_name) to process."""
    if league_slug is not None:
        league_name = LEAGUE_SLUG_MAP.get(league_slug)
        if league_name is None:
            known = ", ".join(LEAGUE_SLUG_MAP.keys())
            raise ValueError(
                f"Unknown league slug '{league_slug}'. Known slugs: {known}"
            )
        cur.execute(
            "SELECT league_id, league_name FROM leagues WHERE league_name = %s",
            (league_name,),
        )
    else:
        cur.execute("SELECT league_id, league_name FROM leagues ORDER BY league_name")
    return cur.fetchall()


def backfill_league(
    cur,
    league_id: int,
    league_name: str,
    season_id: int,
) -> tuple[int, int, int]:
    """
    Backfill player_season_stats for one league/season.

    Returns (players_found, records_created, records_skipped).
    """
    # All teams in this league
    cur.execute(
        "SELECT team_id FROM teams WHERE league_id = %s",
        (league_id,),
    )
    team_ids = [row[0] for row in cur.fetchall()]

    players_found = 0
    created = 0
    skipped = 0

    for team_id in team_ids:
        # Players whose current club is this team
        cur.execute(
            "SELECT player_id FROM players WHERE current_team_id = %s",
            (team_id,),
        )
        player_ids = [row[0] for row in cur.fetchall()]
        players_found += len(player_ids)

        for player_id in player_ids:
            # INSERT minimal row; skip silently if the unique tuple already exists
            cur.execute(
                """
                INSERT INTO player_season_stats
                    (player_id, team_id, season_id, league_id, data_source_id)
                VALUES (%s, %s, %s, %s, NULL)
                ON CONFLICT (player_id, team_id, season_id, league_id) DO NOTHING
                """,
                (player_id, team_id, season_id, league_id),
            )
            if cur.rowcount == 1:
                created += 1
            else:
                skipped += 1

    return players_found, created, skipped


def run_backfill(season_name: str, league_slug: str | None = None) -> None:
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn:
            with conn.cursor() as cur:
                season_id = get_season_id(cur, season_name)
                logger.info("Season '%s' → season_id=%d", season_name, season_id)

                leagues = get_leagues(cur, league_slug)
                if not leagues:
                    logger.warning("No leagues found for filter '%s'", league_slug)
                    return

                total_found = total_created = total_skipped = 0

                for league_id, league_name in leagues:
                    found, created, skipped = backfill_league(
                        cur, league_id, league_name, season_id
                    )
                    total_found += found
                    total_created += created
                    total_skipped += skipped
                    print(
                        f"  {league_name:<32} "
                        f"players={found:>4}  "
                        f"created={created:>4}  "
                        f"skipped={skipped:>4}"
                    )

                print(
                    f"\nTotals — players found: {total_found}, "
                    f"created: {total_created}, "
                    f"skipped (already existed): {total_skipped}"
                )
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill minimal player_season_stats rows from current_team_id."
    )
    parser.add_argument(
        "--season",
        required=True,
        help='Season name as stored in DB, e.g. "2023-24"',
    )
    parser.add_argument(
        "--league",
        required=False,
        default=None,
        help=(
            "League slug to restrict backfill (e.g. premier-league). "
            "Omit to run all leagues."
        ),
    )
    args = parser.parse_args()
    run_backfill(args.season, args.league)
