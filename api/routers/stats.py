"""General stats/overview endpoints."""
from fastapi import APIRouter
from database.connection import query

router = APIRouter(prefix="/api/v1/stats", tags=["stats"])


@router.get("/overview")
def overview():
    """Platform data overview."""
    rows = query("""
        SELECT
            (SELECT COUNT(*) FROM players) as players,
            (SELECT COUNT(*) FROM teams) as teams,
            (SELECT COUNT(*) FROM player_match_stats) as player_match_rows,
            (SELECT COUNT(*) FROM team_match_stats) as team_match_rows,
            (SELECT COUNT(*) FROM loaded_files) as files_loaded,
            (SELECT MIN(match_date) FROM player_match_stats) as earliest_match,
            (SELECT MAX(match_date) FROM player_match_stats) as latest_match
    """, as_dict=True)
    return {"data": rows[0] if rows else {}}


@router.get("/competitions")
def competitions():
    """List all competitions with match counts."""
    rows = query("""
        SELECT competition_name, COUNT(*) as matches,
               COUNT(DISTINCT wyscout_player_id) as players,
               MIN(match_date) as first_match,
               MAX(match_date) as last_match
        FROM player_match_stats
        GROUP BY competition_name
        ORDER BY matches DESC
    """, as_dict=True)
    return {"data": rows}


@router.get("/leaderboard")
def leaderboard(
    stat: str = "goals",
    competition: str = None,
    min_minutes: int = 900,
    limit: int = 20,
):
    """Player leaderboard for a given stat."""
    valid_stats = {
        'goals', 'assists', 'xg', 'xa', 'shots', 'passes',
        'dribbles', 'interceptions', 'progressive_runs',
        'duels_won', 'touches_in_box',
    }
    if stat not in valid_stats:
        return {"error": f"Invalid stat. Use: {sorted(valid_stats)}"}

    conditions = ["1=1"]
    params = []
    if competition:
        conditions.append("pms.competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT p.name, p.wyscout_id, p.primary_position,
               SUM(pms.{stat})::int as total,
               COUNT(*) as matches,
               SUM(pms.minutes_played) as minutes,
               ROUND((SUM(pms.{stat})::numeric /
                      NULLIF(SUM(pms.minutes_played), 0) * 90), 3) as per90
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE {where}
        GROUP BY p.player_id
        HAVING SUM(pms.minutes_played) >= %s
        ORDER BY total DESC NULLS LAST
        LIMIT %s
    """, params + [min_minutes, limit], as_dict=True)

    return {"data": rows, "stat": stat}


@router.get("/top-performers")
def top_performers(
    position: str = None,
    competition: str = None,
    limit: int = 20,
):
    """Top players by performance score."""
    conditions = ["ps.performance_score IS NOT NULL"]
    params = []

    if position:
        conditions.append("ps.position_group = %s")
        params.append(position)
    if competition:
        conditions.append("c.name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT p.player_id, p.name, p.wyscout_id,
               ps.position_group, ps.performance_score,
               ps.percentile_rank, ps.minutes_total, ps.matches_total,
               ps.goals_p90, ps.assists_p90, ps.xg_p90, ps.xa_p90,
               c.name as competition_name
        FROM player_scores ps
        JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN competitions c ON c.competition_id = ps.competition_id
        WHERE {where}
        ORDER BY ps.performance_score DESC
        LIMIT %s
    """, params + [limit], as_dict=True)

    return {"data": rows}
