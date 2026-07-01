"""General stats/overview endpoints."""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from database.connection import query
from api.models.base import ResponseSingle, ResponseList
from api.models.stats import PlatformOverview
from api.models.competition import CompetitionSummary
from api.models.player import (
    LeaderboardEntry, PlayerPerformanceScore
)

router = APIRouter(prefix="/api/v1/stats", tags=["stats"])


@router.get("/overview", response_model=ResponseSingle[PlatformOverview])
def overview():
    """Platform data overview."""
    rows = query("""
        SELECT
            (SELECT COUNT(*) FROM players)::int as players,
            (SELECT COUNT(*) FROM teams)::int as teams,
            (SELECT COUNT(*) FROM player_match_stats)::int as player_match_rows,
            (SELECT COUNT(*) FROM team_match_stats)::int as team_match_rows,
            (SELECT COUNT(*) FROM loaded_files)::int as files_loaded,
            (SELECT MIN(match_date) FROM player_match_stats) as earliest_match,
            (SELECT MAX(match_date) FROM player_match_stats) as latest_match
    """, as_dict=True)
    return {"data": rows[0] if rows else {}}


@router.get("/competitions", response_model=ResponseList[CompetitionSummary])
def competitions():
    """List all competitions with match counts."""
    rows = query("""
        SELECT competition_name, COUNT(*)::int as matches,
               COUNT(DISTINCT wyscout_player_id)::int as players,
               MIN(match_date) as first_match,
               MAX(match_date) as last_match
        FROM player_match_stats
        GROUP BY competition_name
        ORDER BY matches DESC
    """, as_dict=True)
    return {"data": rows}


@router.get("/leaderboard", response_model=ResponseList[LeaderboardEntry])
def leaderboard(
    stat: str = "goals",
    competition: Optional[str] = None,
    min_minutes: int = 450,
    limit: int = Query(20, ge=1, le=100),
):
    """Player leaderboard for a given stat."""
    valid_stats = {
        'goals', 'assists', 'xg', 'xa', 'shots', 'passes',
        'dribbles', 'interceptions', 'progressive_runs',
        'duels_won', 'touches_in_box',
    }
    if stat not in valid_stats:
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid stat", "detail": f"Use one of: {sorted(valid_stats)}"}
        )

    conditions = ["1=1"]
    params = []
    if competition:
        conditions.append("pms.competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    # Note: Using dynamic column name in SQL requires careful validation (done above)
    rows = query(f"""
        SELECT p.name, p.wyscout_id, p.primary_position,
               SUM(pms.{stat})::int as total,
               COUNT(*)::int as matches,
               SUM(pms.minutes_played)::int as minutes,
               ROUND((SUM(pms.{stat})::numeric /
                      NULLIF(SUM(pms.minutes_played), 0) * 90), 3)::float as per90
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE {where}
        GROUP BY p.player_id
        HAVING SUM(pms.minutes_played) >= %s
        ORDER BY total DESC NULLS LAST
        LIMIT %s
    """, params + [min_minutes, limit], as_dict=True)

    return {"data": rows}


@router.get("/top-performers", response_model=ResponseList[PlayerPerformanceScore])
def top_performers(
    position: Optional[str] = None,
    competition: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
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
               ps.position_group, ps.performance_score::float,
               ps.percentile_rank::float, ps.minutes_total, ps.matches_total,
               ps.goals_p90::float, ps.assists_p90::float, ps.xg_p90::float, ps.xa_p90::float,
               c.name as competition_name
        FROM player_scores ps
        JOIN players p ON p.player_id = ps.player_id
        LEFT JOIN competitions c ON c.competition_id = ps.competition_id
        WHERE {where}
        ORDER BY ps.performance_score DESC
        LIMIT %s
    """, params + [limit], as_dict=True)

    return {"data": rows}
