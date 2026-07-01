"""Team API endpoints."""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from database.connection import query
from api.models.base import ResponseSingle, ResponseList
from api.models.team import TeamSummary, TeamMatchStat, TeamSeasonAgg

router = APIRouter(prefix="/api/v1/teams", tags=["teams"])


@router.get("", response_model=ResponseList[TeamSummary])
def list_teams(
    search: Optional[str] = Query(None),
    competition: Optional[str] = Query(None, description="Filter by league/competition name"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List teams with match counts."""
    conditions = []
    params = []

    if search:
        conditions.append("t.normalised_name LIKE %s")
        params.append(f"%{search.lower()}%")
    if competition:
        # Filter by competition using the team_match_stats join
        conditions.append("tms.competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    rows = query(f"""
        SELECT t.team_id, t.wyscout_id, t.name, t.country,
               COUNT(tms.id) as matches
        FROM teams t
        LEFT JOIN team_match_stats tms ON t.team_id = tms.team_id
        {where}
        GROUP BY t.team_id
        ORDER BY t.name
        LIMIT %s OFFSET %s
    """, params + [limit, offset], as_dict=True)

    count_rows = query(f"SELECT COUNT(*) FROM teams t {where}", params)
    total = count_rows[0][0] if count_rows else 0

    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/{team_id}", response_model=ResponseSingle[TeamSummary])
def get_team(team_id: int):
    """Get team detail."""
    rows = query("""
        SELECT team_id, wyscout_id, name, country
        FROM teams WHERE team_id = %s
    """, (team_id,), as_dict=True)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail={"error": "Team not found", "detail": f"id={team_id}"}
        )
    return {"data": rows[0]}


@router.get("/{team_id}/matches", response_model=ResponseList[TeamMatchStat])
def get_team_matches(
    team_id: int,
    competition: Optional[str] = Query(None),
    result: Optional[str] = Query(None, description="W/D/L"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Get match-by-match stats for a team."""
    conditions = ["team_id = %s"]
    params = [team_id]

    if competition:
        conditions.append("competition_name LIKE %s")
        params.append(f"%{competition}%")
    if result:
        conditions.append("result = %s")
        params.append(result.upper())

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT match_label, competition_name, match_date,
               is_home, formation, result,
               goals, xg, shots, possession_pct,
               passes, passes_accurate, ppda,
               duels, duels_won
        FROM team_match_stats
        WHERE {where}
        ORDER BY match_date DESC
        LIMIT %s OFFSET %s
    """, params + [limit, offset], as_dict=True)

    count_rows = query(
        f"SELECT COUNT(*) FROM team_match_stats WHERE {where}", params)
    total = count_rows[0][0] if count_rows else 0

    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/{team_id}/season-stats", response_model=ResponseList[TeamSeasonAgg])
def get_team_season_stats(team_id: int):
    """Aggregated stats per competition for a team."""
    rows = query("""
        SELECT competition_name,
               COUNT(*)::int as matches,
               SUM(CASE WHEN result='W' THEN 1 ELSE 0 END)::int as wins,
               SUM(CASE WHEN result='D' THEN 1 ELSE 0 END)::int as draws,
               SUM(CASE WHEN result='L' THEN 1 ELSE 0 END)::int as losses,
               COALESCE(SUM(goals), 0)::int as goals_scored,
               COALESCE(SUM(conceded_goals), 0)::int as goals_conceded,
               ROUND(AVG(xg)::numeric, 2)::float as avg_xg,
               ROUND(AVG(possession_pct)::numeric, 1)::float as avg_possession,
               ROUND(AVG(ppda)::numeric, 2)::float as avg_ppda,
               ROUND(AVG(passes_accurate)::numeric /
                     NULLIF(AVG(passes)::numeric, 0) * 100, 1)::float as pass_acc_pct
        FROM team_match_stats
        WHERE team_id = %s
        GROUP BY competition_name
        ORDER BY matches DESC
    """, (team_id,), as_dict=True)

    return {"data": rows}
