"""Team API endpoints."""
from fastapi import APIRouter, Query
from database.connection import query

router = APIRouter(prefix="/api/v1/teams", tags=["teams"])


@router.get("")
def list_teams(
    search: str = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List teams with match counts."""
    conditions = []
    params = []

    if search:
        conditions.append("t.normalised_name LIKE %s")
        params.append(f"%{search.lower()}%")

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


@router.get("/{team_id}")
def get_team(team_id: int):
    """Get team detail."""
    rows = query("""
        SELECT team_id, wyscout_id, name, normalised_name, country
        FROM teams WHERE team_id = %s
    """, (team_id,), as_dict=True)
    if not rows:
        return {"error": "Team not found", "detail": f"id={team_id}"}
    return {"data": rows[0]}


@router.get("/{team_id}/matches")
def get_team_matches(
    team_id: int,
    competition: str = Query(None),
    result: str = Query(None, description="W/D/L"),
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


@router.get("/{team_id}/season-stats")
def get_team_season_stats(team_id: int):
    """Aggregated stats per competition for a team."""
    rows = query("""
        SELECT competition_name,
               COUNT(*) as matches,
               SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN result='D' THEN 1 ELSE 0 END) as draws,
               SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) as losses,
               COALESCE(SUM(goals), 0) as goals_scored,
               COALESCE(SUM(conceded_goals), 0) as goals_conceded,
               ROUND(AVG(xg)::numeric, 2) as avg_xg,
               ROUND(AVG(possession_pct)::numeric, 1) as avg_possession,
               ROUND(AVG(ppda)::numeric, 2) as avg_ppda,
               ROUND(AVG(passes_accurate)::numeric /
                     NULLIF(AVG(passes)::numeric, 0) * 100, 1) as pass_acc_pct
        FROM team_match_stats
        WHERE team_id = %s
        GROUP BY competition_name
        ORDER BY matches DESC
    """, (team_id,), as_dict=True)

    return {"data": rows}
