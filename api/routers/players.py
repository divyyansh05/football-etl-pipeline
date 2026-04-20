"""Player API endpoints."""
from fastapi import APIRouter, Query
from database.connection import query

router = APIRouter(prefix="/api/v1/players", tags=["players"])


@router.get("")
def list_players(
    search: str = Query(None, description="Search by name"),
    position: str = Query(None, description="Filter by position_group"),
    min_minutes: int = Query(450, ge=0),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List players with aggregated stats."""
    conditions = []
    params = []

    if search:
        conditions.append("p.normalised_name LIKE %s")
        params.append(f"%{search.lower()}%")
    if position:
        conditions.append("p.position_group = %s")
        params.append(position)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    rows = query(f"""
        SELECT p.player_id, p.wyscout_id, p.name,
               p.position_group, p.primary_position, p.nationality,
               COUNT(pms.id) as matches,
               COALESCE(SUM(pms.goals), 0) as total_goals,
               COALESCE(SUM(pms.assists), 0) as total_assists,
               ROUND(AVG(pms.xg)::numeric, 3) as avg_xg
        FROM players p
        LEFT JOIN player_match_stats pms ON p.player_id = pms.player_id
        {where}
        GROUP BY p.player_id
        HAVING COALESCE(SUM(pms.minutes_played), 0) >= %s
        ORDER BY total_goals DESC
        LIMIT %s OFFSET %s
    """, params + [min_minutes, limit, offset], as_dict=True)

    count_rows = query(f"""
        SELECT COUNT(*) as cnt FROM (
            SELECT p.player_id
            FROM players p
            LEFT JOIN player_match_stats pms ON p.player_id = pms.player_id
            {where}
            GROUP BY p.player_id
            HAVING COALESCE(SUM(pms.minutes_played), 0) >= %s
        ) sub
    """, params + [min_minutes])
    total = count_rows[0][0] if count_rows else 0

    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/{player_id}")
def get_player(player_id: int):
    """Get player detail."""
    rows = query("""
        SELECT player_id, wyscout_id, name, normalised_name,
               date_of_birth, nationality, position_group,
               primary_position, height_cm, preferred_foot
        FROM players WHERE player_id = %s
    """, (player_id,), as_dict=True)
    if not rows:
        return {"error": "Player not found", "detail": f"id={player_id}"}
    return {"data": rows[0]}


@router.get("/{player_id}/matches")
def get_player_matches(
    player_id: int,
    competition: str = Query(None),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Get match-by-match stats for a player."""
    conditions = ["player_id = %s"]
    params = [player_id]

    if competition:
        conditions.append("competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT match_label, competition_name, match_date,
               position_played, minutes_played,
               goals, assists, xg, xa,
               shots, shots_on_target,
               passes, passes_accurate,
               dribbles, dribbles_successful,
               duels, duels_won,
               interceptions, progressive_runs, touches_in_box,
               yellow_cards, red_cards
        FROM player_match_stats
        WHERE {where}
        ORDER BY match_date DESC
        LIMIT %s OFFSET %s
    """, params + [limit, offset], as_dict=True)

    count_rows = query(
        f"SELECT COUNT(*) FROM player_match_stats WHERE {where}",
        params)
    total = count_rows[0][0] if count_rows else 0

    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/{player_id}/season-stats")
def get_player_season_stats(player_id: int):
    """Aggregated stats per competition for a player."""
    rows = query("""
        SELECT competition_name,
               COUNT(*) as matches,
               SUM(minutes_played) as minutes,
               COALESCE(SUM(goals), 0) as goals,
               COALESCE(SUM(assists), 0) as assists,
               ROUND(SUM(xg)::numeric, 2) as xg,
               ROUND(SUM(xa)::numeric, 2) as xa,
               SUM(shots) as shots,
               ROUND((SUM(passes)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 1) as passes_per90,
               ROUND((SUM(goals)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 3) as goals_per90,
               ROUND((SUM(xg)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 3) as xg_per90
        FROM player_match_stats
        WHERE player_id = %s
        GROUP BY competition_name
        ORDER BY matches DESC
    """, (player_id,), as_dict=True)

    return {"data": rows}


@router.get("/compare")
def compare_players(
    ids: str = Query(..., description="Comma-separated player IDs"),
    competition: str = Query(None),
    min_minutes: int = Query(450, ge=0),
):
    """Compare 2-5 players side by side with per-90 stats."""
    try:
        player_ids = [int(x.strip()) for x in ids.split(",")]
    except ValueError:
        return {"error": "ids must be comma-separated integers"}

    if len(player_ids) < 2 or len(player_ids) > 5:
        return {"error": "Provide 2-5 player IDs"}

    placeholders = ",".join(["%s"] * len(player_ids))

    conditions = [f"pms.player_id IN ({placeholders})"]
    params = list(player_ids)
    if competition:
        conditions.append("pms.competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT p.player_id, p.name, p.position_group, p.primary_position,
               COUNT(*) as matches,
               SUM(pms.minutes_played) as minutes,
               COALESCE(SUM(pms.goals), 0) as goals,
               COALESCE(SUM(pms.assists), 0) as assists,
               ROUND(SUM(pms.xg)::numeric, 2) as xg,
               ROUND(SUM(pms.xa)::numeric, 2) as xa,
               ROUND((SUM(pms.goals)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as goals_p90,
               ROUND((SUM(pms.assists)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as assists_p90,
               ROUND((SUM(pms.xg)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as xg_p90,
               ROUND((SUM(pms.xa)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as xa_p90,
               ROUND((SUM(pms.shots)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as shots_p90,
               ROUND((SUM(pms.dribbles_successful)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as dribbles_p90,
               ROUND((SUM(pms.progressive_runs)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as prog_runs_p90,
               ROUND((SUM(pms.key_passes)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as key_passes_p90,
               ROUND((SUM(pms.interceptions)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3) as interceptions_p90,
               ROUND((SUM(pms.duels_won)::numeric / NULLIF(SUM(pms.duels),0) * 100), 1) as duels_won_pct,
               ROUND((SUM(pms.passes_accurate)::numeric / NULLIF(SUM(pms.passes),0) * 100), 1) as pass_acc_pct
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE {where}
        GROUP BY p.player_id
        HAVING SUM(pms.minutes_played) >= %s
        ORDER BY p.player_id
    """, params + [min_minutes], as_dict=True)

    return {"data": rows}


@router.get("/{player_id}/scores")
def get_player_scores(player_id: int):
    """Performance scores for a player across competitions."""
    rows = query("""
        SELECT ps.position_group, ps.minutes_total, ps.matches_total,
               ps.performance_score, ps.percentile_rank,
               ps.goals_p90, ps.assists_p90, ps.xg_p90, ps.xa_p90,
               c.name as competition_name
        FROM player_scores ps
        LEFT JOIN competitions c ON c.competition_id = ps.competition_id
        WHERE ps.player_id = %s
        ORDER BY ps.performance_score DESC
    """, (player_id,), as_dict=True)

    return {"data": rows}
