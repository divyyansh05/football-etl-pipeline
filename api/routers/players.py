"""Player API endpoints."""
from fastapi import APIRouter, Query, HTTPException, Response
from fastapi.responses import StreamingResponse
from typing import List, Optional
from database.connection import query
from api.models.base import ResponseSingle, ResponseList, ErrorResponse
from api.models.player import (
    PlayerSummary, PlayerDetail, PlayerMatchStat, 
    PlayerSeasonAgg, PlayerPerformanceScore, PlayerCompareEntry
)

router = APIRouter(prefix="/api/v1/players", tags=["players"])


@router.get("", response_model=ResponseList[PlayerSummary])
def list_players(
    search: Optional[str] = Query(None, description="Search by name"),
    position: Optional[str] = Query(None, description="Filter by position_group"),
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


@router.get("/compare", response_model=ResponseList[PlayerCompareEntry])
def compare_players(
    ids: str = Query(..., description="Comma-separated player IDs"),
    competition: Optional[str] = Query(None),
    min_minutes: int = Query(450, ge=0),
):
    """Compare 2-5 players side by side with per-90 stats."""
    try:
        player_ids = [int(x.strip()) for x in ids.split(",")]
    except ValueError:
        raise HTTPException(
            status_code=400, 
            detail={"error": "Invalid format", "detail": "ids must be comma-separated integers"}
        )

    if len(player_ids) < 2 or len(player_ids) > 5:
        raise HTTPException(
            status_code=400, 
            detail={"error": "Invalid count", "detail": "Provide 2-5 player IDs"}
        )

    placeholders = ",".join(["%s"] * len(player_ids))

    conditions = [f"pms.player_id IN ({placeholders})"]
    params = list(player_ids)
    if competition:
        conditions.append("pms.competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    rows = query(f"""
        SELECT p.player_id, p.name, p.position_group, p.primary_position,
               COUNT(*)::int as matches,
               SUM(pms.minutes_played)::int as minutes,
               COALESCE(SUM(pms.goals), 0)::int as goals,
               COALESCE(SUM(pms.assists), 0)::int as assists,
               ROUND(SUM(pms.xg)::numeric, 2)::float as xg,
               ROUND(SUM(pms.xa)::numeric, 2)::float as xa,
               ROUND((SUM(pms.goals)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as goals_p90,
               ROUND((SUM(pms.assists)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as assists_p90,
               ROUND((SUM(pms.xg)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as xg_p90,
               ROUND((SUM(pms.xa)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as xa_p90,
               ROUND((SUM(pms.shots)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as shots_p90,
               ROUND((SUM(pms.dribbles_successful)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as dribbles_p90,
               ROUND((SUM(pms.progressive_runs)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as prog_runs_p90,
               ROUND((SUM(pms.key_passes)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as key_passes_p90,
               ROUND((SUM(pms.interceptions)::numeric / NULLIF(SUM(pms.minutes_played),0) * 90), 3)::float as interceptions_p90,
               ROUND((SUM(pms.duels_won)::numeric / NULLIF(SUM(pms.duels),0) * 100), 1)::float as duels_won_pct,
               ROUND((SUM(pms.passes_accurate)::numeric / NULLIF(SUM(pms.passes),0) * 100), 1)::float as pass_acc_pct
        FROM player_match_stats pms
        JOIN players p ON p.player_id = pms.player_id
        WHERE {where}
        GROUP BY p.player_id
        HAVING SUM(pms.minutes_played) >= %s
        ORDER BY p.player_id
    """, params + [min_minutes], as_dict=True)

    return {"data": rows}


@router.get("/{player_id}", response_model=ResponseSingle[PlayerDetail])
def get_player(player_id: int):
    """Get player detail."""
    rows = query("""
        SELECT player_id, wyscout_id, name, normalised_name,
               date_of_birth, nationality, position_group,
               primary_position, height_cm, preferred_foot
        FROM players WHERE player_id = %s
    """, (player_id,), as_dict=True)
    if not rows:
        raise HTTPException(
            status_code=404, 
            detail={"error": "Player not found", "detail": f"id={player_id}"}
        )
    return {"data": rows[0]}


@router.get("/{player_id}/matches", response_model=ResponseList[PlayerMatchStat])
def get_player_matches(
    player_id: int,
    competition: Optional[str] = Query(None),
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


@router.get("/{player_id}/season-stats", response_model=ResponseList[PlayerSeasonAgg])
def get_player_season_stats(player_id: int):
    """Aggregated stats per competition for a player."""
    rows = query("""
        SELECT competition_name,
               COUNT(*)::int as matches,
               SUM(minutes_played)::int as minutes,
               COALESCE(SUM(goals), 0)::int as goals,
               COALESCE(SUM(assists), 0)::int as assists,
               ROUND(SUM(xg)::numeric, 2)::float as xg,
               ROUND(SUM(xa)::numeric, 2)::float as xa,
               SUM(shots)::int as shots,
               ROUND((SUM(passes)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 1)::float as passes_per90,
               ROUND((SUM(goals)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 3)::float as goals_per90,
               ROUND((SUM(xg)::numeric / NULLIF(SUM(minutes_played), 0) * 90), 3)::float as xg_per90
        FROM player_match_stats
        WHERE player_id = %s
        GROUP BY competition_name
        ORDER BY matches DESC
    """, (player_id,), as_dict=True)

    return {"data": rows}


@router.get("/{player_id}/scores", response_model=ResponseList[PlayerPerformanceScore])
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


@router.get("/{player_id}/radar", response_model=ResponseSingle[dict[str, float]])
def get_player_radar(player_id: int):
    """Retrieve player radar chart data directly computing peer percentiles."""
    # 1. Verify player exists
    player_check = query("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
    if not player_check:
        raise HTTPException(
            status_code=404, 
            detail={"error": "Player not found", "detail": f"id={player_id}"}
        )

    # 2. Check minute constraints (min_minutes=450 defaults to True in query)
    minutes_check = query("""
        SELECT COALESCE(SUM(minutes_played), 0)
        FROM player_match_stats
        WHERE player_id = %s
    """, (player_id,))
    
    total_minutes = minutes_check[0][0] if minutes_check else 0
    if total_minutes < 450:
        raise HTTPException(
            status_code=400,
            detail={"error": "Insufficient minutes", "detail": f"Player has {total_minutes} minutes. Minimum required is 450."}
        )

    # 3. Dynamically import compute_radar_data and execute
    try:
        from analytics.radar import compute_radar_data
        radar_data = compute_radar_data(player_id, min_minutes=450)
        
        # In case the player config is valid but data generation fails internally
        if not radar_data:
            return {"data": {}}

        return {"data": radar_data}
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail={"error": "Internal Error", "detail": "Analytics module not available."}
        )


@router.get("/{player_id}/form")
def get_player_form(
    player_id: int,
    n: int = Query(20, ge=1, le=50, description="Number of recent matches"),
    competition: Optional[str] = Query(None),
):
    """
    Player match-by-match form data for the last N games.
    Designed for frontend charting (Chart.js / Plotly.js / D3.js).
    Includes rolling xG average for trend lines.
    """
    # Verify player exists
    player_check = query("""
        SELECT p.player_id, p.name, p.position_group
        FROM players p WHERE p.player_id = %s
    """, (player_id,), as_dict=True)
    if not player_check:
        raise HTTPException(status_code=404,
            detail={"error": "Player not found", "detail": f"id={player_id}"})

    player = player_check[0]

    conditions = ["player_id = %s"]
    params = [player_id]
    if competition:
        conditions.append("competition_name LIKE %s")
        params.append(f"%{competition}%")

    where = " AND ".join(conditions)

    form = query(f"""
        SELECT match_label, competition_name, match_date,
               COALESCE(minutes_played, 0) as minutes_played,
               COALESCE(goals, 0) as goals,
               COALESCE(assists, 0) as assists,
               COALESCE(xg, 0.0) as xg,
               COALESCE(xa, 0.0) as xa,
               COALESCE(shots, 0) as shots,
               COALESCE(key_passes, 0) as key_passes,
               COALESCE(dribbles_successful, 0) as dribbles_successful,
               COALESCE(touches_in_box, 0) as touches_in_box
        FROM player_match_stats
        WHERE {where} AND match_date IS NOT NULL
        ORDER BY match_date DESC
        LIMIT %s
    """, params + [n], as_dict=True)

    # Compute rolling 5-game xG average (oldest first)
    form_chron = list(reversed(form))
    rolling_xg = []
    for i in range(len(form_chron)):
        window = form_chron[max(0, i - 4):i + 1]
        avg = round(sum(float(m['xg']) for m in window) / len(window), 3)
        rolling_xg.append(avg)

    total_goals   = sum(m['goals'] for m in form)
    total_assists = sum(m['assists'] for m in form)
    avg_xg        = round(sum(float(m['xg']) for m in form) / max(len(form), 1), 3)

    return {
        "player_id":    player_id,
        "player_name":  player['name'],
        "position_group": player.get('position_group'),
        "form":         form,
        "rolling_xg_avg": rolling_xg,
        "summary": {
            "matches":          len(form),
            "goals":            total_goals,
            "assists":          total_assists,
            "avg_xg_per_game":  avg_xg,
        }
    }


@router.get("/{player_id}/report")
def download_player_report(player_id: int):
    """
    Generate and stream a PDF scouting report for a player.
    Includes: player bio header, radar chart, form timeline, season stats table.
    Returns a downloadable PDF binary.
    """
    # Verify player exists
    player_check = query(
        "SELECT name FROM players WHERE player_id = %s", (player_id,))
    if not player_check:
        raise HTTPException(status_code=404,
            detail={"error": "Player not found", "detail": f"id={player_id}"})

    player_name = player_check[0][0]

    try:
        from analytics.report_generator import generate_player_report
        pdf_bytes = generate_player_report(player_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500,
            detail={"error": "Report generation failed", "detail": str(e)})

    safe_name = player_name.replace(' ', '_').replace('.', '').replace('/', '')
    filename = f"{safe_name}_Scout_Report.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/{player_id}/similar")
def get_similar_players(
    player_id: int,
    n: int = Query(10, ge=1, le=30, description="Number of similar players to return"),
    same_position: bool = Query(True, description="Restrict to same position group"),
    min_minutes: int = Query(450, description="Minimum minutes threshold"),
):
    """
    Find the N most similar players using pgvector cosine similarity on stat_vector.
    Falls back to performance-score proximity if vectors have not been computed yet.
    """
    # Verify player exists and get their position + vector
    player_row = query("""
        SELECT p.player_id, p.name, p.position_group,
               ps.stat_vector::text AS vec_text,
               ps.performance_score
        FROM players p
        LEFT JOIN player_scores ps ON ps.player_id = p.player_id
        WHERE p.player_id = %s
        ORDER BY ps.performance_score DESC NULLS LAST
        LIMIT 1
    """, (player_id,), as_dict=True)

    if not player_row:
        raise HTTPException(status_code=404,
            detail={"error": "Player not found", "detail": f"id={player_id}"})

    target = player_row[0]
    has_vector = target.get('vec_text') and target['vec_text'] != 'None'
    position_group = target.get('position_group') or 'MID'

    pos_filter = "AND p.position_group = %s" if same_position else ""
    pos_param = [position_group] if same_position else []

    if has_vector:
        # ── pgvector cosine similarity ─────────────────────────────────────
        vec_str = target['vec_text']
        rows = query(f"""
            SELECT
                p.player_id, p.name, p.position_group, p.primary_position,
                p.nationality,
                ps.performance_score, ps.percentile_rank, ps.minutes_total,
                ps.goals_p90, ps.xg_p90, ps.assists_p90,
                1 - (ps.stat_vector <=> %s::vector) AS similarity
            FROM player_scores ps
            JOIN players p ON p.player_id = ps.player_id
            WHERE ps.stat_vector IS NOT NULL
              AND ps.player_id != %s
              AND ps.minutes_total >= %s
              {pos_filter}
            ORDER BY ps.stat_vector <=> %s::vector
            LIMIT %s
        """, [vec_str, player_id, min_minutes] + pos_param + [vec_str, n],
        as_dict=True)

        method = "pgvector_cosine"
    else:
        # ── Fallback: score-proximity ───────────────────────────────────────
        target_score = float(target.get('performance_score') or 50)
        rows = query(f"""
            SELECT
                p.player_id, p.name, p.position_group, p.primary_position,
                p.nationality,
                ps.performance_score, ps.percentile_rank, ps.minutes_total,
                ps.goals_p90, ps.xg_p90, ps.assists_p90,
                NULL::float AS similarity
            FROM player_scores ps
            JOIN players p ON p.player_id = ps.player_id
            WHERE ps.player_id != %s
              AND ps.minutes_total >= %s
              {pos_filter}
            ORDER BY ABS(ps.performance_score - %s)
            LIMIT %s
        """, [player_id, min_minutes] + pos_param + [target_score, n],
        as_dict=True)

        method = "score_proximity_fallback"

    return {
        "player_id": player_id,
        "player_name": target['name'],
        "position_group": position_group,
        "method": method,
        "similar_players": [
            {
                "player_id":       r["player_id"],
                "name":            r["name"],
                "position_group":  r["position_group"],
                "primary_position":r["primary_position"],
                "nationality":     r["nationality"],
                "performance_score": float(r["performance_score"]) if r["performance_score"] else None,
                "percentile_rank": float(r["percentile_rank"]) if r["percentile_rank"] else None,
                "minutes_total":   r["minutes_total"],
                "goals_p90":       float(r["goals_p90"]) if r["goals_p90"] else None,
                "xg_p90":          float(r["xg_p90"]) if r["xg_p90"] else None,
                "assists_p90":     float(r["assists_p90"]) if r["assists_p90"] else None,
                "similarity":      round(float(r["similarity"]), 4) if r["similarity"] else None,
            }
            for r in rows
        ]
    }
