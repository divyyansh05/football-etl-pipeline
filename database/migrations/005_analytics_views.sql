-- Migration 005: Gold views updated with analytics score columns.
-- No BEGIN/COMMIT — runner owns the transaction.
-- Uses DROP + CREATE VIEW pattern (same as migration 003) so the view
-- is replaced atomically when player_scores columns are added.

-- ── v_players_current_season ────────────────────────────────────────────────
-- Extended to LEFT JOIN player_scores so all players with >= 450 mins appear,
-- even those whose scores have not yet been computed.
DROP VIEW IF EXISTS v_players_current_season;

CREATE VIEW v_players_current_season AS
SELECT
    p.player_id,
    p.player_name,
    p.position_group,
    p."position",
    p.nationality,
    EXTRACT(year FROM age(now(), p.date_of_birth::timestamptz))::integer AS age,
    p.date_of_birth,
    p.height_cm,
    t.team_id,
    t.team_name,
    l.league_id,
    l.league_name,
    s.season_name,
    pss.minutes,
    pss.matches_played,
    pss.goals,
    pss.assists,
    pss.shots,
    pss.xg,
    pss.npxg,
    pss.xa,
    pss.xg_chain,
    pss.xg_buildup,
    pss.aerial_duels_won,
    pss.aerial_win_pct,
    pss.tackles_won,
    pss.tackles_won_pct,
    pss.interceptions,
    pss.clearances,
    pss.recoveries,
    pss.key_passes,
    pss.big_chances_created,
    pss.accurate_passes_pct,
    pss.accurate_final_third,
    pss.sofascore_rating,
    pss.saves,
    pss.clean_sheets,
    pss.fotmob_collected,
    pss.sofascore_collected,
    pss.understat_collected,
    -- analytics score columns (NULL until compute_scores.py has run)
    ps.performance_score,
    ps.percentile_rank,
    ps.goals_p90,
    ps.assists_p90,
    ps.xg_p90,
    ps.xa_p90,
    ps.shots_p90,
    ps.key_passes_p90,
    ps.tackles_p90,
    ps.interceptions_p90,
    ps.aerial_won_p90,
    ps.successful_dribbles_p90,
    ps.recoveries_p90
FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
JOIN teams t ON pss.team_id = t.team_id
JOIN leagues l ON pss.league_id = l.league_id
JOIN seasons s ON pss.season_id = s.season_id
LEFT JOIN player_scores ps ON (
    ps.player_id = pss.player_id
    AND ps.season_id = pss.season_id
    AND ps.league_id = pss.league_id
)
WHERE s.is_current = TRUE
  AND pss.minutes >= 450;
