-- Migration 003: update Gold views.
-- All DDL uses CREATE OR REPLACE VIEW (idempotent).
-- No BEGIN/COMMIT — runner owns the transaction.
--
-- Canonical view list (as of March 2026, discovered from schema v2):
--   v_coverage_summary
--   v_player_last5
--   v_players_current_season   ← fixed: was hardcoded '2025-26'
--   v_team_current_elo

-- ── v_players_current_season ────────────────────────────────────────────────
-- Was: WHERE s.season_name = '2025-26'  ← hardcoded, violates rule
-- Fix: WHERE s.is_current = TRUE  ← dynamic, reads from seasons table
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
    pss.understat_collected
FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
JOIN teams t ON pss.team_id = t.team_id
JOIN leagues l ON pss.league_id = l.league_id
JOIN seasons s ON pss.season_id = s.season_id
WHERE s.is_current = TRUE
  AND pss.minutes >= 450;

-- ── v_coverage_summary ──────────────────────────────────────────────────────
-- No stale refs — fotmob_collected column exists in schema.
-- Recreate as-is for idempotency.
CREATE OR REPLACE VIEW v_coverage_summary AS
SELECT
    l.league_name,
    s.season_name,
    COUNT(DISTINCT pss.player_id) AS total_players,
    COUNT(DISTINCT CASE WHEN pss.minutes >= 450 THEN pss.player_id END) AS active_players,
    COUNT(DISTINCT CASE WHEN pss.xg IS NOT NULL THEN pss.player_id END) AS with_xg,
    COUNT(DISTINCT CASE WHEN pss.aerial_duels_won IS NOT NULL THEN pss.player_id END) AS with_deep_stats,
    COUNT(DISTINCT CASE WHEN pss.sofascore_rating IS NOT NULL THEN pss.player_id END) AS with_rating,
    ROUND(
        (COUNT(DISTINCT CASE WHEN pss.xg IS NOT NULL THEN pss.player_id END)::numeric * 100.0)
        / NULLIF(COUNT(DISTINCT pss.player_id), 0)::numeric,
        1
    ) AS xg_pct,
    ROUND(
        (COUNT(DISTINCT CASE WHEN pss.aerial_duels_won IS NOT NULL THEN pss.player_id END)::numeric * 100.0)
        / NULLIF(COUNT(DISTINCT pss.player_id), 0)::numeric,
        1
    ) AS deep_pct,
    ROUND(
        (COUNT(DISTINCT CASE WHEN
            pss.sofascore_collected AND pss.understat_collected
        THEN pss.player_id END)::numeric * 100.0)
        / NULLIF(COUNT(DISTINCT pss.player_id), 0)::numeric,
        1
    ) AS all_sources_pct
FROM player_season_stats pss
JOIN leagues l ON pss.league_id = l.league_id
JOIN seasons s ON pss.season_id = s.season_id
GROUP BY l.league_name, s.season_name
ORDER BY s.season_name DESC, l.league_name;

-- ── v_team_current_elo ───────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_team_current_elo AS
SELECT DISTINCT ON (te.team_id)
    te.team_id,
    t.team_name,
    l.league_name,
    te.elo_rating,
    te.elo_rank,
    te.elo_date
FROM team_elo te
JOIN teams t ON te.team_id = t.team_id
JOIN leagues l ON t.league_id = l.league_id
ORDER BY te.team_id, te.elo_date DESC;

-- ── v_player_last5 ───────────────────────────────────────────────────────────
-- References player_match_stats which will be empty (match tables not populated).
-- View is valid; SELECT COUNT(*) returns 0 cleanly.
CREATE OR REPLACE VIEW v_player_last5 AS
SELECT
    ranked.player_id,
    ranked.player_name,
    ranked.position_group,
    ranked.team_name,
    ranked.league_name,
    ranked.match_date,
    ranked.matchweek,
    ranked.minutes_played,
    ranked.is_starter,
    ranked.position_played,
    ranked.goals,
    ranked.assists,
    ranked.xg,
    ranked.xa,
    ranked.sofascore_rating,
    ranked.fotmob_rating,
    ranked.aerial_duels_won,
    ranked.tackles_won,
    ranked.key_passes,
    ranked.recoveries,
    ranked.match_recency
FROM (
    SELECT
        pms.player_id,
        p.player_name,
        p.position_group,
        t.team_name,
        l.league_name,
        m.match_date,
        m.matchweek,
        pms.minutes_played,
        pms.is_starter,
        pms.position_played,
        pms.goals,
        pms.assists,
        pms.xg,
        pms.xa,
        pms.sofascore_rating,
        pms.fotmob_rating,
        pms.aerial_duels_won,
        pms.tackles_won,
        pms.key_passes,
        pms.recoveries,
        ROW_NUMBER() OVER (PARTITION BY pms.player_id ORDER BY m.match_date DESC) AS match_recency
    FROM player_match_stats pms
    JOIN players p ON pms.player_id = p.player_id
    JOIN matches m ON pms.match_id = m.match_id
    JOIN teams t ON pms.team_id = t.team_id
    JOIN leagues l ON m.league_id = l.league_id
    WHERE pms.minutes_played > 0
) ranked
WHERE ranked.match_recency <= 5;
