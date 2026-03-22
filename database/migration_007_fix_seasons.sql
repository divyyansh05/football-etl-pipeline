-- Migration 007: Fix orphan seasons and is_current flags
-- Fixes single-year API-Football season format ('2025', '2026') and
-- consolidates is_current to only 2025-26.
-- Run: psql -h localhost -p 5434 -U postgres -d football_data -f database/migration_007_fix_seasons.sql

-- ============================================
-- Step 1: Fix is_current flags
-- ============================================
BEGIN;

UPDATE seasons SET is_current = FALSE
WHERE season_name NOT IN ('2025-26');

UPDATE seasons SET is_current = TRUE
WHERE season_name = '2025-26';

COMMIT;

-- ============================================
-- Step 2: Merge orphan '2025' (season_id=52) into '2024-25' (season_id=5)
-- ============================================
BEGIN;

UPDATE player_season_stats
SET season_id = 5
WHERE season_id = 52
AND NOT EXISTS (
    SELECT 1 FROM player_season_stats p2
    WHERE p2.player_id = player_season_stats.player_id
    AND p2.season_id = 5
);

DELETE FROM player_season_stats WHERE season_id = 52;

UPDATE team_season_stats
SET season_id = 5
WHERE season_id = 52
AND NOT EXISTS (
    SELECT 1 FROM team_season_stats t2
    WHERE t2.team_id = team_season_stats.team_id
    AND t2.season_id = 5
);

DELETE FROM team_season_stats WHERE season_id = 52;

COMMIT;

-- ============================================
-- Step 3: Merge orphan '2026' (season_id=51) into '2025-26' (season_id=50)
-- ============================================
BEGIN;

UPDATE player_season_stats
SET season_id = 50
WHERE season_id = 51
AND NOT EXISTS (
    SELECT 1 FROM player_season_stats p2
    WHERE p2.player_id = player_season_stats.player_id
    AND p2.season_id = 50
);

DELETE FROM player_season_stats WHERE season_id = 51;

UPDATE team_season_stats
SET season_id = 50
WHERE season_id = 51
AND NOT EXISTS (
    SELECT 1 FROM team_season_stats t2
    WHERE t2.team_id = team_season_stats.team_id
    AND t2.season_id = 50
);

DELETE FROM team_season_stats WHERE season_id = 51;

COMMIT;

-- ============================================
-- Step 4: Delete the now-empty orphan season rows
-- ============================================
BEGIN;

DELETE FROM seasons WHERE season_id IN (51, 52);

DELETE FROM seasons WHERE season_name = '2020/2021';

COMMIT;

-- ============================================
-- Step 5: Verify final state
-- ============================================
SELECT season_id, season_name, is_current FROM seasons ORDER BY start_year;
