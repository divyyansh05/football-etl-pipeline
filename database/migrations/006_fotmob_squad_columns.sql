-- Migration 006: FotMob squad coverage columns
-- Adds is_injured to player_season_stats for injury status tracking.
-- All other FotMob-related columns (fotmob_id on players, fotmob_collected
-- on player_season_stats) already exist in the schema v2 baseline.
--
-- Idempotent: all ALTER TABLE statements use IF NOT EXISTS guards.
-- Safe to re-run against a DB that already has these columns.

-- is_injured: tracks current injury status fetched from FotMob squad page
ALTER TABLE player_season_stats
    ADD COLUMN IF NOT EXISTS is_injured BOOLEAN DEFAULT FALSE;

-- Index for is_injured (useful for scouting queries filtering healthy players)
CREATE INDEX IF NOT EXISTS idx_pss_is_injured
    ON player_season_stats (is_injured)
    WHERE is_injured = TRUE;

-- Ensure player_name_norm is populated for any rows that might be NULL
-- (defensive: SofaScoreETL should fill this, but guards against gaps)
UPDATE players
SET player_name_norm = lower(
        regexp_replace(
            translate(
                player_name,
                'ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖòóôõöÙÚÛÜùúûüÝýÑñÇçÆæØøÅå',
                'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuYyNnCcAaOoAa'
            ),
            '[^a-zA-Z0-9 ]', '', 'g'
        )
    )
WHERE player_name_norm IS NULL;
