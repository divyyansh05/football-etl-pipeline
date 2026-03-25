-- Migration 002: rename xag→xa, handle data_sources.
-- All DDL guarded for idempotency.

-- Rename xag → xa if xag column still exists
-- (In schema v2, column is already named 'xa' — this is a no-op if already correct)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='player_season_stats' AND column_name='xag'
    ) THEN
        ALTER TABLE player_season_stats RENAME COLUMN xag TO xa;
    END IF;
END $$;

-- data_sources: historical — dead source
-- Table does not exist in schema v2.
-- Decision (Option A): table was removed in schema v2 rebuild.
-- Confirmed by schema audit: SELECT COUNT(*) FROM pg_tables WHERE tablename='data_sources' = 0.
-- Zero executable code references remain after ETL rewrites in Steps 5-7.
-- This block is intentionally a no-op; left as documentation.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='data_sources') THEN
        DROP TABLE data_sources;
    END IF;
END $$;
