-- Migration 001: Add API-Football ID columns
-- Date: 2026-02-04
-- Purpose: Add external ID columns for API-Football integration
--
-- This migration adds api_football_id columns to leagues, teams, players, and matches
-- to enable efficient lookups and avoid redundant API calls.
--
-- Run with: docker exec football_etl_db psql -U postgres -d football_data -f /path/to/this/file.sql
-- Or copy to: docker exec -i football_etl_db psql -U postgres -d football_data < migration_001_api_football_ids.sql

BEGIN;

-- Add api_football_id to leagues table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'leagues' AND column_name = 'api_football_id'
    ) THEN
        ALTER TABLE leagues ADD COLUMN api_football_id INTEGER;
        RAISE NOTICE 'Added api_football_id column to leagues table';
    ELSE
        RAISE NOTICE 'api_football_id column already exists on leagues table';
    END IF;
END $$;

-- Create index on leagues.api_football_id
CREATE INDEX IF NOT EXISTS idx_leagues_api_football_id ON leagues(api_football_id);

-- Add api_football_id to teams table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'teams' AND column_name = 'api_football_id'
    ) THEN
        ALTER TABLE teams ADD COLUMN api_football_id INTEGER;
        RAISE NOTICE 'Added api_football_id column to teams table';
    ELSE
        RAISE NOTICE 'api_football_id column already exists on teams table';
    END IF;
END $$;

-- Create index on teams.api_football_id
CREATE INDEX IF NOT EXISTS idx_teams_api_football_id ON teams(api_football_id);

-- Add api_football_id to players table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'players' AND column_name = 'api_football_id'
    ) THEN
        ALTER TABLE players ADD COLUMN api_football_id INTEGER;
        RAISE NOTICE 'Added api_football_id column to players table';
    ELSE
        RAISE NOTICE 'api_football_id column already exists on players table';
    END IF;
END $$;

-- Create index on players.api_football_id
CREATE INDEX IF NOT EXISTS idx_players_api_football_id ON players(api_football_id);

-- Add api_football_fixture_id to matches table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'matches' AND column_name = 'api_football_fixture_id'
    ) THEN
        ALTER TABLE matches ADD COLUMN api_football_fixture_id INTEGER;
        RAISE NOTICE 'Added api_football_fixture_id column to matches table';
    ELSE
        RAISE NOTICE 'api_football_fixture_id column already exists on matches table';
    END IF;
END $$;

-- Create index on matches.api_football_fixture_id
CREATE INDEX IF NOT EXISTS idx_matches_api_football_fixture_id ON matches(api_football_fixture_id);

-- Update known API-Football league IDs
UPDATE leagues SET api_football_id = 39 WHERE league_name = 'Premier League' AND api_football_id IS NULL;
UPDATE leagues SET api_football_id = 140 WHERE league_name = 'La Liga' AND api_football_id IS NULL;
UPDATE leagues SET api_football_id = 135 WHERE league_name = 'Serie A' AND api_football_id IS NULL;
UPDATE leagues SET api_football_id = 78 WHERE league_name = 'Bundesliga' AND api_football_id IS NULL;
UPDATE leagues SET api_football_id = 61 WHERE league_name = 'Ligue 1' AND api_football_id IS NULL;

COMMIT;

-- Verify the migration
SELECT 'Migration 001 completed successfully!' as status;

SELECT
    'leagues' as table_name,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'leagues' AND column_name = 'api_football_id') as has_column
UNION ALL
SELECT
    'teams',
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'teams' AND column_name = 'api_football_id')
UNION ALL
SELECT
    'players',
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'players' AND column_name = 'api_football_id')
UNION ALL
SELECT
    'matches',
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'matches' AND column_name = 'api_football_fixture_id');
