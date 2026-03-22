-- Migration 002: FotMob integration + League expansion
-- Adds FotMob ID columns, 3 new leagues, player enrichment, Transfermarkt prep
-- Run after schema.sql and schema_api_football.sql

BEGIN;

-- ============================================
-- ADD FOTMOB ID COLUMNS
-- ============================================

ALTER TABLE leagues ADD COLUMN IF NOT EXISTS fotmob_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_leagues_fotmob_id ON leagues(fotmob_id);

ALTER TABLE teams ADD COLUMN IF NOT EXISTS fotmob_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_teams_fotmob_id ON teams(fotmob_id);

ALTER TABLE players ADD COLUMN IF NOT EXISTS fotmob_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_players_fotmob_id ON players(fotmob_id);

ALTER TABLE matches ADD COLUMN IF NOT EXISTS fotmob_match_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_matches_fotmob_match_id ON matches(fotmob_match_id);

-- ============================================
-- ADD PLAYER ENRICHMENT COLUMNS (from FotMob)
-- ============================================

ALTER TABLE players ADD COLUMN IF NOT EXISTS market_value VARCHAR(50);
ALTER TABLE players ADD COLUMN IF NOT EXISTS current_team_id INTEGER REFERENCES teams(team_id);
ALTER TABLE players ADD COLUMN IF NOT EXISTS weight_kg INTEGER;

-- Ensure preferred_foot exists
ALTER TABLE players ADD COLUMN IF NOT EXISTS preferred_foot VARCHAR(10);

-- ============================================
-- TRANSFERMARKT FUTURE PREPARATION
-- ============================================

ALTER TABLE players ADD COLUMN IF NOT EXISTS market_value_eur BIGINT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS market_value_date DATE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS contract_end_date DATE;
ALTER TABLE teams ADD COLUMN IF NOT EXISTS squad_market_value_eur BIGINT;

-- ============================================
-- ADD 3 NEW LEAGUES WITH FOTMOB IDS
-- ============================================

INSERT INTO leagues (league_name, country, tier, fotmob_id) VALUES
    ('Eredivisie', 'Netherlands', 1, 57),
    ('Brasileiro Serie A', 'Brazil', 1, 268),
    ('Argentina Primera', 'Argentina', 1, 112)
ON CONFLICT (league_name, country) DO UPDATE SET fotmob_id = EXCLUDED.fotmob_id;

-- ============================================
-- SET FOTMOB IDS FOR EXISTING 5 LEAGUES
-- ============================================

UPDATE leagues SET fotmob_id = 47 WHERE league_name = 'Premier League' AND country = 'England';
UPDATE leagues SET fotmob_id = 87 WHERE league_name = 'La Liga' AND country = 'Spain';
UPDATE leagues SET fotmob_id = 55 WHERE league_name = 'Serie A' AND country = 'Italy';
UPDATE leagues SET fotmob_id = 54 WHERE league_name = 'Bundesliga' AND country = 'Germany';
UPDATE leagues SET fotmob_id = 53 WHERE league_name = 'Ligue 1' AND country = 'France';

-- ============================================
-- ENSURE HISTORICAL SEASONS EXIST
-- ============================================

INSERT INTO seasons (season_name, start_year, end_year, is_current) VALUES
    ('2022-23', 2022, 2023, FALSE),
    ('2023-24', 2023, 2024, FALSE),
    ('2024-25', 2024, 2025, TRUE)
ON CONFLICT (season_name) DO NOTHING;

UPDATE seasons SET is_current = FALSE;
UPDATE seasons SET is_current = TRUE WHERE season_name = '2024-25';

-- ============================================
-- ADD FOTMOB DATA SOURCE
-- ============================================

INSERT INTO data_sources (source_name, base_url, reliability_score)
VALUES ('fotmob', 'https://www.fotmob.com/api/', 92)
ON CONFLICT (source_name) DO UPDATE SET reliability_score = 92;

-- ============================================
-- CLEANUP: REMOVE OLD DATA SOURCES
-- ============================================

-- Remove orphaned data from old sources before deleting sources
-- (Only if those sources actually exist)
DO $$
DECLARE
    old_source_ids INTEGER[];
BEGIN
    SELECT ARRAY_AGG(source_id) INTO old_source_ids
    FROM data_sources
    WHERE source_name IN ('fbref', 'soccerway_api', 'understat');

    IF old_source_ids IS NOT NULL THEN
        DELETE FROM player_match_stats WHERE data_source_id = ANY(old_source_ids);
        DELETE FROM team_match_stats WHERE data_source_id = ANY(old_source_ids);
    END IF;
END $$;

DELETE FROM data_sources WHERE source_name IN ('fbref', 'soccerway_api', 'understat');

-- ============================================
-- DROP LEGACY COLUMNS (fbref_id, understat_id)
-- ============================================

ALTER TABLE leagues DROP COLUMN IF EXISTS fbref_id;
ALTER TABLE leagues DROP COLUMN IF EXISTS understat_name;
ALTER TABLE teams DROP COLUMN IF EXISTS fbref_id;
ALTER TABLE teams DROP COLUMN IF EXISTS understat_id;
ALTER TABLE players DROP COLUMN IF EXISTS fbref_id;
ALTER TABLE players DROP COLUMN IF EXISTS understat_id;

COMMIT;
