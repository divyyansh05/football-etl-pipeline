-- Schema updates for API-Football integration
-- Adds API-Football specific IDs to avoid redundant lookups
-- Run this after the main schema.sql

-- ============================================
-- ADD API-FOOTBALL ID COLUMNS
-- ============================================

-- Add API-Football ID to leagues table
ALTER TABLE leagues
ADD COLUMN IF NOT EXISTS api_football_id INTEGER;

-- Add unique index on api_football_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_leagues_api_football_id
ON leagues(api_football_id) WHERE api_football_id IS NOT NULL;

-- Add API-Football ID to teams table
ALTER TABLE teams
ADD COLUMN IF NOT EXISTS api_football_id INTEGER;

-- Add index on api_football_id for fast lookups
CREATE INDEX IF NOT EXISTS idx_teams_api_football_id
ON teams(api_football_id) WHERE api_football_id IS NOT NULL;

-- Add API-Football ID to players table
ALTER TABLE players
ADD COLUMN IF NOT EXISTS api_football_id INTEGER;

-- Add unique index on api_football_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_api_football_id
ON players(api_football_id) WHERE api_football_id IS NOT NULL;

-- Add API-Football fixture ID to matches table
ALTER TABLE matches
ADD COLUMN IF NOT EXISTS api_football_fixture_id INTEGER;

-- Add unique index on api_football_fixture_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_matches_api_football_fixture_id
ON matches(api_football_fixture_id) WHERE api_football_fixture_id IS NOT NULL;

-- ============================================
-- POPULATE LEAGUE API-FOOTBALL IDS
-- ============================================

-- Set known API-Football league IDs
UPDATE leagues SET api_football_id = 39 WHERE league_name = 'Premier League' AND country = 'England';
UPDATE leagues SET api_football_id = 140 WHERE league_name = 'La Liga' AND country = 'Spain';
UPDATE leagues SET api_football_id = 135 WHERE league_name = 'Serie A' AND country = 'Italy';
UPDATE leagues SET api_football_id = 78 WHERE league_name = 'Bundesliga' AND country = 'Germany';
UPDATE leagues SET api_football_id = 61 WHERE league_name = 'Ligue 1' AND country = 'France';

-- ============================================
-- API REQUEST TRACKING TABLE
-- ============================================

-- Create table to track API usage per day
CREATE TABLE IF NOT EXISTS api_usage_tracking (
    tracking_id SERIAL PRIMARY KEY,
    tracking_date DATE NOT NULL DEFAULT CURRENT_DATE,
    source_name VARCHAR(50) NOT NULL DEFAULT 'api_football',
    endpoint VARCHAR(100),
    league_key VARCHAR(50),
    requests_count INTEGER DEFAULT 1,
    last_request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tracking_date, source_name)
);

-- Create detailed request log
CREATE TABLE IF NOT EXISTS api_request_log (
    request_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL DEFAULT 'api_football',
    endpoint VARCHAR(100) NOT NULL,
    params JSONB,
    response_status INTEGER,
    response_time_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for efficient date lookups
CREATE INDEX IF NOT EXISTS idx_api_usage_date ON api_usage_tracking(tracking_date);
CREATE INDEX IF NOT EXISTS idx_api_request_log_date ON api_request_log(created_at);

-- ============================================
-- COLLECTION JOB TRACKING
-- ============================================

-- Track ETL job executions
CREATE TABLE IF NOT EXISTS etl_job_runs (
    job_run_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,  -- 'daily', 'priority', 'full', 'manual'
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',  -- 'running', 'completed', 'failed'
    leagues_processed TEXT[],
    teams_collected INTEGER DEFAULT 0,
    matches_collected INTEGER DEFAULT 0,
    players_collected INTEGER DEFAULT 0,
    requests_used INTEGER DEFAULT 0,
    error_message TEXT,
    job_config JSONB
);

-- Index for job tracking queries
CREATE INDEX IF NOT EXISTS idx_etl_job_runs_date ON etl_job_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_etl_job_runs_status ON etl_job_runs(status);

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to get today's API usage count
CREATE OR REPLACE FUNCTION get_api_usage_today(p_source VARCHAR DEFAULT 'api_football')
RETURNS INTEGER AS $$
DECLARE
    usage_count INTEGER;
BEGIN
    SELECT COALESCE(requests_count, 0) INTO usage_count
    FROM api_usage_tracking
    WHERE tracking_date = CURRENT_DATE AND source_name = p_source;

    RETURN COALESCE(usage_count, 0);
END;
$$ LANGUAGE plpgsql;

-- Function to increment API usage
CREATE OR REPLACE FUNCTION increment_api_usage(
    p_source VARCHAR DEFAULT 'api_football',
    p_endpoint VARCHAR DEFAULT NULL,
    p_league VARCHAR DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    INSERT INTO api_usage_tracking (tracking_date, source_name, endpoint, league_key, requests_count)
    VALUES (CURRENT_DATE, p_source, p_endpoint, p_league, 1)
    ON CONFLICT (tracking_date, source_name)
    DO UPDATE SET
        requests_count = api_usage_tracking.requests_count + 1,
        last_request_time = CURRENT_TIMESTAMP,
        endpoint = COALESCE(EXCLUDED.endpoint, api_usage_tracking.endpoint),
        league_key = COALESCE(EXCLUDED.league_key, api_usage_tracking.league_key)
    RETURNING requests_count INTO new_count;

    RETURN new_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get remaining requests for today
CREATE OR REPLACE FUNCTION get_remaining_requests(
    p_source VARCHAR DEFAULT 'api_football',
    p_daily_limit INTEGER DEFAULT 100
)
RETURNS INTEGER AS $$
BEGIN
    RETURN p_daily_limit - get_api_usage_today(p_source);
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- VIEWS FOR MONITORING
-- ============================================

-- View: Daily API usage summary
CREATE OR REPLACE VIEW v_daily_api_usage AS
SELECT
    tracking_date,
    source_name,
    requests_count,
    100 - requests_count as remaining_requests,
    last_request_time,
    CASE
        WHEN requests_count >= 100 THEN 'EXHAUSTED'
        WHEN requests_count >= 90 THEN 'LOW'
        WHEN requests_count >= 50 THEN 'MODERATE'
        ELSE 'GOOD'
    END as quota_status
FROM api_usage_tracking
ORDER BY tracking_date DESC;

-- View: Recent job runs
CREATE OR REPLACE VIEW v_recent_job_runs AS
SELECT
    job_run_id,
    job_name,
    job_type,
    started_at,
    completed_at,
    EXTRACT(EPOCH FROM (completed_at - started_at))::INTEGER as duration_seconds,
    status,
    array_length(leagues_processed, 1) as leagues_count,
    teams_collected,
    matches_collected,
    players_collected,
    requests_used,
    error_message
FROM etl_job_runs
ORDER BY started_at DESC
LIMIT 50;

-- View: Team with all external IDs
CREATE OR REPLACE VIEW v_teams_with_ids AS
SELECT
    t.team_id,
    t.team_name,
    l.league_name,
    t.api_football_id,
    t.fotmob_id,
    t.transfermarkt_id,
    CASE WHEN t.api_football_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_api_football_id
FROM teams t
JOIN leagues l ON t.league_id = l.league_id
ORDER BY l.league_name, t.team_name;

-- ============================================
-- ADD API-FOOTBALL DATA SOURCE
-- ============================================

INSERT INTO data_sources (source_name, base_url, reliability_score)
VALUES ('api_football', 'https://api-football-v1.p.rapidapi.com', 98)
ON CONFLICT (source_name) DO UPDATE SET reliability_score = 98;
