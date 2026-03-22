-- Migration 004: Identity Resolution Layer
-- Creates infrastructure for cross-source player matching
-- Part of ISSUE-025 fix

BEGIN;

-- ============================================
-- PLAYER IDENTITY RESOLUTION
-- ============================================

-- Cross-source player ID mappings
CREATE TABLE IF NOT EXISTS player_id_mappings (
    mapping_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,

    -- External source IDs
    fotmob_id INTEGER,
    api_football_id INTEGER,
    transfermarkt_id VARCHAR(50),
    statsbomb_id INTEGER,
    understat_id INTEGER,
    whoscored_id INTEGER,
    sofascore_id INTEGER,

    -- Matching metadata
    confidence_score DECIMAL(4,2) DEFAULT 0.00,  -- 0-1 scale
    match_method VARCHAR(50),  -- 'exact_id', 'fuzzy_name', 'manual', 'auto'
    verified_by VARCHAR(50),   -- 'system', 'manual', 'high_confidence'
    verified_at TIMESTAMP,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure one mapping per player
    UNIQUE(player_id)
);

-- Create indexes for fast lookups by external ID
CREATE INDEX IF NOT EXISTS idx_player_mappings_fotmob ON player_id_mappings(fotmob_id);
CREATE INDEX IF NOT EXISTS idx_player_mappings_api_football ON player_id_mappings(api_football_id);
CREATE INDEX IF NOT EXISTS idx_player_mappings_transfermarkt ON player_id_mappings(transfermarkt_id);
CREATE INDEX IF NOT EXISTS idx_player_mappings_statsbomb ON player_id_mappings(statsbomb_id);
CREATE INDEX IF NOT EXISTS idx_player_mappings_understat ON player_id_mappings(understat_id);
CREATE INDEX IF NOT EXISTS idx_player_mappings_confidence ON player_id_mappings(confidence_score);

-- Team identity resolution
CREATE TABLE IF NOT EXISTS team_id_mappings (
    mapping_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,

    -- External source IDs
    fotmob_id INTEGER,
    api_football_id INTEGER,
    transfermarkt_id VARCHAR(50),
    statsbomb_id INTEGER,
    whoscored_id INTEGER,
    sofascore_id INTEGER,

    -- Matching metadata
    confidence_score DECIMAL(4,2) DEFAULT 0.00,
    verified_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_mappings_fotmob ON team_id_mappings(fotmob_id);
CREATE INDEX IF NOT EXISTS idx_team_mappings_api_football ON team_id_mappings(api_football_id);

-- ============================================
-- DATA QUALITY TRACKING (ISSUE-026)
-- ============================================

-- Track ETL run history with quality metrics
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id SERIAL PRIMARY KEY,
    run_type VARCHAR(50) NOT NULL,  -- 'fotmob_daily', 'api_football_weekly', etc.
    source_name VARCHAR(50) NOT NULL,

    -- Timing
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,

    -- Input/Output counts
    records_fetched INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,

    -- Quality metrics
    quality_score DECIMAL(4,2),  -- Overall quality 0-100
    completeness_score DECIMAL(4,2),  -- % of expected fields populated
    validity_score DECIMAL(4,2),  -- % passing validation rules
    anomaly_count INTEGER DEFAULT 0,

    -- Context
    parameters JSONB,  -- Input parameters (leagues, seasons, etc.)
    error_summary TEXT,

    status VARCHAR(20) DEFAULT 'running'  -- 'running', 'completed', 'failed', 'partial'
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_type ON etl_runs(run_type);
CREATE INDEX IF NOT EXISTS idx_etl_runs_source ON etl_runs(source_name);
CREATE INDEX IF NOT EXISTS idx_etl_runs_status ON etl_runs(status);
CREATE INDEX IF NOT EXISTS idx_etl_runs_started ON etl_runs(started_at DESC);

-- Track data quality issues
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id SERIAL PRIMARY KEY,
    etl_run_id INTEGER REFERENCES etl_runs(run_id),

    -- Issue classification
    severity VARCHAR(20) NOT NULL,  -- 'critical', 'high', 'medium', 'low'
    category VARCHAR(50) NOT NULL,  -- 'missing_data', 'invalid_value', 'anomaly', 'duplicate'

    -- Issue details
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    record_id INTEGER,
    issue_description TEXT NOT NULL,

    -- Affected data
    expected_value TEXT,
    actual_value TEXT,

    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_issues_severity ON data_quality_issues(severity);
CREATE INDEX IF NOT EXISTS idx_quality_issues_category ON data_quality_issues(category);
CREATE INDEX IF NOT EXISTS idx_quality_issues_unresolved ON data_quality_issues(resolved) WHERE resolved = FALSE;

-- ============================================
-- BRONZE/SILVER/GOLD PATTERN (ISSUE-027)
-- ============================================

-- Raw payload storage (Bronze layer)
CREATE TABLE IF NOT EXISTS raw_payloads (
    payload_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,

    -- The raw response
    payload JSONB NOT NULL,

    -- Request context
    request_params JSONB,
    response_status INTEGER,

    -- Timing
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,

    -- ETL tracking
    etl_run_id INTEGER REFERENCES etl_runs(run_id),
    processing_status VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'processed', 'failed', 'skipped'

    -- Deduplication
    payload_hash VARCHAR(64)  -- SHA256 of payload for dedup
);

CREATE INDEX IF NOT EXISTS idx_raw_payloads_source ON raw_payloads(source_name);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_endpoint ON raw_payloads(endpoint);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_status ON raw_payloads(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_fetched ON raw_payloads(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_payloads_hash ON raw_payloads(payload_hash);

-- Incremental load tracking (watermarks)
CREATE TABLE IF NOT EXISTS etl_watermarks (
    watermark_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,  -- 'player', 'team', 'match', 'stats'

    -- Watermark value (timestamp or ID)
    last_processed_at TIMESTAMP,
    last_processed_id VARCHAR(100),

    -- Metadata
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_name, entity_type)
);

-- ============================================
-- DERIVED SCOUTING VIEWS (ISSUE-024)
-- ============================================

-- Progressive profiles view
CREATE OR REPLACE VIEW vw_progressive_profiles AS
SELECT
    p.player_id,
    p.player_name,
    p.position,
    p.nationality,
    t.team_name,
    l.league_name,
    s.season_name,

    -- Basic stats
    pss.matches_played,
    pss.minutes,
    pss.goals,
    pss.assists,

    -- xG metrics
    pss.xg,
    pss.npxg,
    pss.xag AS xa,
    CASE WHEN pss.minutes > 0
         THEN ROUND((pss.xg::numeric / pss.minutes * 90), 2)
         ELSE 0 END AS xg_per_90,
    CASE WHEN pss.minutes > 0
         THEN ROUND((pss.xag::numeric / pss.minutes * 90), 2)
         ELSE 0 END AS xa_per_90,

    -- Goal contribution metrics
    pss.goals + pss.assists AS goal_contributions,
    CASE WHEN pss.minutes > 0
         THEN ROUND(((pss.goals + pss.assists)::numeric / pss.minutes * 90), 2)
         ELSE 0 END AS gc_per_90,

    -- Progressive actions (estimated from available data)
    pss.key_passes,
    pss.dribbles_completed AS successful_dribbles,

    -- Defensive actions
    pss.tackles,
    pss.interceptions,
    pss.clearances,
    pss.blocks,

    -- Composite scores
    CASE
        WHEN p.position IN ('Forward', 'Attacker', 'ST', 'CF', 'LW', 'RW')
        THEN ROUND((COALESCE(pss.xg, 0) + COALESCE(pss.xag, 0) * 0.8)::numeric, 2)
        WHEN p.position IN ('Midfielder', 'MID', 'CM', 'CAM', 'CDM')
        THEN ROUND((COALESCE(pss.xag, 0) * 1.2 + COALESCE(pss.key_passes, 0) * 0.05)::numeric, 2)
        WHEN p.position IN ('Defender', 'DEF', 'CB', 'LB', 'RB')
        THEN ROUND((COALESCE(pss.tackles, 0) * 0.1 + COALESCE(pss.interceptions, 0) * 0.15 + COALESCE(pss.clearances, 0) * 0.05)::numeric, 2)
        ELSE 0
    END AS position_adjusted_score

FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
LEFT JOIN teams t ON pss.team_id = t.team_id
LEFT JOIN leagues l ON pss.league_id = l.league_id
LEFT JOIN seasons s ON pss.season_id = s.season_id
WHERE pss.minutes >= 90;  -- At least 1 full match

-- PPDA (Passes Per Defensive Action) team view
-- Requires match-level data, approximated from available stats
CREATE OR REPLACE VIEW vw_team_pressing_profile AS
SELECT
    t.team_id,
    t.team_name,
    l.league_name,
    s.season_name,

    -- Team season stats
    tss.matches_played,
    tss.goals_for,
    tss.goals_against,
    tss.xg_for,
    tss.xg_against,

    -- Performance differential
    ROUND((tss.goals_for::numeric - COALESCE(tss.xg_for, tss.goals_for)), 2) AS goals_vs_xg,
    ROUND((tss.goals_against::numeric - COALESCE(tss.xg_against, tss.goals_against)), 2) AS goals_conceded_vs_xga,

    -- Points per game
    CASE WHEN tss.matches_played > 0
         THEN ROUND((tss.points::numeric / tss.matches_played), 2)
         ELSE 0 END AS points_per_game,

    -- xG-based expected points (simplified)
    CASE WHEN tss.matches_played > 0 AND tss.xg_for IS NOT NULL AND tss.xg_against IS NOT NULL
         THEN ROUND(((COALESCE(tss.xg_for, 0) - COALESCE(tss.xg_against, 0)) / tss.matches_played * 3), 2)
         ELSE NULL END AS x_points_per_game

FROM team_season_stats tss
JOIN teams t ON tss.team_id = t.team_id
JOIN leagues l ON tss.league_id = l.league_id
JOIN seasons s ON tss.season_id = s.season_id;

-- Shot quality zones view (approximation)
CREATE OR REPLACE VIEW vw_shot_quality AS
SELECT
    p.player_id,
    p.player_name,
    t.team_name,
    l.league_name,
    s.season_name,

    pss.shots,
    pss.shots_on_target,
    pss.goals,
    pss.xg,

    -- Shot conversion metrics
    CASE WHEN pss.shots > 0
         THEN ROUND((pss.shots_on_target::numeric / pss.shots * 100), 1)
         ELSE 0 END AS shot_accuracy_pct,
    CASE WHEN pss.shots > 0
         THEN ROUND((pss.goals::numeric / pss.shots * 100), 1)
         ELSE 0 END AS conversion_rate_pct,

    -- xG per shot (shot quality indicator)
    CASE WHEN pss.shots > 0 AND pss.xg IS NOT NULL
         THEN ROUND((pss.xg::numeric / pss.shots), 3)
         ELSE NULL END AS xg_per_shot,

    -- Finishing quality (goals vs xG)
    CASE WHEN pss.xg > 0
         THEN ROUND((pss.goals::numeric / pss.xg), 2)
         ELSE NULL END AS finishing_ratio

FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
LEFT JOIN teams t ON pss.team_id = t.team_id
LEFT JOIN leagues l ON pss.league_id = l.league_id
LEFT JOIN seasons s ON pss.season_id = s.season_id
WHERE pss.shots > 0;

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to find player by any external ID
CREATE OR REPLACE FUNCTION find_player_by_external_id(
    p_fotmob_id INTEGER DEFAULT NULL,
    p_api_football_id INTEGER DEFAULT NULL,
    p_transfermarkt_id VARCHAR DEFAULT NULL,
    p_statsbomb_id INTEGER DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_player_id INTEGER;
BEGIN
    -- Try fotmob_id first (primary source)
    IF p_fotmob_id IS NOT NULL THEN
        SELECT player_id INTO v_player_id
        FROM player_id_mappings
        WHERE fotmob_id = p_fotmob_id;

        IF v_player_id IS NOT NULL THEN
            RETURN v_player_id;
        END IF;

        -- Fallback to players table
        SELECT player_id INTO v_player_id
        FROM players
        WHERE fotmob_id = p_fotmob_id;

        IF v_player_id IS NOT NULL THEN
            RETURN v_player_id;
        END IF;
    END IF;

    -- Try api_football_id
    IF p_api_football_id IS NOT NULL THEN
        SELECT player_id INTO v_player_id
        FROM player_id_mappings
        WHERE api_football_id = p_api_football_id;

        IF v_player_id IS NOT NULL THEN
            RETURN v_player_id;
        END IF;
    END IF;

    -- Try transfermarkt_id
    IF p_transfermarkt_id IS NOT NULL THEN
        SELECT player_id INTO v_player_id
        FROM player_id_mappings
        WHERE transfermarkt_id = p_transfermarkt_id;

        IF v_player_id IS NOT NULL THEN
            RETURN v_player_id;
        END IF;
    END IF;

    -- Try statsbomb_id
    IF p_statsbomb_id IS NOT NULL THEN
        SELECT player_id INTO v_player_id
        FROM player_id_mappings
        WHERE statsbomb_id = p_statsbomb_id;

        IF v_player_id IS NOT NULL THEN
            RETURN v_player_id;
        END IF;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Migrate existing IDs to mappings table
INSERT INTO player_id_mappings (player_id, fotmob_id, confidence_score, match_method, verified_by)
SELECT
    player_id,
    fotmob_id,
    1.00,  -- High confidence for existing IDs
    'exact_id',
    'system'
FROM players
WHERE fotmob_id IS NOT NULL
ON CONFLICT (player_id) DO UPDATE SET
    fotmob_id = EXCLUDED.fotmob_id,
    updated_at = CURRENT_TIMESTAMP;

-- Migrate team IDs
INSERT INTO team_id_mappings (team_id, fotmob_id, confidence_score)
SELECT
    team_id,
    fotmob_id,
    1.00
FROM teams
WHERE fotmob_id IS NOT NULL
ON CONFLICT (team_id) DO UPDATE SET
    fotmob_id = EXCLUDED.fotmob_id,
    updated_at = CURRENT_TIMESTAMP;

COMMIT;
