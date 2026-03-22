-- Schema for Match Events (Goals, Cards, Substitutions, etc.)
-- This enables detailed match-level analysis
-- Run this after the main schema

-- ============================================
-- MATCH EVENTS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS match_events (
    event_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,  -- 'goal', 'card', 'subst', 'var'
    event_detail VARCHAR(100),         -- 'Normal Goal', 'Penalty', 'Own Goal', 'Yellow Card', etc.
    event_time INTEGER NOT NULL,       -- Minute of the event
    event_time_extra INTEGER,          -- Added time minutes (e.g., 90+3)

    -- Player involved
    player_id INTEGER REFERENCES players(player_id),
    player_name VARCHAR(150),          -- Denormalized for quick access

    -- Secondary player (assist, substituted player)
    player2_id INTEGER REFERENCES players(player_id),
    player2_name VARCHAR(150),

    -- Team
    team_id INTEGER REFERENCES teams(team_id),
    team_name VARCHAR(100),

    -- Additional details
    comments TEXT,

    -- External IDs
    api_football_event_id INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_event_type CHECK (event_type IN (
        'goal', 'card', 'subst', 'var', 'penalty_missed', 'penalty_saved'
    ))
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_match_events_match ON match_events(match_id);
CREATE INDEX IF NOT EXISTS idx_match_events_player ON match_events(player_id);
CREATE INDEX IF NOT EXISTS idx_match_events_team ON match_events(team_id);
CREATE INDEX IF NOT EXISTS idx_match_events_type ON match_events(event_type);
CREATE INDEX IF NOT EXISTS idx_match_events_time ON match_events(event_time);

-- ============================================
-- MATCH LINEUPS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS match_lineups (
    lineup_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    player_id INTEGER REFERENCES players(player_id),
    player_name VARCHAR(150) NOT NULL,

    -- Position and role
    position VARCHAR(20),              -- GK, CB, LB, etc.
    grid_position VARCHAR(10),         -- Formation grid (e.g., "1:1" for GK)
    is_starter BOOLEAN DEFAULT TRUE,
    jersey_number INTEGER,

    -- Rating/performance
    rating DECIMAL(3,1),               -- Match rating (0-10)
    minutes_played INTEGER,

    -- Captain/substitute info
    is_captain BOOLEAN DEFAULT FALSE,
    substituted_in_minute INTEGER,     -- NULL if starter
    substituted_out_minute INTEGER,    -- NULL if played full match

    -- External IDs
    api_football_player_id INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id, team_id, player_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_match_lineups_match ON match_lineups(match_id);
CREATE INDEX IF NOT EXISTS idx_match_lineups_team ON match_lineups(team_id);
CREATE INDEX IF NOT EXISTS idx_match_lineups_player ON match_lineups(player_id);

-- ============================================
-- MATCH STATISTICS TABLE (Team-level)
-- ============================================

CREATE TABLE IF NOT EXISTS match_statistics (
    stat_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(team_id),

    -- Possession
    possession_percent DECIMAL(4,1),

    -- Shots
    shots_total INTEGER,
    shots_on_target INTEGER,
    shots_off_target INTEGER,
    shots_blocked INTEGER,
    shots_inside_box INTEGER,
    shots_outside_box INTEGER,

    -- Passing
    passes_total INTEGER,
    passes_accurate INTEGER,
    passes_percent DECIMAL(4,1),

    -- Fouls and cards
    fouls INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,

    -- Corners and set pieces
    corner_kicks INTEGER,
    offsides INTEGER,

    -- Goalkeeper
    goalkeeper_saves INTEGER,

    -- Ball possession
    ball_possession DECIMAL(4,1),

    -- Expected stats (if available)
    expected_goals DECIMAL(4,2),
    expected_goals_against DECIMAL(4,2),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id, team_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_match_statistics_match ON match_statistics(match_id);
CREATE INDEX IF NOT EXISTS idx_match_statistics_team ON match_statistics(team_id);

-- ============================================
-- PLAYER MATCH STATISTICS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS player_match_stats (
    stat_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id INTEGER NOT NULL REFERENCES players(player_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),

    -- Time
    minutes_played INTEGER,
    is_starter BOOLEAN DEFAULT FALSE,

    -- Rating
    rating DECIMAL(3,1),

    -- Attack
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots_total INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,

    -- Passing
    passes_total INTEGER DEFAULT 0,
    passes_accurate INTEGER DEFAULT 0,
    passes_key INTEGER DEFAULT 0,
    passes_accuracy DECIMAL(4,1),

    -- Defense
    tackles_total INTEGER DEFAULT 0,
    tackles_won INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,

    -- Duels
    duels_total INTEGER DEFAULT 0,
    duels_won INTEGER DEFAULT 0,
    aerial_duels_total INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,

    -- Dribbles
    dribbles_attempts INTEGER DEFAULT 0,
    dribbles_success INTEGER DEFAULT 0,
    dribbles_past INTEGER DEFAULT 0,  -- times dribbled past

    -- Fouls
    fouls_committed INTEGER DEFAULT 0,
    fouls_drawn INTEGER DEFAULT 0,

    -- Cards
    yellow_card BOOLEAN DEFAULT FALSE,
    red_card BOOLEAN DEFAULT FALSE,

    -- Offsides
    offsides INTEGER DEFAULT 0,

    -- Goalkeeper specific
    saves INTEGER DEFAULT 0,
    goals_conceded INTEGER DEFAULT 0,
    penalty_saves INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id, player_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_player_match_stats_match ON player_match_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player ON player_match_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_team ON player_match_stats(team_id);

-- ============================================
-- SHOT EVENTS TABLE (for xG calculations)
-- ============================================

CREATE TABLE IF NOT EXISTS shot_events (
    shot_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),

    -- Timing
    minute INTEGER NOT NULL,
    minute_extra INTEGER,

    -- Location (normalized 0-100 scale)
    location_x DECIMAL(5,2),           -- 0 = own goal line, 100 = opponent goal line
    location_y DECIMAL(5,2),           -- 0 = left touchline, 100 = right touchline

    -- Shot details
    shot_type VARCHAR(50),             -- 'open_play', 'penalty', 'free_kick', 'corner', 'header'
    body_part VARCHAR(20),             -- 'right_foot', 'left_foot', 'head', 'other'
    technique VARCHAR(50),             -- 'normal', 'volley', 'half_volley', 'lob', etc.

    -- Outcome
    outcome VARCHAR(20) NOT NULL,      -- 'goal', 'saved', 'off_target', 'blocked', 'post'
    is_goal BOOLEAN DEFAULT FALSE,

    -- Expected Goals
    xg DECIMAL(4,3),                   -- Expected goals value (0-1)
    xg_model VARCHAR(50),              -- Which model calculated xG

    -- Context
    assist_player_id INTEGER REFERENCES players(player_id),
    play_pattern VARCHAR(50),          -- 'regular_play', 'counter_attack', 'set_piece', etc.
    under_pressure BOOLEAN,

    -- External data
    api_football_shot_id INTEGER,
    statsbomb_shot_id VARCHAR(100),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_shot_events_match ON shot_events(match_id);
CREATE INDEX IF NOT EXISTS idx_shot_events_player ON shot_events(player_id);
CREATE INDEX IF NOT EXISTS idx_shot_events_team ON shot_events(team_id);
CREATE INDEX IF NOT EXISTS idx_shot_events_is_goal ON shot_events(is_goal);
CREATE INDEX IF NOT EXISTS idx_shot_events_xg ON shot_events(xg);

-- ============================================
-- VIEWS FOR MATCH ANALYSIS
-- ============================================

-- View: Match events summary
CREATE OR REPLACE VIEW v_match_events_summary AS
SELECT
    m.match_id,
    m.match_date,
    ht.team_name as home_team,
    at.team_name as away_team,
    m.home_score,
    m.away_score,
    COUNT(CASE WHEN me.event_type = 'goal' THEN 1 END) as total_goals,
    COUNT(CASE WHEN me.event_type = 'card' AND me.event_detail = 'Yellow Card' THEN 1 END) as yellow_cards,
    COUNT(CASE WHEN me.event_type = 'card' AND me.event_detail = 'Red Card' THEN 1 END) as red_cards,
    COUNT(CASE WHEN me.event_type = 'subst' THEN 1 END) as substitutions
FROM matches m
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
LEFT JOIN match_events me ON m.match_id = me.match_id
GROUP BY m.match_id, m.match_date, ht.team_name, at.team_name, m.home_score, m.away_score;

-- View: Player match performance
CREATE OR REPLACE VIEW v_player_match_performance AS
SELECT
    pms.match_id,
    m.match_date,
    p.player_name,
    t.team_name,
    pms.minutes_played,
    pms.rating,
    pms.goals,
    pms.assists,
    pms.shots_total,
    pms.shots_on_target,
    pms.passes_total,
    pms.passes_accuracy,
    pms.tackles_won,
    pms.interceptions,
    pms.duels_won,
    pms.dribbles_success,
    pms.yellow_card,
    pms.red_card
FROM player_match_stats pms
JOIN matches m ON pms.match_id = m.match_id
JOIN players p ON pms.player_id = p.player_id
JOIN teams t ON pms.team_id = t.team_id;

-- View: xG by player per match
CREATE OR REPLACE VIEW v_player_match_xg AS
SELECT
    se.match_id,
    m.match_date,
    p.player_name,
    t.team_name,
    COUNT(*) as shots,
    SUM(CASE WHEN se.is_goal THEN 1 ELSE 0 END) as goals,
    SUM(se.xg) as total_xg,
    SUM(CASE WHEN se.is_goal THEN 1 ELSE 0 END) - SUM(se.xg) as xg_overperformance
FROM shot_events se
JOIN matches m ON se.match_id = m.match_id
JOIN players p ON se.player_id = p.player_id
JOIN teams t ON se.team_id = t.team_id
GROUP BY se.match_id, m.match_date, p.player_name, t.team_name;

-- View: Team xG per match
CREATE OR REPLACE VIEW v_team_match_xg AS
SELECT
    se.match_id,
    m.match_date,
    l.league_name,
    CASE
        WHEN se.team_id = m.home_team_id THEN ht.team_name
        ELSE at.team_name
    END as team_name,
    CASE
        WHEN se.team_id = m.home_team_id THEN 'home'
        ELSE 'away'
    END as venue,
    CASE
        WHEN se.team_id = m.home_team_id THEN m.home_score
        ELSE m.away_score
    END as goals_scored,
    COUNT(*) as shots,
    SUM(se.xg) as total_xg,
    AVG(se.xg) as avg_xg_per_shot
FROM shot_events se
JOIN matches m ON se.match_id = m.match_id
JOIN leagues l ON m.league_id = l.league_id
JOIN teams ht ON m.home_team_id = ht.team_id
JOIN teams at ON m.away_team_id = at.team_id
GROUP BY se.match_id, m.match_date, l.league_name, se.team_id, m.home_team_id,
         ht.team_name, at.team_name, m.home_score, m.away_score;

-- ============================================
-- FUNCTIONS FOR MATCH EVENTS
-- ============================================

-- Function to get scorers for a match
CREATE OR REPLACE FUNCTION get_match_scorers(p_match_id INTEGER)
RETURNS TABLE (
    player_name VARCHAR(150),
    team_name VARCHAR(100),
    minute INTEGER,
    event_detail VARCHAR(100)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        me.player_name,
        me.team_name,
        me.event_time as minute,
        me.event_detail
    FROM match_events me
    WHERE me.match_id = p_match_id
    AND me.event_type = 'goal'
    ORDER BY me.event_time;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate team xG for a season
CREATE OR REPLACE FUNCTION get_team_season_xg(p_team_id INTEGER, p_season VARCHAR)
RETURNS TABLE (
    matches_played BIGINT,
    goals_scored BIGINT,
    total_xg DECIMAL,
    xg_per_match DECIMAL,
    shots_total BIGINT,
    xg_per_shot DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(DISTINCT se.match_id) as matches_played,
        SUM(CASE WHEN se.is_goal THEN 1 ELSE 0 END)::BIGINT as goals_scored,
        SUM(se.xg) as total_xg,
        SUM(se.xg) / NULLIF(COUNT(DISTINCT se.match_id), 0) as xg_per_match,
        COUNT(*)::BIGINT as shots_total,
        SUM(se.xg) / NULLIF(COUNT(*), 0) as xg_per_shot
    FROM shot_events se
    JOIN matches m ON se.match_id = m.match_id
    WHERE se.team_id = p_team_id
    AND m.season = p_season;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- COMMENT ON TABLES
-- ============================================

COMMENT ON TABLE match_events IS 'Stores all match events: goals, cards, substitutions, VAR decisions';
COMMENT ON TABLE match_lineups IS 'Stores starting XI and substitutes for each match';
COMMENT ON TABLE match_statistics IS 'Team-level match statistics (possession, shots, passes, etc.)';
COMMENT ON TABLE player_match_stats IS 'Individual player statistics for each match';
COMMENT ON TABLE shot_events IS 'Detailed shot data including location and xG values';
