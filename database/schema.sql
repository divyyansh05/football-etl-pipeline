-- Football Data Pipeline Database Schema
-- PostgreSQL 13+

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop tables if they exist (for clean rebuilds, use with caution in prod)
-- CASCADE will remove dependent tables
DROP TABLE IF EXISTS player_match_stats CASCADE;
DROP TABLE IF EXISTS team_match_stats CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS player_season_stats CASCADE;
DROP TABLE IF EXISTS team_season_stats CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS seasons CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS scrape_metadata CASCADE;

-- ============================================
-- REFERENCE TABLES
-- ============================================

-- Data Sources: Track which source provided each piece of data
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) UNIQUE NOT NULL,
    base_url VARCHAR(255),
    reliability_score INTEGER DEFAULT 100,
    last_successful_scrape TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Leagues
CREATE TABLE leagues (
    league_id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    tier INTEGER DEFAULT 1,
    fotmob_id INTEGER,
    transfermarkt_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_name, country)
);

-- Seasons
CREATE TABLE seasons (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(20) NOT NULL UNIQUE,  -- e.g., "2023-24"
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ENTITY TABLES
-- ============================================

-- Teams
CREATE TABLE teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_short_name VARCHAR(50),
    league_id INTEGER REFERENCES leagues(league_id),
    fotmob_id INTEGER,
    transfermarkt_id VARCHAR(50),
    founded_year INTEGER,
    stadium VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_name, league_id)
);

CREATE INDEX idx_teams_league ON teams(league_id);
CREATE INDEX idx_teams_name ON teams(team_name);

-- Players
CREATE TABLE players (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    position VARCHAR(20),
    preferred_foot VARCHAR(10),
    height_cm INTEGER,
    fotmob_id INTEGER,
    transfermarkt_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_players_name ON players(player_name);
CREATE INDEX idx_players_position ON players(position);

-- ============================================
-- MATCH TABLES
-- ============================================

-- Matches
CREATE TABLE matches (
    match_id SERIAL PRIMARY KEY,
    league_id INTEGER REFERENCES leagues(league_id),
    season_id INTEGER REFERENCES seasons(season_id),
    match_date DATE NOT NULL,
    matchday INTEGER,
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    home_score INTEGER,
    away_score INTEGER,
    home_xg DECIMAL(5,2),
    away_xg DECIMAL(5,2),
    attendance INTEGER,
    referee VARCHAR(100),
    venue VARCHAR(100),
    match_status VARCHAR(20) DEFAULT 'completed',  -- completed, postponed, cancelled
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(league_id, season_id, home_team_id, away_team_id, match_date)
);

CREATE INDEX idx_matches_league_season ON matches(league_id, season_id);
CREATE INDEX idx_matches_date ON matches(match_date);
CREATE INDEX idx_matches_teams ON matches(home_team_id, away_team_id);

-- Team Match Stats
CREATE TABLE team_match_stats (
    team_match_stat_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(team_id),
    is_home BOOLEAN NOT NULL,
    goals INTEGER DEFAULT 0,
    shots INTEGER,
    shots_on_target INTEGER,
    possession DECIMAL(5,2),
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_accuracy DECIMAL(5,2),
    fouls_committed INTEGER,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    corners INTEGER,
    offsides INTEGER,
    xg DECIMAL(5,2),
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, team_id)
);

CREATE INDEX idx_team_match_stats_match ON team_match_stats(match_id);
CREATE INDEX idx_team_match_stats_team ON team_match_stats(team_id);

-- Player Match Stats
CREATE TABLE player_match_stats (
    player_match_stat_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),
    minutes_played INTEGER DEFAULT 0,
    started BOOLEAN DEFAULT FALSE,
    position_played VARCHAR(20),
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg DECIMAL(5,3),
    xa DECIMAL(5,3),  -- Expected assists
    key_passes INTEGER DEFAULT 0,
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    progressive_passes INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    aerials_won INTEGER DEFAULT 0,
    aerials_lost INTEGER DEFAULT 0,
    dribbles_completed INTEGER DEFAULT 0,
    dribbles_attempted INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    fouls_won INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    touches INTEGER DEFAULT 0,
    data_source_id INTEGER REFERENCES data_sources(source_id),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, player_id)
);

CREATE INDEX idx_player_match_stats_match ON player_match_stats(match_id);
CREATE INDEX idx_player_match_stats_player ON player_match_stats(player_id);

-- ============================================
-- AGGREGATED SEASON STATS
-- ============================================

-- Team Season Stats
CREATE TABLE team_season_stats (
    team_season_stat_id SERIAL PRIMARY KEY,
    team_id INTEGER REFERENCES teams(team_id),
    season_id INTEGER REFERENCES seasons(season_id),
    league_id INTEGER REFERENCES leagues(league_id),
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goal_difference INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    xg_for DECIMAL(6,2),
    xg_against DECIMAL(6,2),
    league_position INTEGER,
    data_source_id INTEGER REFERENCES data_sources(source_id),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season_id, league_id)
);

CREATE INDEX idx_team_season_stats_team_season ON team_season_stats(team_id, season_id);
CREATE INDEX idx_team_season_stats_league_season ON team_season_stats(league_id, season_id);

-- Player Season Stats
CREATE TABLE player_season_stats (
    player_season_stat_id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES players(player_id),
    team_id INTEGER REFERENCES teams(team_id),
    season_id INTEGER REFERENCES seasons(season_id),
    league_id INTEGER REFERENCES leagues(league_id),
    matches_played INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes INTEGER DEFAULT 0,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    passes_completed INTEGER DEFAULT 0,
    passes_attempted INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    dribbles_completed INTEGER DEFAULT 0,
    dribbles_attempted INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    fouls_won INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    rating DECIMAL(4,2),
    data_source_id INTEGER REFERENCES data_sources(source_id),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, team_id, season_id, league_id)
);

CREATE INDEX idx_player_season_stats_player ON player_season_stats(player_id);
CREATE INDEX idx_player_season_stats_team_season ON player_season_stats(team_id, season_id);

-- ============================================
-- INITIAL DATA SEEDS
-- ============================================

INSERT INTO data_sources (source_name, base_url, reliability_score) VALUES
    ('fotmob', 'https://www.fotmob.com/api/', 92),
    ('api_football', 'https://api-football-v1.p.rapidapi.com', 98),
    ('statsbomb', 'https://github.com/statsbomb/open-data', 85),
    ('transfermarkt', 'https://www.transfermarkt.com', 80)
    ON CONFLICT (source_name) DO NOTHING;

INSERT INTO leagues (league_name, country, tier, fotmob_id) VALUES
    ('Premier League', 'England', 1, 47),
    ('La Liga', 'Spain', 1, 87),
    ('Serie A', 'Italy', 1, 55),
    ('Bundesliga', 'Germany', 1, 54),
    ('Ligue 1', 'France', 1, 53),
    ('Eredivisie', 'Netherlands', 1, 57),
    ('Brasileiro Serie A', 'Brazil', 1, 268),
    ('Argentina Primera', 'Argentina', 1, 112)
    ON CONFLICT (league_name, country) DO NOTHING;

INSERT INTO seasons (season_name, start_year, end_year, is_current) VALUES
    ('2020-21', 2020, 2021, FALSE),
    ('2021-22', 2021, 2022, FALSE),
    ('2022-23', 2022, 2023, FALSE),
    ('2023-24', 2023, 2024, FALSE),
    ('2024-25', 2024, 2025, TRUE)
    ON CONFLICT (season_name) DO NOTHING;
