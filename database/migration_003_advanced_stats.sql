-- Migration 003: Advanced Stats Columns
-- Adds columns for progressive actions, pressing stats, and xA from StatsBomb/Understat
-- Run after schema.sql

-- ============================================
-- PLAYER MATCH STATS - Advanced Metrics
-- ============================================

-- Progressive Actions (from StatsBomb)
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS progressive_carries INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS progressive_passes_received INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS carries_into_final_third INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS carries_into_penalty_area INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS passes_into_final_third INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS passes_into_penalty_area INTEGER DEFAULT 0;

-- Pressing Stats (from StatsBomb)
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS pressures INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS pressure_regains INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS counterpressures INTEGER DEFAULT 0;

-- Shot Creating Actions (from StatsBomb)
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS shot_creating_actions INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS goal_creating_actions INTEGER DEFAULT 0;

-- Advanced Expected Metrics (from Understat)
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS npxg DECIMAL(5,3);  -- Non-penalty xG
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS xg_chain DECIMAL(5,3);
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS xg_buildup DECIMAL(5,3);

-- Passing Breakdown
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS long_passes_completed INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS long_passes_attempted INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS through_balls INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS crosses_completed INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS crosses_attempted INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS switches INTEGER DEFAULT 0;

-- Duels Breakdown
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS ground_duels_won INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS ground_duels_lost INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS aerial_duels_won INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS aerial_duels_lost INTEGER DEFAULT 0;

-- Goalkeeper Advanced (from StatsBomb)
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS psxg DECIMAL(5,3);  -- Post-shot xG faced
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS saves INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS punches INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS crosses_stopped INTEGER DEFAULT 0;
ALTER TABLE player_match_stats ADD COLUMN IF NOT EXISTS sweeper_actions INTEGER DEFAULT 0;

-- ============================================
-- PLAYER SEASON STATS - Advanced Aggregates
-- ============================================

-- Progressive Actions
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS progressive_passes INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS progressive_carries INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS progressive_passes_received INTEGER DEFAULT 0;

-- Pressing
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS pressures INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS pressure_regains INTEGER DEFAULT 0;

-- Chance Creation
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS shot_creating_actions INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS goal_creating_actions INTEGER DEFAULT 0;

-- Expected Metrics
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS xg DECIMAL(6,2);
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS xa DECIMAL(6,2);
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS npxg DECIMAL(6,2);
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS xg_chain DECIMAL(6,2);
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS xg_buildup DECIMAL(6,2);

-- Penalty Stats (from API-Football)
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalties_scored INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalties_missed INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalties_won INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS penalties_conceded INTEGER DEFAULT 0;

-- Substitution Stats (from API-Football)
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS subbed_in INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS subbed_out INTEGER DEFAULT 0;
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS bench_appearances INTEGER DEFAULT 0;

-- ============================================
-- TEAM MATCH STATS - Advanced Metrics
-- ============================================

-- Pressing
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS pressures INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS pressure_regains INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS ppda DECIMAL(5,2);  -- Passes per Defensive Action

-- Progressive
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS progressive_passes INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS progressive_carries INTEGER DEFAULT 0;

-- Shot zones
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS shots_inside_box INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS shots_outside_box INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS big_chances_created INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS big_chances_missed INTEGER DEFAULT 0;

-- Defensive
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS tackles INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS interceptions INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS clearances INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS blocks INTEGER DEFAULT 0;
ALTER TABLE team_match_stats ADD COLUMN IF NOT EXISTS saves INTEGER DEFAULT 0;

-- ============================================
-- TEAM SEASON STATS - Advanced Aggregates
-- ============================================

ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS npxg_for DECIMAL(6,2);
ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS npxg_against DECIMAL(6,2);
ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS ppda DECIMAL(5,2);
ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS oppda DECIMAL(5,2);  -- Opponent PPDA
ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS deep_completions INTEGER DEFAULT 0;  -- Passes within 20m of goal
ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS deep_completions_allowed INTEGER DEFAULT 0;

-- ============================================
-- PLAYERS TABLE - Additional Bio Fields
-- ============================================

-- From API-Football
ALTER TABLE players ADD COLUMN IF NOT EXISTS weight_kg INTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS birth_place VARCHAR(100);
ALTER TABLE players ADD COLUMN IF NOT EXISTS birth_country VARCHAR(50);
ALTER TABLE players ADD COLUMN IF NOT EXISTS photo_url VARCHAR(255);
ALTER TABLE players ADD COLUMN IF NOT EXISTS is_injured BOOLEAN DEFAULT FALSE;

-- From FotMob
ALTER TABLE players ADD COLUMN IF NOT EXISTS market_value VARCHAR(50);
ALTER TABLE players ADD COLUMN IF NOT EXISTS contract_until DATE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS shirt_number INTEGER;

-- External IDs
ALTER TABLE players ADD COLUMN IF NOT EXISTS understat_id INTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS statsbomb_id INTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS api_football_id INTEGER;

-- ============================================
-- TEAMS TABLE - Additional Fields
-- ============================================

-- From API-Football
ALTER TABLE teams ADD COLUMN IF NOT EXISTS team_code VARCHAR(10);
ALTER TABLE teams ADD COLUMN IF NOT EXISTS logo_url VARCHAR(255);
ALTER TABLE teams ADD COLUMN IF NOT EXISTS stadium_capacity INTEGER;
ALTER TABLE teams ADD COLUMN IF NOT EXISTS stadium_surface VARCHAR(50);
ALTER TABLE teams ADD COLUMN IF NOT EXISTS stadium_city VARCHAR(100);

-- External IDs
ALTER TABLE teams ADD COLUMN IF NOT EXISTS api_football_id INTEGER;
ALTER TABLE teams ADD COLUMN IF NOT EXISTS understat_id INTEGER;

-- ============================================
-- MATCHES TABLE - Additional Fields
-- ============================================

ALTER TABLE matches ADD COLUMN IF NOT EXISTS fotmob_match_id INTEGER;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS api_football_fixture_id INTEGER;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS understat_match_id INTEGER;

-- Create indexes for external IDs
CREATE INDEX IF NOT EXISTS idx_players_fotmob_id ON players(fotmob_id);
CREATE INDEX IF NOT EXISTS idx_players_understat_id ON players(understat_id);
CREATE INDEX IF NOT EXISTS idx_players_statsbomb_id ON players(statsbomb_id);
CREATE INDEX IF NOT EXISTS idx_players_api_football_id ON players(api_football_id);

CREATE INDEX IF NOT EXISTS idx_teams_fotmob_id ON teams(fotmob_id);
CREATE INDEX IF NOT EXISTS idx_teams_api_football_id ON teams(api_football_id);
CREATE INDEX IF NOT EXISTS idx_teams_understat_id ON teams(understat_id);

CREATE INDEX IF NOT EXISTS idx_matches_fotmob_id ON matches(fotmob_match_id);
CREATE INDEX IF NOT EXISTS idx_matches_api_football_id ON matches(api_football_fixture_id);
CREATE INDEX IF NOT EXISTS idx_matches_understat_id ON matches(understat_match_id);

-- ============================================
-- DATA SOURCES - Add new sources
-- ============================================

INSERT INTO data_sources (source_name, base_url, reliability_score) VALUES
    ('understat', 'https://understat.com', 90),
    ('statsbomb_open', 'https://github.com/statsbomb/open-data', 95)
ON CONFLICT (source_name) DO NOTHING;

-- ============================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================

COMMENT ON COLUMN player_match_stats.progressive_passes IS 'Passes that move ball >=10m towards goal (StatsBomb)';
COMMENT ON COLUMN player_match_stats.progressive_carries IS 'Carries that move ball >=10m towards goal (StatsBomb)';
COMMENT ON COLUMN player_match_stats.pressures IS 'Number of pressing actions (StatsBomb)';
COMMENT ON COLUMN player_match_stats.shot_creating_actions IS 'Actions leading to shots within 2 actions (StatsBomb)';
COMMENT ON COLUMN player_match_stats.goal_creating_actions IS 'Actions leading to goals within 2 actions (StatsBomb)';
COMMENT ON COLUMN player_match_stats.npxg IS 'Non-penalty expected goals (Understat)';
COMMENT ON COLUMN player_match_stats.xg_chain IS 'xG from possessions player was involved in (Understat)';
COMMENT ON COLUMN player_match_stats.xg_buildup IS 'xG from possessions player was involved in excluding shots/key passes (Understat)';
COMMENT ON COLUMN player_match_stats.psxg IS 'Post-shot expected goals for goalkeepers (StatsBomb)';
COMMENT ON COLUMN team_match_stats.ppda IS 'Passes allowed Per Defensive Action in attacking third';
