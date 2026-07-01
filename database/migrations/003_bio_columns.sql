-- Migration 003: Complete Player Biography Columns
-- Adds all biographical fields available from Wyscout GraphQL player() query.
-- Uses IF NOT EXISTS for idempotent execution.

ALTER TABLE players
    ADD COLUMN IF NOT EXISTS full_name VARCHAR(300),
    ADD COLUMN IF NOT EXISTS weight_kg INTEGER,
    ADD COLUMN IF NOT EXISTS image_url TEXT,
    ADD COLUMN IF NOT EXISTS current_team_name VARCHAR(200),
    ADD COLUMN IF NOT EXISTS current_team_wyscout_id INTEGER,
    ADD COLUMN IF NOT EXISTS on_loan BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS passport_countries TEXT,
    ADD COLUMN IF NOT EXISTS transfermarkt_id INTEGER;

-- Index for Transfermarkt cross-referencing
CREATE INDEX IF NOT EXISTS idx_players_tm_id ON players (transfermarkt_id);
