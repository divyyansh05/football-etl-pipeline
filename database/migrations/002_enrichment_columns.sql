-- Migration 002: Secondary Enrichment Columns
-- Adds integration columns for SofaScore, Transfermarkt, and FotMob.
-- This uses IF NOT EXISTS to ensure safe, idempotent execution.

ALTER TABLE players
    ADD COLUMN IF NOT EXISTS sofascore_id INTEGER,
    ADD COLUMN IF NOT EXISTS sofascore_rating FLOAT,
    ADD COLUMN IF NOT EXISTS market_value_eur BIGINT,
    ADD COLUMN IF NOT EXISTS contract_expires DATE,
    ADD COLUMN IF NOT EXISTS fotmob_id INTEGER;

-- Create an index to speed up ID resolution when joining systems
CREATE INDEX IF NOT EXISTS idx_players_sofascore_id ON players (sofascore_id);
CREATE INDEX IF NOT EXISTS idx_players_fotmob_id ON players (fotmob_id);
