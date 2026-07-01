-- Migration: Enable pgvector and add stat_vector to player_scores
-- Run this ONCE against the football_platform database.
-- Usage:
--   docker exec -i football_db psql -U postgres -d football_platform < database/migrations/003_add_stat_vector.sql

-- 1. Enable the pgvector extension (already available in pgvector/pgvector:pg15 image)
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Add stat_vector column to player_scores
--    Dimension: 20 — one per position-agnostic normalised per-90 stat
--    (goals_p90, xg_p90, assists_p90, xa_p90, shots_p90, key_passes_p90,
--     dribbles_succ_p90, progressive_runs_p90, touches_box_p90,
--     interceptions_p90, clearances_p90, recoveries_p90,
--     aerial_won_pct, duels_won_pct, passes_acc_pct,
--     losses_p90, gk_saves_p90, gk_conceded_p90, gk_xg_save_p90, gk_passes_acc_pct)
ALTER TABLE player_scores
    ADD COLUMN IF NOT EXISTS stat_vector vector(20);

-- 3. Create HNSW index for fast approximate nearest-neighbour search
--    (cosine distance — best for normalised stat vectors)
CREATE INDEX IF NOT EXISTS idx_player_scores_vector_hnsw
    ON player_scores
    USING hnsw (stat_vector vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
