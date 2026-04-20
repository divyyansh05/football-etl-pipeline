-- Fix player_scores table: drop composite PK, add serial id + unique index
-- Allows NULL competition_id and season_id for cross-competition scores

ALTER TABLE player_scores DROP CONSTRAINT IF EXISTS player_scores_pkey;

ALTER TABLE player_scores ADD COLUMN IF NOT EXISTS id SERIAL;

ALTER TABLE player_scores ADD PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_scores_pk
    ON player_scores (player_id, COALESCE(competition_id, 0));

ALTER TABLE player_scores ALTER COLUMN competition_id DROP NOT NULL;
ALTER TABLE player_scores ALTER COLUMN season_id DROP NOT NULL;
