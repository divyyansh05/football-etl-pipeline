-- Migration 001: sofascore_id canonical, created_by columns,
-- archive null-ssid players, unique constraints.
-- Runner owns transaction. No BEGIN/COMMIT here.
-- All DDL guarded for idempotency against end-state schema.sql.

-- Guard 1: match_lineups
DO $$
DECLARE cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM match_lineups ml
      JOIN players p ON ml.player_id = p.player_id
      WHERE p.sofascore_id IS NULL;
    IF cnt > 0 THEN RAISE EXCEPTION
      'Precondition: % NULL-ssid players in match_lineups', cnt;
    END IF;
END $$;

-- Guard 2: match_events.player_id
DO $$
DECLARE cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM match_events me
      JOIN players p ON me.player_id = p.player_id
      WHERE p.sofascore_id IS NULL;
    IF cnt > 0 THEN RAISE EXCEPTION
      'Precondition: % NULL-ssid players in match_events.player_id', cnt;
    END IF;
END $$;

-- Guard 3: match_events.player2_id
DO $$
DECLARE cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM match_events me
      JOIN players p ON me.player2_id = p.player_id
      WHERE p.sofascore_id IS NULL;
    IF cnt > 0 THEN RAISE EXCEPTION
      'Precondition: % NULL-ssid players in match_events.player2_id', cnt;
    END IF;
END $$;

-- Guard 4: no duplicate sofascore_id in players
DO $$
DECLARE cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM (
        SELECT sofascore_id FROM players
          WHERE sofascore_id IS NOT NULL
          GROUP BY sofascore_id HAVING COUNT(*) > 1) d;
    IF cnt > 0 THEN RAISE EXCEPTION
      'Precondition: % duplicate sofascore_id in players', cnt;
    END IF;
END $$;

-- Guard 5: no duplicate sofascore_id in teams
DO $$
DECLARE cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM (
        SELECT sofascore_id FROM teams
          WHERE sofascore_id IS NOT NULL
          GROUP BY sofascore_id HAVING COUNT(*) > 1) d;
    IF cnt > 0 THEN RAISE EXCEPTION
      'Precondition: % duplicate sofascore_id in teams', cnt;
    END IF;
END $$;

-- Step 1: Add created_by to players FIRST (before LIKE players archive),
-- so archive table mirrors final column structure.
ALTER TABLE players
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(50) DEFAULT 'sofascore';

-- Update existing default that was 'fotmob'
ALTER TABLE players
    ALTER COLUMN created_by SET DEFAULT 'sofascore';

-- Step 2: Add created_by to teams
ALTER TABLE teams
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(50) DEFAULT 'sofascore';

-- Step 3: Archive table (mirrors final players structure)
CREATE TABLE IF NOT EXISTS players_archive_null_ssid
    (LIKE players INCLUDING DEFAULTS);

ALTER TABLE players_archive_null_ssid
    ADD COLUMN IF NOT EXISTS archived_reason VARCHAR(100)
        DEFAULT 'null_sofascore_id',
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NOW();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'players_archive_null_ssid_pkey'
    ) THEN
        ALTER TABLE players_archive_null_ssid
            ADD CONSTRAINT players_archive_null_ssid_pkey
            PRIMARY KEY (player_id);
    END IF;
END $$;

INSERT INTO players_archive_null_ssid
SELECT p.*, 'null_sofascore_id', NOW()
FROM players p WHERE p.sofascore_id IS NULL
ON CONFLICT (player_id) DO NOTHING;

-- Clean child tables
DELETE FROM player_season_stats WHERE player_id IN (
    SELECT player_id FROM players WHERE sofascore_id IS NULL);
DELETE FROM player_match_stats WHERE player_id IN (
    SELECT player_id FROM players WHERE sofascore_id IS NULL);
DELETE FROM players WHERE sofascore_id IS NULL;

-- fotmob_id nullable
ALTER TABLE players ALTER COLUMN fotmob_id DROP NOT NULL;

-- sofascore_id NOT NULL
ALTER TABLE players ALTER COLUMN sofascore_id SET NOT NULL;

-- players.sofascore_id unique constraint (exact name matches schema.sql)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'players_sofascore_id_key'
    ) THEN
        ALTER TABLE players
            ADD CONSTRAINT players_sofascore_id_key UNIQUE (sofascore_id);
    END IF;
END $$;

-- teams.sofascore_id unique constraint (exact name matches schema.sql)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'teams_sofascore_id_key'
    ) THEN
        ALTER TABLE teams
            ADD CONSTRAINT teams_sofascore_id_key UNIQUE (sofascore_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_players_dob ON players(date_of_birth);
CREATE INDEX IF NOT EXISTS idx_players_nationality ON players(nationality);
CREATE INDEX IF NOT EXISTS idx_players_created_by ON players(created_by);
CREATE INDEX IF NOT EXISTS idx_teams_created_by ON teams(created_by);
