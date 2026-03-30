-- Migration 004: analytics foundation tables
-- No BEGIN/COMMIT — runner owns the transaction.
-- All DDL guarded with IF NOT EXISTS for idempotency.

CREATE TABLE IF NOT EXISTS player_scores (
    player_id           INTEGER NOT NULL REFERENCES players(player_id),
    season_id           INTEGER NOT NULL REFERENCES seasons(season_id),
    league_id           INTEGER NOT NULL REFERENCES leagues(league_id),
    position_group      VARCHAR(10) NOT NULL,
    minutes             INTEGER NOT NULL,
    performance_score   NUMERIC(6,2),
    percentile_rank     NUMERIC(5,2),
    goals_p90           NUMERIC(6,3),
    assists_p90         NUMERIC(6,3),
    xg_p90              NUMERIC(6,3),
    xa_p90              NUMERIC(6,3),
    shots_p90           NUMERIC(6,3),
    key_passes_p90      NUMERIC(6,3),
    tackles_p90         NUMERIC(6,3),
    interceptions_p90   NUMERIC(6,3),
    aerial_won_p90      NUMERIC(6,3),
    successful_dribbles_p90 NUMERIC(6,3),
    recoveries_p90      NUMERIC(6,3),
    score_computed_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, season_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_player_scores_league_season
    ON player_scores(league_id, season_id);
CREATE INDEX IF NOT EXISTS idx_player_scores_position
    ON player_scores(position_group);
CREATE INDEX IF NOT EXISTS idx_player_scores_score
    ON player_scores(performance_score DESC);
