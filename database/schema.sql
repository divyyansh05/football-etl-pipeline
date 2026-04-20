-- football-data-platform schema
-- Primary source: Wyscout
-- Clean, no legacy columns

-- Competitions
CREATE TABLE IF NOT EXISTS competitions (
    competition_id    SERIAL PRIMARY KEY,
    wyscout_id        INTEGER UNIQUE,
    name              VARCHAR(100) NOT NULL,
    country           VARCHAR(100),
    competition_type  VARCHAR(50),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Seasons
CREATE TABLE IF NOT EXISTS seasons (
    season_id         SERIAL PRIMARY KEY,
    wyscout_id        INTEGER UNIQUE,
    competition_id    INTEGER REFERENCES competitions(competition_id),
    season_name       VARCHAR(20),
    start_year        INTEGER,
    end_year          INTEGER,
    is_current        BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_id           SERIAL PRIMARY KEY,
    wyscout_id        INTEGER UNIQUE,
    name              VARCHAR(200) NOT NULL,
    normalised_name   VARCHAR(200),
    country           VARCHAR(100),
    logo_url          VARCHAR(500),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Players (canonical identity)
CREATE TABLE IF NOT EXISTS players (
    player_id         SERIAL PRIMARY KEY,
    wyscout_id        INTEGER UNIQUE,
    name              VARCHAR(200) NOT NULL,
    normalised_name   VARCHAR(200),
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    date_of_birth     DATE,
    nationality       VARCHAR(100),
    position_group    VARCHAR(10),
    primary_position  VARCHAR(30),
    height_cm         INTEGER,
    preferred_foot    VARCHAR(10),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Player season mapping
CREATE TABLE IF NOT EXISTS player_seasons (
    id                SERIAL PRIMARY KEY,
    player_id         INTEGER REFERENCES players(player_id),
    team_id           INTEGER REFERENCES teams(team_id),
    competition_id    INTEGER REFERENCES competitions(competition_id),
    season_id         INTEGER REFERENCES seasons(season_id),
    wyscout_season_id INTEGER,
    UNIQUE (player_id, team_id, season_id)
);

-- Player match stats (core table — one row per player per match)
CREATE TABLE IF NOT EXISTS player_match_stats (
    id                      SERIAL PRIMARY KEY,

    -- Identity
    player_id               INTEGER REFERENCES players(player_id),
    wyscout_player_id       INTEGER NOT NULL,
    wyscout_player_name     VARCHAR(200) NOT NULL,

    -- Match context
    match_label             VARCHAR(300),
    competition_name        VARCHAR(150),
    match_date              DATE,
    position_played         VARCHAR(50),
    minutes_played          INTEGER,

    -- Volume stats
    total_actions           INTEGER,
    successful_actions      INTEGER,

    -- Goals / xG
    goals                   INTEGER DEFAULT 0,
    assists                 INTEGER DEFAULT 0,
    shots                   INTEGER,
    shots_on_target         INTEGER,
    xg                      NUMERIC(7,4),
    xa                      NUMERIC(7,4),
    npxg                    NUMERIC(7,4),
    xg_on_target            NUMERIC(7,4),

    -- Chance creation
    shot_assists            INTEGER,
    second_assists          INTEGER,
    key_passes              INTEGER,
    smart_passes            INTEGER,
    smart_passes_acc        INTEGER,
    through_passes          INTEGER,
    through_passes_acc      INTEGER,

    -- Passing
    passes                  INTEGER,
    passes_accurate         INTEGER,
    long_passes             INTEGER,
    long_passes_accurate    INTEGER,
    crosses                 INTEGER,
    crosses_accurate        INTEGER,
    forward_passes          INTEGER,
    forward_passes_acc      INTEGER,
    back_passes             INTEGER,
    back_passes_acc         INTEGER,
    lateral_passes          INTEGER,
    lateral_passes_acc      INTEGER,
    passes_final_third      INTEGER,
    passes_final_third_acc  INTEGER,
    passes_penalty_area     INTEGER,
    passes_penalty_area_acc INTEGER,

    -- Dribbling / attacking
    dribbles                INTEGER,
    dribbles_successful     INTEGER,
    progressive_runs        INTEGER,
    touches_in_box          INTEGER,
    offensive_duels         INTEGER,
    offensive_duels_won     INTEGER,

    -- Defending
    defensive_duels         INTEGER,
    defensive_duels_won     INTEGER,
    aerial_duels            INTEGER,
    aerial_duels_won        INTEGER,
    duels                   INTEGER,
    duels_won               INTEGER,
    loose_ball_duels        INTEGER,
    loose_ball_duels_won    INTEGER,
    interceptions           INTEGER,
    sliding_tackles         INTEGER,
    sliding_tackles_succ    INTEGER,
    clearances              INTEGER,

    -- Ball winning/losing
    losses_own_half         INTEGER,
    recoveries_opp_half     INTEGER,
    recoveries              INTEGER,
    losses                  INTEGER,

    -- Discipline
    fouls_committed         INTEGER,
    fouls_suffered          INTEGER,
    offsides                INTEGER,
    yellow_cards            INTEGER DEFAULT 0,
    red_cards               INTEGER DEFAULT 0,

    -- GK specific
    gk_saves                INTEGER,
    gk_saves_reflex         INTEGER,
    gk_super_saves          INTEGER,
    gk_conceded             INTEGER,
    gk_xg_save              NUMERIC(7,4),
    gk_exits                INTEGER,
    gk_claims               INTEGER,
    gk_punches              INTEGER,
    gk_sweeps               INTEGER,
    gk_passes               INTEGER,
    gk_passes_acc           INTEGER,
    gk_goal_kicks           INTEGER,
    gk_short_kicks          INTEGER,
    gk_long_kicks           INTEGER,

    -- Metadata
    data_source             VARCHAR(20) DEFAULT 'wyscout',
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (wyscout_player_name, match_date,
            competition_name, minutes_played)
);

-- Team match stats
CREATE TABLE IF NOT EXISTS team_match_stats (
    id                      SERIAL PRIMARY KEY,

    -- Identity
    team_id                 INTEGER REFERENCES teams(team_id),
    wyscout_team_id         INTEGER NOT NULL,
    wyscout_team_name       VARCHAR(200) NOT NULL,
    is_home                 BOOLEAN,

    -- Match context
    match_label             VARCHAR(300),
    competition_name        VARCHAR(150),
    match_date              DATE,
    duration                INTEGER,
    formation               VARCHAR(50),
    result                  VARCHAR(10),

    -- Attacking
    goals                   INTEGER,
    xg                      NUMERIC(7,4),
    shots                   INTEGER,
    shots_on_target         INTEGER,
    positional_attacks      INTEGER,
    positional_shots        INTEGER,
    counterattacks          INTEGER,
    counter_shots           INTEGER,
    set_pieces              INTEGER,
    set_piece_shots         INTEGER,
    corners                 INTEGER,
    free_kicks              INTEGER,
    crosses                 INTEGER,
    crosses_accurate        INTEGER,
    deep_crosses            INTEGER,
    deep_passes             INTEGER,
    touches_in_box          INTEGER,
    penalty_area_entries    INTEGER,
    offsides                INTEGER,

    -- Defending
    conceded_goals          INTEGER,
    shots_against           INTEGER,
    shots_against_target    INTEGER,
    defensive_duels         INTEGER,
    defensive_duels_won     INTEGER,
    aerial_duels            INTEGER,
    aerial_duels_won        INTEGER,
    interceptions           INTEGER,
    clearances              INTEGER,
    sliding_tackles         INTEGER,

    -- Passing
    passes                  INTEGER,
    passes_accurate         INTEGER,
    possession_pct          NUMERIC(5,2),
    forward_passes          INTEGER,
    forward_passes_acc      INTEGER,
    back_passes             INTEGER,
    back_passes_acc         INTEGER,
    long_passes             INTEGER,
    long_passes_acc         INTEGER,
    progressive_passes      INTEGER,
    progressive_passes_acc  INTEGER,
    passes_final_third      INTEGER,
    passes_final_third_acc  INTEGER,
    smart_passes            INTEGER,
    smart_passes_acc        INTEGER,

    -- Duels
    duels                   INTEGER,
    duels_won               INTEGER,
    offensive_duels         INTEGER,
    offensive_duels_won     INTEGER,

    -- Ball zones
    losses                  INTEGER,
    losses_low              INTEGER,
    losses_mid              INTEGER,
    losses_high             INTEGER,
    recoveries              INTEGER,
    recoveries_low          INTEGER,
    recoveries_mid          INTEGER,
    recoveries_high         INTEGER,

    -- Advanced
    ppda                    NUMERIC(8,3),
    match_tempo             NUMERIC(6,2),
    avg_pass_length         NUMERIC(6,2),
    avg_shot_distance       NUMERIC(6,2),

    -- Metadata
    data_source             VARCHAR(20) DEFAULT 'wyscout',
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (wyscout_team_name, match_date, competition_name, is_home)
);

-- File tracking (prevent re-processing)
CREATE TABLE IF NOT EXISTS loaded_files (
    id          SERIAL PRIMARY KEY,
    filename    VARCHAR(400) UNIQUE NOT NULL,
    file_type   VARCHAR(20),
    entity_id   INTEGER,
    rows_loaded INTEGER,
    loaded_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Analytics (computed scores)
CREATE TABLE IF NOT EXISTS player_scores (
    player_id           INTEGER REFERENCES players(player_id),
    competition_id      INTEGER REFERENCES competitions(competition_id),
    season_id           INTEGER REFERENCES seasons(season_id),
    position_group      VARCHAR(10),
    minutes_total       INTEGER,
    matches_total       INTEGER,
    performance_score   NUMERIC(6,2),
    percentile_rank     NUMERIC(5,2),
    goals_p90           NUMERIC(6,3),
    assists_p90         NUMERIC(6,3),
    xg_p90              NUMERIC(6,3),
    xa_p90              NUMERIC(6,3),
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, competition_id, season_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pms_player
    ON player_match_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_pms_wyscout_id
    ON player_match_stats(wyscout_player_id);
CREATE INDEX IF NOT EXISTS idx_pms_date
    ON player_match_stats(match_date);
CREATE INDEX IF NOT EXISTS idx_pms_competition
    ON player_match_stats(competition_name);
CREATE INDEX IF NOT EXISTS idx_tms_team
    ON team_match_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_tms_date
    ON team_match_stats(match_date);
CREATE INDEX IF NOT EXISTS idx_players_wyscout
    ON players(wyscout_id);
CREATE INDEX IF NOT EXISTS idx_players_norm
    ON players(normalised_name);
