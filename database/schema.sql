--
-- PostgreSQL database dump
--

\restrict ESgAkcdXJCQqSqfeOsSx6nf2peqLIw05O5oBadHw1VBAT6XKb9nUCqhgIvyFCZR

-- Dumped from database version 15.15
-- Dumped by pg_dump version 15.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: unaccent; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;


--
-- Name: EXTENSION unaccent; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION unaccent IS 'text search dictionary that removes accents';


--
-- Name: immutable_unaccent(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.immutable_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $_$
    SELECT unaccent($1);
$_$;


--
-- Name: set_player_name_norm(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_player_name_norm() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.player_name_norm := LOWER(immutable_unaccent(NEW.player_name));
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: etl_run_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.etl_run_log (
    run_id integer NOT NULL,
    source character varying(50) NOT NULL,
    league_name character varying(100),
    season_name character varying(20),
    run_started timestamp with time zone DEFAULT now(),
    run_completed timestamp with time zone,
    status character varying(20) DEFAULT 'running'::character varying,
    players_processed integer DEFAULT 0,
    players_enriched integer DEFAULT 0,
    players_skipped integer DEFAULT 0,
    players_unmatched integer DEFAULT 0,
    errors_count integer DEFAULT 0,
    notes text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: etl_run_log_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.etl_run_log_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_run_log_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.etl_run_log_run_id_seq OWNED BY public.etl_run_log.run_id;


--
-- Name: leagues; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.leagues (
    league_id integer NOT NULL,
    league_name character varying(100) NOT NULL,
    country character varying(100) NOT NULL,
    soccerdata_key character varying(50),
    fotmob_id integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: leagues_league_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.leagues_league_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: leagues_league_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.leagues_league_id_seq OWNED BY public.leagues.league_id;


--
-- Name: match_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.match_events (
    event_id integer NOT NULL,
    match_id integer NOT NULL,
    event_type character varying(30) NOT NULL,
    event_detail character varying(100),
    minute integer NOT NULL,
    minute_extra integer,
    player_id integer,
    player2_id integer,
    team_id integer,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT valid_event_type CHECK (((event_type)::text = ANY ((ARRAY['goal'::character varying, 'own_goal'::character varying, 'card'::character varying, 'substitution'::character varying, 'penalty_scored'::character varying, 'penalty_missed'::character varying, 'var'::character varying])::text[])))
);


--
-- Name: match_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.match_events_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: match_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.match_events_event_id_seq OWNED BY public.match_events.event_id;


--
-- Name: match_lineups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.match_lineups (
    lineup_id integer NOT NULL,
    match_id integer NOT NULL,
    team_id integer NOT NULL,
    player_id integer NOT NULL,
    is_starter boolean DEFAULT true,
    position_played character varying(50),
    jersey_number integer,
    subbed_in_minute integer,
    subbed_out_minute integer,
    is_captain boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: match_lineups_lineup_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.match_lineups_lineup_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: match_lineups_lineup_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.match_lineups_lineup_id_seq OWNED BY public.match_lineups.lineup_id;


--
-- Name: matches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.matches (
    match_id integer NOT NULL,
    fotmob_match_id integer NOT NULL,
    league_id integer NOT NULL,
    season_id integer NOT NULL,
    home_team_id integer NOT NULL,
    away_team_id integer NOT NULL,
    match_date date NOT NULL,
    kickoff_time timestamp with time zone,
    matchweek integer,
    home_score integer,
    away_score integer,
    home_xg numeric(5,2),
    away_xg numeric(5,2),
    status character varying(20) DEFAULT 'scheduled'::character varying,
    venue character varying(200),
    referee character varying(150),
    attendance integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: matches_match_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.matches_match_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: matches_match_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.matches_match_id_seq OWNED BY public.matches.match_id;


--
-- Name: player_match_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.player_match_stats (
    stat_id integer NOT NULL,
    player_id integer NOT NULL,
    match_id integer NOT NULL,
    team_id integer NOT NULL,
    season_id integer NOT NULL,
    is_starter boolean,
    minutes_played integer,
    position_played character varying(50),
    goals integer,
    assists integer,
    shots integer,
    shots_on_target integer,
    shots_inside_box integer,
    xg numeric(6,3),
    xa numeric(6,3),
    big_chances_missed integer,
    tackles integer,
    tackles_won integer,
    interceptions integer,
    clearances integer,
    aerial_duels integer,
    aerial_duels_won integer,
    duels integer,
    duels_won integer,
    recoveries integer,
    dispossessed integer,
    dribbled_past integer,
    passes_attempted integer,
    passes_accurate integer,
    key_passes integer,
    big_chances_created integer,
    yellow_cards integer,
    red_cards integer,
    fouls_committed integer,
    fouls_won integer,
    fotmob_rating numeric(4,2),
    sofascore_rating numeric(4,2),
    data_source character varying(20) DEFAULT 'fotmob'::character varying,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: player_match_stats_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.player_match_stats_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: player_match_stats_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.player_match_stats_stat_id_seq OWNED BY public.player_match_stats.stat_id;


--
-- Name: player_season_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.player_season_stats (
    stat_id integer NOT NULL,
    player_id integer NOT NULL,
    team_id integer NOT NULL,
    season_id integer NOT NULL,
    league_id integer NOT NULL,
    matches_played integer,
    matches_started integer,
    minutes integer,
    goals integer,
    assists integer,
    shots integer,
    shots_on_target integer,
    shots_inside_box integer,
    shots_outside_box integer,
    yellow_cards integer,
    red_cards integer,
    xg numeric(8,3),
    npxg numeric(8,3),
    xa numeric(8,3),
    xg_chain numeric(8,3),
    xg_buildup numeric(8,3),
    aerial_duels_won integer,
    aerial_duels_lost integer,
    aerial_win_pct numeric(5,2),
    ground_duels_won integer,
    ground_duels_lost integer,
    ground_duels_won_pct numeric(5,2),
    duels_won integer,
    duels_won_pct numeric(5,2),
    tackles integer,
    tackles_won integer,
    tackles_won_pct numeric(5,2),
    interceptions integer,
    clearances integer,
    recoveries integer,
    dispossessed integer,
    dribbled_past integer,
    fouls_committed integer,
    fouls_won integer,
    error_lead_to_goal integer,
    key_passes integer,
    big_chances_created integer,
    big_chances_missed integer,
    accurate_passes_pct numeric(5,2),
    accurate_long_balls integer,
    accurate_final_third integer,
    successful_dribbles integer,
    touches integer,
    possession_won_att_third integer,
    saves integer,
    save_pct numeric(5,2),
    goals_conceded integer,
    clean_sheets integer,
    punches integer,
    high_claims integer,
    sofascore_rating numeric(4,2),
    fotmob_collected boolean DEFAULT false,
    sofascore_collected boolean DEFAULT false,
    understat_collected boolean DEFAULT false,
    last_updated timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: player_season_stats_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.player_season_stats_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: player_season_stats_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.player_season_stats_stat_id_seq OWNED BY public.player_season_stats.stat_id;


--
-- Name: players; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.players (
    player_id integer NOT NULL,
    player_name character varying(200) NOT NULL,
    player_name_norm character varying(200),
    fotmob_id integer,
    sofascore_id integer NOT NULL,
    understat_id integer,
    "position" character varying(50),
    position_group character varying(10),
    position_source character varying(20),
    nationality character varying(100),
    date_of_birth date,
    height_cm integer,
    preferred_foot character varying(10),
    shirt_number integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    created_by character varying(50) DEFAULT 'sofascore'::character varying
);


--
-- Name: players_player_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.players_player_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: players_player_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.players_player_id_seq OWNED BY public.players.player_id;


--
-- Name: seasons; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.seasons (
    season_id integer NOT NULL,
    season_name character varying(20) NOT NULL,
    understat_key character varying(10),
    start_year integer NOT NULL,
    end_year integer NOT NULL,
    is_current boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: seasons_season_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.seasons_season_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: seasons_season_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.seasons_season_id_seq OWNED BY public.seasons.season_id;


--
-- Name: team_elo; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_elo (
    elo_id integer NOT NULL,
    team_id integer,
    team_name_clubelo character varying(150) NOT NULL,
    elo_date date NOT NULL,
    elo_rating numeric(8,3) NOT NULL,
    elo_rank integer,
    league character varying(50),
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: team_elo_elo_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.team_elo_elo_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: team_elo_elo_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.team_elo_elo_id_seq OWNED BY public.team_elo.elo_id;


--
-- Name: team_match_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_match_stats (
    stat_id integer NOT NULL,
    match_id integer NOT NULL,
    team_id integer NOT NULL,
    is_home boolean NOT NULL,
    goals integer,
    shots integer,
    shots_on_target integer,
    shots_inside_box integer,
    shots_outside_box integer,
    possession_pct numeric(5,2),
    passes_total integer,
    passes_accurate integer,
    pass_accuracy_pct numeric(5,2),
    corners integer,
    offsides integer,
    fouls integer,
    yellow_cards integer,
    red_cards integer,
    xg numeric(5,3),
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: team_match_stats_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.team_match_stats_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: team_match_stats_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.team_match_stats_stat_id_seq OWNED BY public.team_match_stats.stat_id;


--
-- Name: team_season_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_season_stats (
    stat_id integer NOT NULL,
    team_id integer NOT NULL,
    season_id integer NOT NULL,
    league_id integer NOT NULL,
    matches_played integer,
    wins integer,
    draws integer,
    losses integer,
    goals_for integer,
    goals_against integer,
    points integer,
    league_position integer,
    avg_xg_per_match numeric(6,3),
    avg_xa_per_match numeric(6,3),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: team_season_stats_stat_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.team_season_stats_stat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: team_season_stats_stat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.team_season_stats_stat_id_seq OWNED BY public.team_season_stats.stat_id;


--
-- Name: teams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.teams (
    team_id integer NOT NULL,
    team_name character varying(150) NOT NULL,
    league_id integer NOT NULL,
    fotmob_id integer,
    sofascore_id integer,
    clubelo_name character varying(150),
    country character varying(100),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    created_by character varying(50) DEFAULT 'sofascore'::character varying
);


--
-- Name: teams_team_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.teams_team_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: teams_team_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.teams_team_id_seq OWNED BY public.teams.team_id;


--
-- Name: unmatched_players_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.unmatched_players_log (
    log_id integer NOT NULL,
    source character varying(50) NOT NULL,
    player_name character varying(200) NOT NULL,
    player_name_norm character varying(200),
    team_name character varying(150),
    league_name character varying(100),
    season_name character varying(20),
    reason character varying(100),
    best_candidate character varying(200),
    best_score numeric(5,3),
    logged_at timestamp with time zone DEFAULT now()
);


--
-- Name: unmatched_players_log_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.unmatched_players_log_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: unmatched_players_log_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.unmatched_players_log_log_id_seq OWNED BY public.unmatched_players_log.log_id;


--
-- Name: v_coverage_summary; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_coverage_summary AS
 SELECT l.league_name,
    s.season_name,
    count(DISTINCT pss.player_id) AS total_players,
    count(DISTINCT
        CASE
            WHEN (pss.minutes >= 450) THEN pss.player_id
            ELSE NULL::integer
        END) AS active_players,
    count(DISTINCT
        CASE
            WHEN (pss.xg IS NOT NULL) THEN pss.player_id
            ELSE NULL::integer
        END) AS with_xg,
    count(DISTINCT
        CASE
            WHEN (pss.aerial_duels_won IS NOT NULL) THEN pss.player_id
            ELSE NULL::integer
        END) AS with_deep_stats,
    count(DISTINCT
        CASE
            WHEN (pss.sofascore_rating IS NOT NULL) THEN pss.player_id
            ELSE NULL::integer
        END) AS with_rating,
    round((((count(DISTINCT
        CASE
            WHEN (pss.xg IS NOT NULL) THEN pss.player_id
            ELSE NULL::integer
        END))::numeric * 100.0) / (NULLIF(count(DISTINCT pss.player_id), 0))::numeric), 1) AS xg_pct,
    round((((count(DISTINCT
        CASE
            WHEN (pss.aerial_duels_won IS NOT NULL) THEN pss.player_id
            ELSE NULL::integer
        END))::numeric * 100.0) / (NULLIF(count(DISTINCT pss.player_id), 0))::numeric), 1) AS deep_pct,
    round((((count(DISTINCT
        CASE
            WHEN (pss.fotmob_collected AND pss.sofascore_collected AND pss.understat_collected) THEN pss.player_id
            ELSE NULL::integer
        END))::numeric * 100.0) / (NULLIF(count(DISTINCT pss.player_id), 0))::numeric), 1) AS all_sources_pct
   FROM ((public.player_season_stats pss
     JOIN public.leagues l ON ((pss.league_id = l.league_id)))
     JOIN public.seasons s ON ((pss.season_id = s.season_id)))
  GROUP BY l.league_name, s.season_name
  ORDER BY s.season_name DESC, l.league_name;


--
-- Name: v_player_last5; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_player_last5 AS
 SELECT ranked.player_id,
    ranked.player_name,
    ranked.position_group,
    ranked.team_name,
    ranked.league_name,
    ranked.match_date,
    ranked.matchweek,
    ranked.minutes_played,
    ranked.is_starter,
    ranked.position_played,
    ranked.goals,
    ranked.assists,
    ranked.xg,
    ranked.xa,
    ranked.sofascore_rating,
    ranked.fotmob_rating,
    ranked.aerial_duels_won,
    ranked.tackles_won,
    ranked.key_passes,
    ranked.recoveries,
    ranked.match_recency
   FROM ( SELECT pms.player_id,
            p.player_name,
            p.position_group,
            t.team_name,
            l.league_name,
            m.match_date,
            m.matchweek,
            pms.minutes_played,
            pms.is_starter,
            pms.position_played,
            pms.goals,
            pms.assists,
            pms.xg,
            pms.xa,
            pms.sofascore_rating,
            pms.fotmob_rating,
            pms.aerial_duels_won,
            pms.tackles_won,
            pms.key_passes,
            pms.recoveries,
            row_number() OVER (PARTITION BY pms.player_id ORDER BY m.match_date DESC) AS match_recency
           FROM ((((public.player_match_stats pms
             JOIN public.players p ON ((pms.player_id = p.player_id)))
             JOIN public.matches m ON ((pms.match_id = m.match_id)))
             JOIN public.teams t ON ((pms.team_id = t.team_id)))
             JOIN public.leagues l ON ((m.league_id = l.league_id)))
          WHERE (pms.minutes_played > 0)) ranked
  WHERE (ranked.match_recency <= 5);


--
-- Name: v_players_current_season; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_players_current_season AS
 SELECT p.player_id,
    p.player_name,
    p.position_group,
    p."position",
    p.nationality,
    (EXTRACT(year FROM age(now(), (p.date_of_birth)::timestamp with time zone)))::integer AS age,
    p.date_of_birth,
    p.height_cm,
    t.team_id,
    t.team_name,
    l.league_id,
    l.league_name,
    s.season_name,
    pss.minutes,
    pss.matches_played,
    pss.goals,
    pss.assists,
    pss.shots,
    pss.xg,
    pss.npxg,
    pss.xa,
    pss.xg_chain,
    pss.xg_buildup,
    pss.aerial_duels_won,
    pss.aerial_win_pct,
    pss.tackles_won,
    pss.tackles_won_pct,
    pss.interceptions,
    pss.clearances,
    pss.recoveries,
    pss.key_passes,
    pss.big_chances_created,
    pss.accurate_passes_pct,
    pss.accurate_final_third,
    pss.sofascore_rating,
    pss.saves,
    pss.clean_sheets,
    pss.fotmob_collected,
    pss.sofascore_collected,
    pss.understat_collected
   FROM ((((public.player_season_stats pss
     JOIN public.players p ON ((pss.player_id = p.player_id)))
     JOIN public.teams t ON ((pss.team_id = t.team_id)))
     JOIN public.leagues l ON ((pss.league_id = l.league_id)))
     JOIN public.seasons s ON ((pss.season_id = s.season_id)))
  WHERE (s.is_current = TRUE AND (pss.minutes >= 450));


--
-- Name: v_team_current_elo; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_team_current_elo AS
 SELECT DISTINCT ON (te.team_id) te.team_id,
    t.team_name,
    l.league_name,
    te.elo_rating,
    te.elo_rank,
    te.elo_date
   FROM ((public.team_elo te
     JOIN public.teams t ON ((te.team_id = t.team_id)))
     JOIN public.leagues l ON ((t.league_id = l.league_id)))
  ORDER BY te.team_id, te.elo_date DESC;


--
-- Name: etl_run_log run_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etl_run_log ALTER COLUMN run_id SET DEFAULT nextval('public.etl_run_log_run_id_seq'::regclass);


--
-- Name: leagues league_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leagues ALTER COLUMN league_id SET DEFAULT nextval('public.leagues_league_id_seq'::regclass);


--
-- Name: match_events event_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events ALTER COLUMN event_id SET DEFAULT nextval('public.match_events_event_id_seq'::regclass);


--
-- Name: match_lineups lineup_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups ALTER COLUMN lineup_id SET DEFAULT nextval('public.match_lineups_lineup_id_seq'::regclass);


--
-- Name: matches match_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches ALTER COLUMN match_id SET DEFAULT nextval('public.matches_match_id_seq'::regclass);


--
-- Name: player_match_stats stat_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats ALTER COLUMN stat_id SET DEFAULT nextval('public.player_match_stats_stat_id_seq'::regclass);


--
-- Name: player_season_stats stat_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats ALTER COLUMN stat_id SET DEFAULT nextval('public.player_season_stats_stat_id_seq'::regclass);


--
-- Name: players player_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.players ALTER COLUMN player_id SET DEFAULT nextval('public.players_player_id_seq'::regclass);


--
-- Name: seasons season_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.seasons ALTER COLUMN season_id SET DEFAULT nextval('public.seasons_season_id_seq'::regclass);


--
-- Name: team_elo elo_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_elo ALTER COLUMN elo_id SET DEFAULT nextval('public.team_elo_elo_id_seq'::regclass);


--
-- Name: team_match_stats stat_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_match_stats ALTER COLUMN stat_id SET DEFAULT nextval('public.team_match_stats_stat_id_seq'::regclass);


--
-- Name: team_season_stats stat_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats ALTER COLUMN stat_id SET DEFAULT nextval('public.team_season_stats_stat_id_seq'::regclass);


--
-- Name: teams team_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams ALTER COLUMN team_id SET DEFAULT nextval('public.teams_team_id_seq'::regclass);


--
-- Name: unmatched_players_log log_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unmatched_players_log ALTER COLUMN log_id SET DEFAULT nextval('public.unmatched_players_log_log_id_seq'::regclass);


--
-- Name: etl_run_log etl_run_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.etl_run_log
    ADD CONSTRAINT etl_run_log_pkey PRIMARY KEY (run_id);


--
-- Name: leagues leagues_fotmob_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leagues
    ADD CONSTRAINT leagues_fotmob_id_key UNIQUE (fotmob_id);


--
-- Name: leagues leagues_league_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leagues
    ADD CONSTRAINT leagues_league_name_key UNIQUE (league_name);


--
-- Name: leagues leagues_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.leagues
    ADD CONSTRAINT leagues_pkey PRIMARY KEY (league_id);


--
-- Name: match_events match_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events
    ADD CONSTRAINT match_events_pkey PRIMARY KEY (event_id);


--
-- Name: match_lineups match_lineups_match_id_team_id_player_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups
    ADD CONSTRAINT match_lineups_match_id_team_id_player_id_key UNIQUE (match_id, team_id, player_id);


--
-- Name: match_lineups match_lineups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups
    ADD CONSTRAINT match_lineups_pkey PRIMARY KEY (lineup_id);


--
-- Name: matches matches_fotmob_match_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_fotmob_match_id_key UNIQUE (fotmob_match_id);


--
-- Name: matches matches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_pkey PRIMARY KEY (match_id);


--
-- Name: player_match_stats player_match_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_pkey PRIMARY KEY (stat_id);


--
-- Name: player_match_stats player_match_stats_player_id_match_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_player_id_match_id_key UNIQUE (player_id, match_id);


--
-- Name: player_season_stats player_season_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_pkey PRIMARY KEY (stat_id);


--
-- Name: player_season_stats player_season_stats_player_id_team_id_season_id_league_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_player_id_team_id_season_id_league_id_key UNIQUE (player_id, team_id, season_id, league_id);


--
-- Name: players players_fotmob_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_fotmob_id_key UNIQUE (fotmob_id);


--
-- Name: players players_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);


--
-- Name: players players_sofascore_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_sofascore_id_key UNIQUE (sofascore_id);


--
-- Name: players players_understat_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_understat_id_key UNIQUE (understat_id);


--
-- Name: seasons seasons_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.seasons
    ADD CONSTRAINT seasons_pkey PRIMARY KEY (season_id);


--
-- Name: seasons seasons_season_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.seasons
    ADD CONSTRAINT seasons_season_name_key UNIQUE (season_name);


--
-- Name: team_elo team_elo_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_elo
    ADD CONSTRAINT team_elo_pkey PRIMARY KEY (elo_id);


--
-- Name: team_elo team_elo_team_name_clubelo_elo_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_elo
    ADD CONSTRAINT team_elo_team_name_clubelo_elo_date_key UNIQUE (team_name_clubelo, elo_date);


--
-- Name: team_match_stats team_match_stats_match_id_team_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_match_stats
    ADD CONSTRAINT team_match_stats_match_id_team_id_key UNIQUE (match_id, team_id);


--
-- Name: team_match_stats team_match_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_match_stats
    ADD CONSTRAINT team_match_stats_pkey PRIMARY KEY (stat_id);


--
-- Name: team_season_stats team_season_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats
    ADD CONSTRAINT team_season_stats_pkey PRIMARY KEY (stat_id);


--
-- Name: team_season_stats team_season_stats_team_id_season_id_league_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats
    ADD CONSTRAINT team_season_stats_team_id_season_id_league_id_key UNIQUE (team_id, season_id, league_id);


--
-- Name: teams teams_fotmob_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_fotmob_id_key UNIQUE (fotmob_id);


--
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- Name: teams teams_team_name_league_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_team_name_league_id_key UNIQUE (team_name, league_id);


--
-- Name: teams teams_sofascore_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_sofascore_id_key UNIQUE (sofascore_id);


--
-- Name: unmatched_players_log unmatched_players_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unmatched_players_log
    ADD CONSTRAINT unmatched_players_log_pkey PRIMARY KEY (log_id);


--
-- Name: idx_events_match_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_match_id ON public.match_events USING btree (match_id);


--
-- Name: idx_events_player_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_player_id ON public.match_events USING btree (player_id);


--
-- Name: idx_events_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_events_type ON public.match_events USING btree (event_type);


--
-- Name: idx_lineups_match_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lineups_match_id ON public.match_lineups USING btree (match_id);


--
-- Name: idx_lineups_player_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lineups_player_id ON public.match_lineups USING btree (player_id);


--
-- Name: idx_lineups_team_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_lineups_team_id ON public.match_lineups USING btree (team_id);


--
-- Name: idx_matches_away_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_matches_away_team ON public.matches USING btree (away_team_id);


--
-- Name: idx_matches_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_matches_date ON public.matches USING btree (match_date);


--
-- Name: idx_matches_fotmob_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_matches_fotmob_id ON public.matches USING btree (fotmob_match_id);


--
-- Name: idx_matches_home_team; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_matches_home_team ON public.matches USING btree (home_team_id);


--
-- Name: idx_matches_league_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_matches_league_season ON public.matches USING btree (league_id, season_id);


--
-- Name: idx_players_fotmob_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_fotmob_id ON public.players USING btree (fotmob_id);


--
-- Name: idx_players_name_norm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_name_norm ON public.players USING btree (player_name_norm);


--
-- Name: idx_players_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_name_trgm ON public.players USING gin (player_name_norm public.gin_trgm_ops);


--
-- Name: idx_players_position_grp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_position_grp ON public.players USING btree (position_group);


--
-- Name: idx_players_sofascore_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_sofascore_id ON public.players USING btree (sofascore_id);


--
-- Name: idx_players_understat_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_players_understat_id ON public.players USING btree (understat_id);


--
-- Name: idx_pms_match_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_match_id ON public.player_match_stats USING btree (match_id);


--
-- Name: idx_pms_player_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_player_id ON public.player_match_stats USING btree (player_id);


--
-- Name: idx_pms_player_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_player_season ON public.player_match_stats USING btree (player_id, season_id);


--
-- Name: idx_pms_team_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pms_team_season ON public.player_match_stats USING btree (team_id, season_id);


--
-- Name: idx_pss_league_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pss_league_season ON public.player_season_stats USING btree (league_id, season_id);


--
-- Name: idx_pss_player_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pss_player_id ON public.player_season_stats USING btree (player_id);


--
-- Name: idx_pss_player_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pss_player_season ON public.player_season_stats USING btree (player_id, season_id);


--
-- Name: idx_pss_team_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pss_team_season ON public.player_season_stats USING btree (team_id, season_id);


--
-- Name: idx_team_elo_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_elo_date ON public.team_elo USING btree (elo_date);


--
-- Name: idx_team_elo_team_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_elo_team_id ON public.team_elo USING btree (team_id);


--
-- Name: idx_teams_fotmob_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_teams_fotmob_id ON public.teams USING btree (fotmob_id);


--
-- Name: idx_teams_league_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_teams_league_id ON public.teams USING btree (league_id);


--
-- Name: idx_teams_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_teams_name ON public.teams USING btree (team_name);


--
-- Name: idx_tms_match_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tms_match_id ON public.team_match_stats USING btree (match_id);


--
-- Name: idx_tms_team_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tms_team_id ON public.team_match_stats USING btree (team_id);


--
-- Name: idx_tss_league_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tss_league_season ON public.team_season_stats USING btree (league_id, season_id);


--
-- Name: idx_tss_team_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tss_team_season ON public.team_season_stats USING btree (team_id, season_id);


--
-- Name: idx_unmatched_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_unmatched_source ON public.unmatched_players_log USING btree (source, logged_at);


--
-- Name: players trg_player_name_norm; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_player_name_norm BEFORE INSERT OR UPDATE OF player_name ON public.players FOR EACH ROW EXECUTE FUNCTION public.set_player_name_norm();


--
-- Name: match_events match_events_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events
    ADD CONSTRAINT match_events_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id) ON DELETE CASCADE;


--
-- Name: match_events match_events_player2_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events
    ADD CONSTRAINT match_events_player2_id_fkey FOREIGN KEY (player2_id) REFERENCES public.players(player_id);


--
-- Name: match_events match_events_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events
    ADD CONSTRAINT match_events_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- Name: match_events match_events_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_events
    ADD CONSTRAINT match_events_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: match_lineups match_lineups_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups
    ADD CONSTRAINT match_lineups_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id) ON DELETE CASCADE;


--
-- Name: match_lineups match_lineups_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups
    ADD CONSTRAINT match_lineups_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- Name: match_lineups match_lineups_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_lineups
    ADD CONSTRAINT match_lineups_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: matches matches_away_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_away_team_id_fkey FOREIGN KEY (away_team_id) REFERENCES public.teams(team_id);


--
-- Name: matches matches_home_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_home_team_id_fkey FOREIGN KEY (home_team_id) REFERENCES public.teams(team_id);


--
-- Name: matches matches_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_league_id_fkey FOREIGN KEY (league_id) REFERENCES public.leagues(league_id);


--
-- Name: matches matches_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.matches
    ADD CONSTRAINT matches_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.seasons(season_id);


--
-- Name: player_match_stats player_match_stats_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id) ON DELETE CASCADE;


--
-- Name: player_match_stats player_match_stats_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- Name: player_match_stats player_match_stats_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.seasons(season_id);


--
-- Name: player_match_stats player_match_stats_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_match_stats
    ADD CONSTRAINT player_match_stats_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: player_season_stats player_season_stats_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_league_id_fkey FOREIGN KEY (league_id) REFERENCES public.leagues(league_id);


--
-- Name: player_season_stats player_season_stats_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- Name: player_season_stats player_season_stats_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.seasons(season_id);


--
-- Name: player_season_stats player_season_stats_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.player_season_stats
    ADD CONSTRAINT player_season_stats_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: team_elo team_elo_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_elo
    ADD CONSTRAINT team_elo_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: team_match_stats team_match_stats_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_match_stats
    ADD CONSTRAINT team_match_stats_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.matches(match_id) ON DELETE CASCADE;


--
-- Name: team_match_stats team_match_stats_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_match_stats
    ADD CONSTRAINT team_match_stats_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: team_season_stats team_season_stats_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats
    ADD CONSTRAINT team_season_stats_league_id_fkey FOREIGN KEY (league_id) REFERENCES public.leagues(league_id);


--
-- Name: team_season_stats team_season_stats_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats
    ADD CONSTRAINT team_season_stats_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.seasons(season_id);


--
-- Name: team_season_stats team_season_stats_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_season_stats
    ADD CONSTRAINT team_season_stats_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- Name: teams teams_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_league_id_fkey FOREIGN KEY (league_id) REFERENCES public.leagues(league_id);


--
-- PostgreSQL database dump complete
--

\unrestrict ESgAkcdXJCQqSqfeOsSx6nf2peqLIw05O5oBadHw1VBAT6XKb9nUCqhgIvyFCZR

