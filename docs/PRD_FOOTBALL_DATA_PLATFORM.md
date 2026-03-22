# Product Requirements Document (PRD)
# Professional Football Data Platform

**Version:** 1.0
**Date:** January 2026
**Document Owner:** Engineering Team
**Status:** Active Development

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Vision & Objectives](#2-vision--objectives)
3. [System Architecture](#3-system-architecture)
4. [Data Requirements](#4-data-requirements)
5. [Technical Specifications](#5-technical-specifications)
6. [API & Data Sources](#6-api--data-sources)
7. [Database Schema](#7-database-schema)
8. [ETL Pipeline Architecture](#8-etl-pipeline-architecture)
9. [Scheduler & Automation](#9-scheduler--automation)
10. [CLI & Developer Tools](#10-cli--developer-tools)
11. [Current Implementation Status](#11-current-implementation-status)
12. [Gap Analysis & Vulnerabilities](#12-gap-analysis--vulnerabilities)
13. [Team Requirements](#13-team-requirements)
14. [Roadmap & Milestones](#14-roadmap--milestones)
15. [Quality Standards](#15-quality-standards)
16. [Appendix](#appendix)

---

## 1. Executive Summary

### 1.1 Product Overview

The Football Data Platform is a professional-grade data infrastructure designed to collect, process, store, and serve comprehensive football (soccer) data at the quality level of industry leaders like FotMob, SofaScore, and Opta. The platform serves as the foundational data layer for advanced football analytics, scouting systems, and algorithmic analysis applications.

### 1.2 Business Context

Modern football analytics requires:
- **Real-time match data** with event-level granularity
- **Historical data** spanning multiple seasons for trend analysis
- **Player-level metrics** for scouting and performance evaluation
- **Team tactical data** for strategic analysis
- **Competition/league data** for market intelligence

### 1.3 Target Users

| User Type | Use Cases |
|-----------|-----------|
| Data Scientists | Building predictive models, xG calculations, player valuation |
| Scouts | Player identification, performance comparison, market analysis |
| Analysts | Match analysis, tactical patterns, opponent preparation |
| Application Developers | Building dashboards, mobile apps, API consumers |
| Researchers | Academic studies, statistical analysis |

### 1.4 Key Success Metrics

| Metric | Target | Current |
|--------|--------|---------|
| Data Completeness | 95%+ | ~70% |
| Data Freshness | < 24 hours | 24 hours |
| API Uptime | 99.9% | N/A |
| Coverage (Leagues) | 20+ top leagues | 5 leagues |
| Historical Depth | 10+ seasons | 6 seasons |

---

## 2. Vision & Objectives

### 2.1 Product Vision

*To build the most comprehensive, reliable, and developer-friendly football data infrastructure that enables any modern football analytics application.*

### 2.2 Strategic Objectives

#### Phase 1: Foundation (Current)
- [x] Core database schema for football entities
- [x] Multi-source data integration (API-Football, FBref, StatsBomb)
- [x] Basic ETL pipelines
- [ ] Complete API usage tracking
- [ ] Production-ready scheduler

#### Phase 2: Expansion
- [ ] Event-level match data (passes, shots, tackles)
- [ ] Real-time data streaming
- [ ] Extended league coverage (20+ leagues)
- [ ] Player tracking data integration

#### Phase 3: Intelligence
- [ ] Derived metrics calculation (xG, xA, PPDA)
- [ ] Data quality scoring
- [ ] Anomaly detection
- [ ] Automated data reconciliation

#### Phase 4: Scale
- [ ] Multi-region deployment
- [ ] GraphQL API layer
- [ ] Real-time websocket feeds
- [ ] Enterprise SLA support

### 2.3 Design Principles

1. **Data Accuracy First** - Every data point must be traceable to its source
2. **Idempotent Operations** - ETL runs should be safely re-runnable
3. **Graceful Degradation** - System continues operating when one source fails
4. **API-Conscious** - Respect rate limits, minimize redundant calls
5. **Schema Evolution** - Support backward-compatible changes
6. **Audit Trail** - Track all data modifications and sources

---

## 3. System Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES LAYER                                │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────────────┤
│ API-Football│   FBref     │  StatsBomb  │  Soccerway  │  Future Sources    │
│  (Primary)  │ (Secondary) │   (Free)    │  (Legacy)   │  (Opta, Wyscout)   │
└──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┴─────────┬──────────┘
       │             │             │             │                │
       ▼             ▼             ▼             ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ API Clients  │  │ Web Scrapers │  │ File Parsers │  │ Rate Limiter │    │
│  │              │  │              │  │              │  │              │    │
│  │ - HTTP/REST  │  │ - Selenium   │  │ - JSON       │  │ - 100/day    │    │
│  │ - Auth       │  │ - BS4        │  │ - CSV        │  │ - Backoff    │    │
│  │ - Retry      │  │ - Headless   │  │ - Parquet    │  │ - Queue      │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ETL PROCESSING LAYER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Extract    │  │  Transform   │  │    Load      │  │   Validate   │    │
│  │              │  │              │  │              │  │              │    │
│  │ - Fetch data │  │ - Normalize  │  │ - Batch      │  │ - Schema     │    │
│  │ - Parse      │  │ - Map IDs    │  │ - Upsert     │  │ - Integrity  │    │
│  │ - Cache      │  │ - Enrich     │  │ - Index      │  │ - Quality    │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STORAGE LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                     PostgreSQL Database                             │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │    │
│  │  │ leagues  │ │  teams   │ │ players  │ │ matches  │ │  stats   │ │    │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │    │
│  │  │standings │ │ events   │ │ tracking │ │ etl_logs │ │ api_usage│ │    │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           APPLICATION LAYER                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  CLI Tools   │  │  REST API    │  │  Dashboard   │  │  Analytics   │    │
│  │              │  │  (Flask)     │  │  (Dash)      │  │  Services    │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Responsibilities

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| API Clients | External API communication | requests, selenium |
| Rate Limiter | API quota management | Custom tracker |
| ETL Engine | Data transformation | Python, pandas |
| Batch Loader | Database operations | SQLAlchemy |
| Scheduler | Job orchestration | APScheduler |
| Database | Data persistence | PostgreSQL 15 |
| CLI | Developer interface | Click, Rich |

### 3.3 Data Flow

```
1. TRIGGER
   └── Scheduler (cron) OR Manual (CLI) OR API (webhook)

2. EXTRACT
   └── API Client fetches data
       └── Rate limiter checks quota
           └── Request logged

3. TRANSFORM
   └── Raw JSON parsed
       └── IDs mapped (API ID → Internal ID)
           └── Data normalized
               └── Relationships resolved

4. LOAD
   └── Batch prepared
       └── Upsert operation (ON CONFLICT)
           └── Indexes updated
               └── Stats recorded

5. VALIDATE
   └── Row counts verified
       └── Referential integrity checked
           └── Quality score calculated
```

---

## 4. Data Requirements

### 4.1 Core Entities

#### 4.1.1 Leagues/Competitions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Internal identifier |
| name | String | Yes | Full name (e.g., "Premier League") |
| code | String | Yes | Short code (e.g., "PL") |
| country | String | Yes | Country name |
| country_code | String | Yes | ISO 3166-1 alpha-2 |
| type | Enum | Yes | league/cup/international |
| tier | Integer | No | Division level (1=top) |
| api_football_id | Integer | No | API-Football identifier |
| fbref_id | String | No | FBref identifier |
| logo_url | URL | No | Competition logo |
| current_season | String | No | Active season (e.g., "2024-25") |

**Coverage Requirements:**
- Tier 1: Big 5 European Leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1)
- Tier 2: Secondary European (Eredivisie, Primeira Liga, Belgian Pro League)
- Tier 3: Top South American (Brasileirão, Argentine Primera)
- Tier 4: Other notable (MLS, J-League, Saudi Pro League)
- Cups: UEFA Champions League, Europa League, domestic cups

#### 4.1.2 Teams

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Internal identifier |
| name | String | Yes | Official name |
| short_name | String | No | Abbreviated name |
| code | String | No | 3-letter code |
| country | String | Yes | Country |
| city | String | No | Home city |
| founded | Integer | No | Year founded |
| stadium | String | No | Home stadium |
| stadium_capacity | Integer | No | Capacity |
| api_football_id | Integer | No | API-Football identifier |
| fbref_id | String | No | FBref identifier |
| logo_url | URL | No | Team crest |
| primary_color | String | No | Hex color code |
| secondary_color | String | No | Hex color code |

**Data Quality Rules:**
- Name must be official (not nickname)
- Founded year must be verified
- Stadium must match current season

#### 4.1.3 Players

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Internal identifier |
| name | String | Yes | Full name |
| first_name | String | No | Given name |
| last_name | String | No | Family name |
| display_name | String | No | Preferred display name |
| date_of_birth | Date | Yes | Birth date |
| nationality | String | Yes | Primary nationality |
| secondary_nationality | String | No | Dual nationality |
| height_cm | Integer | No | Height in centimeters |
| weight_kg | Integer | No | Weight in kilograms |
| position | Enum | Yes | Primary position |
| secondary_position | Enum | No | Alternative position |
| preferred_foot | Enum | No | left/right/both |
| current_team_id | UUID | No | FK to teams |
| jersey_number | Integer | No | Current squad number |
| api_football_id | Integer | No | API-Football identifier |
| fbref_id | String | No | FBref identifier |
| photo_url | URL | No | Player photo |

**Position Taxonomy:**
```
GK  - Goalkeeper
DEF - Defender
  CB  - Center Back
  LB  - Left Back
  RB  - Right Back
  WB  - Wing Back
MID - Midfielder
  CDM - Central Defensive Midfielder
  CM  - Central Midfielder
  CAM - Central Attacking Midfielder
  LM  - Left Midfielder
  RM  - Right Midfielder
FWD - Forward
  LW  - Left Winger
  RW  - Right Winger
  CF  - Center Forward
  ST  - Striker
```

#### 4.1.4 Matches/Fixtures

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Internal identifier |
| league_id | UUID | Yes | FK to leagues |
| season | String | Yes | Season (e.g., "2024-25") |
| matchday | Integer | No | Round/matchweek |
| date | DateTime | Yes | Kickoff time (UTC) |
| status | Enum | Yes | scheduled/live/finished/postponed |
| home_team_id | UUID | Yes | FK to teams |
| away_team_id | UUID | Yes | FK to teams |
| home_score | Integer | No | Goals scored (home) |
| away_score | Integer | No | Goals scored (away) |
| home_score_ht | Integer | No | Half-time score (home) |
| away_score_ht | Integer | No | Half-time score (away) |
| venue | String | No | Stadium name |
| referee | String | No | Match referee |
| attendance | Integer | No | Crowd attendance |
| api_football_id | Integer | No | API-Football identifier |

**Match Status Flow:**
```
scheduled → live → finished
          ↘ postponed → rescheduled → live → finished
                      ↘ cancelled
```

#### 4.1.5 Player Season Statistics

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Internal identifier |
| player_id | UUID | Yes | FK to players |
| team_id | UUID | Yes | FK to teams |
| league_id | UUID | Yes | FK to leagues |
| season | String | Yes | Season |
| appearances | Integer | Yes | Total appearances |
| starts | Integer | No | Matches started |
| minutes_played | Integer | Yes | Total minutes |
| goals | Integer | Yes | Goals scored |
| assists | Integer | Yes | Assists |
| yellow_cards | Integer | No | Yellow cards |
| red_cards | Integer | No | Red cards |
| shots | Integer | No | Total shots |
| shots_on_target | Integer | No | Shots on target |
| passes | Integer | No | Total passes |
| pass_accuracy | Decimal | No | Pass completion % |
| key_passes | Integer | No | Key passes |
| dribbles | Integer | No | Successful dribbles |
| tackles | Integer | No | Tackles won |
| interceptions | Integer | No | Interceptions |
| clearances | Integer | No | Clearances |
| duels_won | Integer | No | Duels won |
| aerial_duels_won | Integer | No | Aerial duels won |

### 4.2 Advanced Data (Phase 2)

#### 4.2.1 Match Events

| Event Type | Description | Required Fields |
|------------|-------------|-----------------|
| goal | Goal scored | minute, player_id, assist_player_id, body_part, type (open_play/penalty/free_kick/own_goal) |
| shot | Shot attempted | minute, player_id, x, y, xG, outcome |
| pass | Pass attempted | minute, player_id, start_x, start_y, end_x, end_y, outcome |
| tackle | Tackle attempted | minute, player_id, opponent_id, outcome |
| foul | Foul committed | minute, player_id, card |
| substitution | Player substituted | minute, player_in_id, player_out_id |
| card | Card shown | minute, player_id, type (yellow/red) |

#### 4.2.2 Positional/Tracking Data

| Metric | Frequency | Source |
|--------|-----------|--------|
| Player positions | Per-frame (25fps) | Premium (Opta, StatsBomb) |
| Distance covered | Per-match | API-Football |
| Sprint count | Per-match | Premium |
| Heat maps | Aggregated | Derived |
| Pass networks | Per-match | Derived |

### 4.3 Derived Metrics (Phase 3)

| Metric | Formula/Method | Update Frequency |
|--------|----------------|------------------|
| xG (Expected Goals) | StatsBomb model / Custom ML | Per-shot |
| xA (Expected Assists) | Based on shot quality | Per-pass |
| PPDA (Passes Per Defensive Action) | Opp passes / defensive actions | Per-match |
| Progressive Passes | Passes moving ball significantly forward | Per-match |
| Progressive Carries | Dribbles moving ball significantly forward | Per-match |
| Shot-Creating Actions | Actions leading to shots | Per-match |
| Goal-Creating Actions | Actions leading to goals | Per-match |

---

## 5. Technical Specifications

### 5.1 Technology Stack

| Layer | Technology | Version | Justification |
|-------|------------|---------|---------------|
| Language | Python | 3.11+ | Data ecosystem, team expertise |
| Database | PostgreSQL | 15+ | Robust, JSON support, extensions |
| ORM | SQLAlchemy | 2.0+ | Industry standard, async support |
| Scheduler | APScheduler | 3.10+ | Flexible, persistent jobs |
| HTTP | Requests | 2.31+ | Simple, reliable |
| Scraping | Selenium | 4.16+ | JavaScript rendering |
| Data | Pandas | 2.1+ | Data transformation |
| CLI | Click | 8.1+ | Pythonic CLI framework |
| Console | Rich | 13.7+ | Beautiful output |
| Web | Flask | 3.0+ | Lightweight API |
| Dashboard | Dash | 2.x | Analytics visualization |

### 5.2 Infrastructure Requirements

#### Development Environment
```yaml
Minimum:
  CPU: 4 cores
  RAM: 8GB
  Storage: 50GB SSD
  Docker: Required

Recommended:
  CPU: 8 cores
  RAM: 16GB
  Storage: 100GB SSD
  Docker: Required
```

#### Production Environment
```yaml
Database Server:
  CPU: 8+ cores
  RAM: 32GB
  Storage: 500GB SSD (RAID)
  PostgreSQL: Dedicated instance

Application Server:
  CPU: 4+ cores
  RAM: 16GB
  Storage: 100GB SSD

Scheduler:
  CPU: 2 cores
  RAM: 4GB
  Storage: 20GB SSD
```

### 5.3 Database Configuration

```sql
-- Recommended PostgreSQL settings for this workload
shared_buffers = 8GB
effective_cache_size = 24GB
maintenance_work_mem = 2GB
work_mem = 256MB
max_connections = 100
max_parallel_workers_per_gather = 4

-- Extensions required
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- Text search
CREATE EXTENSION IF NOT EXISTS "btree_gin"; -- Composite indexes
```

### 5.4 API Rate Limits

| Source | Limit | Strategy |
|--------|-------|----------|
| API-Football (RapidAPI) | 100/day | Daily quota tracking, priority queue |
| FBref | None (be respectful) | 2 req/sec, random delays |
| StatsBomb | None (open data) | Bulk download, local cache |
| Soccerway | None (scraping) | 1 req/sec, session management |

### 5.5 Security Requirements

| Requirement | Implementation |
|-------------|----------------|
| API Keys | Environment variables, never in code |
| Database | Password auth, SSL in production |
| Secrets | .env files, gitignored |
| Network | Database not exposed publicly |
| Logging | No sensitive data in logs |

---

## 6. API & Data Sources

### 6.1 API-Football (Primary Source)

**Provider:** RapidAPI
**Tier:** Free (100 requests/day)
**Coverage:** 900+ leagues, live scores, statistics

#### Available Endpoints

| Endpoint | Request Cost | Data Provided |
|----------|--------------|---------------|
| /teams | 1 | Team details, squad |
| /players | 1 | Player info, career stats |
| /fixtures | 1 | Match list, scores, events |
| /fixtures/statistics | 1 | Match statistics |
| /standings | 1 | League table |
| /players/squads | 1 | Current squad |
| /transfers | 1 | Transfer history |
| /injuries | 1 | Injury reports |
| /predictions | 1 | Match predictions |

#### Optimal Collection Strategy

```
Daily Budget: 100 requests

Priority 1 - Essential (50 requests):
  - 5 leagues × standings = 5 requests
  - 5 leagues × fixtures (recent) = 5 requests
  - 5 leagues × teams = 5 requests
  - Top 10 teams × squad = 10 requests
  - Reserve = 25 requests

Priority 2 - Enhancement (40 requests):
  - Player statistics for key players
  - Match events for recent matches

Priority 3 - Depth (10 requests):
  - Transfers
  - Injuries
  - Predictions
```

### 6.2 FBref (Secondary Source)

**Type:** Web scraping
**Coverage:** Comprehensive statistics, historical data

#### Available Data

| Data Type | URL Pattern | Scraping Method |
|-----------|-------------|-----------------|
| League Table | /comps/{id}/schedule | BeautifulSoup |
| Match Report | /matches/{id} | BeautifulSoup |
| Player Stats | /players/{id} | BeautifulSoup |
| Team Stats | /squads/{id} | BeautifulSoup |
| Shooting | /comps/{id}/shooting | BeautifulSoup |
| Passing | /comps/{id}/passing | BeautifulSoup |

#### Scraping Best Practices

```python
# Rate limiting
REQUESTS_PER_SECOND = 2
RANDOM_DELAY_RANGE = (1, 3)  # seconds

# Headers
USER_AGENT = "Mozilla/5.0 (compatible; FootballDataBot/1.0)"

# Error handling
MAX_RETRIES = 3
BACKOFF_FACTOR = 2
```

### 6.3 StatsBomb (Free Tier)

**Type:** Open data JSON files
**Coverage:** Select competitions, event-level data

#### Available Competitions (Free)

| Competition | Seasons | Event Data |
|-------------|---------|------------|
| FIFA World Cup | 2018, 2022 | Full |
| Women's World Cup | 2019, 2023 | Full |
| UEFA Euro | 2020, 2024 | Full |
| La Liga | 2004-2021 | Full |
| Premier League | 2003-04 | Full |
| Champions League | Select finals | Full |

#### Integration Approach

```python
# StatsBomb provides a Python library
from statsbombpy import sb

# Get competitions
competitions = sb.competitions()

# Get matches
matches = sb.matches(competition_id=11, season_id=90)

# Get events (detailed)
events = sb.events(match_id=3788741)
```

### 6.4 Soccerway (Legacy)

**Type:** Web scraping
**Use Case:** Historical data, additional coverage

---

## 7. Database Schema

### 7.1 Core Schema

```sql
-- ===========================================
-- LEAGUES TABLE
-- ===========================================
CREATE TABLE leagues (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    code VARCHAR(10) NOT NULL UNIQUE,
    country VARCHAR(100),
    country_code VARCHAR(3),
    type VARCHAR(20) DEFAULT 'league',
    tier INTEGER,
    logo_url VARCHAR(500),
    current_season VARCHAR(10),

    -- External IDs
    api_football_id INTEGER UNIQUE,
    fbref_id VARCHAR(50) UNIQUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_type CHECK (type IN ('league', 'cup', 'international'))
);

-- ===========================================
-- TEAMS TABLE
-- ===========================================
CREATE TABLE teams (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(50),
    code VARCHAR(10),
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    founded INTEGER,
    stadium VARCHAR(200),
    stadium_capacity INTEGER,
    logo_url VARCHAR(500),
    primary_color VARCHAR(7),
    secondary_color VARCHAR(7),

    -- External IDs
    api_football_id INTEGER UNIQUE,
    fbref_id VARCHAR(50) UNIQUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- PLAYERS TABLE
-- ===========================================
CREATE TABLE players (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(150) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    display_name VARCHAR(100),
    date_of_birth DATE,
    age INTEGER GENERATED ALWAYS AS (
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))
    ) STORED,
    nationality VARCHAR(100),
    secondary_nationality VARCHAR(100),
    height_cm INTEGER,
    weight_kg INTEGER,
    position VARCHAR(10),
    secondary_position VARCHAR(10),
    preferred_foot VARCHAR(10),
    current_team_id UUID REFERENCES teams(id),
    jersey_number INTEGER,
    photo_url VARCHAR(500),

    -- External IDs
    api_football_id INTEGER UNIQUE,
    fbref_id VARCHAR(50) UNIQUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_position CHECK (position IN (
        'GK', 'CB', 'LB', 'RB', 'WB', 'CDM', 'CM', 'CAM',
        'LM', 'RM', 'LW', 'RW', 'CF', 'ST', 'DEF', 'MID', 'FWD'
    )),
    CONSTRAINT valid_foot CHECK (preferred_foot IN ('left', 'right', 'both'))
);

-- ===========================================
-- MATCHES TABLE
-- ===========================================
CREATE TABLE matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    league_id UUID NOT NULL REFERENCES leagues(id),
    season VARCHAR(10) NOT NULL,
    matchday INTEGER,
    date TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'scheduled',
    home_team_id UUID NOT NULL REFERENCES teams(id),
    away_team_id UUID NOT NULL REFERENCES teams(id),
    home_score INTEGER,
    away_score INTEGER,
    home_score_ht INTEGER,
    away_score_ht INTEGER,
    venue VARCHAR(200),
    referee VARCHAR(100),
    attendance INTEGER,

    -- External IDs
    api_football_id INTEGER UNIQUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_status CHECK (status IN (
        'scheduled', 'live', 'finished', 'postponed',
        'cancelled', 'abandoned'
    )),
    CONSTRAINT different_teams CHECK (home_team_id != away_team_id)
);

-- ===========================================
-- PLAYER SEASON STATS TABLE
-- ===========================================
CREATE TABLE player_season_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    player_id UUID NOT NULL REFERENCES players(id),
    team_id UUID NOT NULL REFERENCES teams(id),
    league_id UUID NOT NULL REFERENCES leagues(id),
    season VARCHAR(10) NOT NULL,

    -- Appearances
    appearances INTEGER DEFAULT 0,
    starts INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,

    -- Attack
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,

    -- Passing
    passes INTEGER DEFAULT 0,
    pass_accuracy DECIMAL(5,2),
    key_passes INTEGER DEFAULT 0,

    -- Defense
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    clearances INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,

    -- Duels
    duels_won INTEGER DEFAULT 0,
    aerial_duels_won INTEGER DEFAULT 0,
    dribbles INTEGER DEFAULT 0,

    -- Discipline
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    fouls_committed INTEGER DEFAULT 0,
    fouls_won INTEGER DEFAULT 0,

    -- Goalkeeping (GK only)
    saves INTEGER DEFAULT 0,
    goals_conceded INTEGER DEFAULT 0,
    clean_sheets INTEGER DEFAULT 0,
    penalties_saved INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(player_id, team_id, league_id, season)
);

-- ===========================================
-- STANDINGS TABLE
-- ===========================================
CREATE TABLE standings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    league_id UUID NOT NULL REFERENCES leagues(id),
    season VARCHAR(10) NOT NULL,
    team_id UUID NOT NULL REFERENCES teams(id),
    position INTEGER NOT NULL,
    played INTEGER DEFAULT 0,
    won INTEGER DEFAULT 0,
    drawn INTEGER DEFAULT 0,
    lost INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goal_difference INTEGER GENERATED ALWAYS AS (goals_for - goals_against) STORED,
    points INTEGER DEFAULT 0,
    form VARCHAR(10),

    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(league_id, season, team_id)
);

-- ===========================================
-- TEAM LEAGUE SEASONS (Junction Table)
-- ===========================================
CREATE TABLE team_league_seasons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    team_id UUID NOT NULL REFERENCES teams(id),
    league_id UUID NOT NULL REFERENCES leagues(id),
    season VARCHAR(10) NOT NULL,

    UNIQUE(team_id, league_id, season)
);
```

### 7.2 Tracking Schema

```sql
-- ===========================================
-- API USAGE TRACKING
-- ===========================================
CREATE TABLE api_usage_tracking (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    api_source VARCHAR(50) NOT NULL DEFAULT 'api-football',
    requests_used INTEGER DEFAULT 0,
    requests_limit INTEGER DEFAULT 100,
    last_request_at TIMESTAMP,

    UNIQUE(date, api_source)
);

-- ===========================================
-- API REQUEST LOG (Detailed)
-- ===========================================
CREATE TABLE api_request_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    api_source VARCHAR(50) NOT NULL,
    endpoint VARCHAR(200) NOT NULL,
    parameters JSONB,
    response_status INTEGER,
    response_time_ms INTEGER,
    data_records_returned INTEGER,
    error_message TEXT
);

-- ===========================================
-- ETL JOB RUNS
-- ===========================================
CREATE TABLE etl_job_runs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    job_parameters JSONB,

    CONSTRAINT valid_job_status CHECK (status IN (
        'running', 'completed', 'failed', 'cancelled'
    ))
);
```

### 7.3 Indexes

```sql
-- Performance indexes
CREATE INDEX idx_players_team ON players(current_team_id);
CREATE INDEX idx_players_name ON players USING gin(name gin_trgm_ops);
CREATE INDEX idx_players_position ON players(position);

CREATE INDEX idx_matches_date ON matches(date);
CREATE INDEX idx_matches_league_season ON matches(league_id, season);
CREATE INDEX idx_matches_teams ON matches(home_team_id, away_team_id);
CREATE INDEX idx_matches_status ON matches(status);

CREATE INDEX idx_player_stats_player ON player_season_stats(player_id);
CREATE INDEX idx_player_stats_team ON player_season_stats(team_id);
CREATE INDEX idx_player_stats_season ON player_season_stats(season);

CREATE INDEX idx_standings_league_season ON standings(league_id, season);

-- API tracking indexes
CREATE INDEX idx_api_log_timestamp ON api_request_log(timestamp);
CREATE INDEX idx_api_log_endpoint ON api_request_log(endpoint);
CREATE INDEX idx_etl_jobs_status ON etl_job_runs(status);
```

---

## 8. ETL Pipeline Architecture

### 8.1 Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     ETL PIPELINE FLOW                           │
└─────────────────────────────────────────────────────────────────┘

  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
  │ Source  │────▶│ Extract │────▶│Transform│────▶│  Load   │
  │         │     │         │     │         │     │         │
  └─────────┘     └─────────┘     └─────────┘     └─────────┘
       │               │               │               │
       │               │               │               │
       ▼               ▼               ▼               ▼
  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
  │API/Web  │     │Raw JSON │     │Validated│     │Database │
  │         │     │/HTML    │     │Objects  │     │         │
  └─────────┘     └─────────┘     └─────────┘     └─────────┘
```

### 8.2 ETL Components

#### 8.2.1 API-Football ETL (`etl/api_football_etl.py`)

```python
class APIFootballETL:
    """
    Main ETL orchestrator for API-Football data.

    Responsibilities:
    - Coordinate data collection across entities
    - Manage ID mappings between API and internal IDs
    - Track API usage
    - Handle errors gracefully
    """

    def process_league_season(self, league_key: str, season: int) -> Dict:
        """
        Complete pipeline for a league season.

        Order of operations:
        1. Get/create league record
        2. Fetch and upsert teams
        3. Fetch and upsert players
        4. Fetch and upsert fixtures
        5. Fetch and update standings
        """
        pass
```

#### 8.2.2 Batch Loader (`database/batch_loader.py`)

```python
class BatchLoader:
    """
    Handles efficient bulk database operations.

    Features:
    - Batch upserts with ON CONFLICT
    - Transaction management
    - Progress tracking
    - Error recovery
    """

    def upsert_teams(self, teams: List[Dict]) -> int:
        """Upsert multiple teams atomically."""
        pass

    def upsert_players(self, players: List[Dict]) -> int:
        """Upsert multiple players atomically."""
        pass
```

### 8.3 Data Transformation Rules

#### 8.3.1 ID Mapping

```python
# API-Football ID to Internal UUID mapping strategy
{
    "api_football_id": 33,      # External ID (stable)
    "internal_id": "uuid-...",  # Internal UUID
    "name": "Manchester United"  # Human readable
}

# Lookup priority:
# 1. Match by api_football_id (fastest, unique)
# 2. Match by name + country (fallback)
# 3. Create new record (last resort)
```

#### 8.3.2 Field Mapping

```python
# API-Football to Internal Schema Mapping
TEAM_FIELD_MAPPING = {
    "team.id": "api_football_id",
    "team.name": "name",
    "team.code": "code",
    "team.country": "country",
    "team.founded": "founded",
    "team.logo": "logo_url",
    "venue.name": "stadium",
    "venue.capacity": "stadium_capacity",
    "venue.city": "city"
}

PLAYER_FIELD_MAPPING = {
    "player.id": "api_football_id",
    "player.name": "name",
    "player.firstname": "first_name",
    "player.lastname": "last_name",
    "player.birth.date": "date_of_birth",
    "player.nationality": "nationality",
    "player.height": "height_cm",  # Parse "180 cm" → 180
    "player.weight": "weight_kg",  # Parse "75 kg" → 75
    "player.photo": "photo_url"
}
```

### 8.4 Error Handling

```python
class ETLError(Exception):
    """Base ETL exception."""
    pass

class RateLimitError(ETLError):
    """API rate limit exceeded."""
    pass

class DataValidationError(ETLError):
    """Data failed validation."""
    pass

class TransformationError(ETLError):
    """Data transformation failed."""
    pass

# Retry strategy
RETRY_CONFIG = {
    "max_retries": 3,
    "backoff_factor": 2,
    "retryable_errors": [
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        RateLimitError
    ]
}
```

---

## 9. Scheduler & Automation

### 9.1 Job Schedule

```python
SCHEDULED_JOBS = {
    "daily_collection": {
        "trigger": "cron",
        "hour": 6,
        "minute": 0,
        "timezone": "UTC",
        "description": "Daily data collection for all leagues"
    },
    "standings_update": {
        "trigger": "cron",
        "hour": "*/6",  # Every 6 hours
        "description": "Update league standings"
    },
    "live_matches": {
        "trigger": "interval",
        "minutes": 5,
        "description": "Check for live match updates"
    },
    "quota_reset_alert": {
        "trigger": "cron",
        "hour": 0,
        "minute": 5,
        "timezone": "UTC",
        "description": "Log quota reset confirmation"
    }
}
```

### 9.2 Job Definitions

| Job | Schedule | Budget | Priority | Description |
|-----|----------|--------|----------|-------------|
| `daily_collection` | 06:00 UTC | 90 req | High | Full daily collection |
| `standings_update` | Every 6h | 5 req | Medium | League tables only |
| `live_matches` | Every 5m | 0 req | Low | Check for updates (no API) |
| `quota_check` | 00:00 UTC | 0 req | High | Verify quota reset |

### 9.3 Scheduler Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SCHEDULER SYSTEM                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐     ┌─────────────────────────────┐   │
│  │   APScheduler   │────▶│     Job Store (SQLite)      │   │
│  │  (Executor)     │     │  - Persistent jobs          │   │
│  └────────┬────────┘     │  - Survives restarts        │   │
│           │              └─────────────────────────────┘   │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Job Queue                          │   │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐    │   │
│  │  │Daily   │  │Standings│  │Live    │  │Custom  │    │   │
│  │  │Collect │  │Update  │  │Matches │  │Job     │    │   │
│  │  └────────┘  └────────┘  └────────┘  └────────┘    │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│                          ▼                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                 Execution Engine                     │   │
│  │                                                      │   │
│  │  1. Check quota before execution                    │   │
│  │  2. Execute job function                            │   │
│  │  3. Log results to etl_job_runs                     │   │
│  │  4. Handle failures gracefully                      │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 9.4 Quota Management

```python
class QuotaManager:
    """
    Manages API quota across all jobs.

    Rules:
    1. Never exceed daily limit (100)
    2. Reserve 10% for manual operations
    3. Distribute across jobs by priority
    4. Log all usage for analysis
    """

    DAILY_LIMIT = 100
    RESERVED_BUFFER = 10

    def get_available_quota(self) -> int:
        """Returns requests available for scheduled jobs."""
        used = self.get_requests_today()
        return self.DAILY_LIMIT - self.RESERVED_BUFFER - used

    def can_execute_job(self, estimated_cost: int) -> bool:
        """Check if job can execute within quota."""
        return self.get_available_quota() >= estimated_cost
```

---

## 10. CLI & Developer Tools

### 10.1 CLI Commands

```bash
# Main entry point
python cli.py [COMMAND] [OPTIONS]

# Command Groups
Commands:
  api-football   API-Football data collection commands
  db             Database management commands
  etl            ETL pipeline commands
  scheduler      Scheduler management commands
```

### 10.2 API-Football Commands

```bash
# Collect league data
python cli.py api-football collect-league premier-league --season 2024

# Update standings only
python cli.py api-football update-standings --leagues premier-league la-liga

# Check API quota
python cli.py api-football check-quota

# Run daily collection
python cli.py api-football run-daily

# Test API connection
python cli.py api-football test-connection

# List available leagues
python cli.py api-football list-leagues
```

### 10.3 Database Commands

```bash
# Initialize database
python cli.py db init

# Run migrations
python cli.py db migrate

# Show database statistics
python cli.py db stats

# Export data
python cli.py db export --format csv --table players

# Backup database
python cli.py db backup --output ./backups/
```

### 10.4 ETL Commands

```bash
# Run specific ETL pipeline
python cli.py etl run --source api-football --entity teams

# Validate data quality
python cli.py etl validate --entity players

# Show ETL job history
python cli.py etl history --limit 10

# Dry run (no database writes)
python cli.py etl run --source fbref --dry-run
```

### 10.5 Scheduler Commands

```bash
# Start scheduler daemon
python run_scheduler.py --daemon

# Run job immediately
python run_scheduler.py --run-now

# Set custom time
python run_scheduler.py --hour 8 --minute 30

# List scheduled jobs
python cli.py scheduler list

# Pause/resume jobs
python cli.py scheduler pause daily_collection
python cli.py scheduler resume daily_collection
```

---

## 11. Current Implementation Status

### 11.1 Component Status Matrix

| Component | Status | Completeness | Notes |
|-----------|--------|--------------|-------|
| **Database** |
| Core Schema | ✅ Complete | 100% | All tables created |
| Tracking Schema | ⚠️ Partial | 50% | SQL ready, not applied |
| Indexes | ✅ Complete | 100% | Performance indexes in place |
| **Data Sources** |
| API-Football Client | ✅ Complete | 95% | All endpoints supported |
| FBref Scraper | ⚠️ Partial | 60% | Basic scraping works |
| StatsBomb Integration | ⚠️ Partial | 40% | Library integrated |
| Soccerway Scraper | ⚠️ Legacy | 30% | Needs update |
| **ETL Pipelines** |
| API-Football ETL | ✅ Complete | 90% | Minor tracking improvements needed |
| FBref ETL | ⚠️ Partial | 50% | Needs more entity support |
| StatsBomb ETL | ⚠️ Partial | 40% | Basic integration only |
| Batch Loader | ✅ Complete | 95% | All entities supported |
| **Automation** |
| APScheduler Setup | ✅ Complete | 90% | Jobs defined |
| Job Persistence | ⚠️ Not Started | 0% | SQLite store not configured |
| Quota Tracking | ⚠️ Partial | 60% | In-memory only |
| **CLI** |
| Core Commands | ✅ Complete | 100% | All basic commands |
| API-Football Commands | ✅ Complete | 90% | Full command set |
| Database Commands | ⚠️ Partial | 60% | Needs export/backup |
| **Infrastructure** |
| Docker Setup | ✅ Complete | 100% | PostgreSQL containerized |
| Environment Config | ✅ Complete | 100% | .env configured |
| Logging | ⚠️ Partial | 50% | Basic logging only |

### 11.2 Data Coverage Status

| League | Teams | Players | Matches | Standings | Seasons |
|--------|-------|---------|---------|-----------|---------|
| Premier League | ✅ 20 | ✅ 500+ | ✅ 380+ | ✅ | 2019-2024 |
| La Liga | ✅ 20 | ✅ 500+ | ✅ 380+ | ✅ | 2024 |
| Serie A | ✅ 20 | ✅ 500+ | ✅ 380+ | ✅ | 2024 |
| Bundesliga | ✅ 18 | ✅ 450+ | ✅ 306+ | ✅ | 2024 |
| Ligue 1 | ✅ 18 | ✅ 450+ | ✅ 306+ | ✅ | 2024 |

**Current Totals:**
- Teams: 112
- Players: 718
- Matches: 3,006
- Player Stats Records: 7,600+

### 11.3 Code Metrics

```
Source Files: 36
Total Lines of Code: ~5,000

By Directory:
  scrapers/       : 1,200 lines (API clients, web scrapers)
  etl/            : 800 lines (transformation logic)
  database/       : 700 lines (models, loaders)
  scheduler/      : 400 lines (job definitions)
  cli.py          : 600 lines (command interface)
  utils/          : 300 lines (helpers)
  tests/          : 500 lines (test coverage)
```

---

## 12. Gap Analysis & Vulnerabilities

### 12.1 Critical Gaps

| Gap | Severity | Impact | Remediation |
|-----|----------|--------|-------------|
| **No Production Logging** | High | Can't debug issues | Implement structured logging |
| **No Data Validation** | High | Bad data enters DB | Add schema validation |
| **No Retry Mechanism** | High | Failed requests lost | Implement exponential backoff |
| **No Monitoring** | High | Blind to failures | Add alerting system |
| **Single Point of Failure** | High | No redundancy | Add fallback sources |

### 12.2 Security Vulnerabilities

| Vulnerability | Risk | Current State | Remediation |
|---------------|------|---------------|-------------|
| API Key in .env | Medium | Gitignored but local | Use secrets manager |
| No Auth on CLI | Low | Local use only | Add user authentication |
| Plain DB Password | Medium | Docker default | Use strong passwords |
| No SSL | Medium | Development only | Enable for production |

### 12.3 Data Quality Issues

| Issue | Frequency | Impact | Fix |
|-------|-----------|--------|-----|
| Duplicate Players | Occasional | Data integrity | Add deduplication logic |
| Missing Positions | ~5% of players | Query issues | Add position inference |
| Stale Standings | During matches | Incorrect data | Increase update frequency |
| Missing DOB | ~10% of players | Age calculations fail | Source from multiple APIs |

### 12.4 Technical Debt

| Debt Item | Priority | Effort | Impact |
|-----------|----------|--------|--------|
| FBref scraper outdated | High | Medium | Data source unavailable |
| No type hints | Medium | High | Code maintainability |
| Hardcoded league IDs | Medium | Low | Flexibility |
| No async support | Low | High | Performance |
| No API versioning | Low | Medium | Breaking changes |

### 12.5 Missing Features (vs FotMob/SofaScore)

| Feature | FotMob | SofaScore | Our Platform | Priority |
|---------|--------|-----------|--------------|----------|
| Live Scores | ✅ | ✅ | ❌ | High |
| Match Events (goals, cards) | ✅ | ✅ | ⚠️ Partial | High |
| Player Heat Maps | ✅ | ✅ | ❌ | Medium |
| xG per Shot | ✅ | ✅ | ❌ | High |
| Formation Detection | ✅ | ✅ | ❌ | Medium |
| Momentum Graphs | ✅ | ✅ | ❌ | Low |
| Push Notifications | ✅ | ✅ | ❌ | Low |
| News Integration | ✅ | ✅ | ❌ | Low |
| Transfer Rumors | ✅ | ✅ | ❌ | Low |
| Video Highlights | ✅ | ✅ | ❌ | Low |

---

## 13. Team Requirements

### 13.1 Team Structure

```
┌─────────────────────────────────────────────────────────────┐
│                    ENGINEERING TEAM                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │               Technical Lead (1)                     │   │
│  │  - Architecture decisions                           │   │
│  │  - Code reviews                                     │   │
│  │  - Technical roadmap                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│         ┌────────────────┼────────────────┐                │
│         ▼                ▼                ▼                │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐         │
│  │ Backend    │   │ Data       │   │ DevOps     │         │
│  │ Engineers  │   │ Engineers  │   │ Engineer   │         │
│  │ (2)        │   │ (2)        │   │ (1)        │         │
│  │            │   │            │   │            │         │
│  │ - API      │   │ - ETL      │   │ - Infra    │         │
│  │ - Database │   │ - Scrapers │   │ - CI/CD    │         │
│  │ - Scheduler│   │ - Quality  │   │ - Monitor  │         │
│  └────────────┘   └────────────┘   └────────────┘         │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Total: 6 Engineers
```

### 13.2 Role Definitions

#### Technical Lead
- **Experience:** 7+ years, 3+ years leading teams
- **Skills:** System design, Python, PostgreSQL, API design
- **Responsibilities:**
  - Define technical architecture
  - Review all code changes
  - Mentor team members
  - Interface with stakeholders
  - Make build vs buy decisions

#### Backend Engineer
- **Experience:** 3+ years Python development
- **Skills:** Flask/FastAPI, SQLAlchemy, PostgreSQL, REST APIs
- **Responsibilities:**
  - Build and maintain API layer
  - Database schema design
  - Query optimization
  - Scheduler implementation
  - CLI tool development

#### Data Engineer
- **Experience:** 3+ years data engineering
- **Skills:** ETL pipelines, Web scraping, Data quality
- **Responsibilities:**
  - Build ETL pipelines
  - Integrate new data sources
  - Data validation and cleaning
  - Derived metrics calculation
  - Documentation

#### DevOps Engineer
- **Experience:** 3+ years infrastructure
- **Skills:** Docker, Kubernetes, CI/CD, Monitoring
- **Responsibilities:**
  - Infrastructure as code
  - CI/CD pipelines
  - Monitoring and alerting
  - Performance optimization
  - Security hardening

### 13.3 Hiring Timeline

| Phase | Role | Priority | Timeline |
|-------|------|----------|----------|
| Phase 1 | Technical Lead | Critical | Immediate |
| Phase 1 | Data Engineer | Critical | Immediate |
| Phase 2 | Backend Engineer | High | Month 1-2 |
| Phase 2 | Backend Engineer | High | Month 1-2 |
| Phase 3 | Data Engineer | Medium | Month 3-4 |
| Phase 3 | DevOps Engineer | Medium | Month 3-4 |

### 13.4 External Resources

| Resource | Purpose | Cost Estimate |
|----------|---------|---------------|
| RapidAPI Pro | More API requests | $50-200/month |
| Opta Data | Premium match events | $5,000+/month |
| StatsBomb Pro | Full event data | $2,000+/month |
| Cloud Hosting | Production infra | $500-2,000/month |

---

## 14. Roadmap & Milestones

### 14.1 Phase 1: Foundation Completion (4 weeks)

**Objective:** Complete core infrastructure and fix critical gaps

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 1 | Apply tracking schema, fix logging | Working quota tracking |
| 2 | Add data validation, retry logic | Robust ETL pipelines |
| 3 | Complete scheduler setup | Production scheduler |
| 4 | Documentation, testing | 80% test coverage |

**Success Criteria:**
- [ ] All tracking tables applied
- [ ] Structured logging implemented
- [ ] Retry mechanism working
- [ ] Scheduler running 24/7
- [ ] Documentation complete

### 14.2 Phase 2: Data Expansion (8 weeks)

**Objective:** Expand data coverage and add match events

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 1-2 | Add match events schema | Events table, API integration |
| 3-4 | Integrate StatsBomb fully | Event-level data for select matches |
| 5-6 | Add 10 more leagues | 15 total leagues |
| 7-8 | Historical data backfill | 5+ seasons per league |

**Success Criteria:**
- [ ] Match events captured
- [ ] 15+ leagues supported
- [ ] 5+ seasons of historical data
- [ ] xG data available

### 14.3 Phase 3: Intelligence Layer (8 weeks)

**Objective:** Add derived metrics and quality scoring

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 1-2 | xG/xA calculation engine | Derived metrics |
| 3-4 | Data quality scoring | Quality dashboard |
| 5-6 | Anomaly detection | Automated alerts |
| 7-8 | Data reconciliation | Cross-source validation |

**Success Criteria:**
- [ ] xG calculated for all shots
- [ ] Quality score for each entity
- [ ] Anomalies auto-detected
- [ ] Data sources cross-validated

### 14.4 Phase 4: Scale & Production (8 weeks)

**Objective:** Production-ready, scalable platform

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 1-2 | Kubernetes deployment | Containerized platform |
| 3-4 | GraphQL API | Developer-friendly API |
| 5-6 | Real-time streaming | WebSocket feeds |
| 7-8 | Enterprise features | SLA, monitoring, docs |

**Success Criteria:**
- [ ] 99.9% uptime
- [ ] < 1s API response time
- [ ] Real-time match updates
- [ ] Enterprise documentation

### 14.5 Milestone Summary

| Milestone | Target Date | Key Metric |
|-----------|-------------|------------|
| M1: Foundation Complete | +4 weeks | All critical gaps fixed |
| M2: Event Data | +8 weeks | Match events captured |
| M3: League Expansion | +12 weeks | 15+ leagues |
| M4: Derived Metrics | +16 weeks | xG available |
| M5: Production Ready | +24 weeks | 99.9% uptime |

---

## 15. Quality Standards

### 15.1 Code Standards

```python
# Code style: Black + isort
# Type hints: Required for public APIs
# Docstrings: Google style, required for all functions
# Test coverage: Minimum 80%

# Example function
def process_player_data(
    raw_data: Dict[str, Any],
    team_id: UUID,
    season: str
) -> Optional[PlayerStats]:
    """
    Process raw player data into PlayerStats object.

    Args:
        raw_data: Raw JSON from API-Football
        team_id: Internal team UUID
        season: Season string (e.g., "2024-25")

    Returns:
        PlayerStats object if valid, None if validation fails

    Raises:
        ValidationError: If required fields missing
        TransformationError: If data transformation fails
    """
    pass
```

### 15.2 Testing Requirements

| Test Type | Coverage Target | Tools |
|-----------|-----------------|-------|
| Unit Tests | 80% | pytest |
| Integration Tests | Core flows | pytest |
| E2E Tests | Critical paths | pytest |
| Load Tests | API endpoints | locust |

```python
# Test file structure
tests/
├── unit/
│   ├── test_etl.py
│   ├── test_transformers.py
│   └── test_validators.py
├── integration/
│   ├── test_api_football.py
│   ├── test_database.py
│   └── test_scheduler.py
└── e2e/
    ├── test_full_collection.py
    └── test_cli_commands.py
```

### 15.3 Data Quality Standards

| Metric | Target | Measurement |
|--------|--------|-------------|
| Completeness | >95% | Non-null required fields |
| Accuracy | >99% | Cross-source validation |
| Timeliness | <24h | Data freshness |
| Consistency | 100% | Referential integrity |
| Uniqueness | 100% | No duplicates |

### 15.4 API Standards

```yaml
# API Response Format
success_response:
  status: "success"
  data: {}
  meta:
    total: 100
    page: 1
    per_page: 20

error_response:
  status: "error"
  error:
    code: "VALIDATION_ERROR"
    message: "Invalid season format"
    details: {}
```

### 15.5 Documentation Standards

| Document | Format | Update Frequency |
|----------|--------|------------------|
| API Docs | OpenAPI 3.0 | Every release |
| Code Docs | Sphinx | Every commit |
| User Guide | Markdown | Monthly |
| Architecture | Diagrams | Quarterly |
| Runbooks | Markdown | As needed |

---

## Appendix

### A. Glossary

| Term | Definition |
|------|------------|
| xG | Expected Goals - probability a shot results in a goal |
| xA | Expected Assists - xG value of shots from a player's passes |
| PPDA | Passes Per Defensive Action - pressing intensity metric |
| ETL | Extract, Transform, Load - data pipeline pattern |
| Upsert | Insert or update if exists |
| Idempotent | Operation safe to repeat multiple times |

### B. League ID Mapping

```python
LEAGUE_IDS = {
    'premier-league': 39,   # England
    'la-liga': 140,         # Spain
    'serie-a': 135,         # Italy
    'bundesliga': 78,       # Germany
    'ligue-1': 61,          # France
    'eredivisie': 88,       # Netherlands
    'primeira-liga': 94,    # Portugal
    'championship': 40,     # England 2nd
    'champions-league': 2,  # UEFA
    'europa-league': 3,     # UEFA
}
```

### C. API-Football Response Samples

```json
// Team Response
{
  "team": {
    "id": 33,
    "name": "Manchester United",
    "code": "MUN",
    "country": "England",
    "founded": 1878,
    "logo": "https://media.api-sports.io/football/teams/33.png"
  },
  "venue": {
    "name": "Old Trafford",
    "capacity": 76212,
    "city": "Manchester"
  }
}

// Player Stats Response
{
  "player": {
    "id": 909,
    "name": "Bruno Fernandes"
  },
  "statistics": [{
    "team": {"id": 33},
    "league": {"id": 39},
    "games": {"appearances": 38, "minutes": 3200},
    "goals": {"total": 10, "assists": 8},
    "shots": {"total": 85, "on": 32}
  }]
}
```

### D. Environment Variables

```bash
# Required
API_FOOTBALL_KEY=your_api_key_here
DB_USER=postgres
DB_PASSWORD=secure_password
DB_HOST=localhost
DB_PORT=5434
DB_NAME=football_data

# Optional
LOG_LEVEL=INFO
SCHEDULER_ENABLED=true
RATE_LIMIT_BUFFER=10
```

### E. References

1. API-Football Documentation: https://www.api-football.com/documentation-v3
2. StatsBomb Open Data: https://github.com/statsbomb/open-data
3. FBref: https://fbref.com
4. PostgreSQL 15 Documentation: https://www.postgresql.org/docs/15/
5. APScheduler Documentation: https://apscheduler.readthedocs.io/

---

**Document Revision History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | January 2026 | Engineering | Initial PRD |

---

*This document is confidential and intended for internal use only.*
