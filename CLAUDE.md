# Data-ETL-Pipeline - Claude Context File

> This file provides comprehensive context for Claude to work effectively on this project.
> Part of the larger Scouting Project ecosystem.

## Project Overview

**Purpose**: Football data collection and ETL pipeline supporting professional scouting analytics.

**Status**: Active development - FotMob integration complete, FBref and Soccerway removed.

**Tech Stack**:
- Python 3.x
- PostgreSQL 15 (Docker)
- Flask 3.0.0 (API/Dashboard)
- SQLAlchemy 2.0.23 (ORM)
- APScheduler 3.10.4 (Task scheduling)
- Requests (FotMob API client)

## Quick Reference

### Entry Points
```bash
# Main CLI
python cli.py <command-group> <command> [options]

# Web Server
python server/app.py  # Runs on port 5001

# Scheduler
python run_scheduler.py

# Database (Docker)
docker-compose up -d
```

### Key Commands
```bash
# FotMob (primary source - no API key needed, no request limit)
python cli.py fotmob collect-league --league premier-league
python cli.py fotmob collect-deep --league premier-league
python cli.py fotmob collect-all
python cli.py fotmob test-connection
python cli.py fotmob list-leagues

# API-Football (secondary source - 100 req/day limit)
python cli.py api-football collect-league --league premier-league --season 2024
python cli.py api-football check-quota
python cli.py api-football test-connection

# StatsBomb (historical progressive actions, SCA/GCA)
python cli.py statsbomb list-competitions
python cli.py statsbomb collect-advanced --competition-id 11 --season-id 90

# Understat (xA, npxG, xGChain, xGBuildup)
python cli.py understat test-connection
python cli.py understat collect-league --league premier-league
python cli.py understat collect-all --season 2024

# Full refresh (all sources)
python cli.py full-refresh
python cli.py full-refresh --season 2024 --skip-api-football

# System
python cli.py system health
python cli.py system stats
```

### Database Connection
```
Host: localhost
Port: 5434
Database: football_data
User: postgres
Password: postgres
```

## Project Structure

```
Data-ETL-Pipeline/
├── cli.py                 # Main CLI interface
├── server/
│   └── app.py             # Flask API server (803 lines)
├── etl/
│   ├── fotmob_etl.py          # FotMob pipeline (PRIMARY)
│   ├── api_football_etl.py    # API-Football pipeline (SECONDARY)
│   ├── statsbomb_etl.py       # StatsBomb basic pipeline
│   ├── statsbomb_advanced_etl.py  # StatsBomb advanced (progressive, SCA/GCA)
│   └── understat_etl.py       # Understat xG enrichment
├── scrapers/
│   ├── fotmob/
│   │   ├── client.py          # FotMob API client (rate limiting, caching)
│   │   ├── data_parser.py     # Response parser (leagues, teams, players, matches)
│   │   └── constants.py       # League IDs, names, mappings
│   ├── api_football/client.py
│   ├── statsbomb/client.py
│   └── understat/
│       └── client.py          # Understat web scraper (xA, npxG, xGChain)
├── database/
│   ├── connection.py          # Connection pooling
│   ├── batch_loader.py        # Batch operations
│   ├── schema.sql             # Main schema (11+ tables)
│   ├── schema_api_football.sql
│   ├── migration_002_fotmob_and_leagues.sql
│   └── migration_003_advanced_stats.sql  # Progressive actions, xA, pressing columns
├── utils/
│   ├── api_tracker.py         # API quota management
│   ├── monitoring.py          # Health checks
│   ├── validators.py          # Data validation
│   ├── retry.py               # Retry/circuit breaker
│   ├── deduplication.py       # Duplicate handling (fotmob_id based)
│   └── logging_config.py      # Structured logging
├── scheduler/
│   ├── job_scheduler.py       # APScheduler setup
│   └── jobs.py                # Job definitions (FotMob + API-Football)
├── config/
│   └── settings.py            # Configuration
├── logs/
│   ├── football_etl.log
│   └── errors.log
├── data/
│   ├── raw/                   # Raw scraped data
│   ├── processed/             # Processed data
│   └── cache/                 # Response cache
└── docker-compose.yml         # PostgreSQL + Adminer
```

## Data Sources

| Source | Type | Limit | Status |
|--------|------|-------|--------|
| FotMob | Public JSON API | ~2s rate limit (self-imposed) | **PRIMARY** - Working |
| API-Football | REST API (RapidAPI) | 100 req/day (free) | Secondary - Working |
| StatsBomb | Open data API | Unlimited | Working |

**Removed Sources**: FBref (web scraping, removed), Soccerway (API broken, removed)

## Supported Leagues (8)

| League | Country | FotMob ID | API-Football ID |
|--------|---------|-----------|-----------------|
| Premier League | England | 47 | 39 |
| La Liga | Spain | 87 | 140 |
| Serie A | Italy | 55 | 135 |
| Bundesliga | Germany | 54 | 78 |
| Ligue 1 | France | 53 | 61 |
| Eredivisie | Netherlands | 57 | 88 |
| Brasileiro Serie A | Brazil | 268 | 71 |
| Argentina Primera | Argentina | 112 | 128 |

## Database Schema (Key Tables)

### Reference Tables
- `data_sources` - Track data origin (fotmob, api_football, statsbomb, transfermarkt)
- `leagues` - 8 leagues with fotmob_id
- `seasons` - 2020-21 through 2024-25

### Entity Tables
- `teams` - Team info with fotmob_id for cross-referencing
- `players` - Player demographics with fotmob_id
- `team_season_stats` - Aggregated team stats per season (standings, xG)
- `player_season_stats` - Aggregated player stats per season

### Match Tables
- `matches` - Match records with scores, xG, fotmob_match_id
- `team_match_stats` - Detailed team performance per match
- `player_match_stats` - Detailed player performance (30+ metrics)

## FotMob API Reference

### Endpoints Used
- `GET /api/leagues?id={id}` - League standings, matches, seasons
- `GET /api/leagues?id={id}&season={season}` - Historical season data
- `GET /api/teams?id={id}` - Team details + squad
- `GET /api/playerData?id={id}` - Player bio + season stats + career
- `GET /api/matchDetails?matchId={id}` - Match details + stats + player stats

### Key Response Paths
- Standings: `table.data[0].table.all[i]`
- xG Table: `table.data[0].table.xg[i]`
- Team Squad: `details.sportsTeamJSONLD.athlete`
- Player Info: `playerInformation[]`
- Match Stats: `stats.Periods.All[i].stats[j]` (home=stats[0], away=stats[1])
- Player Match Stats: `playerStats[playerId]`

### Rate Limiting
- 2s delay + random jitter between requests
- Exponential backoff on 429/503 errors
- Browser-like headers required (User-Agent, Referer, Origin)
- Response caching: 1hr for live data, 24hr for historical

## Known Issues

> **Full issue tracking in `ISSUES.md`**. Summary of critical issues below.

### Critical (P0) Issues

| Issue | Description | Status |
|-------|-------------|--------|
| ISSUE-016 | Event backbone not operational (schema exists, ETL doesn't persist) | Open |
| ISSUE-017 | xA/xag column inconsistency (use `xa` not `xag`) | Open |
| ISSUE-018 | Hardcoded API keys in test files (SECURITY) | Open |
| ISSUE-020 | Weekly scheduler runs daily (wrong trigger) | Open |
| ISSUE-021 | Hardcoded season values (2024 instead of dynamic) | Open |

### High (P1) Issues

| Issue | Description | Status |
|-------|-------------|--------|
| ISSUE-022 | NoneType iteration in deep stats parser | Open |
| ISSUE-024 | Missing derived scouting features (PPDA, progressive profiles) | Open |
| ISSUE-025 | Missing identity resolution layer | Open |
| ISSUE-026 | Missing data quality gates | Open |

### Resolved Issues (Recent)

| Issue | Description | Fixed |
|-------|-------------|-------|
| ISSUE-015.1 | xG parser bug (matched "xG against" instead of "xG") | 2026-02-10 |

### Legacy Issues (Mitigated)

### 1. API-Football Free Plan Pagination Limit
**Status**: Open (mitigated)
**Impact**: Cannot fetch full player rosters (only 60 players per team max)
**Mitigation**: FotMob is now primary source and has no pagination limits

### 2. API-Football Missing Schema Columns
**Status**: Resolved
**Impact**: api_football_id, api_football_fixture_id columns missing from some tables
**Fix**: Run `database/migration_001_api_football_ids.sql` and `migration_002_fotmob_and_leagues.sql`

### 3. Rate Limiting Recovery
**Status**: Open (mitigated)
**Impact**: API-Football retries 3x on rate limit, may lose data
**Mitigation**: FotMob has no API key limit; use it as primary

## Environment Variables (.env)

```bash
API_FOOTBALL_KEY=<your-key>  # Optional for API-Football (secondary source)
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5434
DB_NAME=football_data
```

## API Endpoints (Flask Server)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard |
| `/api/stats` | GET | Overall counts |
| `/api/stats/detailed` | GET | Breakdown by league/season |
| `/api/leagues` | GET | League listing |
| `/api/players/top` | GET | Top players with filters |
| `/api/players/<id>` | GET | Player details |
| `/api/teams` | GET | Team listing |
| `/api/teams/<id>` | GET | Team details with squad |
| `/api/matches` | GET | Recent matches |
| `/api/matches/<id>` | GET | Match details |
| `/api/health` | GET | Data health indicators |

## Coding Conventions

### SQL Queries
- Use parameterized queries with `:param_name` syntax
- Always use `fetch=True` for SELECT, `fetch=False` for mutations
- Batch operations via `BatchLoader.batch_upsert()`

### Logging
- Use structured logging via `utils/logging_config.py`
- Error logs go to `logs/errors.log`
- All logs to `logs/football_etl.log`

### Data Validation
- Use schemas from `utils/validators.py`
- Validate before database insertion
- Handle missing/null values gracefully

### Rate Limiting
- FotMob: 2s delay + jitter, exponential backoff on errors
- API-Football: 1 req/sec, max 100/day
- Implement exponential backoff on failures

## Development Workflow

1. **Start Database**: `docker-compose up -d`
2. **Test FotMob**: `python cli.py fotmob test-connection`
3. **Run Health Check**: `python cli.py system health`
4. **Collect Data**: `python cli.py fotmob collect-league --league premier-league`
5. **View Dashboard**: `python server/app.py` -> http://localhost:5001

## Testing

```bash
# Run tests
pytest

# Test FotMob connection
python cli.py fotmob test-connection

# Test API-Football connection
python cli.py api-football test-connection
```

## Important Notes for Claude

1. **This is part of a larger scouting project** - Changes here affect the main scouting application
2. **FotMob is the primary data source** - No API key needed, no daily limit
3. **API-Football is secondary** - Only 100 requests/day on free tier
4. **FBref and Soccerway have been completely removed** - Do not reference them
5. **8 leagues supported** - Including South American leagues (Brazil, Argentina)
6. **fotmob_id is the primary external ID** - Used across teams, players, matches, leagues
7. **Data deduplication uses fotmob_id** - See utils/deduplication.py
8. **Schema includes team_season_stats and player_season_stats** - Used for standings and aggregated stats

## Files to Reference

When working on specific features:
- **CLI commands**: `cli.py`
- **FotMob ETL**: `etl/fotmob_etl.py`
- **FotMob scraper**: `scrapers/fotmob/`
- **API-Football ETL**: `etl/api_football_etl.py`
- **Database operations**: `database/connection.py`, `database/batch_loader.py`
- **API endpoints**: `server/app.py`
- **Configuration**: `config/settings.py`
- **Scheduler jobs**: `scheduler/jobs.py`
- **Logs**: `logs/football_etl.log`, `logs/errors.log`

## Related Documentation

- `ISSUES.md` - Detailed issue tracking with priorities
- `ARCHITECTURE.md` - System architecture diagrams
- `QUICK_START.md` - User getting started guide
- `API_FOOTBALL_GUIDE.md` - API-Football integration details
