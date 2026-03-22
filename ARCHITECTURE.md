# Data-ETL-Pipeline Architecture

> System architecture documentation for the Football Data ETL Pipeline.
> Part of the larger Scouting Project ecosystem.

---

## System Overview

```
+---------------------------------------------------------------------------+
|                         EXTERNAL DATA SOURCES                              |
+----------------+----------------+------------------------------------------+
|    FotMob      | API-Football   |  StatsBomb                               |
| (Public JSON)  | (REST API)     |  (Open Data)                             |
|  No limit      | 100 req/day    |  Unlimited                               |
|  PRIMARY       | SECONDARY      |  SUPPLEMENTARY                           |
+-------+--------+-------+--------+------------------+-----------------------+
        |                |                           |
        v                v                           v
+---------------------------------------------------------------------------+
|                            SCRAPER LAYER                                   |
+----------------+----------------+------------------------------------------+
| FotMobClient   | APIFootball    |  StatsBomb                               |
| + DataParser   | Client         |  Client                                  |
|                |                |                                          |
| - Rate limit   | - Rate limit   | - statsbombpy                            |
|   (2s+jitter)  |   (1 req/sec)  | - Direct API                             |
| - Response     | - Retry logic  |                                          |
|   caching      | - Circuit      |                                          |
| - Browser-like |   breaker      |                                          |
|   headers      |                |                                          |
+-------+--------+-------+--------+------------------+-----------------------+
        |                |                           |
        v                v                           v
+---------------------------------------------------------------------------+
|                             ETL LAYER                                      |
+----------------+----------------+------------------------------------------+
| FotMob         | APIFootball    |  StatsBomb                               |
| ETL            | ETL            |  ETL                                     |
|                |                |                                          |
| - fotmob_id    | - ID mapping   | - Event data                             |
|   caching      | - Quota track  | - Lineups                                |
| - Batch ops    | - Cache mgmt   |                                          |
| - Deep collect |                |                                          |
+-------+--------+-------+--------+------------------+-----------------------+
        |                |                           |
        +----------------+---------------------------+
                                |
                                v
+---------------------------------------------------------------------------+
|                          UTILITY LAYER                                     |
+--------------+--------------+--------------+--------------+---------------+
|  Validators  |  API Tracker |  Retry Logic | Deduplication|  Monitoring   |
|              |              |              |              |               |
| - Schema     | - 100/day    | - Exp backoff| - fotmob_id  | - Health check|
| - Type check | - Per-source | - Circuit    |   matching   | - Alerts      |
| - Cleaning   | - Analytics  |   breaker    | - Merge      | - Metrics     |
+--------------+--------------+--------------+--------------+---------------+
                                |
                                v
+---------------------------------------------------------------------------+
|                         DATABASE LAYER                                     |
+--------------------------------+------------------------------------------+
|       Connection Pool          |           Batch Loader                     |
|                                |                                           |
| - SQLAlchemy QueuePool         | - Batch upsert                            |
| - 5 connections, 10 overflow   | - ON CONFLICT handling                    |
| - Pre-ping validation          | - Configurable chunk size                 |
| - Session management           |                                           |
+--------------------------------+-------------------------------------------+
                                |
                                v
+---------------------------------------------------------------------------+
|                        PostgreSQL 15                                        |
|                                                                            |
|  +-------------+  +-------------+  +-------------+  +-------------+       |
|  |   leagues   |  |    teams    |  |   players   |  |   matches   |       |
|  |  (8 rows)   |  |  (160+rows) |  | (2000+rows) |  | (1900+rows) |       |
|  +-------------+  +-------------+  +-------------+  +-------------+       |
|                                                                            |
|  +-------------+  +-------------+  +-------------+  +-------------+       |
|  |   seasons   |  |team_season_ |  |player_season|  |player_match_|       |
|  |  (5 rows)   |  |   stats     |  |   _stats    |  |   stats     |       |
|  +-------------+  +-------------+  +-------------+  +-------------+       |
|                                                                            |
|  +-------------+  +-------------+                                          |
|  |data_sources |  |team_match_  |                                          |
|  |  (4 rows)   |  |   stats     |                                          |
|  +-------------+  +-------------+                                          |
+---------------------------------------------------------------------------+
```

---

## Component Details

### 1. CLI Interface (`cli.py`)

The main entry point for all data operations.

```
cli.py
+-- fotmob group (PRIMARY)
|   +-- collect-league     # Standings + teams for one league
|   +-- collect-deep       # Full: standings + squads + matches + player stats
|   +-- collect-all        # All 8 leagues (basic or deep)
|   +-- list-leagues       # Show all 8 supported leagues
|   +-- test-connection    # API connectivity test
|
+-- api-football group (SECONDARY)
|   +-- collect-league     # Full league/season data
|   +-- update-standings   # Quick standings update
|   +-- check-quota        # View API usage
|   +-- run-daily          # Scheduled collection
|   +-- test-connection    # API connectivity test
|   +-- list-leagues       # Show supported leagues
|
+-- statsbomb group
|   +-- scrape-open-data   # Open data collection
|
+-- system group
    +-- health             # Health checks
    +-- stats              # Database statistics
    +-- logs               # View recent logs
```

### 2. Scraper Layer

Each scraper handles a specific data source with its own quirks:

#### FotMob Client (PRIMARY)
```python
class FotMobClient:
    # Public JSON API - no key needed
    # Rate limiting: 2s + random jitter between requests
    # Response caching: 1hr live, 24hr historical
    # Browser-like headers for compatibility

    def get_league(league_id)
    def get_league_season(league_id, season)
    def get_team(team_id)
    def get_player(player_id)
    def get_match(match_id)
```

#### API-Football Client (SECONDARY)
```python
class APIFootballClient:
    # Rate limiting: 1 request/second
    # Max: 100 requests/day (free tier)
    # Pagination: max 3 pages on free tier

    def get_teams(league_id, season)
    def get_standings(league_id, season)
    def get_fixtures(league_id, season)
    def get_players(season, team_id, page)
    def get_leagues(country)
```

### 3. ETL Layer

Transforms raw data into database-ready format:

```
Raw Data -> Validation -> Transformation -> Deduplication -> Batch Insert
```

Each ETL module follows this pattern:
1. Fetch data via scraper
2. Parse response with DataParser
3. Map external IDs to internal IDs (cached in-memory)
4. Deduplicate against existing data (fotmob_id based)
5. Batch upsert to database

### 4. Database Schema

#### Entity Relationship Diagram

```
+-------------+     +-------------+     +-------------+
|   leagues   |----<|    teams    |----<|   players   |
|             |     |             |     |             |
| league_id   |     | team_id     |     | player_id   |
| league_name |     | team_name   |     | player_name |
| country     |     | league_id   |     | position    |
| tier        |     | fotmob_id   |     | nationality |
| fotmob_id   |     | stadium     |     | fotmob_id   |
+-------------+     +-------------+     +-------------+
       |                   |                   |
       |                   |                   |
       v                   v                   v
+-------------+     +-------------+     +-------------+
|   seasons   |     |team_season_ |     |player_season|
|             |     |   stats     |     |   _stats    |
| season_id   |     |             |     |             |
| season_name |     | team_id     |     | player_id   |
| start_year  |     | season_id   |     | season_id   |
| end_year    |     | league_id   |     | league_id   |
| is_current  |     | points      |     | team_id     |
+-------------+     | wins/draws/ |     | goals       |
       |            |   losses    |     | assists     |
       |            | xg_for/     |     | minutes     |
       |            |   xg_against|     | rating      |
       |            | league_pos  |     | 20+ metrics |
       |            +-------------+     +-------------+
       |
       v
+-----------------------------------------------------+
|                      matches                         |
|                                                      |
| match_id, league_id, season_id, fotmob_match_id     |
| match_date, home_team_id, away_team_id               |
| home_score, away_score, home_xg, away_xg             |
| venue, referee, match_status                         |
+-----------------------+-----------------------------+
                        |
          +-------------+-------------+
          v                           v
+-----------------+         +-----------------+
| team_match_stats|         |player_match_stat|
|                 |         |                 |
| match_id        |         | match_id        |
| team_id         |         | player_id       |
| is_home         |         | team_id         |
| goals, shots    |         | minutes_played  |
| possession      |         | goals, assists  |
| passes, xg      |         | shots, passes   |
| corners, fouls  |         | tackles, etc.   |
+-----------------+         +-----------------+
```

### 5. Flask API Server

```
server/app.py
|
+-- Dashboard Routes
|   +-- GET /                   # Main dashboard
|
+-- Stats API
|   +-- GET /api/stats          # Overall counts
|   +-- GET /api/stats/detailed # Breakdown by league/season
|   +-- GET /api/health         # Data health indicators
|
+-- Entity APIs
|   +-- GET /api/leagues        # All leagues
|   +-- GET /api/seasons        # All seasons
|   +-- GET /api/teams          # All teams (searchable)
|   +-- GET /api/teams/<id>     # Team details + squad
|   +-- GET /api/players        # All players (filterable)
|   +-- GET /api/players/<id>   # Player details + stats
|   +-- GET /api/players/top    # Top players
|   +-- GET /api/matches        # Recent matches
|   +-- GET /api/matches/<id>   # Match details
|
+-- Page Routes
    +-- GET /teams              # Teams list page
    +-- GET /teams/<id>         # Team detail page
    +-- GET /players            # Players list page
    +-- GET /players/<id>       # Player detail page
    +-- GET /matches            # Matches list page
    +-- GET /matches/<id>       # Match detail page
```

### 6. Scheduler

```
run_scheduler.py
|
+-- APScheduler with SQLite job store
|
+-- FotMob Jobs (Primary - No Limit)
|   +-- fotmob_daily              # Daily standings for all 8 leagues (05:00 UTC)
|   +-- fotmob_weekly_deep        # Weekly full collection (Sunday 02:00 UTC)
|
+-- API-Football Jobs (Secondary - 100/day)
|   +-- api_football_daily        # Daily collection for top 3 leagues (06:00 UTC)
|   +-- priority_standings        # Quick standings update (12:00 UTC)
|   +-- current_season_update     # Evening refresh (18:00 UTC)
```

---

## Data Flow

### Complete ETL Pipeline Flow

```
1. TRIGGER
   +-- CLI Command (manual)
   +-- Scheduler Job (automatic)
   +-- API Request (on-demand)
           |
           v
2. SCRAPER
   +-- Rate limit check
   +-- API/Web request
   +-- Retry on failure (exponential backoff)
   +-- Cache response (TTL-based)
           |
           v
3. PARSING
   +-- FotMobDataParser (static methods)
   +-- Response path navigation
   +-- Type coercion + null handling
           |
           v
4. TRANSFORMATION
   +-- ID mapping (fotmob_id -> internal ID, cached)
   +-- Season format conversion (2024/2025 -> 2024-25)
   +-- Data normalization
           |
           v
5. PERSISTENCE
   +-- Batch upsert (ON CONFLICT)
   +-- Transaction management
   +-- Index updates
           |
           v
6. LOGGING
   +-- Success metrics
   +-- Error tracking
   +-- API usage tracking
```

### Request Flow Example: Collect Premier League Data

```
User: python cli.py fotmob collect-deep --league premier-league

1. CLI parses arguments
   +-- league=premier-league, season=current

2. FotMobETL.process_league_season()
   +-- Look up league_id in DB
   +-- Fetch /api/leagues?id=47
   +-- Parse standings (20 teams)
   +-- Batch upsert to teams + team_season_stats
   +-- Update xG data from xG table

3. FotMobETL.process_league_teams_deep()
   +-- For each of 20 teams:
   |   +-- Fetch /api/teams?id={team_id}
   |   +-- Parse squad
   |   +-- Upsert each player
   +-- Total: ~20 API calls

4. FotMobETL.process_league_matches_deep()
   +-- For each match found in league data:
   |   +-- Fetch /api/matchDetails?matchId={id}
   |   +-- Parse match details
   |   +-- Upsert match record
   |   +-- Insert team_match_stats (home + away)
   |   +-- Insert player_match_stats
   +-- Total: varies by matches played

5. Return Statistics
   +-- {teams: 20, matches: N, players: ~500, api_requests: ~50+}
```

---

## Configuration

### Environment Variables

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `API_FOOTBALL_KEY` | - | No (optional) | RapidAPI key for API-Football |
| `DB_USER` | postgres | Yes | PostgreSQL username |
| `DB_PASSWORD` | postgres | Yes | PostgreSQL password |
| `DB_HOST` | localhost | Yes | Database host |
| `DB_PORT` | 5434 | Yes | Database port |
| `DB_NAME` | football_data | Yes | Database name |

### Rate Limits

| Source | Limit | Strategy |
|--------|-------|----------|
| FotMob | No official limit | 2s + jitter delays, exponential backoff |
| API-Football | 100 req/day, 1 req/sec | Daily tracking, 1s delays |
| StatsBomb | Unlimited | None needed |

---

## Scaling Considerations

### Current Limitations

1. **Single process** - No parallel scraping
2. **In-memory caching** - Cache lost on restart
3. **Single database** - No read replicas
4. **100 req/day API-Football limit** - Constrains secondary source freshness

### Future Improvements

1. **Task queue** (Celery/RQ) for parallel processing
2. **Redis** for distributed caching
3. **Read replicas** for API scaling
4. **Transfermarkt integration** for market value data
5. **Incremental updates** instead of full refreshes
6. **Multi-season backfill** for historical analytics

---

## Security Considerations

1. **API Key Storage**: Use environment variables, never commit to git
2. **Database Credentials**: Same as above
3. **Rate Limiting**: Respect source terms of service
4. **Data Privacy**: Player personal data handling
5. **Input Validation**: Protect against injection (parameterized queries)

---

## Monitoring & Observability

### Logs
- `logs/football_etl.log` - All operations
- `logs/errors.log` - Errors only

### Health Checks
```bash
python cli.py system health
```

### Metrics Tracked
- API requests per source per day
- Records inserted/updated per run
- Scrape duration
- Error rates
- Cache hit rates (FotMob client)
