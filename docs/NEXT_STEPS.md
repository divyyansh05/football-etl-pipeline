# Data-ETL-Pipeline - Next Steps Roadmap

> **Last Updated**: 2026-02-07
> **Status**: Active Development - Core Pipeline Operational

---

## Executive Summary

The Data-ETL-Pipeline is now fully operational with:
- ✅ **FotMob** as primary data source (working, 8 leagues)
- ✅ **API-Football** as secondary source (100 req/day limit)
- ✅ **StatsBomb** for historical progressive actions (working)
- ⚠️ **Understat** scraping broken (page structure changed - ISSUE-013)

**Completed on 2026-02-07:**
- All 3 database migrations executed
- FotMob parser fixed for new API format
- StatsBomb NaN player name bug fixed
- Season format utility created
- 8 leagues with 197 teams collected

---

## Phase 1: Database & Infrastructure ✅ COMPLETED

### 1.1 Migrations - DONE

All three migrations have been executed:

| Migration | Status | Notes |
|-----------|--------|-------|
| 001 | ✅ Complete | API-Football ID columns added |
| 002 | ✅ Complete | FotMob IDs + New Leagues |
| 003 | ✅ Complete | 50+ advanced stats columns |

### 1.2 Current Database State

```
leagues:             10 rows
teams:              197 rows
players:          1,889 rows
matches:          3,041 rows
player_season_stats: 2,201 rows
team_season_stats:   320 rows
```

---

## Phase 2: Test New Integrations (High Priority)

### 2.1 Test Understat Integration

```bash
# Test connection
python cli.py understat test-connection

# Collect single league
python cli.py understat collect-league --league premier-league

# Collect all supported leagues
python cli.py understat collect-all --season 2024
```

**Expected Output**: xA, npxG, xGChain, xGBuildup enrichment for existing player/team records.

### 2.2 Test StatsBomb Advanced Stats

```bash
# List available competitions
python cli.py statsbomb list-competitions

# Collect progressive actions for a competition
python cli.py statsbomb collect-advanced --competition-id 11 --season-id 90
```

**Expected Output**: Progressive passes/carries, SCA/GCA, pressures for historical matches.

### 2.3 Test Full Refresh Workflow

```bash
# Run complete data refresh across all sources
python cli.py full-refresh --season 2024

# Skip API-Football if quota is limited
python cli.py full-refresh --season 2024 --skip-api-football
```

---

## Phase 3: Data Collection (Medium Priority)

### 3.1 Initial Data Load

Recommended order for first-time setup:

1. **FotMob Deep Collection** (unlimited, ~2hrs for all leagues)
   ```bash
   python cli.py fotmob collect-all
   python cli.py fotmob collect-deep --league premier-league
   python cli.py fotmob collect-deep --league la-liga
   # ... repeat for all 8 leagues
   ```

2. **API-Football Supplementary** (100 req/day limit)
   ```bash
   python cli.py api-football check-quota
   python cli.py api-football collect-league --league premier-league --season 2024
   ```

3. **StatsBomb Historical** (progressive actions)
   ```bash
   python cli.py statsbomb collect-advanced --competition-id 11 --season-id 90
   ```

4. **Understat Enrichment** (xG metrics)
   ```bash
   python cli.py understat collect-all --season 2024
   ```

### 3.2 Set Up Scheduled Jobs

The scheduler is configured in `scheduler/jobs.py`. To run:

```bash
python run_scheduler.py
```

**Configured Jobs:**
| Job | Frequency | Description |
|-----|-----------|-------------|
| FotMob Collection | Daily 2am | Full league data from FotMob |
| API-Football Supplementary | Daily 4am | Penalty stats, venues, weight |
| StatsBomb Collection | Weekly Sun 3am | Progressive actions, SCA/GCA |
| Understat Collection | Weekly Sat 3am | xA, npxG, xGChain |
| Full Data Refresh | Monthly 1st 1am | All sources, complete refresh |

---

## Phase 4: Server & Dashboard (Medium Priority)

### 4.1 Verify Server Templates

Check if all required templates exist in `server/templates/`:

```bash
ls -la server/templates/
```

**Required Templates:**
- `index.html` - Main dashboard
- `teams.html` - Team listing
- `team_detail.html` - Single team view
- `players.html` - Player listing
- `player_detail.html` - Single player view
- `matches.html` - Match listing
- `match_detail.html` - Single match view

### 4.2 Run Flask Server

```bash
python server/app.py
# Access at http://localhost:5001
```

### 4.3 API Endpoints to Test

| Endpoint | Description |
|----------|-------------|
| `/api/stats` | Overall data counts |
| `/api/stats/detailed` | Breakdown by league/season |
| `/api/leagues` | League listing |
| `/api/players/top` | Top players with filters |
| `/api/teams` | Team listing |
| `/api/matches` | Recent matches |
| `/api/health` | Data health indicators |

---

## Phase 5: Scouting Analytics Features (Future)

### 5.1 Player Comparison Module

Build comparison views using collected metrics:
- Radar charts for player profiles
- Percentile rankings within position
- Similar player finder using clustering

### 5.2 Advanced Filters

Implement filters for:
- Progressive actions (passes + carries)
- Pressing intensity (pressures, PPDA)
- Expected threat (xG, xA, npxG)
- Shot creation (SCA, GCA)

### 5.3 Export & Reporting

- Player shortlist export (CSV, PDF)
- Scouting report generation
- Data visualization dashboards

---

## Open Issues to Address

### High Priority
- **ISSUE-004**: Run migration_001 to add `api_football_id` columns
- **ISSUE-002**: API-Football pagination limit (mitigated by FotMob)
- **ISSUE-003**: API-Football rate limit retry (mitigated by FotMob)

### Medium Priority
- **ISSUE-008**: Season name formatting inconsistencies across sources
- **ISSUE-010**: Verify server templates exist

### Low Priority
- **ISSUE-011**: Document non-standard port 5434

---

## Quick Reference Commands

```bash
# Database
docker-compose up -d                    # Start PostgreSQL
docker-compose down                     # Stop database

# Data Collection
python cli.py fotmob collect-all        # Primary source (unlimited)
python cli.py api-football check-quota  # Check API-Football limit
python cli.py full-refresh              # All sources

# Monitoring
python cli.py system health             # Health check
python cli.py system stats              # Data statistics

# Server
python server/app.py                    # Start Flask (port 5001)

# Scheduler
python run_scheduler.py                 # Start scheduled jobs
```

---

## File Reference

| Purpose | Files |
|---------|-------|
| CLI Entry | `cli.py` |
| FotMob ETL | `etl/fotmob_etl.py`, `scrapers/fotmob/` |
| API-Football ETL | `etl/api_football_etl.py`, `scrapers/api_football/` |
| StatsBomb ETL | `etl/statsbomb_advanced_etl.py`, `scrapers/statsbomb/` |
| Understat ETL | `etl/understat_etl.py`, `scrapers/understat/` |
| Database | `database/connection.py`, `database/batch_loader.py` |
| Migrations | `database/migration_00*.sql` |
| API Server | `server/app.py` |
| Scheduler | `scheduler/jobs.py`, `run_scheduler.py` |
| Configuration | `config/settings.py`, `.env` |
| Logs | `logs/football_etl.log`, `logs/errors.log` |

---

## Success Metrics

Track these to verify the pipeline is working:

1. **Data Volume**
   - 8 leagues with standings data
   - 100+ teams per league
   - 2000+ players per league
   - Recent matches with xG

2. **Data Quality**
   - No duplicate fotmob_ids
   - Cross-source enrichment working
   - Advanced stats populated (xA, progressive, SCA)

3. **System Health**
   - FotMob: < 1% error rate
   - API-Football: Within 100 req/day
   - Database: < 1s query response

---

*Document maintained as part of Data-ETL-Pipeline project.*
