# Football ETL Pipeline — Agent Context

> Part of the larger Scouting Project ecosystem.
> Updated: March 2026 — Pipeline rebuild complete.

## Architecture Overview

**Canonical rule**: SofaScore is the ONLY source that creates player/team records.
Understat and ClubElo enrich existing records only.

| Source | Role | Method | Rate limit |
|--------|------|--------|------------|
| SofaScore | PRIMARY — player identity + deep stats | Custom HTTP scraper | 1.5s/req |
| Understat | ENRICHMENT — xG, npxG, xA, xGChain, xGBuildup | soccerdata library | library-managed |
| ClubElo | ENRICHMENT — team ELO snapshots | soccerdata library | none (CSV) |

**Dead sources** (do NOT reference in active code):
- FotMob: added x-fm-req HMAC auth
- FBref: 403 Cloudflare blocked
- API-Football: 100 req/day free tier, not needed
- StatsBomb, SoFIFA: removed

## Quick Reference

```bash
# Start database
docker-compose up -d db migrate

# Run backfill (all leagues × all seasons)
python scripts/init_backfill.py

# Dry-run backfill
python scripts/init_backfill.py --dry-run

# Run scheduler
python run_scheduler.py

# Run scheduler with immediate job
python run_scheduler.py --run-now sofascore

# Data quality audit
python scripts/quality_audit.py

# Web server (port 5001)
python server/app.py

# Run tests
.venv311/bin/python -m pytest tests/test_identity.py tests/test_season_utils.py tests/test_migrations.py -v
```

## Database

```
Host: localhost:5434
Database: football_data
User: postgres / Password: postgres
Schema: v2 (14 tables, 4 views)
```

## Project Structure

```
football-etl-pipeline/
├── etl/
│   ├── base_etl.py          # Abstract base: bronze, run logging, DB lookups
│   ├── sofascore_etl.py     # CANONICAL creator of players + teams
│   ├── understat_etl.py     # xG enrichment (enrichment only)
│   └── clubelo_etl.py       # Team ELO snapshots
├── scrapers/
│   └── sofascore/
│       ├── client.py        # SofaScore HTTP client (3s rate limit)
│       └── constants.py     # League IDs, season IDs, stats field map
├── utils/
│   ├── identity_resolution.py  # 4-step Understat→SofaScore matching
│   └── season_utils.py         # Season format conversions
├── database/
│   ├── connection.py            # DatabaseConnection singleton
│   ├── batch_loader.py          # BatchLoader for bulk ops
│   ├── schema.sql               # End-state schema v2
│   └── migrations/
│       ├── runner.py            # Migration runner (advisory lock, idempotent)
│       ├── 001_sofascore_canonical.sql
│       ├── 002_fix_stale_columns.sql
│       └── 003_update_gold_views.sql
├── scripts/
│   ├── init_backfill.py     # Full historical backfill with --dry-run
│   ├── quality_audit.py     # Data quality gate checks
│   └── smoke_test_sources.py
├── scheduler/
│   ├── job_scheduler.py     # APScheduler wrapper
│   └── jobs.py              # 4 jobs: Mon SofaScore, Tue Understat, Wed ClubElo, Thu catchup
├── server/
│   └── app.py               # Flask API + dashboard (port 5001)
├── tests/
│   ├── test_identity.py     # Identity resolver unit tests (mocked DB)
│   ├── test_season_utils.py # Season format conversion tests
│   └── test_migrations.py   # Migration idempotency + DB constraint checks
├── run_scheduler.py         # Scheduler entry point
└── docker-compose.yml       # 5 services: db, migrate, backfill, scheduler, server
```

## Key Design Decisions

1. **Player discovery via top-players/overall** — since `/team/{id}/players` is dead,
   SofaScore ETL collects up to ~400 active players per league/season via 8 stat categories.
   Fringe players (<450 mins) are acceptable gaps — excluded from analytics anyway.

2. **4-step identity matching** in `utils/identity_resolution.py`:
   - Step 0: understat_id fast-path
   - Step 1: norm_name + team + season (HIGH)
   - Step 2: norm_name + league + season, reject if ambiguous (MEDIUM)
   - Step 3: pg_trgm > 0.90 + team + season + position tiebreak (LOW)
   - Step 4: log unmatched, skip

3. **Bronze layer mandatory** — all raw API responses saved to `data/raw/` before DB upserts.

4. **Weekly scheduler** (Mon/Tue/Wed/Thu) rather than daily — SofaScore rate limit is 1.5s/req,
   a full 5-league season refresh takes ~1-2 hours.

## Coding Conventions

- SQL: parameterised `:param_name` syntax, `fetch=True` for SELECT, `fetch=False` for mutations
- All ETL classes inherit `BaseETL` and implement `run(league, season) → dict`
- Upsert only: `INSERT ... ON CONFLICT DO UPDATE` — never INSERT then UPDATE separately
- `DatabaseConnection()` zero-arg singleton constructor
- Bronze → Silver → Gold (views) data flow

## Supported Leagues

| League | Country | SofaScore ID |
|--------|---------|--------------|
| Premier League | England | 17 |
| La Liga | Spain | 8 |
| Serie A | Italy | 23 |
| Bundesliga | Germany | 35 |
| Ligue 1 | France | 34 |

Seasons: 2022-23, 2023-24, 2024-25, 2025-26

## Quality Thresholds

| Check | Threshold | Action if Failed |
|-------|-----------|-----------------|
| Duplicate players | = 0 | Block |
| Orphaned stats | = 0 | Block |
| Position coverage | > 92% | Investigate |
| Understat match rate | > 75% | Review unmatched log |
| SofaScore collection | > 70% | Review |

Run: `python scripts/quality_audit.py`

## Current Data State (2026-03-31)

| League         | Season  | Players | Understat xG | Status |
|----------------|---------|---------|--------------|--------|
| Premier League | 2022-23 | 174     | 162 rows     | ✅ complete |
| Premier League | 2023-24 | 184     | 170 rows     | ✅ complete |
| Premier League | 2024-25 | 182     | 168 rows     | ✅ complete |
| Premier League | 2025-26 | 182     | 173 rows     | ✅ complete |
| La Liga        | 2022-23 | 189     | 162 rows     | ✅ complete |
| La Liga        | 2023-24 | 105     |  91 rows     | ⚠️ partial (403 mid-run) |
| La Liga        | 2024-25 | 0       | 0 rows       | ⏳ pending |
| La Liga        | 2025-26 | 0       | 0 rows       | ⏳ pending |
| Serie A        | all 4   | 0       | 0 rows       | ⏳ pending |
| Bundesliga     | all 4   | 0       | 0 rows       | ⏳ pending |
| Ligue 1        | all 4   | 0       | 0 rows       | ⏳ pending |

**Total**: 632 players, 1016 PSS rows, 1152 ELO records

**Auto-retry**: `com.football-etl.backfill` LaunchAgent runs every hour.
State in `data/backfill_state.json`. Logs in `logs/smart_backfill.log`.
When SofaScore block lifts, remaining 14 league-seasons will populate automatically.

## Notes for AI Agents

- **Root CLAUDE.md is a legacy artifact** — it describes the old FotMob/API-Football
  architecture. The authoritative project memory is in `.claude/CLAUDE.md`.
  When in conflict, `.claude/CLAUDE.md` wins.

- **`starts` column removed** from `/api/players/top` query — this column does not
  exist in the schema v2 `player_season_stats` table. The column was removed in
  `server/app.py` (Step 1, March 2026 rebuild).

- **`data_sources` table disposition (Option B)** — the `data_sources` table was
  removed in the schema v2 rebuild. Zero executable code references remain.
  Migration 002 contains a guarded DROP (no-op if table absent). No further
  action needed; removal milestone noted here.

- **SofaScore 403 from development machine** — the SofaScore API returns 403 when
  called from this IP/environment. This is an IP-level anti-bot block, not a
  code error. The Docker scheduler container may have a different IP profile.
  If smoke tests fail with 403, verify from a clean network environment.
