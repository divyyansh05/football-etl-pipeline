# Football ETL Pipeline — Agent Context

> Part of the larger Scouting Project ecosystem.
> Updated: March 2026 — Pipeline rebuild complete.

## Architecture Overview

**Canonical rule**: SofaScore is the ONLY source that creates player/team records.
Understat and ClubElo enrich existing records only.

| Source | Role | Method | Rate limit |
|--------|------|--------|------------|
| SofaScore | PRIMARY — player identity + deep stats | Custom HTTP scraper | 3s/req |
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

4. **Weekly scheduler** (Mon/Tue/Wed/Thu) rather than daily — SofaScore rate limit is 3s/req,
   a full 5-league season refresh takes ~2-3 hours.

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
