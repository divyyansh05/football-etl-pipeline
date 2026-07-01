# football-data-platform — Agent Instructions

## What This Is
A standalone football data platform. Single DB, multiple consumers.
This is the FINAL backend. Built once. No migrations to other repos.

## Governance Model
- **Technical Director (TD):** Antigravity IDE agent — makes ALL decisions
- **Execution Agents:** Claude Code, Cursor, etc. — write code per TD directives
- **Rule:** No execution agent may make architectural decisions or modify
  files outside their assigned scope. See `.gemini/DELEGATION_PLAYBOOK.md`

## Read These Before Every Session
  .claude/CLAUDE.md          — project state, confirmed IDs, endpoints
  .claude/rules/data.md      — data source rules and column naming
  .claude/rules/etl.md       — ETL patterns, rate limits, error handling
  .claude/rules/database.md  — DB rules, constraints, migrations
  .claude/rules/api.md       — API conventions and response format
  .claude/skills/wyscout-api.md    — all confirmed Wyscout endpoints
  .claude/skills/football-domain.md — football analytics concepts

## How to Start Dev Environment
  bash scripts/dev.sh

## Architecture (never deviate)
  Data sources → ETL → PostgreSQL → FastAPI → consumers
  DB is READ ONLY for all consumers.
  Only ETL scripts write to DB.
  Token management is FULLY AUTOMATIC via token_manager.py.

## Project Structure
  scrapers/wyscout/    — Wyscout API client, token manager, parser
  scrapers/sofascore/  — SofaScore client (secondary)
  scrapers/transfermarkt/ — TM client (secondary)
  scrapers/fotmob/     — FotMob client (secondary)
  etl/                 — ETL loaders (wyscout_loader.py is primary)
  database/            — schema.sql, migrations/, connection.py
  analytics/           — per90.py, player_score.py, compute_scores.py
  api/                 — FastAPI app, routers/, models/
  scripts/             — extract.py (main), dev.sh, verify.py

## Data Collection Sequence
  1. python3 scripts/extract.py (Wyscout — primary, all data)
  2. python3 scripts/enrich_sofascore.py (ratings, IDs)
  3. python3 scripts/enrich_transfermarkt.py (values)
  4. python3 scripts/enrich_fotmob.py (squad completeness)
  5. python3 analytics/compute_scores.py (performance scores)

## Consumers of This Platform
  1. ScoutIQ (~/Projects/scoutiq) — football scouting tool
  2. Future: match analysis tool
  3. Future: agentic AI football analysis system

## DB Quick Reference
  DB: football_platform @ localhost:5434
  Main tables: players, teams, competitions, seasons,
               player_match_stats, team_match_stats,
               player_scores, loaded_files

## Active Decisions
  - Wyscout is primary source. Not SofaScore.
  - Team stats xlsx returns 500 — use JSON endpoint instead.
  - Player/team discovery via GraphQL — not Playwright.
  - South American leagues: available in Wyscout, collect Phase 2.
  - Event data (coordinate-level): future phase via OptaPlayerStats.

## What Not To Build Here
  - No frontend (that's ScoutIQ)
  - No video integration
  - No real-time updates
  - No authentication on the API (consumers handle their own auth)

## Current Build Status
  Last updated: 2026-04-21
  Foundation:           ✅ COMPLETE
  Database schema:      ✅ COMPLETE (8 tables, indexes, constraints)
  Token manager:        ✅ COMPLETE (auto-login, cookie extraction)
  Wyscout client:       ✅ COMPLETE (GraphQL + REST, rate limiting)
  Parser:               ✅ COMPLETE (439-col mapping, GK-aware)
  Player ETL loader:    ✅ COMPLETE (SAVEPOINT tx safety)
  Team ETL loader:      ✅ COMPLETE (SAVEPOINT tx safety)
  Player extraction:    ⚠️ PARTIAL (1,860 players, 261K match rows loaded)
  Team extraction:      ⚠️ STARTED (1 team file, 434 match rows)
  Post-extraction:      ✅ COMPLETE (competitions, positions, scores)
  Analytics scoring:    ✅ COMPLETE (percentile-based, position-aware)
  API endpoints:        ✅ COMPLETE (12 endpoints, /api/v1 prefix)
  Auto-pipeline:        ✅ COMPLETE (nohup daemon, resume-safe)
  Tests:                ❌ NOT STARTED
  Secondary enrichment: ❌ NOT STARTED
