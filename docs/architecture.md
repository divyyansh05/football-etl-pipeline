# football-data-platform Architecture

## System Overview
```
SOURCES                    PLATFORM                    CONSUMERS
───────                    ────────                    ─────────
Wyscout (primary)  ──────→ PostgreSQL ──────────────→ ScoutIQ
SofaScore (enrich) ──────→ football_platform          Match Analysis Tool
Transfermarkt      ──────→ port 5434                  Agentic AI System
FotMob (squads)    ──────→                            Any future project
                           FastAPI
                           port 8000
```

## Data Flow

token_manager.py auto-logins → extracts token
GraphQL → discovers all player IDs per competition
REST → downloads player xlsx (439 cols, career history)
parser.py → cleans and structures xlsx data
wyscout_loader.py → upserts into player_match_stats
compute_scores.py → generates player_scores from match data
FastAPI → serves read-only queries to consumers

## Key Design Principles
- Single source of truth: one DB, many consumers
- Resumable: all scripts safe to interrupt and restart
- Idempotent: re-running any script produces same result
- Automatic auth: no manual token management ever
- Bronze layer: raw xlsx files preserved before DB writes

## Technology Stack
Language:    Python 3.11+
DB:          PostgreSQL 15 (port 5434)
API:         FastAPI + uvicorn
Scraping:    requests + Playwright (login only)
Data:        pandas + openpyxl
Auth:        Playwright → cookie extraction
