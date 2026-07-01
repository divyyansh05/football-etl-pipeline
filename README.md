# Football Data Platform — Backend

> Professional-grade backend data platform and analytics engine. Orchestrates player and team data collection from Wyscout, enriches it via SofaScore, Transfermarkt, FotMob, and ClubELO, and serves clean performance metrics to frontend consumers (like ScoutIQ).

---

## Architecture Overview

```
Wyscout (Primary) ───┐
SofaScore (Ratings)  ├───► PostgreSQL (football_platform) ───► FastAPI REST API
Transfermarkt (Vals) ├───┘        (Port: 5434)                   (Port: 8000)
ClubELO / FotMob ────┘
```

The database acts as a read-only single source of truth for all frontend consumers. Write operations are strictly performed by the ETL loaders and analytics scripts.

---

## Tech Stack & Setup

*   **Runtime:** Python 3.9+
*   **Database:** PostgreSQL 15 + pgvector (port 5434)
*   **Web Framework:** FastAPI + Uvicorn (port 8000)
*   **Data Layer:** psycopg2 (raw connection, transaction safety with SAVEPOINTs)
*   **Web Automation:** Playwright (for automated Hudl/Wyscout session initialization)

---

## Quick Start

### 1. Prerequisites
Ensure you have Docker, Python 3.9+, and Chrome installed on your machine.

### 2. Start the Database
Spin up the PostgreSQL database container with pgvector:
```bash
docker compose up -d db
```

### 3. Initialize the Database Schema
Import the tables, indices, and constraints:
```bash
docker exec -i football_db psql -U postgres -d football_platform < database/schema.sql
```

### 4. Configure Environment
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```
Key configuration values in `.env`:
*   `WYSCOUT_EMAIL` & `WYSCOUT_PASSWORD`: Used by Playwright to automatically log in and capture session cookies (`aengine_dtk`).
*   `DATABASE_URL`: `postgresql://postgres:postgres@localhost:5434/football_platform`

---

## Data Collection & ETL Pipeline

The platform uses a resilient, step-by-step pipeline runner (`scripts/pipeline.py`) supporting automatic token refresh, resumable state, and tiered error handling.

### Full Pipeline Run
To run the complete pipeline (Discovery → Extraction → Post-Extraction):
```bash
python3 scripts/pipeline.py
```
To run as a background daemon (nohup-safe):
```bash
nohup python3 scripts/pipeline.py &
```
You can monitor the status using:
```bash
python3 scripts/pipeline.py --status
tail -f logs/pipeline.log
```

### Post-Extraction Processing
After raw data is loaded, run the post-extraction script to map positions and compute performance percentiles:
```bash
python3 scripts/post_extraction.py
```

### Data Quality Verification
Audit the integrity of the database:
```bash
python3 scripts/verify.py
```

---

## API Reference

Start the FastAPI development server:
```bash
python3 -m uvicorn api.main:app --reload --port 8000
```
Interactive documentation is available at: **http://localhost:8000/docs**

### Main Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/players` | Query and filter player lists |
| `GET` | `/api/v1/players/{id}` | Detailed player profile, per-90 stats, and scores |
| `GET` | `/api/v1/teams/{id}` | Team profile and radar charts |
| `GET` | `/api/v1/competitions` | Available leagues and match coverage |
| `GET` | `/api/v1/dashboard` | KPI summaries and leaderboards |

---

## Project Structure

```
football-data-platform/
├── api/                  # FastAPI routers, endpoints, and validation schemas
├── database/             # schema.sql and connection modules
├── etl/                  # Wyscout and Team XLSX loaders
├── scrapers/             # API clients (Wyscout, SofaScore, Transfermarkt, ClubELO)
│   └── wyscout/
│       └── token_manager.py  # Auto-managed login via Playwright
├── scripts/              # Pipeline runner, verification, and audit tools
├── analytics/            # Percentile scoring and analytics modules
├── docker-compose.yml    # Postgres database container definition
├── requirements.txt
└── README.md
```
