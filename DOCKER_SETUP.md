# Docker Setup

Clone, configure, run one command.

## Requirements
- Docker + Docker Compose v2+
- API-Football key (optional, free tier)

## Setup

1. Clone and configure:
   ```
   git clone <repo-url>
   cd Data-ETL-Pipeline
   cp .env.example .env
   # Edit .env and set API_FOOTBALL_KEY if you have one
   ```

2. Start everything:
   ```
   docker compose up -d
   ```

This will:
- Start PostgreSQL
- Run init service: downloads 4 seasons x 5 leagues (~15-30 min first run)
- Start scheduler: keeps data updated automatically after each matchday

## Monitor

```
docker logs football_etl_init -f      # Watch backfill progress
docker logs football_etl_scheduler -f  # Watch scheduler
open http://localhost:8080             # Adminer DB UI
```

## Stop

```
docker compose down      # Stop, keep data
docker compose down -v   # Stop, delete all data
```

## Re-run backfill only

```
docker compose run --rm init
```
