# Football Data Pipeline - Quick Start Guide

## Status: Ready to Use

The pipeline collects football data from FotMob (primary) and API-Football (secondary) across 8 leagues.

---

## Prerequisites

1. **Docker** - For PostgreSQL database
2. **Python 3.9+** - Runtime
3. **pip dependencies** - `pip install -r requirements.txt`

---

## Setup

### 1. Start Database
```bash
docker-compose up -d
```

### 2. Initialize Schema
```bash
# Connect to database and run schema
docker exec -i football_etl_db psql -U postgres -d football_data < database/schema.sql

# Run FotMob migration (adds fotmob_id columns, new leagues)
docker exec -i football_etl_db psql -U postgres -d football_data < database/migration_002_fotmob_and_leagues.sql
```

### 3. Test Connection
```bash
# FotMob (no API key needed)
python cli.py fotmob test-connection

# API-Football (optional, needs key)
export API_FOOTBALL_KEY='your_key_here'
python cli.py api-football test-connection
```

---

## Collecting Data

### Option 1: Quick Start (Recommended)
```bash
# Collect Premier League standings and teams (~1 API call)
python cli.py fotmob collect-league --league premier-league
```

### Option 2: All Leagues at Once
```bash
# Collect standings for all 8 leagues (~8 API calls, ~20 seconds)
python cli.py fotmob collect-all
```

### Option 3: Deep Collection (Full Data)
```bash
# Full collection: standings + squads + match details + player stats
# Takes longer (~50+ API calls per league)
python cli.py fotmob collect-deep --league premier-league

# Limit matches for testing
python cli.py fotmob collect-deep --league premier-league --max-matches 5
```

### Option 4: Historical Season
```bash
# Collect 2023-24 season data
python cli.py fotmob collect-league --league premier-league --season 2023-24
python cli.py fotmob collect-deep --league la-liga --season 2023-24
```

---

## Supported Leagues

| Key | League | Country |
|-----|--------|---------|
| `premier-league` | Premier League | England |
| `la-liga` | La Liga | Spain |
| `serie-a` | Serie A | Italy |
| `bundesliga` | Bundesliga | Germany |
| `ligue-1` | Ligue 1 | France |
| `eredivisie` | Eredivisie | Netherlands |
| `brasileiro-serie-a` | Brasileiro Serie A | Brazil |
| `argentina-primera` | Argentina Primera | Argentina |

List all leagues:
```bash
python cli.py fotmob list-leagues
```

---

## After Collecting Data

### View System Stats
```bash
python cli.py system stats
```

### Start Dashboard
```bash
python server/app.py
# Open http://localhost:5001
```

### Query Database Directly
```bash
docker exec -it football_etl_db psql -U postgres -d football_data
```

#### Sample Queries

**League Standings:**
```sql
SELECT t.team_name, tss.points, tss.wins, tss.draws, tss.losses,
       tss.goals_for, tss.goals_against, tss.league_position
FROM team_season_stats tss
JOIN teams t ON tss.team_id = t.team_id
JOIN seasons s ON tss.season_id = s.season_id
JOIN leagues l ON tss.league_id = l.league_id
WHERE l.league_name = 'Premier League' AND s.season_name = '2024-25'
ORDER BY tss.league_position;
```

**Top Scorers:**
```sql
SELECT p.player_name, t.team_name, pss.goals, pss.assists, pss.rating
FROM player_season_stats pss
JOIN players p ON pss.player_id = p.player_id
JOIN teams t ON pss.team_id = t.team_id
JOIN leagues l ON pss.league_id = l.league_id
JOIN seasons s ON pss.season_id = s.season_id
WHERE l.league_name = 'Premier League' AND s.season_name = '2024-25'
ORDER BY pss.goals DESC
LIMIT 20;
```

---

## Automated Scheduling

```bash
# Run scheduler (FotMob daily + API-Football daily)
python run_scheduler.py

# Run with immediate collection first
python run_scheduler.py --run-now

# List scheduled jobs
python run_scheduler.py --list-jobs
```

Default schedule (UTC):
- **05:00** - FotMob daily (all 8 leagues, no limit)
- **06:00** - API-Football daily (top 3 leagues, 90 req budget)
- **12:00** - API-Football priority standings
- **18:00** - API-Football quick update
- **Sunday 02:00** - FotMob weekly deep collection

---

## Troubleshooting

### FotMob Connection Issues
- Check internet connection
- FotMob may be temporarily unavailable
- Check `logs/football_etl.log` for details

### Database Connection
```bash
# Check Docker is running
docker ps | grep football_etl_db

# Restart if needed
docker-compose down && docker-compose up -d
```

### API-Football Issues
- Verify key: `python cli.py api-football test-connection`
- Check quota: `python cli.py api-football check-quota`
- Free tier: 100 requests/day max

---

## CLI Reference

```bash
# FotMob commands
python cli.py fotmob --help
python cli.py fotmob collect-league --help
python cli.py fotmob collect-deep --help

# API-Football commands
python cli.py api-football --help

# System commands
python cli.py system health
python cli.py system stats
python cli.py system logs --level ERROR
```
