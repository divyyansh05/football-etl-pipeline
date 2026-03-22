# Football ETL Pipeline

Automated data pipeline for top 5 EU football leagues.

## Coverage
- **Leagues**: Premier League, La Liga, Serie A, Bundesliga, Ligue 1
- **Seasons**: 2022-23, 2023-24, 2024-25, 2025-26 (ongoing)
- **Players**: 10,000+ player-season records
- **Sources**: FotMob + Understat + SofaScore (112 fields/player)

## Key Metrics Collected
- Basic: goals, assists, minutes, shots, cards
- xG metrics: xG, npxG, xA, xGChain, xGBuildup (Understat)
- Deep: aerial duels, tackles won, key passes, rating, ball recovery (SofaScore)

## Quick Start
```bash
git clone https://github.com/yourusername/football-etl-pipeline
cd football-etl-pipeline
cp .env.example .env
# Add your API_FOOTBALL_KEY to .env (optional)
docker compose up -d
```

One command starts everything:
- PostgreSQL database
- Init service: backfills 4 seasons × 5 leagues (~30 min first run)
- Scheduler: automatic weekly updates after each matchday

## Manual Collection
```bash
source .venv311/bin/activate
python cli.py sofascore collect-league --league premier-league --season 2025
python cli.py understat collect-league --league la-liga --season 2025
```

## Health Dashboard
```bash
python server/app.py
# Open http://localhost:5001
```

## Related
- [ScoutIQ](https://github.com/yourusername/scoutiq) — scouting app built on this data
