# football-etl-pipeline — Project Memory
# Updated: March 2026 — Full pipeline rebuild complete

## CONFIRMED DATA SOURCES (tested March 2026)

| Source | Method | Status | Role |
|--------|--------|--------|------|
| SofaScore | Custom scraper | ✅ WORKING | PRIMARY — player identity + deep stats |
| Understat | soccerdata library | ✅ WORKING | xG, npxG, xA, xGChain, xGBuildup |
| ClubElo | soccerdata library | ✅ WORKING | Team ELO ratings |
| FotMob | Custom scraper | ❌ DEAD | Added x-fm-req auth — do not attempt |
| FBref | soccerdata/direct | ❌ DEAD | 403 blocked — do not attempt |
| SoFIFA | soccerdata | ❌ DEAD | Blocked — do not attempt |

## SOFASCORE WORKING ENDPOINTS (verified)
```
BASE = https://api.sofascore.com/api/v1

✅ /unique-tournament/{league_id}/season/{season_id}/standings/total
   → Returns all teams with team_id, name

✅ /player/{player_id}/unique-tournament/{league_id}/season/{season_id}/statistics/overall
   → Returns 112 deep stat fields

✅ /player/{player_id}
   → Returns name, position, dateOfBirthTimestamp, height,
     preferredFoot, shirtNumber, team{id, name}

✅ /unique-tournament/{league_id}/season/{season_id}/top-players/overall
   → Returns top 50 players per category across 8 categories
   → Approx 300-400 unique players per league per season

❌ /team/{id}/players → 404 DEAD
❌ /team/{id}/featured-players → 404 DEAD
❌ /unique-tournament/{id}/season/{id}/top-players/{category} → 404 DEAD
```

## SOFASCORE IDs
```python
LEAGUE_IDS = {
    'Premier League': 17,
    'La Liga': 8,
    'Serie A': 23,
    'Bundesliga': 35,
    'Ligue 1': 34,
}

SEASON_IDS = {
    'Premier League': {2022: 41886, 2023: 52186, 2024: 61627, 2025: 76986},
    'La Liga':        {2022: 42409, 2023: 52376, 2024: 61643, 2025: 77559},
    'Serie A':        {2022: 42415, 2023: 52760, 2024: 63515, 2025: 76457},
    'Bundesliga':     {2022: 42268, 2023: 52608, 2024: 63516, 2025: 77333},
    'Ligue 1':        {2022: 42273, 2023: 52571, 2024: 61736, 2025: 77356},
}
# NOTE: Season key is START YEAR of season
# 2024-25 season → key 2024
# 2025-26 season → key 2025
```

## CORE ARCHITECTURE (IMMUTABLE)

SofaScore is the ONLY source that creates player records.
Every player_id in the DB was created by SofaScore.
Understat and ClubElo ENRICH existing records ONLY.
If enrichment cannot match a player → LOG and SKIP, never create.

### Collection Order (STRICT)
1. SofaScore → creates players, teams, player_season_stats
2. Understat → enriches existing players with xG fields
3. ClubElo → team ELO ratings (separate table, independent)

### Player Discovery Strategy
Since /team/{id}/players is dead, discover players via:
1. top-players/overall endpoint (primary — gets ~400 active players)
2. For each player found: fetch full identity + stats
3. Players not in top-players (deep bench, fringe) are acceptable gaps
   These players have <450 mins anyway and are excluded from analytics

### Player Matching (Understat → SofaScore)
1. Exact: normalised_name + team_name + season → HIGH confidence
2. Exact: normalised_name + league + season → MEDIUM confidence
3. Fuzzy: pg_trgm similarity >0.90 + team + season → LOW confidence
4. No match → log to unmatched_players_log, SKIP

## DATABASE
Host: localhost:5434 | DB: football_data | User: postgres
Schema: v2 applied (14 tables, 4 views) ✅

## QUALITY THRESHOLDS
- Duplicate players: 0
- Orphaned stats: 0
- Position coverage: >85% (SofaScore position field, format: "M","D","F","G")
- Understat match rate: >70%
- Active players per league per season: >300 (with 450+ mins)

## SOFASCORE POSITION CODES
```python
SOFASCORE_POSITION_MAP = {
    'G': 'GK',
    'D': 'DEF',
    'M': 'MID',
    'F': 'FWD',
}
# SofaScore uses single letter codes: G, D, M, F
# No WNG distinction — assign based on context or leave as MID/FWD
```

## CURRENT STATE (updated March/April 2026 — Phase 3 complete)
- Repo cleaned: ✅
- .claude folder: ✅
- Database schema v2: ✅ (14 tables, 4 views applied)
- Migrations 001-003 + runner: ✅
- BaseETL (etl/base_etl.py): ✅
- Identity resolution (utils/identity_resolution.py): ✅
- SofaScore constants + client (scrapers/sofascore/): ✅ (RATE_LIMIT=1.5s)
- SofaScoreETL canonical creator (etl/sofascore_etl.py): ✅
- UnderstatETL enrichment (etl/understat_etl.py): ✅ (smoke: 521 processed)
- ClubEloETL (etl/clubelo_etl.py): ✅ (fixed league format + NaN rank; smoke: 96 records)
- server/app.py fixes (xag→xa, data_sources, stat_id, starts col removed, KNOWN_JOBS→4, updated_at→last_updated): ✅
- Backfill script (scripts/init_backfill.py): ✅ (POPULATED_THRESHOLD fixed: 300→150)
- Quality audit (scripts/quality_audit.py): ✅ (all gates PASS)
- Scheduler 4 jobs (scheduler/__init__.py + jobs.py + run_scheduler.py): ✅
- Docker stack: ✅ (db + migrate + scheduler + server all healthy; volume: data-etl-pipeline_postgres_data external)
- Unit tests 64 passing: ✅
- Dead files removed: etl/fotmob_etl.py, scrapers/fotmob/, tests/test_parsers.py ✅
- stale refs: xag→xa in data_quality.py + conftest.py ✅
- smart_backfill.py + LaunchAgent: ✅ (hourly auto-retry with 26h cooldown on 403)

## CURRENT DATA STATE (as of 2026-03-31)

| League         | Season  | Players | SS  | US  | xG rows |
|----------------|---------|---------|-----|-----|---------|
| Premier League | 2022-23 | 174     | 173 | 162 | 162     |
| Premier League | 2023-24 | 184     | 184 | 170 | 170     |
| Premier League | 2024-25 | 182     | 182 | 168 | 168     |
| Premier League | 2025-26 | 182     | 182 | 173 | 173     |
| La Liga        | 2022-23 | 189     | 189 | 162 | 162     |
| La Liga        | 2023-24 | 105     | 105 |  91 |  91     | ← partial (403 interrupted)
| La Liga 2024-25, 2025-26 | — | 0  |  0  |  0  |  0      | ← pending
| Serie A, Bundesliga, Ligue 1 (all 4) | — | 0 | 0 | 0 | 0 | ← pending

Total: 632 unique players, 1016 PSS rows, 1152 ELO records
All pending leagues: LaunchAgent (com.football-etl.backfill) retrying automatically every hour.
Next retry: 2026-04-01 ~01:06 UTC

## KEY DECISIONS (with reasons)
- FotMob: dead, added x-fm-req HMAC auth + ToS prohibits scraping
- FBref: 403 Cloudflare blocked
- SofaScore: canonical source — standings + top-players + player endpoints all working
- API-Football: $19/month option if we need match-level data later
- Player discovery via top-players/overall: acceptable gap (fringe players excluded by 450min threshold anyway)
- Weekly refresh cadence confirmed
- Top 5 EU leagues, 4 seasons (2022-23 to 2025-26)

## SOFASCORE PLAYER IDENTITY — CONFIRMED FIELDS (tested March 2026)
All fields confirmed working on /player/{id} endpoint:
```python
# Raw response structure:
player = response['player']
player['id']                    # sofascore player ID (integer)
player['name']                  # Full name: "Cole Palmer"
player['shortName']             # "C. Palmer"
player['position']              # Single letter: "G","D","M","F"
player['dateOfBirthTimestamp']  # Unix timestamp: 1020643200
player['height']                # Integer cm: 185
player['preferredFoot']         # "Left"/"Right"/"Both"
player['shirtNumber']           # Integer: 10
player['country']['name']        # "England"  ← CORRECT FIELD (not 'nationality')
player['country']['alpha2']     # "EN"
player['country']['alpha3']     # "ENG"
player['team']['id']            # SofaScore team ID: 38
player['team']['name']          # "Chelsea"

# DOB conversion:
from datetime import datetime, timezone
dob = datetime.fromtimestamp(
    player['dateOfBirthTimestamp'], tz=timezone.utc
).date()
```

## API-FOOTBALL FREE TIER
100 req/day — NOT needed for current architecture.
Keep as backup option if SofaScore blocks us.
Would cost $19/month to upgrade to useful tier.
Decision: Do not integrate unless SofaScore fails.
