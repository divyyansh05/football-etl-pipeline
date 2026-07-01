# football-data-platform — Project Memory

## Agent Governance
This project is managed by a Technical Director (TD) agent in Antigravity IDE.
If you are Claude Code, Cursor, or any execution agent:
1. You are executing a SCOPED TASK given to you by the TD
2. Do NOT make architectural decisions — ask the TD
3. Do NOT modify files outside your assigned scope
4. Do NOT install new dependencies without explicit approval
5. Follow the task prompt exactly — no "improvements" beyond scope
6. Report results in: CHANGED / TESTED / COUNTS / ISSUES format
7. If ambiguous, STOP and say "Need TD decision on: [question]"

Agent instructions: `.gemini/agents/`
Delegation playbook: `.gemini/DELEGATION_PLAYBOOK.md`
TD context: `.gemini/TD_CONTEXT.md`

## What This Is
A standalone, multi-consumer football data platform.
Single source of truth for all football data needs.
Built once. Used by ScoutIQ, match analysis tools,
agentic AI systems, and any future project.

## Architecture
```
Wyscout (primary)  ─┐
SofaScore          ─┤→ PostgreSQL (football_platform) → FastAPI → consumers
Transfermarkt      ─┤
FotMob             ─┘
```

## Database
Host:     localhost
Port:     5434
DB:       football_platform
User:     postgres / Password: postgres
URL:      postgresql://postgres:postgres@localhost:5434/football_platform

## Primary Data Source: Wyscout
Platform: https://wyscout.hudl.com
API base: https://searchapi.wyscout.com

Confirmed working endpoints:

GraphQL:
POST /graphql?token=...&groupId=1059060&subgroupId=93476
→ competitions, seasons, all player IDs per competition

Player stats export (xlsx):
POST /api/v1/match_stats/players/{wyscout_player_id}.xlsx
?token=...&groupId=1059060&subgroupId=93476
→ 439 columns, full career history per player

Team stats (JSON):
GET /api/v1/team_stats/teams/{wyscout_team_id}/stats
?token=...&groupId=1059060&subgroupId=93476
→ match-by-match team stats

Team info:
GET /api/v1/team_stats/teams/{wyscout_team_id}
?token=...&groupId=1059060&subgroupId=93476

Fixed credentials:
groupId:    1059060
subgroupId: 93476
token:      auto-managed via scrapers/wyscout/token_manager.py
(Playwright login → aengine_dtk cookie)
NEVER hardcode. NEVER copy manually.

## Confirmed Competition IDs (Wyscout)
Premier League:   8    (season current=-6963)
La Liga:          7
Serie A:          13
Bundesliga:       9
Ligue 1:          16
Champions League: 10

## Confirmed Team IDs (Wyscout — PL)
Arsenal=660, Chelsea=661, Man Utd=662, Liverpool=663,
Newcastle=664, Aston Villa=665, Fulham=667, Everton=674,
Tottenham=675, Man City=676, Crystal Palace=679,
Wolves=680, Leeds=681, Sunderland=683, West Ham=684,
Nottingham Forest=694, Burnley=698, Brighton=703,
Bournemouth=711, Brentford=722

## Confirmed Player IDs (Wyscout — examples)
Julián Álvarez:   -65351  (439 col, 276 matches, 2019-2026)
Jan Oblak:         85786  (307 col GK-specific)

## Data Depth Confirmed
Player xlsx: 439 columns per match row
- Goals, assists, shots, xG, xA, npxG
- Every pass type (forward/back/lateral/long/smart/through)
- Dribbles, duels, aerial duels (all with success counts)
- Progressive runs, touches in box, shot assists
- Full GK stats: saves, xg_save, sweeps, claims, punches
- Career history 2019-2026, all competitions in one export

Team JSON: 109+ columns per match including PPDA, progressive passes

## Player Discovery (GraphQL)
Step 1: Get season ID
query { competitions(id: [COMP_ID]) { currentSeason { id } } }

Step 2: Get all players
query {
  playersLeaderboard(param: minutes_on_field,
    seasonIds: [SEASON_ID], limit: 1000) {
    playerId name primaryPosition minutesOnField
    goals assists age teams { id name }
  }
}
Returns 500 players per league, all teams covered.
No Playwright needed for discovery — GraphQL only.

## Secondary Sources
SofaScore:    player ratings, sofascore_id linkage
Transfermarkt: market values, contract expiry dates
FotMob:       squad roster completeness (squad pages via NEXT_DATA)

## Scope
Competitions: Top 5 EU + Champions League (+ South American future)
Seasons:      2021/2022 through 2025/2026 (5 seasons)
Players:      ~3,000 unique across all competitions
Match rows:   ~500,000+ across full career histories

## Current Phase
Foundation setup (this session).
Next: Run extraction script scripts/extract.py

## Non-Negotiables
- Token auto-managed. Never hardcode. Never copy manually.
- DB is read-only for all consumers (API, ScoutIQ, etc.)
- ETL never creates duplicate rows (unique constraints + upsert)
- Wyscout is canonical. Do not mix with old pipeline data.
- Bronze files (raw xlsx) saved before any DB write.
- Resumable: skip already-downloaded files, skip loaded files.
