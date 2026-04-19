# Wyscout API Reference

## Authentication
  Token: aengine_dtk cookie value
  Auto-managed by scrapers/wyscout/token_manager.py
  Rotates per browser session.
  Fixed params: groupId=1059060, subgroupId=93476

## Base URL
  https://searchapi.wyscout.com

## Confirmed Working Endpoints

### GraphQL
  POST /graphql?token=...&groupId=1059060&subgroupId=93476
  Content-Type: application/json

  Get competition seasons:
    query { competitions(id: [COMP_ID]) {
      currentSeason { id name }
      previousSeason { id name }
    }}

  Get all players for a season:
    query {
      playersLeaderboard(
        param: minutes_on_field
        seasonIds: [SEASON_ID]
        limit: 1000
      ) {
        playerId name primaryPosition
        minutesOnField goals assists age
        teams { id name }
      }
    }

  Get all teams for a season:
    query {
      teamsLeaderboard(param: goal, seasonIds: [SEASON_ID],
                       limit: 100) {
        teamId name
      }
    }

### REST
  Player xlsx:
    POST /api/v1/match_stats/players/{player_id}.xlsx
         ?token=...&groupId=...&subgroupId=...
    Returns: xlsx, 439 cols, full career history

  Team stats JSON:
    GET /api/v1/team_stats/teams/{team_id}/stats
        ?token=...&groupId=...&from=...&to=...&score=winning,draw,losing

  Team info:
    GET /api/v1/team_stats/teams/{team_id}
        ?token=...&groupId=...&subgroupId=...

  Competitions list:
    GET /api/v1/competitions
        ?token=...&groupId=...
    Returns: 1126 competitions

## Confirmed Competition IDs
  Premier League:   8
  La Liga:          7
  Serie A:          13
  Bundesliga:       9
  Ligue 1:          16
  Champions League: 10

## Known Failed Endpoints (do not retry)
  POST /api/v1/team_stats/teams/{id}/stats.xlsx → 500
  GET  /api/v1/.../competitions/{id}/players    → 404
  GET  /api/v1/.../teams/{id}/players           → 404
  GET  /api/v1/seasons                          → 404
  Use GraphQL instead for discovery.

## xlsx Format
  Player file: 439 columns, one row per match
  Row 0: headers (Match, Competition, Date, Position,
          Minutes played, Goals, Assists, xG, xA, ...)
  Row 1+: data rows
  Compound values: "13/2" = total/accurate (parse split)
  Date format: YYYY-MM-DD

## Rate Limit
  1.5 seconds between requests (be respectful)
