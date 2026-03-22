# Data Depth Analysis Report

> Comprehensive analysis of available stats from FotMob and API-Football sources.
> Document created: February 2026
> Purpose: Guide data acquisition strategy for professional football scouting analytics.

---

## Table of Contents

1. [Team-Level Stats in Depth](#1-team-level-stats-in-depth)
2. [Player-Level Stats in Depth](#2-player-level-stats-in-depth)
3. [Optimized Data Acquisition Strategy](#3-optimized-data-acquisition-strategy)
4. [Missing Must-Have Stats for Modern Scouting](#4-missing-must-have-stats-for-modern-scouting)
5. [Recommendations for Filling Gaps](#5-recommendations-for-filling-gaps)
6. [Summary](#6-summary)

---

## 1. Team-Level Stats in Depth

### 1.1 FotMob Available Team Stats

#### A. League Standings (`/api/leagues`)

| Stat | Field Path | Description |
|------|-----------|-------------|
| Position | `table.data[0].table.all[i].idx` | League position |
| Played | `table.data[0].table.all[i].played` | Matches played |
| Wins | `table.data[0].table.all[i].wins` | Total wins |
| Draws | `table.data[0].table.all[i].draws` | Total draws |
| Losses | `table.data[0].table.all[i].losses` | Total losses |
| Goals For | `scoresStr.split('-')[0]` | Goals scored |
| Goals Against | `scoresStr.split('-')[1]` | Goals conceded |
| Goal Difference | `goalConDiff` | GF - GA |
| Points | `pts` | Total points |

#### B. xG Table (`/api/leagues`)

| Stat | Field Path | Description |
|------|-----------|-------------|
| xG | `table.data[0].table.xg[i].xg` | Expected goals |
| xG Conceded | `xgConceded` | Expected goals against |
| xPoints | `xPoints` | Expected points |
| xPosition | `xPosition` | Position by xG |

#### C. Per-Match Team Stats (`/api/matchDetails`)

| Stat | Key | Description |
|------|-----|-------------|
| Possession | `possession` | Ball possession % |
| Shots | `total_shots` | Total shots |
| Shots on Target | `shots_on_target` | SOT |
| Shots Off Target | `shots_off_target` | SOT - Total |
| Blocked Shots | `blocked_shots` | Blocked by opposition |
| xG | `expected_goals` | Expected goals |
| Passes Completed | `accurate_passes` | Successful passes |
| Passes Attempted | `total_passes` | Total passes |
| Corners | `corners` | Corner kicks |
| Fouls Committed | `fouls` | Fouls |
| Yellow Cards | `yellow_cards` | Yellows |
| Red Cards | `red_cards` | Reds |
| Offsides | `offsides` | Offside calls |
| Tackles | `tackles` | Total tackles |
| Interceptions | `interceptions` | Interceptions |
| Clearances | `clearances` | Clearances |
| Saves | `saves` | GK saves |
| Big Chances | `big_chances` | Big chances created |
| Big Chances Missed | `big_chances_missed` | Big chances missed |

#### D. Team Metadata (`/api/teams`)

| Stat | Field | Description |
|------|-------|-------------|
| Team Name | `details.name` | Official name |
| Short Name | `details.shortName` | Abbreviated |
| Country | `details.country` | Country |
| Primary League ID | `details.primaryLeagueId` | Main league |
| Squad List | `details.sportsTeamJSONLD.athlete` | Player roster |

---

### 1.2 API-Football Available Team Stats

#### A. Standings (`/standings`)

| Stat | Field Path | Description |
|------|-----------|-------------|
| Rank | `rank` | League position |
| Played | `all.played` | Matches played |
| Wins | `all.win` | Total wins |
| Draws | `all.draw` | Total draws |
| Losses | `all.lose` | Total losses |
| Goals For | `all.goals.for` | Goals scored |
| Goals Against | `all.goals.against` | Goals conceded |
| Goal Difference | `goalsDiff` | GF - GA |
| Points | `points` | Total points |
| Form | `form` | Last 5 results (WWLDW) |
| Description | `description` | Status (Champions League, Relegation) |

#### B. Team Info (`/teams`)

| Stat | Field | Description |
|------|-------|-------------|
| Team Name | `team.name` | Official name |
| Team Code | `team.code` | 3-letter code |
| Country | `team.country` | Country |
| Founded | `team.founded` | Year founded |
| National | `team.national` | Is national team |
| Logo | `team.logo` | Logo URL |

#### C. Venue Info (`/teams`)

| Stat | Field | Description |
|------|-------|-------------|
| Stadium Name | `venue.name` | Stadium |
| Address | `venue.address` | Address |
| City | `venue.city` | City |
| Capacity | `venue.capacity` | Capacity |
| Surface | `venue.surface` | Pitch surface |

#### D. Per-Match Team Stats (`/fixtures/statistics`)

| Stat | Type | Description |
|------|------|-------------|
| Shots on Goal | `Shots on Goal` | SOT |
| Shots off Goal | `Shots off Goal` | SOT |
| Total Shots | `Total Shots` | All shots |
| Blocked Shots | `Blocked Shots` | Blocked |
| Shots insidebox | `Shots insidebox` | Box shots |
| Shots outsidebox | `Shots outsidebox` | Outside box |
| Fouls | `Fouls` | Fouls committed |
| Corner Kicks | `Corner Kicks` | Corners |
| Offsides | `Offsides` | Offsides |
| Ball Possession | `Ball Possession` | Possession % |
| Yellow Cards | `Yellow Cards` | Yellows |
| Red Cards | `Red Cards` | Reds |
| Goalkeeper Saves | `Goalkeeper Saves` | Saves |
| Total passes | `Total passes` | Total passes |
| Passes accurate | `Passes accurate` | Completed |
| Passes % | `Passes %` | Accuracy |
| Expected Goals | `expected_goals` | xG (premium only) |

---

## 2. Player-Level Stats in Depth

### 2.1 FotMob Available Player Stats

#### A. Player Bio (`/api/playerData → playerInformation[]`)

| Stat | Field | Description |
|------|-------|-------------|
| Name | `name` | Full name |
| Date of Birth | `birthDate.utcTime` | DOB |
| Nationality | `playerInformation[title='Country']` | Country |
| Height | `playerInformation[title='Height']` | Height in cm |
| Preferred Foot | `playerInformation[title='Foot']` | Left/Right |
| Market Value | `playerInformation[title='Market value']` | Transfer value |
| Shirt Number | `playerInformation[title='Shirt']` | Jersey # |
| Position | `positionDescription.primaryPosition.label` | Primary position |
| Is Captain | `isCaptain` | Captain status |
| Current Team | `primaryTeam.teamId/teamName` | Current club |
| Contract Until | `playerInformation[title='Contract']` | Contract end |

#### B. Season Stats (`/api/playerData → mainLeague.stats[]`)

| Stat | Title Pattern | Description |
|------|--------------|-------------|
| Goals | `*goal*` | Goals scored |
| Assists | `*assist*` | Assists |
| Matches | `*match*` | Appearances |
| Started | `*started*` | Starts |
| Minutes | `*minute*` | Minutes played |
| Rating | `*rating*` | Average rating |
| Yellow Cards | `*yellow*` | Yellows |
| Red Cards | `*red*` | Reds |

#### C. Career History (`/api/playerData → careerHistory.senior.seasonEntries[]`)

| Stat | Field | Description |
|------|-------|-------------|
| Season Name | `seasonName` | Season (2023/2024) |
| Appearances | `appearances` | Total apps |
| Goals | `goals` | Goals |
| Assists | `assists` | Assists |
| Rating | `rating` | Average rating |
| Per-Tournament | `tournamentStats[]` | Breakdown by competition |

#### D. Per-Match Player Stats (`/api/matchDetails → playerStats[id]`)

| Stat | Key | Description |
|------|-----|-------------|
| Rating | `rating.num` | Match rating |
| Goals | `goals` | Goals |
| Assists | `assists` | Assists |
| Total Shots | `total_shots` | All shots |
| Shots on Target | `shots_on_target` | SOT |
| Key Passes | `chances_created` | Key passes |
| Touches | `touches` | Ball touches |
| Tackles Won | `tackles_won` | Successful tackles |
| Interceptions | `interceptions` | Interceptions |
| Clearances | `clearances` | Clearances |
| Duels Won | `duels_won` | Total duels won |
| Ground Duels | `ground_duels_won` | Ground duels |
| Aerial Duels | `aerial_duels_won` | Aerial duels |
| Accurate Passes | `accurate_passes` | Completed passes |
| Total Passes | `total_passes` | Pass attempts |
| Dribbles | `successful_dribbles` | Successful dribbles |
| Minutes Played | `minutes_played` | Minutes |
| Shirt Number | `shirtNumber` | Jersey # |
| Position ID | `positionId` | Position code |
| Is Goalkeeper | `isGoalkeeper` | GK flag |

---

### 2.2 API-Football Available Player Stats

#### A. Player Bio (`/players`)

| Stat | Field | Description |
|------|-------|-------------|
| Name | `player.name` | Full name |
| First Name | `player.firstname` | First name |
| Last Name | `player.lastname` | Last name |
| Age | `player.age` | Current age |
| Date of Birth | `player.birth.date` | DOB |
| Birth Place | `player.birth.place` | Birth city |
| Birth Country | `player.birth.country` | Birth country |
| Nationality | `player.nationality` | Nationality |
| Height | `player.height` | Height (e.g., "178 cm") |
| Weight | `player.weight` | Weight (e.g., "75 kg") |
| Photo | `player.photo` | Photo URL |
| Injured | `player.injured` | Injury status |

#### B. Season Stats (`/players → statistics[0]`)

**Games Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Appearances | `games.appearences` | Total apps |
| Lineups | `games.lineups` | Starts |
| Minutes | `games.minutes` | Minutes played |
| Position | `games.position` | Position |
| Rating | `games.rating` | Avg rating |
| Captain | `games.captain` | Captain count |

**Substitutes Section:**

| Stat | Field | Description |
|------|-------|-------------|
| In | `substitutes.in` | Subbed in count |
| Out | `substitutes.out` | Subbed out count |
| Bench | `substitutes.bench` | Bench appearances |

**Shots Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Total Shots | `shots.total` | All shots |
| Shots on Target | `shots.on` | SOT |

**Goals Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Goals | `goals.total` | Goals scored |
| Assists | `goals.assists` | Assists |
| Conceded | `goals.conceded` | Goals conceded (GK) |
| Saves | `goals.saves` | Saves (GK) |

**Passes Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Total Passes | `passes.total` | All passes |
| Key Passes | `passes.key` | Key passes |
| Accuracy | `passes.accuracy` | Pass accuracy % |

**Tackles Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Total Tackles | `tackles.total` | All tackles |
| Blocks | `tackles.blocks` | Blocks |
| Interceptions | `tackles.interceptions` | Interceptions |

**Duels Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Total Duels | `duels.total` | All duels |
| Duels Won | `duels.won` | Won duels |

**Dribbles Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Attempts | `dribbles.attempts` | Dribble attempts |
| Success | `dribbles.success` | Successful dribbles |
| Past | `dribbles.past` | Dribbled past (defensive) |

**Fouls Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Drawn | `fouls.drawn` | Fouls won |
| Committed | `fouls.committed` | Fouls committed |

**Cards Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Yellow | `cards.yellow` | Yellow cards |
| Yellow→Red | `cards.yellowred` | Second yellows |
| Red | `cards.red` | Straight reds |

**Penalty Section:**

| Stat | Field | Description |
|------|-------|-------------|
| Won | `penalty.won` | Penalties won |
| Committed | `penalty.commited` | Penalties conceded |
| Scored | `penalty.scored` | Penalties scored |
| Missed | `penalty.missed` | Penalties missed |
| Saved | `penalty.saved` | Penalties saved (GK) |

---

## 3. Optimized Data Acquisition Strategy

### 3.1 Principle: FotMob First, API-Football for Gaps

| Advantage | FotMob | API-Football |
|-----------|--------|--------------|
| **Request Limit** | ∞ (no key) | 100/day |
| **xG Data** | ✅ Free | ❌ Premium only |
| **Player Ratings** | ✅ | ✅ |
| **Match Events** | ✅ | ✅ |
| **Squad Roster** | ✅ | ✅ |
| **Market Value** | ✅ | ❌ |
| **Penalty Stats** | ❌ | ✅ |
| **Substitution Stats** | ❌ | ✅ |
| **Birth Place** | ❌ | ✅ |
| **Weight** | ❌ | ✅ |
| **Stadium Capacity** | ❌ | ✅ |
| **Team Founded Year** | ❌ | ✅ |
| **Form String** | ❌ | ✅ |

---

### 3.2 Recommended Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    OPTIMIZED DATA FLOW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              PRIMARY: FotMob (Unlimited)                 │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  ✓ League standings (all 8 leagues)                      │  │
│  │  ✓ xG Table (FREE - not available elsewhere!)            │  │
│  │  ✓ Team rosters / Squad lists                            │  │
│  │  ✓ Player bios (height, foot, market value)              │  │
│  │  ✓ Player season stats (goals, assists, rating, cards)   │  │
│  │  ✓ Player career history                                 │  │
│  │  ✓ Match details with team stats                         │  │
│  │  ✓ Per-match player stats (ratings, shots, tackles...)   │  │
│  │  ✓ Match events (goals, cards, subs)                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │           SECONDARY: API-Football (100/day)              │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │  ✓ Penalty stats (won, committed, scored, missed, saved) │  │
│  │  ✓ Substitution stats (in, out, bench)                   │  │
│  │  ✓ Player weight                                         │  │
│  │  ✓ Birth place/country (more detailed)                   │  │
│  │  ✓ Stadium capacity & surface                            │  │
│  │  ✓ Team founded year                                     │  │
│  │  ✓ Form string (WWLDW)                                   │  │
│  │  ✓ Yellow→Red distinction                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3.3 Avoiding Duplication: Source Priority Matrix

| Data Category | Primary Source | Secondary Source | Dedup Key |
|--------------|----------------|------------------|-----------|
| **League Standings** | FotMob | Skip API-Football | team_id + season_id |
| **xG Data** | FotMob (exclusive) | N/A | team_id + season_id |
| **Team Info** | FotMob | API-Football (venue only) | fotmob_id |
| **Player Bio** | FotMob | API-Football (weight/birth) | fotmob_id |
| **Player Season Stats** | FotMob | API-Football (penalties) | player_id + season_id |
| **Match Basic** | FotMob | Skip API-Football | fotmob_match_id |
| **Match Team Stats** | FotMob | Skip API-Football | match_id + team_id |
| **Match Player Stats** | FotMob | Skip API-Football | match_id + player_id |

---

### 3.4 Recommended Scheduler Configuration

```python
# Daily Jobs (run every day at 02:00 UTC)
FotMob Jobs:
  - fotmob_daily_collection_job()
    → Collect all 8 leagues standings + xG
    → Collect recent matches (last 3 days)
    → ~40-60 API requests, 0 quota usage

# Weekly Jobs (run Sunday 03:00 UTC)
FotMob Jobs:
  - fotmob_weekly_deep_job()
    → Deep player data for all teams
    → Full match history for season
    → ~500-800 requests, 0 quota usage

API-Football Jobs:
  - api_football_supplementary_job()  # NEW - TO IMPLEMENT
    → Penalty stats enrichment (players endpoint)
    → Stadium/venue enrichment (teams endpoint)
    → ~50-80 requests, within 100/day
```

---

## 4. Missing Must-Have Stats for Modern Scouting

These are **critical stats for professional football analytics** that are **NOT available from either FotMob or API-Football (free tier)**:

### 4.1 Advanced Expected Metrics (Critical Gap)

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **xA (Expected Assists)** | HIGH | StatsBomb, Opta, Wyscout |
| **npxG (Non-penalty xG)** | HIGH | StatsBomb, FBref (scraped) |
| **xGOT (xG on Target)** | HIGH | StatsBomb |
| **xGBuildup** | MEDIUM | StatsBomb |
| **xGChain** | MEDIUM | StatsBomb |
| **Post-shot xG** | MEDIUM | StatsBomb, Understat |

### 4.2 Progressive Actions (Critical for Modern Scouting)

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **Progressive Passes** | HIGH | StatsBomb, FBref |
| **Progressive Carries** | HIGH | StatsBomb, FBref |
| **Progressive Passes Received** | HIGH | StatsBomb, FBref |
| **Carries into Final Third** | HIGH | StatsBomb |
| **Carries into Penalty Area** | HIGH | StatsBomb |
| **Passes into Final Third** | MEDIUM | StatsBomb, Wyscout |
| **Passes into Penalty Area** | MEDIUM | StatsBomb, Wyscout |

### 4.3 Pressing & Defensive Actions (Critical Gap)

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **Pressures** | HIGH | StatsBomb, FBref |
| **Successful Pressures** | HIGH | StatsBomb, FBref |
| **Pressure Regains** | HIGH | StatsBomb |
| **PPDA (Passes per Defensive Action)** | HIGH | Opta, Wyscout |
| **Recoveries** | MEDIUM | StatsBomb |
| **Ball Recoveries in Opp Half** | MEDIUM | StatsBomb |
| **Counterpressure Success %** | MEDIUM | StatsBomb |

### 4.4 Passing Detail (Important)

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **Long Passes Completed** | MEDIUM | StatsBomb, FBref |
| **Short Passes Completed** | MEDIUM | StatsBomb |
| **Medium Passes Completed** | MEDIUM | StatsBomb |
| **Switches** | MEDIUM | StatsBomb |
| **Through Balls** | MEDIUM | StatsBomb, Opta |
| **Crosses Completed** | MEDIUM | StatsBomb, FBref |
| **Cross Accuracy %** | MEDIUM | Wyscout |
| **Final Third Passes** | MEDIUM | StatsBomb |

### 4.5 Chance Creation (Important)

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **Shot-Creating Actions (SCA)** | HIGH | StatsBomb, FBref |
| **Goal-Creating Actions (GCA)** | HIGH | StatsBomb, FBref |
| **SCA per 90** | HIGH | FBref |
| **GCA per 90** | HIGH | FBref |
| **Dead-ball SCA** | MEDIUM | StatsBomb |
| **Take-on SCA** | MEDIUM | StatsBomb |

### 4.6 Aerial & Set Pieces

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **Set Piece Goals** | MEDIUM | Opta, StatsBomb |
| **Free Kick Goals** | MEDIUM | Opta |
| **Header Goals** | MEDIUM | Opta, Wyscout |
| **Aerial Duel Win %** | MEDIUM | FBref, Wyscout |

### 4.7 Goalkeeper Advanced

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **PSxG (Post-shot xG faced)** | HIGH | StatsBomb, FBref |
| **PSxG - Goals Allowed** | HIGH | FBref (GA - PSxG) |
| **Crosses Stopped %** | MEDIUM | FBref |
| **Sweeper Actions** | MEDIUM | StatsBomb |
| **Average Distance from Goal** | LOW | StatsBomb |

### 4.8 Possession Value

| Missing Stat | Importance | Alternative Source |
|-------------|------------|-------------------|
| **VAEP (Value Added by Actions)** | HIGH | Custom model |
| **EPV (Expected Possession Value)** | HIGH | Custom model |
| **Off-ball Movement Score** | MEDIUM | Tracking data only |
| **Space Control** | MEDIUM | Tracking data only |

---

## 5. Recommendations for Filling Gaps

### Option 1: StatsBomb Open Data (FREE)

- **Coverage**: Limited (360 matches from select competitions)
- **Stats Available**: All progressive actions, xA, SCA/GCA, pressures
- **Already integrated**: Yes (`statsbomb_etl.py` exists in this project)
- **Limitation**: Historical data only, not live/current season

### Option 2: Understat (FREE, requires scraping)

- **Coverage**: Top 5 European leagues, current season
- **Stats Available**: xG, xA, xGBuildup, xGChain
- **Requires**: Web scraper implementation
- **Legal**: Gray area (web scraping)

### Option 3: FBref (FREE, requires scraping)

- **Coverage**: All major leagues worldwide
- **Stats Available**: Progressive actions, SCA/GCA, pressures, advanced passing
- **Requires**: Web scraper (HTML parsing)
- **Legal**: Gray area (web scraping)
- **Note**: Previously removed from this project due to maintenance burden

### Option 4: Wyscout/Opta (PAID)

- **Coverage**: Comprehensive - all leagues, all stats
- **Stats Available**: Everything including tracking data
- **Cost**: €5,000-50,000/year depending on tier
- **Best for**: Professional scouting departments with budget

### Option 5: Custom xG/xA Model (DIY)

- Build using FotMob shot location data
- Requires ML model training (logistic regression or neural network)
- Can calculate npxG, xA from shot coordinates + context
- Time investment: 2-4 weeks for basic model

---

## 6. Summary

### 6.1 What You GET from Current Sources

| Category | FotMob | API-Football | Combined Coverage |
|----------|--------|--------------|-------------------|
| Basic standings | ✅ | ✅ | ✅ Full |
| xG/xPoints | ✅ | ❌ | ✅ Via FotMob |
| Goals/Assists | ✅ | ✅ | ✅ Full |
| Shots/SOT | ✅ | ✅ | ✅ Full |
| Passes (total) | ✅ | ✅ | ✅ Full |
| Tackles/Interceptions | ✅ | ✅ | ✅ Full |
| Duels/Dribbles | ✅ | ✅ | ✅ Full |
| Cards | ✅ | ✅ | ✅ Full |
| Player ratings | ✅ | ✅ | ✅ Full |
| Market value | ✅ | ❌ | ✅ Via FotMob |
| Penalty stats | ❌ | ✅ | ✅ Via API-Football |
| Weight | ❌ | ✅ | ✅ Via API-Football |
| Stadium info | ❌ | ✅ | ✅ Via API-Football |

### 6.2 Critical GAPS for Professional Scouting

| Priority | Missing Stats | Impact |
|----------|--------------|--------|
| **HIGH** | Progressive Actions (passes, carries) | Cannot assess ball progression ability |
| **HIGH** | xA (Expected Assists) | Cannot evaluate chance creation quality |
| **HIGH** | Pressing Stats (pressures, PPDA) | Cannot assess defensive intensity |
| **HIGH** | Shot-Creating Actions (SCA, GCA) | Cannot evaluate creative contribution |
| **MEDIUM** | Passing Breakdown (long/short/medium) | Limited passing profile analysis |
| **MEDIUM** | Advanced GK (PSxG, PSxG-GA) | Cannot properly evaluate goalkeepers |

### 6.3 Recommended Next Steps

1. **Immediate**: Implement the optimized data flow as described in Section 3
2. **Short-term**: Add supplementary API-Football job for penalty/venue enrichment
3. **Medium-term**: Consider StatsBomb integration for historical progressive action data
4. **Long-term**: Evaluate Understat scraping for xA/npxG OR build custom xG model

---

## Appendix A: FotMob API Endpoints Reference

| Endpoint | URL | Returns |
|----------|-----|---------|
| League | `/api/leagues?id={id}` | Standings, xG table, matches, seasons |
| League Season | `/api/leagues?id={id}&season={season}` | Historical season data |
| Team | `/api/teams?id={id}` | Team info, squad, fixtures |
| Player | `/api/playerData?id={id}` | Bio, season stats, career history |
| Match | `/api/matchDetails?matchId={id}` | Scores, team stats, player stats, events |

## Appendix B: API-Football Endpoints Reference

| Endpoint | URL | Returns |
|----------|-----|---------|
| Leagues | `/leagues` | Available leagues |
| Teams | `/teams?league={id}&season={year}` | Team info with venue |
| Standings | `/standings?league={id}&season={year}` | League table with form |
| Fixtures | `/fixtures?league={id}&season={year}` | Match list |
| Fixture Stats | `/fixtures/statistics?fixture={id}` | Per-match team stats |
| Players | `/players?team={id}&season={year}` | Full player stats |
| Top Scorers | `/players/topscorers?league={id}&season={year}` | Top scorers list |

## Appendix C: Supported Leagues

| League | Country | FotMob ID | API-Football ID |
|--------|---------|-----------|-----------------|
| Premier League | England | 47 | 39 |
| La Liga | Spain | 87 | 140 |
| Serie A | Italy | 55 | 135 |
| Bundesliga | Germany | 54 | 78 |
| Ligue 1 | France | 53 | 61 |
| Eredivisie | Netherlands | 57 | 88 |
| Brasileiro Serie A | Brazil | 268 | 71 |
| Argentina Primera | Argentina | 112 | 128 |

---

## Appendix D: Implementation Status

### Completed Implementations

| Component | Status | File |
|-----------|--------|------|
| FotMob ETL | ✅ Complete | `etl/fotmob_etl.py` |
| FotMob Client | ✅ Complete | `scrapers/fotmob/client.py` |
| API-Football ETL | ✅ Complete | `etl/api_football_etl.py` |
| API-Football Supplementary Job | ✅ Complete | `scheduler/jobs.py` |
| StatsBomb Advanced ETL | ✅ Complete | `etl/statsbomb_advanced_etl.py` |
| Understat Client | ✅ Complete | `scrapers/understat/client.py` |
| Understat ETL | ✅ Complete | `etl/understat_etl.py` |
| Schema Migration | ✅ Complete | `database/migration_003_advanced_stats.sql` |
| CLI Commands | ✅ Complete | `cli.py` |
| Scheduler Jobs | ✅ Complete | `scheduler/jobs.py` |

### New CLI Commands

```bash
# StatsBomb Advanced (progressive actions, SCA/GCA)
python cli.py statsbomb list-competitions
python cli.py statsbomb collect-advanced --competition-id 11 --season-id 90

# Understat (xA, npxG, xGChain, xGBuildup)
python cli.py understat test-connection
python cli.py understat collect-league --league premier-league
python cli.py understat collect-all --season 2024

# Full refresh (all sources)
python cli.py full-refresh
python cli.py full-refresh --season 2024 --skip-api-football
```

### New Scheduled Jobs

| Job | Function | Schedule |
|-----|----------|----------|
| API-Football Supplementary | `api_football_supplementary_job()` | Weekly |
| StatsBomb Collection | `statsbomb_collection_job()` | On-demand |
| Understat Collection | `understat_collection_job()` | Weekly |
| Full Data Refresh | `full_data_refresh_job()` | Weekly |

---

*Document maintained as part of Data-ETL-Pipeline project.*
*Last updated: February 2026*
