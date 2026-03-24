# SoccerData Library — Complete Research Report for Professional Scouting Application

**Report Date:** March 2026  
**Library Version:** 1.8.8 (Released: Jan 16, 2026)  
**Library URL:** https://soccerdata.readthedocs.io  
**GitHub:** https://github.com/probberechts/soccerdata  
**PyPI:** https://pypi.org/project/soccerdata/  
**License:** Apache-2.0  
**Author:** Pieter Robberechts (KU Leuven)  

---

## 1. EXECUTIVE SUMMARY

`soccerdata` is an open-source Python library (NOT a commercial API) that provides a **unified web-scraping interface** for 8 different soccer data websites. It returns data as Pandas DataFrames with consistent column naming and cross-source identifiers. It is **not** a data provider itself — it scrapes public-facing websites and caches results locally.

**Key verdict for scouting application:** This library provides access to **exceptionally deep professional-grade data** (especially through FBref and WhoScored), including Opta event stream data, advanced metrics (xG, xA, VAEP, SPADL), and per-player per-match granular stats. However, it comes with **real fragility risks** (scraper breakage when sites update), **strong anti-scraping protections** (especially WhoScored), and it covers primarily **Top 5 European leagues** out-of-the-box.

---

## 2. INSTALLATION & REQUIREMENTS
```bash
pip install soccerdata
pip install soccerdata[socceraction]  # For SPADL/event analytics
```

**Requirements:**
- Python `>=3.9, <3.14`
- Core dependencies: `pandas>=2.0`, `html5lib`, `seleniumbase`, `wrapper-tls-requests`, `tqdm`, `rich`, `lxml`, `unidecode`, `urllib3<2`
- Optional: `socceraction` (for SPADL event-stream format and VAEP/xT models)
- WhoScored specifically requires **Google Chrome** installed (uses SeleniumBase to bypass Incapsula bot detection)

---

## 3. SUPPORTED DATA SOURCES — OVERVIEW TABLE

| Source | Class | Data Type | Tech Used | Blocker Level | Scouting Value |
|---|---|---|---|---|---|
| FBref | `FBref` | Stats, lineups, events, shots | HTTP (requests) | Low-Medium | ⭐⭐⭐⭐⭐ |
| WhoScored | `WhoScored` | Event stream, schedules, injuries | Selenium + Chrome | Very High | ⭐⭐⭐⭐⭐ |
| Understat | `Understat` | xG, xA, shots, player stats | HTTP (JS variable) | Low | ⭐⭐⭐⭐ |
| Sofascore | `Sofascore` | League tables, schedules | HTTP (requests) | Medium | ⭐⭐⭐ |
| ESPN | `ESPN` | Schedule, matchsheet, lineups | JSON API | Low | ⭐⭐⭐ |
| Football-Data.co.uk | `MatchHistory` | Results, betting odds | CSV download | Very Low | ⭐⭐ |
| ClubElo | `ClubElo` | Team ELO ratings | CSV API | Very Low | ⭐⭐⭐ |
| SoFIFA | `SoFIFA` | EA Sports player/team ratings | HTTP (requests) | Medium | ⭐⭐⭐ |

---

## 4. DETAILED SOURCE-BY-SOURCE BREAKDOWN

---

### 4.1 FBref (fbref.com) — PRIMARY RECOMMENDATION

**Data Origin:** FBref uses **Opta/StatsBomb** underlying data. This is the same data feed used by professional clubs.  
**Class:** `soccerdata.FBref`  
**Cache Location:** `~/soccerdata/data/FBref`

#### Available Leagues (out-of-the-box):
- `'Big 5 European Leagues Combined'` ← most efficient for Top-5
- `'ENG-Premier League'`
- `'ESP-La Liga'`
- `'FRA-Ligue 1'`
- `'GER-Bundesliga'`
- `'ITA-Serie A'`
- `'INT-World Cup'`
- `'INT-Women's World Cup'`
- Custom leagues can be added (no guarantee of correctness)

#### Methods & Data Returned:

**`read_schedule(force_cache=False)`**
Returns: game week, date/time, home team, away team, xG (home/away), score, attendance, venue, referee, match_report URL, game_id

**`read_team_season_stats(stat_type, opponent_stats=False)`**
Aggregated season-level stats per team. Stat types available:
- `'standard'` — goals, assists, xG, npxG, progressive passes/carries/runs
- `'keeper'` — saves, GA, PSxG, CS, OG, CKsv, etc.
- `'keeper_adv'` — PSxG vs GA, launched passes, sweeper actions
- `'shooting'` — shots, SoT%, xG, npxG, distance, body part
- `'passing'` — Cmp, Att, Cmp%, TotDist, PrgDist, short/medium/long breakdown, xAG, KP, 1/3 entries, PPA, CrsPA, PrgP
- `'passing_types'` — live/dead ball, FK, corner, switch, cross, throw-in, pressured/blocked
- `'goal_shot_creation'` — GCA, SCA (shots/goals creating actions and types)
- `'defense'` — tackles (won, mid-third, def-third, att-third), pressures, blocks, interceptions, clearances, errors
- `'possession'` — touches by zone, carries, dribbles, miscontrols, dispossessed, receiving
- `'playing_time'` — MP, starts, min, 90s, Mn/Start, subs, unSub, PPM, onG, onGA, +/-, xG+/-, on-Off
- `'misc'` — cards, fouls, handballs, won/lost penalties

**`read_player_season_stats(stat_type)`**
Same stat types as team_season_stats (above) but per individual player. Key fields in `'standard'`: nation, position, age, born, MP, starts, Min, 90s, Gls, Ast, G+A, xG, npxG, xAG, PrgC, PrgP, PrgR, Per-90 versions of all above.

**`read_team_match_stats(stat_type, team=None, opponent_stats=False)`**
Per-match team logs. Stat types: `'schedule'`, `'keeper'`, `'shooting'`, `'passing'`, `'passing_types'`, `'goal_shot_creation'`, `'defense'`, `'possession'`, `'misc'`

**`read_player_match_stats(stat_type, match_id=None)`**
Per-player stats for a specific match or all matches in selected seasons. Stat types: `'summary'`, `'keepers'`, `'passing'`, `'passing_types'`, `'defense'`, `'possession'`, `'misc'`  
Includes: jersey number, nation, position, age, minutes played, full stat breakdown by type.

**`read_lineup(match_id=None)`**
Returns: jersey_number, player, team, is_starter, position, minutes_played — per game

**`read_events(match_id=None)`**
Match events: goals, yellow/red cards, substitutions — with minute, score, player1, player2, event_type

**`read_shot_events(match_id=None)`**
Every shot event: minute, player, team, xG, PSxG, outcome (goal/saved/missed/blocked), distance, body_part, notes, two shot-creating actions (SCA1, SCA2 with player and event type)

**`read_leagues()` / `read_seasons()`**
Metadata about available leagues and seasons.

#### Scouting Value Assessment:
FBref is the single most data-rich source in this library. It covers aggregate season stats, per-match stats, per-player stats, and shot-level events with xG and shot-creating action chains — all at the match level. This is **professional-grade Opta-powered data** accessible for free via scraping.

---

### 4.2 WhoScored (whoscored.com) — EVENT STREAM POWERHOUSE

**Data Origin:** WhoScored uses **Opta** event-stream data. This is the same feed used by Opta's commercial clients.  
**Class:** `soccerdata.WhoScored`  
**Cache Location:** `~/soccerdata/data/WhoScored`  
**Special Requirement:** Google Chrome must be installed. Uses `SeleniumBase` + ChromeDriver to simulate real browser.

#### Available Leagues: All major WhoScored leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1, and more major leagues worldwide)

#### Methods & Data Returned:

**`read_schedule(force_cache=False)`**
Full match metadata: stage_id, game_id, status, start_time, home/away team + IDs, cards, has_incidents, score, period details, lineup confirmation status, UTC timestamps for kick-off/half-time/2nd half

**`read_missing_players(match_id=None)`**
Pre-match availability: player_id, reason (injured/suspended), status (Doubt/Out)

**`read_events(match_id=None, output_fmt='events', live=False, retry_missing=True, on_error='raise')`**

This is the crown jewel — **full Opta event stream**. Fields per event:
- game_id, period (PreMatch/FirstHalf/SecondHalf/ExtraTime etc.)
- minute, second, expanded_minute
- type (Pass, Shot, Tackle, Foul, Card, Dribble, Save, etc.)
- outcome_type (Successful/Unsuccessful)
- team_id, team, player_id, player
- x, y (start coordinates, 0-100 pitch grid)
- end_x, end_y (end coordinates)
- goal_mouth_y, goal_mouth_z (for shots)
- blocked_x, blocked_y
- qualifiers (rich JSON list: angle, body part, pass direction, header, cross, corner, FK, through ball, assisted, etc.)
- is_touch, is_shot, is_goal
- card_type, related_event_id, related_player_id

**Output Formats for `read_events()`:**
1. `'events'` (default) — full Pandas DataFrame as above
2. `'raw'` — original WhoScored JSON (complete, unprocessed)
3. `'spadl'` — **SPADL format** (requires `socceraction`): standardized action representation with pitch coordinates, action type, body part, result — compatible with xT/VAEP models
4. `'atomic-spadl'` — **Atomic-SPADL** (requires `socceraction`): decomposes complex actions into atomic on-ball events
5. `'loader'` — Returns a `socceraction.data.opta.OptaLoader` instance for full integration with the socceraction analytics framework

**OptaLoader exposes:** games(), teams(), players(), events() — structured for downstream VAEP/xT computation.

#### Scouting Value Assessment:
WhoScored is the **most analytically powerful source** in the library. Full Opta event stream with pitch coordinates means you can compute: pass networks, pressing intensity, space creation, ball recovery zones, individual defensive actions, dribble success rates, and all advanced metrics (xT, VAEP, etc.) using the socceraction integration. This is **commercial-quality data**.

---

### 4.3 Understat (understat.com) — xG SPECIALIST

**Data Origin:** Understat's own xG model (not Opta). Covers top European leagues from 2014/15 onward.  
**Class:** `soccerdata.Understat`  
**Cache Location:** `~/soccerdata/data/Understat`

#### Available Leagues:
- `'ENG-Premier League'`
- `'ESP-La Liga'`
- `'GER-Bundesliga'`
- `'ITA-Serie A'`
- `'FRA-Ligue 1'`
- `'RUS-Premier League'`

#### Methods & Data Returned:

**`read_schedule(include_matches_without_data=True)`**
game_id, date, home/away team + IDs + codes, home/away goals, home/away xG, is_result, has_data, URL

**`read_team_match_stats()`**
Per-match team stats: xG, npxG, xG difference, ppda (passes allowed per defensive action — pressing metric), deep_completions, expected_points, actual points, goals

**`read_player_season_stats()`**
Per player per season: team, position, matches, minutes, goals, xG, np_goals, np_xG, assists, xA, shots, key_passes, yellow/red cards, **xG_chain** (xG from all actions in chains ending in shot), **xG_buildup** (xG contribution in build-up play)

**`read_player_match_stats(match_id=None)`**
Per player per match: position, minutes, goals, own_goals, shots, xG, xA, xG_chain, xG_buildup

**`read_shot_events(match_id=None)`**
Every shot: shot_id, team, player, assist_player_id, assist_player, xG, location_x, location_y (normalized 0-1 pitch), minute, body_part (Left Foot/Right Foot/Head), situation (Open Play/Set Piece/FK/Counter), result (Goal/Saved/Missed/Blocked)

#### Scouting Value Assessment:
Understat is excellent for xG-based analysis. The **xG_chain** and **xG_buildup** metrics are unique — they quantify a player's contribution to shot-creating sequences, making it ideal for evaluating players who don't score but contribute to attacks. Shot location data with xG per shot is perfect for scouting shooters and strikers.

---

### 4.4 Sofascore (sofascore.com) — LIMITED BUT BROAD COVERAGE

**Data Origin:** Sofascore's own internal data  
**Class:** `soccerdata.Sofascore`  
**Cache Location:** `~/soccerdata/data/Sofascore`

#### Methods & Data Returned:

**`read_league_table()`**
Team, MP, W, D, L, GF, GA, GD, Pts

**`read_schedule()`**
Round, week, date, home/away team, home/away score, game_id

**Note:** Sofascore's API in soccerdata is **significantly more limited** than the other sources — only 2 read methods beyond metadata. The Sofascore website itself has far richer data (player ratings, heatmaps, detailed stats) but the soccerdata library currently only exposes schedule and league table.

#### Scouting Value Assessment:
Low within soccerdata's current implementation. The underlying Sofascore platform has rich data but the library barely scratches the surface.

---

### 4.5 ESPN (espn.com JSON API) — BASIC MATCH DATA

**Data Origin:** ESPN's internal JSON API (`site.api.espn.com`)  
**Class:** `soccerdata.ESPN`  
**Cache Location:** `~/soccerdata/data/ESPN`

#### Available Leagues: Wide coverage including Premier League, La Liga, Bundesliga, Serie A, Ligue 1, Champions League, MLS, and many more

#### Methods & Data Returned:

**`read_schedule()`**
date, home_team, away_team, game_id, league_id

**`read_matchsheet(match_id=None)`**
Per team: is_home, venue, attendance, capacity, roster (raw JSON), fouls_committed, yellow_cards, red_cards, offsides, won_corners, saves, possession_pct, total_shots, shots_on_target

**`read_lineup(match_id=None)`**
Per player per match: is_home, position, formation_place, sub_in/sub_out timestamps, appearances, fouls_committed, fouls_suffered, own_goals, red_cards, yellow_cards, goal_assists, shots_on_target, total_goals, total_shots, goals_conceded (GK), saves (GK), shots_faced (GK), offsides

#### Scouting Value Assessment:
Moderate. Good for match result context and basic lineup tracking. Limited analytical depth. Better league coverage than FBref/WhoScored for non-European leagues.

---

### 4.6 Football-Data.co.uk (MatchHistory) — HISTORICAL + BETTING ODDS

**Data Origin:** football-data.co.uk CSV files  
**Class:** `soccerdata.MatchHistory`  
**Cache Location:** `~/soccerdata/data/MatchHistory`

#### Methods & Data Returned:

**`read_games()`**
Full match result including:
- Date, home/away team, Full-time/Half-time result and goals (FTHG, FTAG, FTR, HTHG, HTAG)
- Referee
- Shots (HS/AS), Shots on Target (HST/AST), Fouls (HF/AF), Corners (HC/AC), Yellow/Red cards per team
- **Comprehensive betting odds from multiple bookmakers:** B365 (Bet365), BW (Betway), IW, PS (Pinnacle), WH, VC — for 1X2, Over/Under 2.5, Asian Handicap
- Max and Average odds across bookmakers
- Closing odds versions also available

#### Scouting Value Assessment:
Limited for player scouting. Excellent for **historical match result databases** and **betting market data** if you need odds-based features in models. Good for long historical coverage going back many seasons for European leagues and lower divisions.

---

### 4.7 ClubElo (clubelo.com) — ELO RATINGS

**Data Origin:** clubelo.com CSV API  
**Class:** `soccerdata.ClubElo`  
**Cache Location:** `~/soccerdata/data/ClubElo`  
**Note:** Does not filter by league — covers all European clubs

#### Methods & Data Returned:

**`read_by_date(date=None)`**
For all teams at a given date: rank, country, level (division tier), elo rating, valid_from, valid_to, league name

**`read_team_history(team)`**
Complete ELO history for a single club from earliest available date: rank, team, country, level, elo, to, from

#### Scouting Value Assessment:
Useful as a **contextual feature** for models (opponent strength, league level). ELO history goes back to ~1939 for some clubs. Not a player-level data source.

---

### 4.8 SoFIFA (sofifa.com) — EA SPORTS FC PLAYER RATINGS

**Data Origin:** EA Sports FC (formerly FIFA) game database via sofifa.com  
**Class:** `soccerdata.SoFIFA`  
**Cache Location:** `~/soccerdata/data/SoFIFA`

#### Methods & Data Returned:

**`read_team_ratings()`**
Per team: overall, attack, midfield, defence ratings, transfer_budget, player count, FIFA edition, update date

**`read_player_ratings(team=None)`**
Per player: fifa_edition, update, overallrating, potential, plus **35 individual attribute scores:**
crossing, finishing, heading_accuracy, short_passing, volleys, dribbling, curve, fk_accuracy, long_passing, ball_control, acceleration, sprint_speed, agility, reactions, balance, shot_power, jumping, stamina, strength, long_shots, aggression, interceptions, positioning, vision, penalties, composure, defensive_awareness, standing_tackle, sliding_tackle, gk_diving, gk_handling, gk_kicking, gk_positioning, gk_reflexes

**`read_teams()` / `read_players()`**
Metadata: team/player names, IDs, league, FIFA edition, update date

**`read_versions()`**
Available FIFA release versions (IDs for URL lookup)

#### Scouting Value Assessment:
Unique as a **player attribute profile** data source. EA Sports employs hundreds of data reviewers worldwide to rate players. While not official match data, these ratings are useful as proxy scouting attributes — especially for players without rich statistical coverage. Potential vs. Overall gap is a classic undervaluation signal.

---

## 5. CROSS-SOURCE FEATURES

### Team Name Normalization
Different sources use different team names (e.g., "Man Utd" vs "Manchester United" vs "Manchester United FC"). soccerdata provides a JSON config file at `SOCCERDATA_DIR/config/teamname_replacements.json` to map names to a unified standard:
```json
{
  "Manchester United": ["Man Utd", "Manchester United FC", "Man United"],
  "Tottenham": ["Tottenham Hotspur", "Tottenham Hotspur FC", "Spurs"]
}
```

### Uniform Game IDs
Games are given consistent IDs across sources using the pattern `YYYY-MM-DD HomeTeam-AwayTeam` enabling cross-source joins.

### Socceraction Integration
Install with `pip install soccerdata[socceraction]`. WhoScored events can be exported as `OptaLoader` instances compatible with the full `socceraction` analytics pipeline:
- SPADL action representation
- Atomic-SPADL
- xT (Expected Threat) model computation
- VAEP (Valuing Actions by Estimating Probabilities) model computation
This is **professional-grade** spatial event analysis.

---

## 6. ACCESS METHODS & TECHNICAL EXTRACTION DETAILS

### 6.1 HTTP Requests Scrapers (FBref, Understat, ESPN, MatchHistory, ClubElo, SoFIFA)

These use the `BaseRequestsReader` class. The `wrapper-tls-requests` library (TLS fingerprint spoofing) is used instead of standard `requests` to better mimic real browser TLS handshakes.

**Proxy Support (all scrapers):**
```python
# Tor network
fbref = sd.FBref(proxy='tor')  # Tor must run on port 9050

# Single proxy
fbref = sd.FBref(proxy='http://proxy.server:8080')

# Rotating proxy list
fbref = sd.FBref(proxy=['http://proxy1:8080', 'http://proxy2:8080'])

# Dynamic proxy function (called after failed requests)
fbref = sd.FBref(proxy=lambda: get_next_proxy())
```

### 6.2 Selenium Scraper (WhoScored only)

WhoScored uses **Incapsula bot protection** — standard HTTP requests are blocked. The library uses `SeleniumBase` with ChromeDriver:
```python
ws = sd.WhoScored(
    leagues='ENG-Premier League',
    seasons=2021,
    path_to_browser='/usr/bin/google-chrome',  # optional, auto-detected
    headless=False  # False = headed mode, harder to detect (recommended)
)
```
- A matching ChromeDriver is **auto-downloaded** by SeleniumBase
- `headless=False` is recommended to avoid blocks (but requires display)
- Rate limiting, CAPTCHA, and Incapsula checks are the primary challenges

### 6.3 Caching System
```python
# Environment variables for global config:
export SOCCERDATA_DIR="~/soccerdata"       # Cache directory
export SOCCERDATA_NOCACHE="False"          # Use cached if available
export SOCCERDATA_NOSTORE="False"          # Store downloaded data
export SOCCERDATA_MAXAGE="86400"           # Max cache age in seconds
export SOCCERDATA_LOGLEVEL="INFO"          # Logging level

# Per-instance config:
fbref = sd.FBref(
    data_dir="/custom/cache/path",
    no_cache=True,    # Always re-download
    no_store=True,    # Don't cache to disk
)

# Force refresh current season (normally always re-downloaded):
fbref.read_schedule(force_cache=True)  # Use cache even for current season
```

Cache is stored as local files (JSON/CSV/HTML). The library does **not know when source data changes** — cache management is the user's responsibility.

### 6.4 Live Data
WhoScored supports live event scraping:
```python
ws.read_events(match_id=12345, live=True)  # Bypasses cache for live match
```

---

## 7. BLOCKERS & ANTI-SCRAPING ASSESSMENT

### FBref
- **Blocker Level: Low–Medium**
- Uses `wrapper-tls-requests` for TLS fingerprint spoofing
- FBref has rate limiting; too many rapid requests will result in 429 errors
- Caching mitigates most rate-limit issues for historical data
- **Active Issue (#916):** Error fetching schedule on v1.8.8 — indicates live breakage risk
- Recommended: Use caching aggressively, do not scrape all leagues simultaneously

### WhoScored
- **Blocker Level: Very High**
- Protected by **Incapsula** (enterprise-grade bot detection)
- Requires Selenium + Chrome to simulate a real browser session
- Even with Selenium, blocks occur; `headless=False` is more reliable but harder to run in server environments
- **Active Issue (#909):** ValueError with date format parsing — scraper fragility
- Using rotating proxies is strongly recommended for bulk scraping
- **Cannot be used in serverless/headless CI environments** without special setup
- Scraping rate must be kept very low (human-like delays built in by library)

### Understat
- **Blocker Level: Low**
- Data is embedded in page JavaScript as JSON variables; library extracts via `var` parameter
- No known blocking issues; straightforward scraping

### Sofascore
- **Blocker Level: Medium**
- JSON API endpoints can change; middleware headers required
- No Selenium required

### ESPN
- **Blocker Level: Low**
- Uses official public JSON API (`site.api.espn.com`) — no scraping, structured API response
- Most stable and reliable source

### Football-Data.co.uk
- **Blocker Level: Very Low**
- Direct CSV file downloads — no JS, no protection
- **Active Issue (#927):** HTTP 503 errors fetching Premier League data (current)

### ClubElo
- **Blocker Level: Very Low**
- Official CSV API; intentionally open for public use

### SoFIFA
- **Blocker Level: Medium**
- **Active Issue (#889):** `read_player_ratings` returning only 1 record — known bug in current version

---

## 8. KNOWN ACTIVE BUGS (As of March 2026)

Based on GitHub Issues (28 open, 175 closed):

| Issue | Source | Description | Severity |
|---|---|---|---|
| #927 | MatchHistory | HTTP 503 on Premier League data | High |
| #916 | FBref | Error fetching schedule on v1.8.8 | High |
| #909 | WhoScored | ValueError with date format `'Aug'` | Medium |
| #889 | SoFIFA | `read_player_ratings` returns only 1 record | High |

**Critical Note for Production Use:** This library is explicitly described by the maintainer as: *"any changes to the scraped websites will break the package. Hence, do not expect that all code will work all the time."*

---

## 9. LEAGUE COVERAGE

### Out-of-the-Box Supported Leagues

| League | FBref | WhoScored | Understat | ESPN | MatchHistory | ClubElo | Sofascore |
|---|---|---|---|---|---|---|---|
| ENG-Premier League | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ESP-La Liga | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| GER-Bundesliga | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ITA-Serie A | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| FRA-Ligue 1 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| RUS-Premier League | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| INT-World Cup | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ |
| Champions League | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| MLS, other | ❌ | varies | ❌ | ✅ | some | varies | ✅ |

**FBref can be extended** to other leagues manually (e.g., Eredivisie, Primeira Liga, Championship) but requires providing the correct league ID — not guaranteed to parse correctly.

---

## 10. DATA DEPTH FOR SCOUTING APPLICATION — MATRIX

| Data Category | FBref | WhoScored | Understat | Notes |
|---|---|---|---|---|
| Match schedule / fixtures | ✅ Full | ✅ Full | ✅ Full | All sources |
| Match results | ✅ | ✅ | ✅ | All sources |
| Team season aggregate stats | ✅ 11 stat types | ❌ | ❌ | FBref only |
| Player season aggregate stats | ✅ 12 stat types | ❌ | ✅ limited | FBref best |
| Expected goals (xG) | ✅ Opta xG | ❌ | ✅ Understat xG | Different models |
| Non-penalty xG (npxG) | ✅ | ❌ | ✅ | |
| Expected assists (xA / xAG) | ✅ xAG | ❌ | ✅ xA | |
| xG chain / buildup | ❌ | ❌ | ✅ | Understat unique |
| Shot events with location | ✅ (limited) | ✅ (x,y coords) | ✅ (normalized) | WhoScored most complete |
| Shot-creating actions | ✅ (SCA, GCA) | ✅ (via events) | ❌ | |
| Full event stream (passes, tackles, etc.) | ❌ | ✅ Full Opta | ❌ | WhoScored only |
| Pitch coordinates per event | ❌ | ✅ (x,y 0-100) | ✅ shots only | |
| Player progressive actions | ✅ (PrgC, PrgP, PrgR) | ✅ (via events) | ❌ | |
| Defensive stats (tackles, pressures, etc.) | ✅ Aggregate | ✅ Per event | ❌ | |
| Pressing (PPDA) | ❌ | ❌ | ✅ | Understat unique |
| Player lineup per match | ✅ | ✅ | ❌ | |
| Minute-level substitutions | ✅ events | ✅ schedule | ❌ | |
| Injured/suspended players | ❌ | ✅ | ❌ | WhoScored unique |
| Player position details | ✅ aggregate | ✅ per game | ✅ | |
| Goalkeeper advanced stats | ✅ (keeper_adv) | ✅ (via events) | ❌ | FBref unique aggregate |
| SPADL action format | ❌ | ✅ | ❌ | For ML models |
| Atomic-SPADL | ❌ | ✅ | ❌ | For ML models |
| VAEP / xT compatible | ❌ | ✅ (OptaLoader) | ❌ | |
| Team ELO rating | ❌ | ❌ | ❌ | ClubElo only |
| EA Sports player attributes | ❌ | ❌ | ❌ | SoFIFA only |
| Betting odds | ❌ | ❌ | ❌ | MatchHistory only |
| Historical depth | ~2000+ | ~2010+ | 2014/15+ | FBref deepest |

---

## 11. RECOMMENDED ARCHITECTURE FOR SCOUTING APPLICATION

Based on the data available, the recommended multi-source extraction strategy is:

### Primary Sources

**FBref** → Season-level and match-level aggregated stats for all players and teams. Use `'Big 5 European Leagues Combined'` for efficiency. Key stat types for scouting: `standard`, `shooting`, `passing`, `goal_shot_creation`, `defense`, `possession`, `misc`, `keeper_adv`.

**WhoScored** → Full Opta event stream per match for spatial analysis, pressing metrics, and ML feature generation. Use `output_fmt='spadl'` or `'loader'` for socceraction integration. This requires Chrome + tolerates blocks well with proxy rotation.

**Understat** → Supplement with xG_chain and xG_buildup metrics, per-shot location data, and PPDA pressing metric.

### Secondary Sources

**SoFIFA** → Player attribute profiles as proxy scouting data for players in leagues with sparse statistical coverage.

**ClubElo** → Opponent/team strength normalization for contextualizing player stats.

**ESPN** → Additional lineup and matchsheet data for leagues not covered by FBref.

### Sample Code Architecture
```python
import soccerdata as sd

# ---- FBref: Aggregate player stats ----
fbref = sd.FBref(
    leagues='ENG-Premier League',
    seasons=['2223', '2324'],
    proxy=['http://proxy1:8080', 'http://proxy2:8080']
)
player_stats = fbref.read_player_season_stats(stat_type='standard')
shot_stats = fbref.read_player_season_stats(stat_type='shooting')
defense_stats = fbref.read_player_season_stats(stat_type='defense')
passing_stats = fbref.read_player_season_stats(stat_type='passing')

# ---- WhoScored: Event stream ----
ws = sd.WhoScored(
    leagues='ENG-Premier League',
    seasons=['2223'],
    headless=False,  # Headed mode more reliable
    proxy=['http://proxy1:8080']
)
schedule = ws.read_schedule()
# Get SPADL events for all games
loader = ws.read_events(output_fmt='loader')

# ---- Understat: xG metrics ----
understat = sd.Understat(
    leagues='ENG-Premier League',
    seasons='2022/2023'
)
player_xg = understat.read_player_season_stats()  # xG_chain, xG_buildup
shots = understat.read_shot_events()  # shot locations + xG

# ---- SoFIFA: Player attributes ----
sofifa = sd.SoFIFA(leagues='ENG-Premier League', versions='latest')
player_attrs = sofifa.read_player_ratings()

# ---- ClubElo: Team strength ----
elo = sd.ClubElo()
team_strength = elo.read_by_date()
```

---

## 12. LEGAL & ETHICAL CONSIDERATIONS

The library's own README explicitly states:

> *"Please use this web scraping tool responsibly and in compliance with the terms of service of the websites you intend to scrape. The software is provided as-is, without any warranty or guarantees of any kind. The developers disclaim any responsibility for misuse, legal consequences, or damages resulting from its use."*

**Important Notes:**
- FBref, WhoScored, and Sofascore's Terms of Service generally **prohibit automated scraping for commercial purposes**
- Data from WhoScored is Opta-licensed data; commercial use of scraped Opta data is legally grey/risky
- For a **commercial professional scouting application**, you should evaluate licensing data directly from Opta, StatsBomb, or other data providers
- For **internal/research/personal** use, the library is widely used in academia and open analytics

---

## 13. STABILITY & MAINTENANCE ASSESSMENT

| Factor | Assessment |
|---|---|
| Active maintenance | Yes — v1.8.8 released Jan 2026 |
| Release frequency | Regular (multiple per year) |
| Open issues | 28 open (some are active breakages) |
| Dependency on website structure | High — any site redesign breaks scrapers |
| Production stability | **Medium–Low** — not suitable as sole data pipeline for production app without monitoring |
| Community | Active (175 closed issues, 17 open PRs) |
| Python version support | 3.9–3.13 |
| License | Apache-2.0 (permissive) |

---

## 14. FINAL VERDICT FOR PROFESSIONAL SCOUTING APP

### CAN this library be used?
**Yes, with important caveats.**

### What it does very well:
- **FBref** provides the most comprehensive free aggregate stats anywhere — this is professional-grade Opta data covering 12 stat categories per player per season
- **WhoScored + socceraction** provides full Opta event stream with coordinates, enabling spatial analysis and VAEP/xT computation — this is commercial-quality data
- **Understat** fills gaps with unique xG model variants (xG_chain, xG_buildup) and PPDA
- **Unified API** across sources with matching team/game identifiers makes multi-source pipelines practical
- **Local caching** makes re-runs fast and reduces scraping load

### What it does poorly:
- **Fragility:** Scrapers break when sites change. Not suitable as a set-and-forget pipeline without alerting/monitoring
- **WhoScored is very hard to scrape at scale** — Incapsula blocks are frequent and require Chrome + proxies
- **Coverage is primarily Top 5 European leagues** — limited for worldwide or lower-division scouting
- **Sofascore integration is underdeveloped** — only schedule and league table
- **No official API** — all data is scraped without authorization, creating legal risk for commercial products
- **Active bugs** in FBref schedule, SoFIFA player ratings, MatchHistory, and WhoScored date parsing

### Recommendation:
- **For a research/prototype scouting application:** Excellent choice — combine FBref + WhoScored + Understat for deep multi-dimensional player profiling
- **For a production commercial scouting application:** Use soccerdata as a development/prototyping tool, then migrate to licensed data from **StatsBomb**, **Opta/Stats Perform**, **Wyscout**, or **InStat** for production
- **Monitoring requirement:** Implement scraper health checks (smoke tests per source) on a weekly basis, as breakages are frequent