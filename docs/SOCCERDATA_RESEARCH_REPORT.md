# SoccerData Research Report for Data-ETL-Pipeline

Date: 2026-02-11  
Scope: Evaluate `soccerdata` for integration into this ETL application before implementation work.

## 1) Executive Summary

SoccerData is a mature, actively maintained scraping framework that can materially improve your scouting backbone, especially for:

- Event-first ingestion (`read_events`, `read_shot_events`/`read_shotmap`, `read_lineup`) from FotMob/WhoScored/FBref.
- Fast expansion of historical and multi-league coverage.
- Standardized IDs/column naming across heterogeneous sources.

Primary adoption blockers in your current environment:

- Your runtime is Python `3.9.6`, while SoccerData `1.8.8` requires `Python >=3.10`.
- Your pinned `pandas==2.1.3`, while SoccerData `1.8.8` requires `pandas>=2.3.0`.

Recommendation:

- Proceed with integration, but only after a controlled platform upgrade (Python + pandas), then start with a narrow POC around event ingestion from FotMob and/or WhoScored into `match_events`, `shot_events`, `match_lineups`.

## 2) What SoccerData Is (and Is Not)

SoccerData is a Python package that normalizes football data extraction from multiple public websites into DataFrames. It is not a guaranteed-stability official API product; it is a scraping toolkit with expected fragility when source websites change.

Key implications:

- Strong acceleration for development velocity and source breadth.
- Ongoing maintenance burden still exists (break/fix cycles due to upstream site changes).

## 3) Version, Maturity, and Governance Snapshot

- Latest package release: `1.8.8` (uploaded `2026-01-16` on PyPI).
- Release cadence: frequent releases through 2024-2026 (active maintenance signal).
- License: Apache-2.0.
- Repo signals (GitHub snapshot in indexed metadata): ~930 stars, active issues/PRs.

Practical assessment:

- Mature enough for production ETL use if wrapped with your own quality gates/retries/fallbacks.
- Not safe to use as an unguarded direct dependency in critical scheduled jobs.

## 4) Source Coverage and Method Surface

From current docs + source code reviewed, core scrapers include:

- `ClubElo`
- `ESPN`
- `FBref`
- `FotMob`
- `MatchHistoryData` (football-data.co.uk style history tables)
- `SoFIFA`
- `Sofascore`
- `Understat`
- `WhoScored`

### 4.1 Method Coverage (high-value for your stack)

| Source | Key methods relevant to scouting ETL |
|---|---|
| FBref | `read_schedule`, `read_team_season_stats`, `read_player_season_stats`, `read_player_match_stats`, `read_team_match_stats`, `read_lineup`, `read_events`, `read_shot_events` |
| FotMob | `read_leagues`, `read_seasons`, `read_schedule`, `read_team_match_stats`, `read_player_match_stats`, `read_lineup`, `read_events`, `read_shotmap`, `read_player_season_stats` |
| Understat | `read_schedule`, `read_team_match_stats`, `read_player_match_stats`, `read_shot_events` |
| WhoScored | `read_schedule`, `read_lineup`, `read_team_match_stats`, `read_player_match_stats`, `read_events` |
| ESPN | `read_schedule`, `read_lineup` |
| ClubElo | `read_by_date`, `read_team_history` |
| SoFIFA | `read_versions`, `read_teams`, `read_players`, `read_team_ratings`, `read_player_ratings` |
| MatchHistoryData | `read_games`, `read_missing_games` |

### 4.2 Output Contract Strength

SoccerData’s design goal is cross-source interoperability:

- Common tabular outputs (`pandas`, with options for `polars`/`raw` in base reader options).
- Consistent entity columns/IDs where possible.
- Optional `prefixed_ids` to avoid cross-source identifier collisions.

This is directly aligned with your identity-resolution roadmap.

## 5) Operational Characteristics (Important for Scheduler Reliability)

### 5.1 Cache and storage behavior

- Default cache root: `~/soccerdata/` (with per-source subfolders).
- Supports `no_cache` and `no_store` modes.
- Per-source cache freshness defaults vary (examples observed in code):  
  `FBref` ~30 days, `FotMob` ~1 day, `Understat` ~1 week, `WhoScored` ~30 days.

### 5.2 Network controls

- Built-in retry/timeout handling.
- Per-source `rate_limit` defaults exposed in constructors.
- Proxy support includes:
  - `proxy="tor"` convenience path.
  - explicit proxy dict/list/callable.
  - paid anti-bot mode via `proxy="zenrows"` + `ZENROWS_API_KEY`.

### 5.3 Stability caveats

The maintainers explicitly warn that upstream site changes can break scrapers. This is expected for all scraping frameworks and must be handled by your ETL with:

- strict input/data-contract validation,
- fallback logic,
- monitorable error budgets,
- alerting.

## 6) Fit-Gap vs Your Current ETL Architecture

## 6.1 Strong fit areas

1. Event-first pipeline acceleration

- Your priorities include operationalizing event tables.
- SoccerData already exposes event/shot/lineup interfaces for FotMob, FBref, WhoScored.
- This can reduce bespoke parser surface area and speed P1 delivery.

2. Understat mitigation path

- Your current Understat parser issue is due HTML/script pattern fragility.
- SoccerData already ships maintained Understat extraction logic, so you can outsource part of parser maintenance.

Important nuance:

- SoccerData’s Understat parser still depends on script extraction patterns, so it is not a hard guarantee against future breakage. It is a maintenance leverage gain, not a permanent fix.

3. Multi-source entity stitching

- `prefixed_ids` and normalized output conventions fit your planned identity-resolution layer.

4. Broader scouting dimensions

- ClubElo and SoFIFA add rating/market-style context useful for recruitment models.
- WhoScored/FBref event streams help derive possession-value style features.

## 6.2 Gaps or constraints

1. Runtime compatibility blockers (P0 blocker)

- Python upgrade needed (`3.9.6` -> `>=3.10`).
- pandas upgrade needed (`2.1.3` -> `>=2.3.0`).

2. Data rights/compliance

- SoccerData does not grant rights to underlying provider data.
- You still need source-level terms review for production usage.

3. Schema mismatch work remains

- Your current `match_events` schema is relatively coarse compared with richer event payloads from SoccerData.
- You will still need schema evolution (especially for pass/body-part/location/outcome/play-pattern dimensions).

4. No free replacement for quota-constrained API-Football enrichment

- SoccerData can reduce dependency in some domains, but does not replicate all API-Football endpoints/metadata guarantees.

## 7) Field-Level Mapping Opportunity for Your Tables

### 7.1 `match_events`

SoccerData event outputs (FotMob/WhoScored/FBref) include richer fields such as:

- `event_type`, `outcome`, `minute`/`second`, `period`,
- `player`, `team`,
- location (`x`, `y`, sometimes `end_x`, `end_y`),
- assist/pass context in some sources.

Action:

- Keep current table for coarse events.
- Add a richer event fact table (or extend current table) for scouting-grade analytics.

### 7.2 `shot_events`

SoccerData shot/event feeds can provide:

- x/y coordinates,
- xG-style probabilities (source-dependent),
- outcome/body-part/situation context.

Action:

- Directly map to existing `shot_events` with source-normalization rules.
- Preserve raw source columns in a bronze layer for traceability.

### 7.3 `match_lineups`

SoccerData lineup payloads include starter flags, positions, shirt number, captain flags (source-dependent).

Action:

- Existing `match_lineups` structure is close; mostly transformation and ID resolution work.

## 8) Risk Register (Project-Specific)

| Risk | Severity | Why it matters here | Mitigation |
|---|---|---|---|
| Python/pandas incompatibility | High | Blocks install/adoption | Upgrade runtime in isolated branch + compatibility test matrix |
| Scraper breakage from site changes | High | Could break scheduled jobs silently | Data quality gates + retry/fallback + alerting + canary runs |
| Source contract drift (`xa`/`xag`) | High | Existing inconsistency in your project | Create canonical metrics dictionary and strict column translator |
| Identity mismatches across sources | Medium-High | Already an issue in your backlog | Introduce mapping tables + confidence-scored resolver |
| Legal/compliance uncertainty | Medium | Production scraping risk | Provider ToS review + documented compliance boundaries |

## 9) Recommended Adoption Plan (Phased)

### Phase 0: Platform readiness (must happen first)

- Upgrade runtime to Python `>=3.10`.
- Upgrade pandas to `>=2.3`.
- Run full regression for current ETL + server endpoints.

Exit criteria:

- Existing jobs and dashboard remain green on upgraded stack.

### Phase 1: Controlled POC (event-first)

- Implement a new adapter module: `etl/sources/soccerdata_adapter.py`.
- Start with `sd.FotMob`:
  - `read_schedule`
  - `read_lineup`
  - `read_events`
  - `read_shotmap`
- Land into bronze raw tables/files plus transformed inserts into:
  - `match_events`
  - `match_lineups`
  - `shot_events`

Exit criteria:

- At least one full league-season backfill with >95% event completeness on finished matches.

### Phase 2: Reliability hardening

- Add pre-load schema checks and post-load DQ gates.
- Add canary job in scheduler (single league daily before broad run).
- Add source health dashboard metrics (success rate, null rates, row deltas).

Exit criteria:

- 2+ weeks stable runs with no silent failures.

### Phase 3: Coverage expansion

- Add `WhoScored.read_events` for richer event taxonomies.
- Evaluate `FBref.read_events` and `read_shot_events` for complementary history depth.
- Use ClubElo/SoFIFA selectively for scouting context features.

## 10) Explicit Recommendation for This Project

Adopt SoccerData as a strategic ingestion abstraction, not as a direct drop-in replacement for all current scrapers.

Priority order:

1. Unblock platform compatibility (Python/pandas).
2. Use SoccerData to operationalize event tables (your biggest near-term scouting value gain).
3. Keep your existing FotMob/API-Football pipelines in parallel during burn-in.
4. Only deprecate current scrapers after comparative quality benchmarks pass.

---

## Sources

- SoccerData docs home: https://soccerdata.readthedocs.io/
- Intro + features: https://soccerdata.readthedocs.io/en/latest/intro.html
- Data sources index: https://soccerdata.readthedocs.io/en/latest/datasources/index.html
- API reference index: https://soccerdata.readthedocs.io/en/latest/reference/index.html
- FBref reference: https://soccerdata.readthedocs.io/en/latest/reference/fbref.html
- FotMob reference: https://soccerdata.readthedocs.io/en/latest/reference/fotmob.html
- Understat reference: https://soccerdata.readthedocs.io/en/latest/reference/understat.html
- WhoScored reference: https://soccerdata.readthedocs.io/en/latest/reference/whoscored.html
- FAQ: https://soccerdata.readthedocs.io/en/latest/faq.html
- Proxy how-to: https://soccerdata.readthedocs.io/en/latest/howto/proxy.html
- Changelog: https://soccerdata.readthedocs.io/en/latest/changelog.html
- PyPI project: https://pypi.org/project/soccerdata/
- PyPI JSON metadata: https://pypi.org/pypi/soccerdata/json
- GitHub repository: https://github.com/probberechts/soccerdata
- License (Apache-2.0): https://raw.githubusercontent.com/probberechts/soccerdata/master/LICENSE.rst
- Source files reviewed (raw):
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/fbref.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/fotmob.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/understat.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/whoscored.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/espn.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/clubelo.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/sofifa.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/sofascore.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/matchhistorydata.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/data_parser.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/_common.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/soccerdata/_config.py
  - https://raw.githubusercontent.com/probberechts/soccerdata/master/pyproject.toml
