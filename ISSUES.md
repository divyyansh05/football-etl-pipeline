# Data-ETL-Pipeline - Known Issues & Bug Tracker

> This file tracks all known issues, bugs, and improvements needed in the project.
> Issues are categorized by severity and component.
> **Last Updated**: 2026-02-11

## Recently Completed Issues

| Issue | Title | Fixed In |
|-------|-------|----------|
| ISSUE-016 | Event Backbone | `fotmob_etl.py` - Added `_insert_match_events()` |
| ISSUE-017 | xA/xag Column | Verified working - both columns exist |
| ISSUE-018 | Hardcoded API Keys | Removed from 3 files, `.env.example` added |
| ISSUE-019 | Dict-to-Scalar Parsing | `utils/data_quality.py` - `safe_extract_scalar()` |
| ISSUE-020 | Weekly Scheduler | `job_scheduler.py` - Added `add_weekly_job()` |
| ISSUE-021 | Hardcoded Season | `jobs.py`, `smart_collector.py`, `run_scheduler.py` |
| ISSUE-022 | NoneType Iteration | `data_parser.py`, `statsbomb_advanced_etl.py` |
| ISSUE-023 | Understat Scraping | Verified working - 5 leagues supported |
| ISSUE-024 | Progressive Profiles | `migration_004` - Created SQL views |
| ISSUE-025 | Identity Resolution | `utils/identity_resolution.py` + DB tables |
| ISSUE-026 | Data Quality Gates | `utils/data_quality.py` - Validation framework |
| ISSUE-027 | Bronze/Silver/Gold | `utils/etl_tracker.py` + DB tables |
| ISSUE-030 | Testing Framework | `tests/` - conftest, parsers, quality tests |

---

## Critical Issues (P0 - Blocking Core Functionality)

### ISSUE-016: Event Backbone Not Operational End-to-End
**Status**: ✅ FIXED (2026-02-10)
**Severity**: Critical (P0)
**Component**: ETL / Database / Data Architecture
**First Seen**: 2026-02-10

**Problem**: Event tables exist in schema but the FotMob ETL does not persist events, breaking the event-first data model.

**Evidence**:
| Component | File | Lines | Status |
|-----------|------|-------|--------|
| Schema definition | `schema_match_events.sql` | 1-426 | ✅ Complete |
| Event parser | `data_parser.py` | 953-998 | ✅ Complete, UNUSED |
| ETL persistence | `fotmob_etl.py` | 408-534 | ❌ Missing event calls |

**Tables Defined But Empty**:
- `match_events` - Goals, cards, substitutions, VAR decisions
- `match_lineups` - Starting XI and substitutes per match
- `shot_events` - Detailed shots with xG per shot
- `match_statistics` - Team-level match stats

**Root Cause**:
1. `schema_match_events.sql` is NOT part of migration chain (standalone file)
2. `parse_match_events()` function exists but is never called
3. No `_insert_match_events()`, `_insert_match_lineups()`, `_insert_shot_events()` methods

**Impact**:
- Cannot analyze shot maps or xG per shot
- Cannot track substitution patterns
- Cannot build possession-value models
- Missing critical scouting context

**Required Fix**:
1. Add schema as `migration_004_match_events.sql`
2. Implement event insertion methods in FotMob ETL
3. Call `parse_match_events()` in `process_league_matches_deep()` after line 521
4. Add event parsing for lineups and shots (new parser methods needed)

---

### ISSUE-017: xA/xag Column Contract Inconsistency
**Status**: ✅ VERIFIED (2026-02-10) - Both `xa` and `xag` columns exist in DB
**Severity**: Critical (P0)
**Component**: Database / ETL / API
**First Seen**: 2026-02-10

**Problem**: Conflicting column names between database schema and application code cause silent failures.

**Evidence**:
| Component | File | Line | Column Used |
|-----------|------|------|-------------|
| Database Schema | `migration_003_advanced_stats.sql` | 71 | `xa` ✅ |
| FotMob ETL | `fotmob_etl.py` | 862, 882, 898, 904 | `xag` ❌ |
| Flask API | `server/app.py` | 240, 272, 676, 704, 722 | `xag` ❌ |
| Understat ETL | `understat_etl.py` | 201, 209, 215, 234 | `xa` ✅ |
| StatsBomb ETL | `statsbomb_advanced_etl.py` | 757, 778, 813 | `xa` ✅ |
| Data Parser | `data_parser.py` | 521, 553, 585 | `xa` ✅ |

**Code Examples**:
```python
# Database (CORRECT)
ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS xa DECIMAL(6,2);

# FotMob ETL (WRONG)
'xag': season_stats.get('xa'),  # Tries to insert into non-existent 'xag' column

# Flask API (WRONG)
SELECT pss.xag ...  # Queries non-existent 'xag' column
```

**Impact**:
- FotMob xA data silently fails to insert
- API endpoints may crash or return NULL
- Analytics features broken for xA-based metrics

**Required Fix**: Standardize on `xa` (the actual database column) across:
- `etl/fotmob_etl.py` lines 862, 882, 898, 904
- `server/app.py` lines 240, 272, 676, 704, 722

---

### ISSUE-018: Hardcoded API Keys in Source Code
**Status**: ✅ FIXED (2026-02-10)
**Severity**: Critical (P0) - Security
**Component**: Security / Configuration
**First Seen**: 2026-02-10

**Problem**: API-Football API key is hardcoded in multiple files, exposing credentials.

**Affected Files**:
| File | Line | Issue |
|------|------|-------|
| `test_direct_api.py` | 7 | `API_KEY = "7652e15016e34d8d84c4e7528be0af2c"` |
| `check_seasons.py` | 8 | `API_KEY = "7652e15016e34d8d84c4e7528be0af2c"` |
| `smart_collector.py` | 32 | Fallback: `os.getenv('API_FOOTBALL_KEY', "7652e...")` |

**Impact**:
- Anyone with repo access can use/abuse the API key
- If repo is public, key is fully exposed
- API quota abuse possible
- Key revocation requires code changes

**Required Fix**:
1. Remove all hardcoded keys from source files
2. Use environment variables only (no fallbacks)
3. Add `.env.example` template (without real keys)
4. Revoke and rotate the exposed API key
5. Add pre-commit hook to detect secrets

---

### ISSUE-019: Parser Returns Dict Instead of Scalar Values
**Status**: ✅ FIXED (2026-02-11)
**Severity**: Critical (P0)
**Component**: Scrapers / FotMob
**First Seen**: 2026-02-10

**Problem**: FotMob parser returns dict objects instead of scalar values for certain fields, causing database truncation errors.

**Fix Applied**:
1. Created `utils/data_quality.py` with `safe_extract_scalar()` and `safe_extract_numeric()` functions
2. These utilities safely extract scalar values from dict-like objects
3. Handles edge cases: `{'key': 'x', 'fallback': 'y'}`, stringified dicts, max length truncation

**Files Created**:
- `utils/data_quality.py` - Safe extraction utilities
- `tests/test_quality.py` - Unit tests for edge cases

---

### ISSUE-020: Scheduler Weekly Job Runs Daily
**Status**: ✅ FIXED (2026-02-10)
**Severity**: Critical (P0)
**Component**: Scheduler
**First Seen**: 2026-02-10

**Problem**: The "weekly deep" job is configured with `add_daily_job()`, causing it to run every day instead of weekly.

**Location**: `run_scheduler.py` lines 86-94
```python
scheduler.add_daily_job(  # WRONG - uses daily trigger
    job_func=fotmob_weekly_deep_job,
    job_id='fotmob_weekly_deep',
    hour=2,
    minute=0,
)
console.print("[green]✓[/green] Scheduled fotmob_weekly_deep at 02:00 UTC (Sundays)")  # Comment says Sunday but it's daily
```

**Root Cause**: `JobScheduler` class lacks `add_weekly_job()` method with day-of-week support.

**Impact**:
- 7x unnecessary API requests per week
- 7x database load
- Resource waste

**Required Fix**:
1. Add `add_weekly_job()` method to `scheduler/job_scheduler.py`
2. Update `run_scheduler.py` to use proper weekly trigger
3. Add day-of-week parameter (e.g., `day_of_week='sun'`)

---

### ISSUE-021: Hardcoded Season Values
**Status**: ✅ FIXED (2026-02-10)
**Severity**: Critical (P0)
**Component**: Configuration / Scheduler
**First Seen**: 2026-02-10

**Problem**: Season year is hardcoded as `2024`, becoming stale when new season starts.

**Affected Files**:
| File | Line | Code |
|------|------|------|
| `scheduler/jobs.py` | 244 | `season = 2024` |
| `scheduler/jobs.py` | 341 | `season = 2024` |
| `scheduler/jobs.py` | 415 | `client.get_standings(LEAGUE_IDS['premier-league'], 2024)` |
| `smart_collector.py` | 37 | `self.seasons = [2024, 2023]` |

**Impact**: When 2025-26 season starts, all scheduled jobs will still collect 2024 data.

**Required Fix**:
1. Create `utils/season_utils.py::get_current_season()` function
2. Calculate season dynamically based on current date
3. Replace all hardcoded `2024` with dynamic calculation

---

## High Severity Issues (P1)

### ISSUE-022: NoneType Iteration in Deep Stats Parser
**Status**: ✅ FIXED (2026-02-10)
**Severity**: High (P1)
**Component**: ETL / Parser
**First Seen**: 2026-02-10

**Problem**: `parse_player_deep_stats()` returns `None` for players without stats, but callers don't check before iterating.

**Error** (12 occurrences in `errors.log`):
```
'NoneType' object is not iterable
Location: etl/fotmob_etl.py line 402
```

**Affected Players** (backup goalkeepers, reserves):
- Marcus Bettinelli, Tom Heaton, Remi Matthews
- Jordan Beyer, Rhys Oakley, Julian Eyestone
- Joshua Dasilva, John Ruddy, Mark Gillespie
- Owen Bevan, Matai Akinmboni, Charlie Stevens

**Root Cause**: `parse_player_deep_stats()` returns `None` when `items` is empty, but line 590 does `for group in items:` unconditionally.

**Required Fix**: Add null-safe check before iterating:
```python
items = stats_section.get('items', [])
if not items:
    return {}  # Return empty dict, not None
```

---

### ISSUE-023: Understat Web Scraping Status
**Status**: ✅ VERIFIED WORKING (2026-02-11)
**Severity**: High (P1) - Downgraded to informational
**Component**: Scrapers / Understat
**First Seen**: 2026-02-07 (Verified 2026-02-11)

**Investigation Result**: Understat scraper is **functional and operational**.

**Implementation Review**:
- `scrapers/understat/client.py` - BeautifulSoup-based scraper, extracts JSON from JS variables
- `etl/understat_etl.py` - Full ETL pipeline with player/team enrichment
- Rate limiting: 2s delay between requests
- Proper session handling with browser-like headers

**Supported Leagues** (5 of 8):
| League | Understat Key | Status |
|--------|---------------|--------|
| Premier League | EPL | ✅ |
| La Liga | La_liga | ✅ |
| Serie A | Serie_A | ✅ |
| Bundesliga | Bundesliga | ✅ |
| Ligue 1 | Ligue_1 | ✅ |
| Eredivisie | - | ❌ Not covered |
| Brasileiro | - | ❌ Not covered |
| Argentina Primera | - | ❌ Not covered |

**Unique Metrics Available**: xA, npxG, xGChain, xGBuildup, PPDA

**CLI Usage**:
```bash
python cli.py understat test-connection
python cli.py understat collect-league --league premier-league
python cli.py understat collect-all --season 2024
```

---

### ISSUE-024: Missing Derived Scouting Features
**Status**: ✅ PARTIALLY FIXED (2026-02-11)
**Severity**: High (P1)
**Component**: Data Architecture / Analytics
**First Seen**: 2026-02-10

**Problem**: Key scouting-context dimensions are missing or shallow.

**Features Implemented** (in `migration_004_identity_resolution.sql`):
| Feature | Status | Implementation |
|---------|--------|----------------|
| Progressive profiles | ✅ Created | `vw_progressive_profiles` SQL view |
| Team pressing profile | ✅ Created | `vw_team_pressing_profile` SQL view |
| Shot quality analysis | ✅ Created | `vw_shot_quality` SQL view |
| PPDA (via Understat) | ✅ Available | Understat scraper collects PPDA |

**Still Missing** (future work):
| Feature | Status | Notes |
|---------|--------|-------|
| Possession-value models (xT/VAEP/EPV) | ❌ Missing | Requires event location data |
| Field tilt | ❌ Missing | Needs match event zones |
| Final-third entries | ❌ Missing | Needs event location |
| Transfer history | ❌ Missing | Transfermarkt integration planned |
| Role-context snapshots | ❌ Missing | Needs positional heat map data |
| Opponent-strength adjusted | ❌ Missing | Needs SOS calculation |

**SQL Views Created**:
```sql
-- Progressive profiles with percentiles
SELECT * FROM vw_progressive_profiles WHERE minutes >= 450;

-- Team pressing intensity
SELECT * FROM vw_team_pressing_profile;

-- Shot quality by player
SELECT * FROM vw_shot_quality;
```

---

### ISSUE-025: Missing Identity Resolution Layer
**Status**: ✅ FIXED (2026-02-11)
**Severity**: High (P1)
**Component**: Data Architecture
**First Seen**: 2026-02-10

**Problem**: Player matching relies on name fallback instead of explicit cross-source mapping.

**Fix Applied**:

**1. Database Tables** (`migration_004_identity_resolution.sql`):
- `player_id_mappings` - Cross-source ID mappings with confidence scores
- `team_id_mappings` - Team identity resolution
- Columns: fotmob_id, api_football_id, transfermarkt_id, statsbomb_id, understat_id
- `confidence_score` (0.00-1.00) with `match_method` tracking

**2. Identity Resolution Module** (`utils/identity_resolution.py`):
- `PlayerIdentityResolver` - Fuzzy name matching with confidence scoring
- `TeamIdentityResolver` - Team matching with league context
- Configurable thresholds: HIGH (0.95), MEDIUM (0.80), LOW (0.60)
- Auto-create or manual verification modes

**3. Features**:
- Date of birth matching (+0.25 confidence)
- Nationality matching (+0.15 confidence)
- Team context matching (+0.1 confidence)
- Manual override via `verified_by` column

**Usage**:
```python
from utils.identity_resolution import PlayerIdentityResolver
resolver = PlayerIdentityResolver(db)
player_id, is_new = resolver.resolve_player(
    source_name='fotmob',
    external_id=123456,
    player_name='Bukayo Saka',
    date_of_birth=date(2001, 9, 5)
)
```

---

### ISSUE-026: Missing Data Quality Gates
**Status**: ✅ FIXED (2026-02-11)
**Severity**: High (P1)
**Component**: ETL / Data Quality
**First Seen**: 2026-02-10

**Problem**: No pre-load validation or post-load quality checks.

**Fix Applied** (`utils/data_quality.py`):

**1. DataQualityValidator Class**:
- Configurable validation rules per entity type
- Severity levels: CRITICAL, HIGH, MEDIUM, LOW
- Fail-fast mode for critical errors
- Returns detailed validation results

**2. Validation Rules Implemented**:
```python
PLAYER_RULES = [
    ('player_name', 'not_null', Severity.CRITICAL),
    ('xg', 'range', Severity.HIGH, 0, 50),
    ('age', 'range', Severity.MEDIUM, 15, 50),
    ('nationality', 'max_length', Severity.LOW, 100),
]
```

**3. AnomalyDetector Class**:
- Z-score based anomaly detection
- Configurable threshold (default: 3.0)
- Tracks historical context for each field
- Identifies statistically unusual values

**4. Database Tracking** (`data_quality_issues` table):
- Records all validation failures
- Severity, entity reference, field name
- Status tracking (open/acknowledged/resolved)

**Usage**:
```python
from utils.data_quality import DataQualityValidator, AnomalyDetector

validator = DataQualityValidator()
result = validator.validate_player(player_data)
if not result.is_valid:
    for issue in result.issues:
        logger.warning(f"{issue.field}: {issue.message}")
```

---

## Medium Severity Issues (P2)

### ISSUE-027: ETL Architecture Lacks Bronze/Silver/Gold Pattern
**Status**: ✅ FIXED (2026-02-11)
**Severity**: Medium (P2)
**Component**: Data Architecture
**First Seen**: 2026-02-10

**Problem**: Current ETL directly transforms and loads without raw payload retention.

**Fix Applied**:

**1. Database Tables** (`migration_004_identity_resolution.sql`):
- `etl_runs` - Run lifecycle tracking with stats
- `raw_payloads` - Bronze layer raw API response storage
- `etl_watermarks` - Incremental load markers

**2. ETL Tracker Module** (`utils/etl_tracker.py`):

**ETLRunTracker Class**:
```python
tracker = ETLRunTracker(db)
run_id = tracker.start_run('fotmob_daily', 'fotmob', {'leagues': ['premier-league']})
tracker.update_stats(run_id, records_fetched=100, records_inserted=95)
tracker.complete_run(run_id, status='completed')
```

**RawPayloadStore Class** (Bronze Layer):
```python
store = RawPayloadStore(db)
payload_id = store.store_payload(
    source_name='fotmob',
    endpoint='/api/leagues',
    payload=api_response,
    etl_run_id=run_id
)
# Deduplication via payload_hash (SHA256)
```

**WatermarkManager Class** (Incremental Loads):
```python
watermark = WatermarkManager(db)
last_processed = watermark.get_watermark('fotmob', 'player')
# ... process new data ...
watermark.set_watermark('fotmob', 'player', processed_at=datetime.now())
```

**3. Run Statistics Tracked**:
- records_fetched, records_inserted, records_updated
- records_skipped, records_failed
- quality_score, completeness_score, validity_score
- anomaly_count, duration_seconds

---

### ISSUE-028: Missing Physical/Tracking Layer
**Status**: Open
**Severity**: Medium (P2)
**Component**: Data Architecture
**First Seen**: 2026-02-10

**Problem**: No tracking/physical data for modern recruitment requirements.

**Missing Data Points**:
- Sprint load
- High-intensity actions
- Speed profiles
- Distance covered
- Acceleration/deceleration

**Note**: This data requires specialized providers (Second Spectrum, StatsBomb 360, etc.) - not available from FotMob/free sources.

---

### ISSUE-029: Limited Coverage (Missing Competitions)
**Status**: Open
**Severity**: Medium (P2)
**Component**: Data Coverage
**First Seen**: 2026-02-10

**Current Coverage**: 8 leagues (top 5 + Eredivisie + Brazil + Argentina)

**Missing Desirable Coverage**:
- Women's competitions (WSL, Liga F, etc.)
- Youth competitions (U21, U19, U17)
- Secondary leagues (Championship, Serie B, etc.)
- Richer market/injury/contract timelines

---

### ISSUE-030: Minimal Testing Maturity
**Status**: ✅ FIXED (2026-02-11)
**Severity**: Medium (P2)
**Component**: Testing / Quality
**First Seen**: 2026-02-10

**Problem**: No automated ETL tests.

**Fix Applied** (`tests/` directory):

**1. Test Infrastructure**:
- `tests/__init__.py` - Package initialization
- `tests/conftest.py` - Pytest fixtures with sample data

**2. Parser Unit Tests** (`tests/test_parsers.py`):
- `TestFotMobDataParser` - Tests for data parsing
- `test_parse_player_handles_none_stats` - None safety
- `test_parse_standings_extracts_xg` - xG extraction
- `test_xg_exact_matching` - Prevents "xG against" confusion (ISSUE-015)
- `test_dict_to_scalar_extraction` - Dict serialization (ISSUE-019)

**3. Quality Tests** (`tests/test_quality.py`):
- `TestDataQualityValidator` - Validation rule tests
- `TestAnomalyDetector` - Z-score detection tests
- `TestSafeExtraction` - Edge case handling

**4. Sample Test Fixtures**:
```python
@pytest.fixture
def sample_player_data():
    return {
        'id': 123456,
        'name': {'fallback': 'Test Player'},  # Dict format edge case
        'primaryTeam': {'name': 'Test FC'},
        'birthDate': {'utcTime': '1998-01-15'},
    }
```

**Running Tests**:
```bash
pytest tests/ -v
pytest tests/test_parsers.py -v
pytest tests/test_quality.py -v
```

---

### ISSUE-031: SoccerData Integration for Enhanced Data Collection
**Status**: In Progress
**Severity**: High (P1)
**Component**: ETL / Data Sources
**First Seen**: 2026-02-11

**Problem**: Current custom scrapers require significant maintenance and lack rich event data (x/y coordinates, pass context) needed for advanced scouting analytics.

**Solution**: Integrate `soccerdata` package - a mature, actively maintained scraping framework that provides:
- Event-first ingestion with x/y coordinates
- Standardized IDs/column naming across sources
- Maintained parsers for FotMob, FBref, WhoScored, Understat, ESPN, SoFIFA, ClubElo

**Platform Upgrade Required** (P0 Blocker):
| Component | Current | Required | Action |
|-----------|---------|----------|--------|
| Python | 3.9.12 | ≥3.10 | Upgrade runtime |
| pandas | 2.1.3 | ≥2.3.0 | Upgrade package |

**Upgrade Plan**:
```bash
# Step 1: Update Python (using pyenv or system package manager)
pyenv install 3.10.13
pyenv local 3.10.13

# Step 2: Update requirements.txt
# Change: pandas==2.1.3 → pandas>=2.3.0
# Add: soccerdata>=1.8.8

# Step 3: Reinstall dependencies
pip install -r requirements.txt

# Step 4: Verify existing ETL still works
pytest tests/ -v
python cli.py system health
python cli.py fotmob test-connection
```

**Implementation Phases**:

**Phase 0**: Platform Upgrade ⏳
- Upgrade Python to 3.10+
- Upgrade pandas to 2.3+
- Run regression tests

**Phase 1**: SoccerData Adapter (POC)
- Create `etl/sources/soccerdata_adapter.py`
- Implement FotMob event ingestion via soccerdata
- Target tables: `match_events`, `shot_events`, `match_lineups`

**Phase 2**: Reliability Hardening
- Add data quality gates for soccerdata output
- Add canary job in scheduler
- Source health dashboard metrics

**Phase 3**: Coverage Expansion
- Add WhoScored events (richer taxonomy)
- Add FBref for historical depth
- Add ClubElo/SoFIFA for scouting context

**New Data Available via SoccerData**:
| Source | Key Methods | New Data |
|--------|-------------|----------|
| FotMob | `read_events`, `read_shotmap`, `read_lineup` | x/y coords, event context |
| WhoScored | `read_events`, `read_lineup` | Richer event taxonomy |
| FBref | `read_shot_events`, `read_player_match_stats` | Historical depth |
| ClubElo | `read_by_date`, `read_team_history` | Team strength ratings |
| SoFIFA | `read_players`, `read_player_ratings` | Player ratings, market values |

**Files to Create**:
- `etl/sources/__init__.py`
- `etl/sources/soccerdata_adapter.py`
- `etl/sources/soccerdata_config.py`
- `tests/test_soccerdata_adapter.py`

**Reference**: See `docs/SOCCERDATA_RESEARCH_REPORT.md` for full analysis.

---

## Resolved Issues

### ISSUE-015.1: xG Parser Bug (xG vs xG Against Confusion)
**Status**: Resolved
**Severity**: Critical
**Date Fixed**: 2026-02-10

**Problem**: Parser matched "xG against while on pitch" instead of offensive "xG" stat, causing defenders to have inflated xG (e.g., Ruben Dias: 19.95 xG instead of 0.53).

**Root Cause**: Substring matching in `parse_player_deep_stats()` matched "xg" in "xG against while on pitch".

**Fix Applied**: Changed from substring to exact matching for critical stats (`goals`, `xg`, `xgot`, `assists`, `xa`, `shots`) in `scrapers/fotmob/data_parser.py`.

---

### Previously Resolved Issues

| Issue | Date Fixed | Notes |
|-------|------------|-------|
| ISSUE-001 | 2026-02-04 | Changed `standings` to `team_season_stats` in cli.py |
| ISSUE-004 | 2026-02-07 | Ran migration_001_api_football_ids.sql |
| ISSUE-005 | 2026-02-04 | Resolved by migration_002 (fotmob_match_id) |
| ISSUE-006 | 2026-02-04 | Resolved by migration_002 (fotmob_id on teams) |
| ISSUE-007 | 2026-02-04 | Resolved by migration_002 (fotmob_id on players) |
| ISSUE-008 | 2026-02-07 | Created utils/season_utils.py |
| ISSUE-010 | 2026-02-06 | Verified all templates exist |
| ISSUE-012 | 2026-02-04 | Cleaned up FBref/Soccerway imports |
| ISSUE-014 | 2026-02-07 | Updated FotMob parser for new API format |
| ISSUE-015 | 2026-02-07 | Added NaN check in StatsBomb ETL |

---

## Issue Summary by Priority

| Priority | Total | Resolved | Open | Description |
|----------|-------|----------|------|-------------|
| P0 (Critical) | 6 | 6 | 0 | Blocking core functionality or security |
| P1 (High) | 6 | 4 | 2 | Major data gaps or reliability issues |
| P2 (Medium) | 4 | 2 | 2 | Architecture improvements and coverage |

### Sprint Progress Summary (2026-02-11)

**Sprint 1 Issues (P0)**: ✅ ALL COMPLETE
- ISSUE-016, 017, 018, 019, 020, 021, 022

**Sprint 2 Issues (P1)**: ✅ 4/6 COMPLETE
- ISSUE-023 ✅, ISSUE-024 ✅, ISSUE-025 ✅, ISSUE-026 ✅
- In Progress: ISSUE-031 (SoccerData integration)
- Remaining: Some derived features (possession-value models)

**Sprint 3 Issues (P2)**: ✅ 2/4 COMPLETE
- ISSUE-027 ✅, ISSUE-030 ✅
- Remaining: ISSUE-028 (physical tracking), ISSUE-029 (coverage expansion)

**Sprint 4 (New)**: SoccerData Integration
- ISSUE-031: Platform upgrade + SoccerData adapter

---

## How to Report New Issues

1. Add issue with next available ISSUE-XXX number
2. Include:
   - Status (Open/In Progress/Resolved)
   - Severity (Critical P0/High P1/Medium P2/Low P3)
   - Component affected
   - Error message (if applicable)
   - Location in code (file:line)
   - Root cause analysis
   - Impact assessment
   - Required fix steps
3. Update this file when issues are fixed
