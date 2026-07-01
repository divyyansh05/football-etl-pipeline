# football-data-platform Audit Report
Date: 2026-04-20
Auditor role: Senior Data Engineering + Platform Engineering review

## Scope Reviewed
- Extraction pipeline (Wyscout discovery/download/load)
- Parser behavior for Wyscout xlsx stats
- Database schema and constraints
- API surface and contract compliance
- Operational readiness (tests, migrations, env compatibility)
- Alignment with project rules in .claude/rules/*.md and AGENTS.md

## Executive Summary
Current implementation is a strong foundation, but it is not production-ready yet.

Key conclusion:
- The known compound value issue ("5/2") is not reproducible on sampled current Wyscout exports.
- The largest blockers are platform reliability and contract compliance (API standards, transaction safety, migration discipline, missing test coverage, Python package compatibility).

Delivery risk level: High
Production readiness estimate: 35/100

## Evidence-Based Findings (Ordered by Severity)

### Critical
1. Row-level error handling can silently lose previously inserted rows in the same file load.
- Evidence: rollback is called inside row loop in one transaction.
- Location: etl/wyscout_loader.py#L150
- Impact: a single bad row may roll back earlier successful inserts, but loaded counter still increments, causing false success metrics and partial/corrupted load accounting.
- Required fix:
  - Use per-row SAVEPOINT (or separate transaction batches).
  - Increment counters only after durable write.
  - Track attempted/succeeded/failed rows separately.

2. API contract is not aligned with required /api/v1 conventions and response envelopes.
- Evidence: only /api/health exists; no /api/v1 prefix and no standardized {data: ...} or {error,detail} envelope.
- Location: api/main.py#L22
- Impact: contract break for ScoutIQ and future consumers; prevents stable integration.
- Required fix:
  - Move health to /api/v1/health.
  - Add domain routers under api/routers with required list/single/error schemas.

3. Environment compatibility blocker: Python 3.13 + pinned psycopg2-binary==2.9.9 fails build.
- Evidence: install fails with pg_config error for current env.
- Location: requirements.txt
- Impact: new developer setup and CI will fail; extraction/API cannot run cleanly.
- Required fix:
  - Standardize runtime to Python 3.11 (project rule says 3.11+; current lock set works best on 3.11).
  - Or upgrade DB driver strategy (psycopg[binary] / compatible wheels) and pin tested versions.

### High
4. Team extraction path is missing from main extraction flow.
- Evidence: script processes players only; no team JSON pull/load pipeline.
- Location: scripts/extract.py#L58
- Impact: Phase 1 objective incomplete; team_match_stats remains empty or stale.
- Required fix:
  - Add team discovery + team_stats JSON ingestion in same resumable pipeline.

5. Scope mismatch: extraction covers current + previous season only, not 2021/22 through 2025/26 target.
- Evidence: get_season_id returns two seasons and those only are processed.
- Location: scripts/extract.py#L74
- Impact: historical completeness is below business target.
- Required fix:
  - Add season strategy config: explicit season range with backfill mode.

6. API layer skeleton is incomplete vs architecture claims.
- Evidence: api/routers contains only __init__.py; api/models contains only __init__.py.
- Location: api/routers/__init__.py, api/models/__init__.py
- Impact: no consumer-facing domain endpoints for players/teams/scores.
- Required fix:
  - Implement player/team/competition/score routers + pydantic response models.

7. Analytics phase is not implemented though schema has player_scores table.
- Evidence: analytics directory only has __init__.py.
- Location: analytics/__init__.py
- Impact: Phase 3 blocked; no performance_score population.
- Required fix:
  - Implement per90, percentile, scoring pipeline and scheduled run.

8. Migrations discipline is not yet in place.
- Evidence: database/migrations is empty while schema exists.
- Location: database/migrations
- Impact: non-repeatable schema evolution and no deployment-safe DDL lifecycle.
- Required fix:
  - Add baseline migration 001_initial.sql and migration runner workflow.

### Medium
9. NULL-handling policy conflict with defaults and parser behavior.
- Evidence:
  - schema defaults 0 for goals/assists/yellow_cards/red_cards.
  - parser forces red_cards to 0 when minute field absent.
- Location: database/schema.sql#L87, database/schema.sql#L155, database/schema.sql#L156, scrapers/wyscout/parser.py#L158
- Impact: unknown vs zero may be conflated; violates data rule semantics.
- Required fix:
  - Preserve NULL when stat not collected; only set 0 when explicit evidence of zero exists.

10. Discovery cache and run control are missing force-refresh/staleness controls.
- Evidence: discovery cache reused indefinitely unless file absent.
- Location: scripts/extract.py#L47
- Impact: stale player universe during mid-season updates.
- Required fix:
  - Add --force-discovery and optional TTL/metadata.

11. Lack of systemic failure guardrails in extraction loop.
- Evidence: no consecutive failure threshold / source abort logic in script.
- Location: scripts/extract.py#L99
- Impact: long noisy runs with low signal; hard to identify source-level breakages.
- Required fix:
  - Implement tiered failure handling counters (per .claude/rules/etl.md).

12. Read-only consumer policy is documented but not technically enforced at DB connection level.
- Evidence: single shared connection helper allows execute/update calls from any runtime path.
- Location: database/connection.py#L32
- Impact: accidental writes from API/consumer code remain possible.
- Required fix:
  - Split read-only and write-capable DB roles and use read-only credentials in API.

### Low
13. CORS policy is always open and not environment-sensitive.
- Evidence: allow_origins=['*'] hardcoded.
- Location: api/main.py#L14
- Impact: acceptable in dev, risky for production posture.
- Required fix:
  - Gate CORS origins by environment variable.

14. AGENTS.md status section is stale vs actual repo state.
- Evidence: marks database/token/extraction as pending despite implemented files.
- Location: AGENTS.md#L67
- Impact: coordination confusion for contributors and autonomous agents.
- Required fix:
  - Update build status after each major session.

## Parser Verification Result (Known "5/2" Concern)
Validation run on real files:
- data/raw/wyscout/players/-65351_Julian_Alvarez.xlsx (439 cols, 277 rows)
- data/raw/wyscout/players/85786_Jan_Oblak.xlsx (307 cols, 229 rows)

Observed:
- No stat fields with "N/M" compound cell format were found in sampled current exports.
- The only slash-like string detected was season label (e.g., 2025/2026).
- Parser produced expected numeric totals/accurate values for sampled keys (passes, passes_accurate, dribbles, smart_passes, gk_passes, etc.).

Conclusion:
- Immediate blocker is not a current-format parsing failure.
- Still recommended: add defensive parser support for legacy/mixed compound values to future-proof ingestion.

## Plan Issues vs Target Phases
1. Phase 1 claims team extraction and full player extraction readiness, but code currently handles player flow only and only two seasons.
2. Phase 2 data quality is not codified as automated checks/tests.
3. Phase 3 analytics layer is largely not implemented.
4. Phase 4 API is minimal and not contract-compliant.
5. Phase 5 enrichment scaffolding exists in folders but no executable ETL scripts in this repo snapshot.

## Recommended Remediation Roadmap (Execution Order)

### Sprint 0 (Stabilize Platform, 1-2 days)
1. Lock runtime to Python 3.11 and verify clean install from requirements.
2. Add smoke CI: import parser, parse one fixture, DB connection check (optional mock), API app startup.
3. Fix loader transaction handling with SAVEPOINT strategy.
4. Add extraction run summary metrics (attempted/succeeded/failed/skipped).

Exit criteria:
- Fresh setup succeeds on clean machine.
- A file load with one malformed row still keeps other valid rows committed.

### Sprint 1 (Complete Phase 1, 2-4 days)
1. Extend extraction to all required seasons (configurable range).
2. Implement team discovery + team stats JSON loader into team_match_stats.
3. Add count guards and tiered error handling per ETL rules.
4. Add --force-discovery and --force-download options.

Exit criteria:
- Player + team data loaded for configured competitions/seasons.
- Re-run is idempotent with stable counts.

### Sprint 2 (Data Quality + Testing, 2-3 days)
1. Build tests for parser mappings and null semantics.
2. Add data quality checks:
   - uniqueness checks for both match tables
   - NULL-vs-zero validations on sampled stat columns
   - row count deltas and anomaly thresholds
3. Add verification script output report artifact.

Exit criteria:
- tests/ has meaningful coverage for parser/loader logic.
- quality checks pass on a representative dataset slice.

### Sprint 3 (Analytics + API, 3-5 days)
1. Implement analytics jobs to populate player_scores with min_minutes guard.
2. Implement API routers and pydantic models using /api/v1 prefix and envelope format.
3. Add pagination defaults/limits and score non-null filter in queries.
4. Add environment-aware CORS and read-only DB role for API.

Exit criteria:
- ScoutIQ-consumable endpoints available and contract-compliant.
- player_scores populated and queryable.

### Sprint 4 (Secondary Enrichment, 3-5 days)
1. Add SofaScore, Transfermarkt, FotMob ETL scripts with strict enrichment-only behavior.
2. Implement identity matching ladder and ambiguity handling.
3. Add source-level quality checks and enrichment freshness metadata.

Exit criteria:
- Enrichment columns filled without creating new canonical players/teams.

## Claude Code Execution Backlog (Priority)
P0
1. Fix loader rollback logic in etl/wyscout_loader.py.
2. Standardize Python 3.11 runtime and dependency install reproducibility.
3. Build /api/v1 router scaffolding with response envelopes.

P1
4. Add team extraction + loader path.
5. Expand season scope configuration to full target range.
6. Add parser/loader unit tests with fixture files.

P2
7. Implement analytics compute pipeline.
8. Add data quality audit script and CI checks.
9. Implement enrichment scripts with identity matching ladder.

## Final Recommendation
Do not start full-scale 5,970-player extraction until P0 is complete.

Safe pre-extraction gate:
1. Environment installs cleanly.
2. Loader transaction fix merged.
3. Parser tests pass on outfield + GK fixtures.
4. One end-to-end pilot run (single competition) completes with consistent counts.
