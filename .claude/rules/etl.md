# ETL Rules

## Collection Order (IMMUTABLE)
  1. Wyscout    → player match stats, team match stats (primary)
  2. SofaScore  → ratings and ID linkage (enrichment)
  3. Transfermarkt → market values (enrichment)
  4. FotMob     → squad completeness (enrichment)

## Token Management
  scrapers/wyscout/token_manager.py handles all auth.
  get_token() returns valid token always.
  On 401/403: auto-refresh via Playwright login.
  Credentials: WYSCOUT_EMAIL + WYSCOUT_PASSWORD from .env
  NEVER hardcode token. NEVER pass token as argument.

## Download Rules
  Always save Bronze (raw xlsx) before any DB write.
  Bronze path: data/raw/wyscout/players/{player_id}_{name}.xlsx
  Check file exists AND size > 2000 bytes before skipping.
  If file exists but is small (< 2000): re-download.

## ETL Class Rules
  All ETL classes: never create new player or team rows
    EXCEPT WyscoutLoader which creates canonical identity.
  Secondary sources (SofaScore, TM, FotMob): enrichment only.
    They match to existing players by normalised_name or wyscout_id.
    If no match: log and skip. Never insert.

## Identity Matching (secondary sources → Wyscout players)
  Step 1: exact wyscout_id match (fastest, most reliable)
  Step 2: exact normalised_name match (unique result only)
  Step 3: fuzzy ratio > 0.90 + position match
  Step 4: log unmatched, skip. Never guess.
  Ambiguous matches (2+ candidates): skip, log 'ambiguous'.

## Count Guard
  Record player count before and after each ETL run.
  Assert: count after >= count before (ETL never deletes).
  Secondary ETLs assert: count unchanged (never insert).

## Rate Limiting
  Wyscout: 1.5 seconds between requests
  SofaScore: 1.5 seconds between requests
  Transfermarkt: 4.0 seconds between requests
  FotMob: 3.0 seconds between requests

## Resumability
  Every script is resumable from interruption.
  Already-downloaded files: skip.
  Already-loaded files: skip (loaded_files table).
  Discovery cache: data/raw/wyscout/discovery/players.json
    Rebuild only if cache is missing or --force flag used.

## Error Handling
  TIER 1 — single record failure: log, skip, continue
  TIER 2 — systemic failure (3+ consecutive, auth break):
    stop that source, report, leave partial state
  TIER 3 — architecture change: explicit user instruction only
