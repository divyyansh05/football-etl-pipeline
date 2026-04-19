# Data Rules

## Primary Source: Wyscout
All player and team match stats come from Wyscout.
Wyscout data is the authority — never overwrite with
data from secondary sources.

## Secondary Sources (enrichment only)
  SofaScore:    adds sofascore_id, sofascore_rating
  Transfermarkt: adds market_value_eur, contract_expires
  FotMob:       adds squad completeness for roster-only players

## Key Tables
  players              — canonical identity (wyscout_id is key)
  teams                — canonical team identity
  competitions         — league/cup metadata
  seasons              — season metadata with wyscout season IDs
  player_match_stats   — 439-col match rows (PRIMARY DATA)
  team_match_stats     — team match stats (JSON source)
  player_scores        — computed analytics (derived)
  loaded_files         — tracks which files have been loaded

## Column Naming
  Use snake_case. Match Wyscout field names where possible.
  Compound stats (e.g. "13/2 total/accurate"):
    passes INTEGER        ← total
    passes_accurate INTEGER ← accurate

## NULL Handling
  NULL = stat not collected or not applicable.
  Never default to 0 for NULL stats.
  0 means zero. NULL means unknown.
  Display NULL as "—" in UI.

## Wyscout Player ID Note
  Some Wyscout player IDs are negative (e.g. -65351).
  This is valid. Store as INTEGER. Index on it.

## Duplicate Prevention
  player_match_stats: UNIQUE (wyscout_player_name, match_date,
                               competition_name, minutes_played)
  team_match_stats:   UNIQUE (wyscout_team_name, match_date,
                               competition_name, is_home)
  All inserts use ON CONFLICT DO UPDATE.

## File Tracking
  loaded_files table tracks every processed xlsx.
  Always check before loading. Skip if already loaded.
  Re-running scripts is always safe.
