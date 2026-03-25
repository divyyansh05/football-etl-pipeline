# Database Rules

## Naming Conventions
- Tables: snake_case, plural (players, teams, matches, player_season_stats)
- Columns: snake_case (player_id, team_name, fotmob_id)
- Primary keys: always named {table_singular}_id (player_id, team_id)
- Foreign keys: always named {referenced_table_singular}_id
- Indexes: idx_{table}_{column(s)} (idx_players_fotmob_id)
- Unique constraints: uq_{table}_{column(s)}

## Required Columns on Every Table
- Every table has a single integer primary key
- Every table has created_at TIMESTAMPTZ DEFAULT NOW()
- Every table has updated_at TIMESTAMPTZ DEFAULT NOW()

## NULL Handling Rules
- player_name: NEVER NULL
- fotmob_id: NULLABLE on players (legacy column; sofascore_id is the canonical key)
- position: NULLABLE (filled where available)
- All stat columns: NULLABLE (not zero — NULL means "not collected")
- Use COALESCE in queries, never assume 0

## The Golden Rule
Zero duplicates. Zero orphaned records.
Run data-quality agent after every ETL run to verify.

## Query Standards
- Always use parameterised queries: WHERE player_id = :pid
- Never use string formatting: WHERE player_id = {pid}  ← NEVER
- Use EXPLAIN ANALYZE for any query on tables > 100k rows
- All JOINs must use indexed columns

## Index Requirements
- All foreign key columns must be indexed
- All columns used in WHERE clauses of frequent queries must be indexed
- All columns used in JOIN conditions must be indexed
