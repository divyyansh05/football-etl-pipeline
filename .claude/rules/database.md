# Database Rules

## Connection
  postgresql://postgres:postgres@localhost:5434/football_platform
  Read from .env: DATABASE_URL
  Connection via: database/connection.py

## Migration Rules
  All schema changes via database/migrations/*.sql
  Files named: 001_initial.sql, 002_feature.sql etc.
  Plain SQL only — no BEGIN/COMMIT in migration files.
  Runner owns transactions.
  All DDL uses IF NOT EXISTS guards.

## Query Rules
  All queries parameterised. Never string format SQL.
  Never SELECT * — always name columns explicitly.
  All list queries have explicit LIMIT.
  Use COALESCE for nullable enrichment columns.

## Key Constraints
  players.wyscout_id:    UNIQUE, NOT NULL after initial load
  teams.wyscout_id:      UNIQUE, NOT NULL
  player_match_stats:    UNIQUE (wyscout_player_name, match_date,
                                  competition_name, minutes_played)
  team_match_stats:      UNIQUE (wyscout_team_name, match_date,
                                  competition_name, is_home)
  loaded_files.filename: UNIQUE

## Read-Only for Consumers
  API, ScoutIQ, analytics tools: READ ONLY.
  Only ETL scripts write to DB.
  Never ALTER TABLE in ETL or API code.

## Wyscout ID Type
  INTEGER. Can be negative (e.g. -65351). This is valid.
