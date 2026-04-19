# API Rules

## Framework
  FastAPI + Python 3.11+
  Port: 8000
  All routes: /api/v1/ prefix
  DB: psycopg2-binary, read-only

## Response Format
  List endpoints:
    { "data": [...], "total": N, "limit": N, "offset": N }
  Single object:
    { "data": {...} }
  Errors:
    { "error": "message", "detail": "..." }

## Query Defaults
  All list endpoints: default limit=20, max=100
  All player queries: default min_minutes=450
  All score queries: WHERE performance_score IS NOT NULL

## Consumers
  ScoutIQ frontend (React + Vite, port 5173)
  Future: match analysis tools, agentic AI systems
  CORS: allow all origins in dev, restrict in prod

## Pydantic Models
  All response models defined in api/models/
  One file per domain: player.py, team.py, competition.py
  Match DB column names exactly.
