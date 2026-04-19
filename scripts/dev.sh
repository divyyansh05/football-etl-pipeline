#!/bin/bash
# Start football-data-platform API server
# Usage: bash scripts/dev.sh

set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Starting football-data-platform API..."
echo "DB: postgresql://postgres:postgres@localhost:5434/football_platform"

uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
