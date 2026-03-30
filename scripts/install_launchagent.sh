#!/usr/bin/env bash
# install_launchagent.sh — Install the football-etl hourly backfill LaunchAgent.
#
# What this does:
#   1. Fills placeholders in the plist template (Python path, project path, home)
#   2. Copies plist to ~/Library/LaunchAgents/
#   3. Unloads any existing job with the same label (safe no-op if first install)
#   4. Loads the new job
#
# Run from anywhere:
#   bash scripts/install_launchagent.sh

set -euo pipefail

LABEL="com.football-etl.backfill"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$(command -v python3)"
HOME_DIR="$HOME"
CONDA_BIN="$(dirname "$PYTHON_BIN")"

PLIST_SRC="$PROJECT_DIR/scripts/com.football-etl.backfill.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Installing LaunchAgent: $LABEL"
echo " Project:  $PROJECT_DIR"
echo " Python:   $PYTHON_BIN"
echo " Plist →   $PLIST_DEST"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Create logs dir (LaunchAgent will write stdout/stderr there before the script runs)
mkdir -p "$PROJECT_DIR/logs" "$PROJECT_DIR/data"

# Substitute placeholders
sed \
  -e "s|PYTHON_PLACEHOLDER|$PYTHON_BIN|g" \
  -e "s|PROJECT_PLACEHOLDER|$PROJECT_DIR|g" \
  -e "s|CONDA_PLACEHOLDER|$CONDA_BIN|g" \
  -e "s|HOME_PLACEHOLDER|$HOME_DIR|g" \
  "$PLIST_SRC" > "$PLIST_DEST"

echo "✓ Plist written to $PLIST_DEST"

# Unload existing job if loaded (ignore errors — job may not exist yet)
launchctl unload "$PLIST_DEST" 2>/dev/null || true

# Load the job
launchctl load "$PLIST_DEST"
echo "✓ LaunchAgent loaded"

# Show status
echo ""
echo "Job status:"
launchctl list | grep "$LABEL" || echo "  (not listed yet — will appear on first run)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Done. The backfill will run every hour automatically."
echo " Logs:  $PROJECT_DIR/logs/smart_backfill.log"
echo " State: $PROJECT_DIR/data/backfill_state.json"
echo ""
echo " To check state:   cat data/backfill_state.json"
echo " To tail logs:     tail -f logs/smart_backfill.log"
echo " To uninstall:     launchctl unload $PLIST_DEST && rm $PLIST_DEST"
echo " To trigger now:   launchctl start $LABEL"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
