#!/bin/bash
# Launch the extraction pipeline as a background daemon.
# Survives terminal close, lid close, and session logout.
#
# Usage:
#   bash scripts/run_pipeline.sh          # Start pipeline
#   bash scripts/run_pipeline.sh status   # Check status
#   bash scripts/run_pipeline.sh stop     # Stop pipeline

set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VENV="$PROJECT_ROOT/.venv/bin/activate"
PID_FILE="$PROJECT_ROOT/logs/pipeline.pid"
LOG_FILE="$PROJECT_ROOT/logs/pipeline.log"

# Ensure venv exists
if [ ! -f "$VENV" ]; then
    echo "ERROR: .venv not found at $VENV"
    exit 1
fi

SUBCMD="${1:-start}"
shift 2>/dev/null || true   # consume subcommand so "$@" only has pipeline flags

case "$SUBCMD" in
    start)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Pipeline already running (PID $PID)"
                echo "Use: bash scripts/run_pipeline.sh status"
                exit 1
            else
                echo "Cleaning stale PID file..."
                rm "$PID_FILE"
            fi
        fi

        echo "Starting pipeline..."
        echo "Log: $LOG_FILE"
        echo "State: $PROJECT_ROOT/data/pipeline_state.json"

        # caffeinate prevents macOS sleep while pipeline runs
        # nohup + disown ensures survival after terminal close
        source "$VENV"
        nohup caffeinate -i python3 scripts/pipeline.py "$@" \
            >> "$LOG_FILE" 2>&1 &
        BGPID=$!
        disown "$BGPID"

        echo "Pipeline started (PID $BGPID)"
        echo "Monitor: tail -f $LOG_FILE"
        echo "Status:  bash scripts/run_pipeline.sh status"
        ;;

    status)
        source "$VENV"
        python3 scripts/pipeline.py --status
        ;;

    stop)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Stopping pipeline (PID $PID)..."
                kill "$PID"
                sleep 2
                if kill -0 "$PID" 2>/dev/null; then
                    echo "Force killing..."
                    kill -9 "$PID"
                fi
                echo "Pipeline stopped"
            else
                echo "Pipeline not running (stale PID)"
                rm "$PID_FILE"
            fi
        else
            echo "No PID file found — pipeline not running"
        fi
        ;;

    logs)
        tail -f "$LOG_FILE"
        ;;

    *)
        echo "Usage: $0 {start|status|stop|logs}"
        exit 1
        ;;
esac
