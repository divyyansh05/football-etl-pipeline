#!/usr/bin/env python3
"""
smart_backfill.py — Auto-retry wrapper for init_backfill.py.

State machine
─────────────
  idle    → probe SofaScore → OK: run backfill  /  blocked: write retry_after
  blocked → wait retry_after → probe → OK: run  /  still blocked: extend wait
  running → stale PID (process died) → treat as idle, re-probe

State is persisted in data/backfill_state.json so it survives restarts.

Sleep prevention
────────────────
The actual backfill subprocess is wrapped in:
  caffeinate -s -i <python> init_backfill.py

  -s  prevent system sleep while on AC power   ← keeps Mac awake with lid closed
  -i  prevent idle sleep (display/disk sleep)

On battery + lid closed the Mac will still sleep; the LaunchAgent will run
the overdue job as soon as the Mac next wakes (lid opens or charger connects).

Called by ~/Library/LaunchAgents/com.football-etl.backfill.plist every hour.
"""
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── Paths ──────────────────────────────────────────────────────────────────────

PROJECT_DIR = Path(__file__).resolve().parent.parent
STATE_FILE  = PROJECT_DIR / "data" / "backfill_state.json"
PID_FILE    = PROJECT_DIR / "data" / "backfill.pid"
LOG_FILE    = PROJECT_DIR / "logs" / "smart_backfill.log"

RETRY_HOURS = 26   # hours to wait after detecting a block before re-probing
PYTHON      = sys.executable  # same interpreter that launched this script

# ── Logging ────────────────────────────────────────────────────────────────────

LOG_FILE.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger("smart_backfill")

# ── State helpers ──────────────────────────────────────────────────────────────

def read_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"status": "idle", "retry_after": None, "message": None}


def write_state(status: str, retry_after: Optional[str] = None, message: Optional[str] = None):
    state = {
        "status":      status,
        "retry_after": retry_after,
        "message":     message,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))
    log.info(f"State → {status}" + (f" | retry after {retry_after}" if retry_after else ""))

# ── Concurrency guard ─────────────────────────────────────────────────────────

def is_already_running() -> bool:
    """Return True if another smart_backfill instance is live (PID file + process check)."""
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)   # signal 0 = existence check only, no kill
        return True
    except (OSError, ValueError):
        PID_FILE.unlink(missing_ok=True)
        return False

# ── SofaScore probe ───────────────────────────────────────────────────────────

def probe_sofascore() -> bool:
    """
    Quick liveness check — one GET to PL 2025 standings.
    Returns True if SofaScore is accessible, False on 403 / empty / exception.
    """
    sys.path.insert(0, str(PROJECT_DIR))
    os.chdir(PROJECT_DIR)

    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_DIR / ".env")
    except Exception:
        pass

    try:
        from scrapers.sofascore.client import SofaScoreClient
        client = SofaScoreClient()
        teams = client.get_standings("Premier League", 2025)
        accessible = len(teams) > 0
        log.info(f"SofaScore probe: {'OK' if accessible else 'BLOCKED (empty standings)'}")
        return accessible
    except Exception as exc:
        log.warning(f"SofaScore probe exception: {exc}")
        return False

# ── Backfill runner ───────────────────────────────────────────────────────────

def run_backfill() -> bool:
    """
    Run init_backfill.py wrapped in caffeinate to prevent macOS sleep.
    Returns True on exit code 0.
    """
    cmd = [
        "caffeinate", "-s", "-i",        # prevent system + idle sleep
        PYTHON,
        str(PROJECT_DIR / "scripts" / "init_backfill.py"),
    ]
    log.info(f"Launching: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    log.info(f"init_backfill.py exited with code {result.returncode}")
    return result.returncode == 0

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("smart_backfill invoked")

    # ── 1. Concurrency guard ─────────────────────────────────────────────────
    if is_already_running():
        log.info("Another instance is already running (PID file active). Exiting.")
        return

    PID_FILE.write_text(str(os.getpid()))

    try:
        state = read_state()
        now   = datetime.now(timezone.utc)

        # ── 2. Blocked state: check if retry window has elapsed ───────────────
        if state["status"] == "blocked" and state.get("retry_after"):
            retry_after = datetime.fromisoformat(state["retry_after"])
            if now < retry_after:
                remaining = retry_after - now
                h = remaining.total_seconds() / 3600
                log.info(
                    f"Status: BLOCKED — {h:.1f}h until next probe "
                    f"(retry at {retry_after.strftime('%Y-%m-%d %H:%M UTC')}). Exiting."
                )
                return
            log.info("Retry window elapsed. Re-probing SofaScore...")
        else:
            log.info(f"Status: {state['status']}. Probing SofaScore...")

        # ── 3. Probe ─────────────────────────────────────────────────────────
        if not probe_sofascore():
            retry_at = (now + timedelta(hours=RETRY_HOURS)).isoformat()
            write_state("blocked", retry_after=retry_at,
                        message="SofaScore 403 / empty standings on probe")
            log.warning(f"SofaScore is BLOCKED. Will retry at {retry_at}")
            return

        # ── 4. Run backfill ───────────────────────────────────────────────────
        log.info("SofaScore accessible. Starting full backfill...")
        write_state("running", message="Backfill in progress")

        success = run_backfill()

        if success:
            write_state("idle", message="Backfill completed successfully")
            log.info("Backfill completed successfully.")
        else:
            # Non-zero exit — could be mid-run block or a real error.
            # Re-probe to distinguish the two cases.
            log.warning("Backfill exited non-zero. Re-probing SofaScore to classify failure...")
            if not probe_sofascore():
                retry_at = (now + timedelta(hours=RETRY_HOURS)).isoformat()
                write_state("blocked", retry_after=retry_at,
                            message="SofaScore blocked mid-run")
                log.warning(f"SofaScore blocked mid-run. Next retry: {retry_at}")
            else:
                # SofaScore fine — some other error (DB, parsing, etc.)
                # Mark idle so we retry next hour rather than waiting 26h.
                write_state("idle", message="Backfill failed (non-block reason) — will retry next hour")
                log.error("Backfill failed for a non-SofaScore reason. Will retry next hour.")

    finally:
        PID_FILE.unlink(missing_ok=True)
        log.info("smart_backfill done.")


if __name__ == "__main__":
    main()
