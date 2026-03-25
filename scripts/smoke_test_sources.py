#!/usr/bin/env python3
"""
Source verification smoke tests.
Six checks covering all three active data sources.

Run: python3 scripts/smoke_test_sources.py

Exit 0 = all PASS. Exit 1 = any FAIL.
"""
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

SMOKE_LEAGUE = "Premier League"
SMOKE_SEASON = "2025-26"
SMOKE_SEASON_YEAR = 2025          # start year of 2025-26
SMOKE_PLAYER_ID = 839956          # Erling Haaland — known stable ID

results = []


def check(label, fn):
    print(f"  [{label}] ", end="", flush=True)
    try:
        fn()
        print("PASS")
        results.append((label, True, None))
    except Exception as e:
        print(f"FAIL — {e}")
        traceback.print_exc()
        results.append((label, False, str(e)))


# ── Check 1: import soccerdata ──────────────────────────────────────────────
def _check_import():
    import soccerdata as sd
    v = sd.__version__
    assert v, "soccerdata.__version__ is empty"
    print(f"(v{v}) ", end="")


check("1-import-soccerdata", _check_import)


# ── Check 2: Understat player season stats ──────────────────────────────────
def _check_understat():
    import soccerdata as sd
    us = sd.Understat(leagues="ENG-Premier League", seasons="2526")
    df = us.read_player_season_stats()
    assert not df.empty, "DataFrame is empty"
    required = {"xg", "np_xg", "xa", "xg_chain", "xg_buildup", "position"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"
    print(f"(shape={df.shape}) ", end="")


check("2-understat-stats", _check_understat)


# ── Check 3: ClubElo read_by_date ────────────────────────────────────────────
def _check_clubelo():
    import soccerdata as sd
    elo = sd.ClubElo()
    df = elo.read_by_date("2025-01-01")
    assert not df.empty, "ClubElo DataFrame is empty"
    required = {"elo", "league"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"
    # Filter to top-5 EU leagues for range check — global dataset includes low-ELO amateur clubs
    top5 = df[df["league"].isin([
        "ENG-Premier League", "ESP-La Liga",
        "ITA-Serie A", "GER-Bundesliga", "FRA-Ligue 1",
    ])]
    assert len(top5) >= 80, f"Expected >=80 top-5 teams, got {len(top5)}"
    min_elo = top5["elo"].min()
    max_elo = top5["elo"].max()
    assert min_elo > 1300, f"ELO min too low for top-5: {min_elo}"
    assert max_elo < 2200, f"ELO max too high: {max_elo}"
    print(f"(rows={len(df)}, top5_elo={min_elo:.0f}-{max_elo:.0f}) ", end="")


check("3-clubelo-date", _check_clubelo)


# ── Check 4: SofaScore standings ─────────────────────────────────────────────
def _check_sofascore_standings():
    import requests

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://www.sofascore.com/",
    }
    BASE = "https://api.sofascore.com/api/v1"
    league_id = 17      # Premier League
    season_id = 76986   # 2025-26

    url = f"{BASE}/unique-tournament/{league_id}/season/{season_id}/standings/total"
    time.sleep(1.5)
    r = requests.get(url, headers=HEADERS, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()
    rows = data.get("standings", [{}])[0].get("rows", [])
    assert len(rows) >= 15, f"Only {len(rows)} teams"
    print(f"(teams={len(rows)}) ", end="")


check("4-sofascore-standings", _check_sofascore_standings)


# ── Check 5: SofaScore top-players/overall ───────────────────────────────────
def _check_sofascore_top_players():
    import requests

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://www.sofascore.com/",
    }
    BASE = "https://api.sofascore.com/api/v1"
    league_id = 17
    season_id = 76986

    url = f"{BASE}/unique-tournament/{league_id}/season/{season_id}/top-players/overall"
    time.sleep(1.5)
    r = requests.get(url, headers=HEADERS, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()

    unique_ids = set()
    for category_block in data.get("topPlayers", {}).values():
        for entry in category_block:
            p = entry.get("player", {})
            pid = p.get("id")
            if pid:
                unique_ids.add(pid)

    assert len(unique_ids) >= 100, f"Only {len(unique_ids)} unique player IDs"
    print(f"(unique_players={len(unique_ids)}) ", end="")


check("5-sofascore-top-players", _check_sofascore_top_players)


# ── Check 6: SofaScore /player/{id} identity ─────────────────────────────────
def _check_sofascore_player_identity():
    import requests

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://www.sofascore.com/",
    }
    BASE = "https://api.sofascore.com/api/v1"

    url = f"{BASE}/player/{SMOKE_PLAYER_ID}"
    time.sleep(1.5)
    r = requests.get(url, headers=HEADERS, timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()

    p = data.get("player", {})
    assert p.get("name"), "Missing name"
    # NOTE: nationality is under 'country' not 'nationality' (confirmed March 2026)
    country = p.get("country", {})
    assert country.get("name"), f"Missing country.name; keys={list(p.keys())}"
    dob_ts = p.get("dateOfBirthTimestamp")
    assert isinstance(dob_ts, int) and dob_ts > 0, f"Bad DOB timestamp: {dob_ts}"
    position = p.get("position")
    assert position in ("G", "D", "M", "F", None), f"Unexpected position value: {position!r}"
    print(f"(name={p.get('name')!r}, pos={position}, country={country.get('name')!r}) ", end="")


check("6-sofascore-player-identity", _check_sofascore_player_identity)


# ── Summary ──────────────────────────────────────────────────────────────────
print()
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"Results: {passed}/{total} PASS")

failures = [(label, err) for label, ok, err in results if not ok]
if failures:
    print("FAILED:")
    for label, err in failures:
        print(f"  {label}: {err}")
    sys.exit(1)

print("All smoke tests PASS.")
sys.exit(0)
