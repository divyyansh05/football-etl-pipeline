"""
SofaScore API client.

Working endpoints (confirmed March 2026):
  /unique-tournament/{lid}/season/{sid}/standings/total
      → team identities for the season
  /unique-tournament/{lid}/season/{sid}/top-players/overall
      → ~300-400 unique player IDs per league/season (8 categories × top-50)
  /player/{pid}
      → full player identity (name, position, dob, height, nationality, team)
  /player/{pid}/unique-tournament/{lid}/season/{sid}/statistics/overall
      → 112 stat fields for the season

DEAD endpoints — never call:
  /team/{id}/players       → 404
  /team/{id}/featured-players → 404
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from scrapers.sofascore.constants import (
    LEAGUE_IDS,
    SEASON_IDS,
    POSITION_MAP,
    TOP_PLAYER_CATEGORIES,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sofascore.com/api/v1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
}


class SofaScoreClient:
    """
    Thin HTTP wrapper around the SofaScore public API.

    Rate limit: 1.5 s between requests.
    On 429: back off 60 s then retry once.
    """

    RATE_LIMIT = 1.5
    RETRY_429_SLEEP = 60

    def __init__(self, rate_limit: float = RATE_LIMIT):
        self._rate_limit = rate_limit
        self._session = requests.Session()
        self._session.headers.update(HEADERS)
        self.requests_made = 0
        self.requests_failed = 0

    # ── Private HTTP ─────────────────────────────────────────────────────────

    def _get(self, path: str) -> Optional[Dict]:
        """GET {BASE_URL}{path} with rate limiting and single 429 retry."""
        url = f"{BASE_URL}{path}"
        time.sleep(self._rate_limit)
        try:
            resp = self._session.get(url, timeout=20)
            self.requests_made += 1

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                logger.debug(f"404: {url}")
                return None
            if resp.status_code == 429:
                logger.warning(f"429 rate-limited — sleeping {self.RETRY_429_SLEEP}s then retrying")
                time.sleep(self.RETRY_429_SLEEP)
                resp2 = self._session.get(url, timeout=20)
                self.requests_made += 1
                if resp2.status_code == 200:
                    return resp2.json()
                logger.error(f"Still {resp2.status_code} after retry: {url}")
                self.requests_failed += 1
                return None

            logger.warning(f"HTTP {resp.status_code}: {url}")
            self.requests_failed += 1
            return None

        except requests.RequestException as exc:
            logger.error(f"Request exception [{url}]: {exc}")
            self.requests_failed += 1
            return None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_standings(
        self, league_name: str, season_year: int
    ) -> List[Dict]:
        """
        Return list of teams from league standings.

        Each dict: {sofascore_id, team_name, slug}
        Returns [] on failure.
        """
        lid = LEAGUE_IDS.get(league_name)
        sid = SEASON_IDS.get(league_name, {}).get(season_year)
        if not lid or not sid:
            logger.error(f"No IDs for {league_name!r} / {season_year}")
            return []

        data = self._get(
            f"/unique-tournament/{lid}/season/{sid}/standings/total"
        )
        if not data or "standings" not in data:
            logger.error(f"Standings empty for {league_name} {season_year}")
            return []

        teams = []
        for row in data["standings"][0].get("rows", []):
            team = row.get("team", {})
            tid = team.get("id")
            if tid:
                teams.append({
                    "sofascore_id": tid,
                    "team_name":    team.get("name", ""),
                    "slug":         team.get("slug", ""),
                })

        logger.info(f"Standings: {len(teams)} teams for {league_name} {season_year}")
        return teams

    def get_top_player_ids(
        self, league_name: str, season_year: int
    ) -> List[int]:
        """
        Return unique player SofaScore IDs from the top-players/overall
        endpoint (up to ~400 players per league/season).

        Collects all categories defined in TOP_PLAYER_CATEGORIES.
        """
        lid = LEAGUE_IDS.get(league_name)
        sid = SEASON_IDS.get(league_name, {}).get(season_year)
        if not lid or not sid:
            logger.error(f"No IDs for {league_name!r} / {season_year}")
            return []

        data = self._get(
            f"/unique-tournament/{lid}/season/{sid}/top-players/overall"
        )
        if not data:
            return []

        seen: set = set()
        top_players = data.get("topPlayers", {})

        for category in TOP_PLAYER_CATEGORIES:
            category_data = top_players.get(category, {})
            # Handles both {"players": [...]} and direct list forms
            entries = (
                category_data.get("players", [])
                if isinstance(category_data, dict)
                else category_data
            )
            for entry in entries:
                player = entry.get("player", {}) if isinstance(entry, dict) else {}
                pid = player.get("id")
                if pid:
                    seen.add(int(pid))

        logger.info(
            f"top-players: {len(seen)} unique player IDs "
            f"for {league_name} {season_year}"
        )
        return list(seen)

    def get_player_identity(self, player_id: int) -> Optional[Dict]:
        """
        Fetch full player identity from /player/{id}.

        Returns dict or None on error.  Confirmed fields (March 2026):
          sofascore_id, name, short_name, position, position_group,
          dob (date), height_cm, preferred_foot, shirt_number,
          nationality, team_sofascore_id, team_name
        """
        data = self._get(f"/player/{player_id}")
        if not data or "player" not in data:
            return None

        p = data["player"]

        # Date of birth from unix timestamp
        dob = None
        ts = p.get("dateOfBirthTimestamp")
        if ts:
            try:
                dob = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            except (OSError, OverflowError, ValueError):
                pass

        # Nationality: actual field is player['country'] (not 'nationality')
        country_block = p.get("country") or p.get("nationality") or {}
        nationality = country_block.get("name")

        position_code = p.get("position")  # "G","D","M","F"
        position_group = POSITION_MAP.get(position_code) if position_code else None

        team = p.get("team") or {}

        return {
            "sofascore_id":       p.get("id"),
            "name":               p.get("name"),
            "short_name":         p.get("shortName"),
            "position":           position_code,
            "position_group":     position_group,
            "dob":                dob,
            "height_cm":          p.get("height"),
            "preferred_foot":     p.get("preferredFoot"),
            "shirt_number":       p.get("shirtNumber"),
            "nationality":        nationality,
            "team_sofascore_id":  team.get("id"),
            "team_name":          team.get("name"),
        }

    def get_player_stats(
        self,
        player_id: int,
        league_name: str,
        season_year: int,
    ) -> Optional[Dict]:
        """
        Fetch season statistics for a player.

        Returns the raw statistics dict from the API, or None.
        """
        lid = LEAGUE_IDS.get(league_name)
        sid = SEASON_IDS.get(league_name, {}).get(season_year)
        if not lid or not sid:
            return None

        data = self._get(
            f"/player/{player_id}"
            f"/unique-tournament/{lid}"
            f"/season/{sid}"
            f"/statistics/overall"
        )
        if not data or "statistics" not in data:
            return None

        return data["statistics"]
