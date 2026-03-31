"""
FotMob squad scraper — __NEXT_DATA__ extraction.

Fetches squad data from www.fotmob.com/teams/{id}/squad/{slug} pages.
No authentication required — parses the embedded __NEXT_DATA__ JSON block.
Does NOT use api.fotmob.com (that endpoint requires HMAC auth since early 2026).

Rate limit: RATE_LIMIT_SECONDS between requests (default 3.0s).
"""
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from scrapers.fotmob.constants import FOTMOB_ROLE_TO_POSITION, RATE_LIMIT_SECONDS

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fotmob.com/",
}

# Track last request time for rate limiting
_last_request_time: float = 0.0


def _rate_limit() -> None:
    """Enforce minimum gap between requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < RATE_LIMIT_SECONDS:
        time.sleep(RATE_LIMIT_SECONDS - elapsed)
    _last_request_time = time.time()


def get_squad(
    fotmob_team_id: int,
    slug: str,
    bronze_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch squad members for a team from FotMob's __NEXT_DATA__ JSON.

    Skips coach entries (role.key == 'coach' or group title == 'coach').
    Saves raw JSON to bronze_path if provided.

    Args:
        fotmob_team_id: FotMob team ID (integer embedded in URL).
        slug: URL slug for the team (e.g. 'arsenal', 'real-madrid').
        bronze_path: Optional path to write raw __NEXT_DATA__ JSON to.

    Returns:
        List of player dicts with standardised keys:
          - fotmob_id (int)
          - name (str)
          - shirt_number (int or None)
          - ccode (str)  — country code e.g. 'ENG'
          - cname (str)  — country name e.g. 'England'
          - position_group (str) — 'GK','DEF','MID','FWD' or None
          - position_raw (str)   — positionIdsDesc e.g. 'CB', 'CM,CDM'
          - height_cm (int or None)
          - date_of_birth (str or None) — 'YYYY-MM-DD'
          - is_injured (bool)
          - rating (float or None)
          - goals (int)
          - assists (int)
          - yellow_cards (int)
          - red_cards (int)

        Returns empty list on HTTP error or missing data.
    """
    _rate_limit()

    url = f"https://www.fotmob.com/teams/{fotmob_team_id}/squad/{slug}"
    try:
        response = requests.get(url, headers=_HEADERS, timeout=25)
    except requests.RequestException as exc:
        logger.error(f"Network error fetching {url}: {exc}")
        return []

    if response.status_code != 200:
        logger.warning(
            f"FotMob squad HTTP {response.status_code} for team {fotmob_team_id} "
            f"({slug})"
        )
        return []

    # Parse __NEXT_DATA__
    soup = BeautifulSoup(response.text, "html.parser")
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not tag:
        logger.warning(
            f"No __NEXT_DATA__ in response for team {fotmob_team_id} ({slug})"
        )
        return []

    try:
        next_data = json.loads(tag.string)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.error(f"Failed to parse __NEXT_DATA__ for team {fotmob_team_id}: {exc}")
        return []

    # Save bronze layer
    if bronze_path is not None:
        try:
            Path(bronze_path).parent.mkdir(parents=True, exist_ok=True)
            with open(bronze_path, "w") as f:
                json.dump(next_data, f, default=str, indent=2)
        except Exception as exc:
            logger.warning(f"Bronze save failed for team {fotmob_team_id}: {exc}")

    # Navigate to squad data
    fallback = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("fallback", {})
    )
    team_key = f"team-{fotmob_team_id}"
    team_data = fallback.get(team_key)
    if not team_data:
        logger.warning(
            f"Team key '{team_key}' missing or None in fallback for {url}"
        )
        return []

    squad_wrapper = team_data.get("squad", {})
    if not squad_wrapper:
        logger.warning(f"No squad key in team data for team {fotmob_team_id}")
        return []

    # squad.squad is a list of groups: keepers, defenders, midfielders, attackers, coach
    squad_groups = squad_wrapper.get("squad", [])
    if not squad_groups:
        logger.warning(f"Empty squad groups for team {fotmob_team_id}")
        return []

    players: List[Dict[str, Any]] = []
    for group in squad_groups:
        group_title = (group.get("title") or "").lower()
        if group_title == "coach":
            continue  # skip coaching staff

        for member in group.get("members", []):
            role_key = (member.get("role") or {}).get("key", "")
            if role_key == "coach":
                continue

            # Map role key to canonical position group
            position_group = FOTMOB_ROLE_TO_POSITION.get(role_key)

            # Extract injury status — injury field is None when not injured
            injury_data = member.get("injury")
            is_injured = injury_data is not None

            players.append({
                "fotmob_id": member.get("id"),
                "name": member.get("name", ""),
                "shirt_number": member.get("shirtNumber"),
                "ccode": member.get("ccode", ""),
                "cname": member.get("cname", ""),
                "position_group": position_group,
                "position_raw": member.get("positionIdsDesc", ""),
                "height_cm": member.get("height"),
                "date_of_birth": member.get("dateOfBirth"),
                "is_injured": is_injured,
                "rating": member.get("rating"),
                "goals": member.get("goals", 0) or 0,
                "assists": member.get("assists", 0) or 0,
                "yellow_cards": member.get("ycards", 0) or 0,
                "red_cards": member.get("rcards", 0) or 0,
            })

    logger.info(
        f"FotMob squad: team_id={fotmob_team_id} ({slug}) → "
        f"{len(players)} players"
    )
    return players
