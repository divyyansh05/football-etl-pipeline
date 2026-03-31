"""
FotMob team discovery — extract team IDs from __NEXT_DATA__ standings tables.

Uses a known anchor team per league to load its squad page, then reads
the embedded league standings table to discover all team IDs in that league.

Run directly:
    python scrapers/fotmob/team_discovery.py

The anchor teams used per league are stable top-division clubs unlikely
to be relegated. The standings table embedded in their squad page lists
all current-season teams in the same league.

Rate limit: 3s between requests.
"""
import json
import logging
import time
from typing import Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fotmob.com/",
}

# Anchor teams: stable clubs used to load squad pages and extract league tables.
# (fotmob_team_id, slug, league_name)
ANCHOR_TEAMS = [
    (9825, "arsenal", "Premier League"),
    (8633, "real-madrid", "La Liga"),
    (8636, "inter", "Serie A"),
    (9823, "fc-bayern-munchen", "Bundesliga"),
    (8689, "lorient", "Ligue 1"),
]


def discover_league_teams(
    anchor_team_id: int,
    anchor_slug: str,
) -> Dict[int, Tuple[str, str]]:
    """
    Discover all team IDs in a league via the anchor team's squad page.

    The squad page embeds a full league standings table in __NEXT_DATA__
    at: fallback[team-{id}].table[0].data.table.all[]

    Each entry has: id (int), name (str), pageUrl (str).
    The pageUrl format is '/teams/{id}/squad/{slug}'.

    Args:
        anchor_team_id: FotMob team ID of the anchor team.
        anchor_slug: URL slug for the anchor team.

    Returns:
        Dict mapping fotmob_team_id -> (team_name, slug).
        Returns empty dict on failure.
    """
    time.sleep(3.0)
    url = f"https://www.fotmob.com/teams/{anchor_team_id}/squad/{anchor_slug}"
    logger.info(f"Fetching: {url}")

    try:
        response = requests.get(url, headers=_HEADERS, timeout=25)
    except requests.RequestException as exc:
        logger.error(f"Network error: {exc}")
        return {}

    if response.status_code != 200:
        logger.error(f"HTTP {response.status_code} for {url}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not tag:
        logger.error(f"No __NEXT_DATA__ in response for {url}")
        return {}

    next_data = json.loads(tag.string)
    fallback = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("fallback", {})
    )
    team_key = f"team-{anchor_team_id}"
    team_data = fallback.get(team_key)
    if not team_data:
        logger.error(f"Key '{team_key}' missing/None in fallback")
        return {}

    table_list = team_data.get("table", [])
    if not table_list:
        logger.error(f"No table data for team {anchor_team_id}")
        return {}

    inner_data = table_list[0].get("data", {})
    if not inner_data:
        logger.error(f"No inner_data in table[0] for team {anchor_team_id}")
        return {}

    all_teams = inner_data.get("table", {}).get("all", [])
    if not all_teams:
        logger.error(f"Empty table.all for team {anchor_team_id}")
        return {}

    league_name = inner_data.get("leagueName", "Unknown")
    logger.info(f"  League: {league_name} — {len(all_teams)} teams")

    result: Dict[int, Tuple[str, str]] = {}
    for t in all_teams:
        tid = t.get("id")
        name = t.get("name", "")
        page_url = t.get("pageUrl", "")
        # pageUrl format: '/teams/{id}/squad/{slug}'
        parts = page_url.strip("/").split("/")
        slug = parts[-1] if len(parts) >= 3 else name.lower().replace(" ", "-")
        result[tid] = (name, slug)

    return result


def main() -> None:
    """Discover and print all team IDs for all 5 leagues."""
    all_discoveries: Dict[str, Dict[int, Tuple[str, str]]] = {}

    for team_id, slug, league_name in ANCHOR_TEAMS:
        logger.info(f"\n=== {league_name} ===")
        teams = discover_league_teams(team_id, slug)
        all_discoveries[league_name] = teams
        for tid, (name, tslug) in sorted(teams.items(), key=lambda x: x[1][0]):
            print(f"  {tid}: {name!r} (slug={tslug!r})")
        print(f"  Total: {len(teams)}")

    # Print as Python dict for easy copy-paste into constants.py
    print("\n\n# === FOTMOB_TEAM_IDS (copy-paste into constants.py) ===")
    print("FOTMOB_TEAM_IDS = {")
    for league_name, teams in all_discoveries.items():
        print(f'    "{league_name}": {{')
        for tid, (name, tslug) in sorted(teams.items(), key=lambda x: x[1][0]):
            print(f'        {tid}: ("{name}", "{tslug}"),')
        print("    },")
    print("}")


if __name__ == "__main__":
    main()
