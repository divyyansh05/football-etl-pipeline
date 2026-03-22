"""
Understat client using the understat package (async).
Replaces broken custom scraper - data moved to JS-rendered architecture.
"""
import asyncio
import logging
import time
from typing import List, Dict, Optional
import aiohttp
from understat import Understat

logger = logging.getLogger(__name__)

LEAGUE_MAPPINGS = {
    'premier-league': 'epl',
    'la-liga': 'la_liga',
    'serie-a': 'serie_a',
    'bundesliga': 'bundesliga',
    'ligue-1': 'ligue_1',
}

class UnderstatClient:
    def __init__(self, rate_limit_delay: float = 2.0):
        self.rate_limit_delay = rate_limit_delay
        self.total_requests = 0
        self.failed_requests = 0

    def get_league_players(self, league: str, season: int) -> List[Dict]:
        return asyncio.run(self._get_league_players(league, season))

    async def _get_league_players(self, league: str, season: int) -> List[Dict]:
        understat_league = LEAGUE_MAPPINGS.get(league)
        if not understat_league:
            logger.error(f"Unknown league: {league}")
            return []

        logger.info(f"Fetching {league} {season} players via understat package")
        try:
            async with aiohttp.ClientSession() as session:
                understat = Understat(session)
                self.total_requests += 1
                raw = await understat.get_league_players(understat_league, season)
                time.sleep(self.rate_limit_delay)

            players = []
            for p in raw:
                players.append({
                    'understat_id': int(p.get('id', 0)),
                    'name': p.get('player_name', ''),
                    'team': p.get('team_title', ''),
                    'position': p.get('position', ''),
                    'games': int(p.get('games', 0)),
                    'minutes': int(p.get('time', 0)),
                    'goals': int(p.get('goals', 0)),
                    'assists': int(p.get('assists', 0)),
                    'shots': int(p.get('shots', 0)),
                    'key_passes': int(p.get('key_passes', 0)),
                    'xg': float(p.get('xG', 0)),
                    'xa': float(p.get('xA', 0)),
                    'npxg': float(p.get('npxG', 0)),
                    'xg_chain': float(p.get('xGChain', 0)),
                    'xg_buildup': float(p.get('xGBuildup', 0)),
                    'npg': int(p.get('npg', 0)),
                    'yellow_cards': int(p.get('yellow_cards', 0)),
                    'red_cards': int(p.get('red_cards', 0)),
                })

            logger.info(f"Found {len(players)} players for {league} {season}")
            return players

        except Exception as e:
            logger.error(f"Failed to fetch {league} {season}: {e}")
            self.failed_requests += 1
            return []

    def get_statistics(self) -> Dict:
        return {
            'total_requests': self.total_requests,
            'failed_requests': self.failed_requests,
            'success_rate': (self.total_requests - self.failed_requests) / max(self.total_requests, 1) * 100
        }
