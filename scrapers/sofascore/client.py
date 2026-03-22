"""
SofaScore scraper client.
Fetches deep player statistics (112 fields) including aerial duels,
ground duels, progressive passing, ball recovery, and more.

Strategy:
1. Get all team IDs from league standings
2. Get all player IDs from team squads
3. Fetch per-player season stats via player stats endpoint
4. Rate limit: 1.5s between requests to avoid blocking
"""
import logging
import time
import requests
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.sofascore.com/',
}

BASE_URL = 'https://api.sofascore.com/api/v1'

LEAGUE_IDS = {
    'premier-league': 17,
    'la-liga': 8,
    'serie-a': 23,
    'bundesliga': 35,
    'ligue-1': 34,
}

SEASON_IDS = {
    'premier-league': {2022: 41886, 2023: 52186, 2024: 61627, 2025: 76986},
    'la-liga':        {2022: 42409, 2023: 52376, 2024: 61643, 2025: 77559},
    'serie-a':        {2022: 42415, 2023: 52760, 2024: 63515, 2025: 76457},
    'bundesliga':     {2022: 42268, 2023: 52608, 2024: 63516, 2025: 77333},
    'ligue-1':        {2022: 42273, 2023: 52571, 2024: 61736, 2025: 77356},
}

# Hardcoded team IDs per league — used as fallback when standings API returns 403
# These are the DB team_ids mapped to SofaScore team slugs
# Key insight: our DB team_ids ARE the SofaScore team_ids (same source)
LEAGUE_TEAM_IDS = {
    'premier-league': [
        333,  # Arsenal
        345,  # Aston Villa
        729,  # Brighton & Hove Albion
        728,  # AFC Bournemouth
        342,  # Brentford
        353,  # Burnley
        338,  # Chelsea
        341,  # Crystal Palace
        334,  # Everton
        329,  # Fulham
        731,  # Ipswich Town
        331,  # Liverpool
        339,  # Manchester City
        326,  # Manchester United
        727,  # Newcastle United
        344,  # Nottingham Forest
        332,  # Southampton
        730,  # Tottenham Hotspur
        734,  # West Ham United
        736,  # Wolverhampton Wanderers
        # Relegated/promoted teams also included for historical coverage
        335,  # Leicester
        362,  # Sheffield Utd
        365,  # Luton
        340,  # Brighton (old id)
        327,  # Newcastle (old id)
        337,  # West Ham (old id)
        336,  # Tottenham (old id)
        330,  # Wolves (old id)
        328,  # Bournemouth (old id)
    ],
    'la-liga': [
        368,  # Athletic Club
        367,  # Atletico Madrid
        366,  # Barcelona
        374,  # Celta Vigo
        786,  # Deportivo Alaves
        375,  # Espanyol
        379,  # Getafe
        380,  # Girona
        385,  # Mallorca
        383,  # Osasuna
        384,  # Rayo Vallecano
        378,  # Real Betis
        376,  # Real Madrid
        381,  # Real Sociedad
        372,  # Sevilla
        369,  # Valencia
        370,  # Villarreal
        371,  # Las Palmas
        373,  # Leganes
        796,  # Real Oviedo
        # Historical
        401,  # Almeria
        402,  # Cadiz
        1320, # Granada
        789,  # Elche
    ],
    'serie-a': [
        414,  # Atalanta
        415,  # Bologna
        408,  # Cagliari
        424,  # Como
        416,  # Fiorentina
        411,  # Genoa
        816,  # Hellas Verona
        419,  # Inter
        412,  # Juventus
        406,  # Lazio
        423,  # Lecce
        798,  # Milan
        409,  # Napoli
        422,  # Parma
        801,  # Roma
        417,  # Torino
        410,  # Udinese
        421,  # Venezia
        420,  # Empoli
        425,  # Monza
        # Historical
        443,  # Salernitana
        442,  # Frosinone
        1144, # Sampdoria
        1142, # Spezia
        427,  # Sassuolo
    ],
    'bundesliga': [
        446,  # Bayern München
        454,  # Bayer Leverkusen
        452,  # Borussia Dortmund
        450,  # Borussia Mönchengladbach
        455,  # Eintracht Frankfurt
        458,  # RB Leipzig
        457,  # VfB Stuttgart
        461,  # Union Berlin
        449,  # Werder Bremen
        447,  # SC Freiburg
        448,  # VfL Wolfsburg
        453,  # 1899 Hoffenheim
        451,  # FSV Mainz 05
        456,  # FC Augsburg
        819,  # Hoffenheim
        824,  # Freiburg
        827,  # Augsburg
        830,  # Wolfsburg
        832,  # Mainz 05
        833,  # St. Pauli
        834,  # FC Heidenheim
        463,  # Holstein Kiel
        483,  # 1. FC Köln
        1218, # Bochum
        # Historical
        1221, # Schalke 04
        1222, # Hertha BSC
        1358, # Darmstadt
    ],
    'ligue-1': [
        484,  # Angers
        498,  # Auxerre
        846,  # Brest
        499,  # Le Havre
        501,  # Lens
        485,  # Lille
        486,  # Lyon
        487,  # Marseille
        492,  # Monaco
        488,  # Montpellier
        489,  # Nantes
        490,  # Nice
        835,  # Paris Saint-Germain
        493,  # Reims
        494,  # Rennes
        495,  # Strasbourg
        496,  # Toulouse
        848,  # Paris FC
        515,  # Lorient
        500,  # Metz
        # Historical
        516,  # Clermont Foot
        1240, # AC Ajaccio
        1241, # Troyes
        1509, # Saint-Etienne
    ],
}


class SofaScoreClient:
    def __init__(self, rate_limit_delay: float = 1.5):
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.total_requests = 0
        self.failed_requests = 0

    def _get(self, url: str) -> Optional[Dict]:
        """Make GET request with rate limiting and error handling."""
        try:
            time.sleep(self.rate_limit_delay)
            response = self.session.get(url, timeout=15)
            self.total_requests += 1

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.debug(f"404 Not Found: {url}")
                return None
            elif response.status_code == 429:
                logger.warning("Rate limited — sleeping 30s")
                time.sleep(30)
                return self._get(url)
            else:
                logger.warning(f"HTTP {response.status_code}: {url}")
                self.failed_requests += 1
                return None

        except Exception as e:
            logger.error(f"Request failed: {url} — {e}")
            self.failed_requests += 1
            return None

    def get_league_team_ids(self, league: str, season_year: int) -> List[Dict]:
        """Get all team IDs for a league season.
        Tries standings API first, falls back to hardcoded IDs if 403."""
        league_id = LEAGUE_IDS.get(league)
        season_id = SEASON_IDS.get(league, {}).get(season_year)

        if not league_id or not season_id:
            logger.error(f"Unknown league/season: {league}/{season_year}")
            return []

        url = f"{BASE_URL}/unique-tournament/{league_id}/season/{season_id}/standings/total"
        try:
            time.sleep(self.rate_limit_delay)
            response = self.session.get(url, timeout=15)
            self.total_requests += 1

            if response.status_code == 200:
                data = response.json()
                if data and 'standings' in data:
                    teams = []
                    for row in data['standings'][0].get('rows', []):
                        team = row.get('team', {})
                        teams.append({
                            'team_id': team.get('id'),
                            'team_name': team.get('name'),
                            'team_slug': team.get('slug'),
                        })
                    logger.info(f"Got {len(teams)} teams from standings API: {league} {season_year}")
                    return teams

            # 403 or other error — use hardcoded fallback
            logger.warning(
                f"Standings API returned {response.status_code} for {league} {season_year}"
                " — using hardcoded team IDs"
            )

        except Exception as e:
            logger.warning(f"Standings API failed: {e} — using hardcoded team IDs")

        # Fallback: use hardcoded team IDs
        hardcoded_ids = LEAGUE_TEAM_IDS.get(league, [])
        if not hardcoded_ids:
            logger.error(f"No hardcoded team IDs for {league}")
            return []

        seen = set()
        teams = []
        for tid in hardcoded_ids:
            if tid not in seen:
                seen.add(tid)
                teams.append({
                    'team_id': tid,
                    'team_name': f'team_{tid}',  # placeholder, not used downstream
                    'team_slug': str(tid),
                })

        logger.info(f"Using {len(teams)} hardcoded team IDs for {league} {season_year}")
        return teams

    def get_team_player_ids(self, team_id: int) -> List[Dict]:
        """Get all player IDs from a team squad."""
        url = f"{BASE_URL}/team/{team_id}/players"
        data = self._get(url)

        if not data or 'players' not in data:
            return []

        players = []
        for p in data.get('players', []):
            player = p.get('player', {})
            players.append({
                'sofascore_id': player.get('id'),
                'name': player.get('name'),
                'short_name': player.get('shortName'),
                'position': player.get('position'),
                'slug': player.get('slug'),
            })

        return players

    def get_player_season_stats(
        self, player_id: int, league: str, season_year: int
    ) -> Optional[Dict]:
        """
        Get full stats (112 fields) for a player in a specific league season.
        Returns None if player has no stats for that season.
        """
        league_id = LEAGUE_IDS.get(league)
        season_id = SEASON_IDS.get(league, {}).get(season_year)

        if not league_id or not season_id:
            return None

        url = f"{BASE_URL}/player/{player_id}/unique-tournament/{league_id}/season/{season_id}/statistics/overall"
        data = self._get(url)

        if not data or 'statistics' not in data:
            return None

        stats = data['statistics']
        stats['sofascore_player_id'] = player_id
        stats['sofascore_league_id'] = league_id
        stats['sofascore_season_id'] = season_id
        return stats

    def get_all_league_players(
        self, league: str, season_year: int
    ) -> List[Dict]:
        """
        Full collection pipeline for one league/season.
        1. Get all teams from standings
        2. Get all players from each team squad
        3. Fetch stats for each player
        Returns list of player stat dicts.
        """
        logger.info(f"Starting SofaScore collection: {league} {season_year}")

        teams = self.get_league_team_ids(league, season_year)
        if not teams:
            logger.error(f"No teams found for {league} {season_year}")
            return []

        # Collect all unique players across all teams
        seen_player_ids = set()
        all_players = []

        for team in teams:
            team_id = team['team_id']
            players = self.get_team_player_ids(team_id)
            for p in players:
                pid = p['sofascore_id']
                if pid and pid not in seen_player_ids:
                    seen_player_ids.add(pid)
                    p['team_id'] = team_id
                    p['team_name'] = team['team_name']
                    all_players.append(p)

        logger.info(f"Found {len(all_players)} unique players for {league} {season_year}")

        # Fetch stats for each player
        results = []
        for i, player in enumerate(all_players):
            pid = player['sofascore_id']
            stats = self.get_player_season_stats(pid, league, season_year)
            if stats:
                stats['player_name'] = player['name']
                stats['player_position'] = player.get('position')
                stats['player_sofascore_id'] = pid
                stats['team_name'] = player['team_name']
                stats['team_sofascore_id'] = player['team_id']
                results.append(stats)

            if (i + 1) % 20 == 0:
                logger.info(f"  Progress: {i+1}/{len(all_players)} players")

        logger.info(
            f"Collected stats for {len(results)}/{len(all_players)} players "
            f"({self.total_requests} requests, {self.failed_requests} failures)"
        )
        return results
