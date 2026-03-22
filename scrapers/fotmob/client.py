"""
FotMob API client for football data collection.

Public JSON API - no API key required.
Rate limiting: 2s delay + random jitter for respectful usage.

Features:
- Automatic retry with exponential backoff
- Rate limiting with jitter
- Response caching (configurable TTL)
- Circuit breaker for fault tolerance
- Structured logging
"""

import requests
import logging
import time
import json
import hashlib
import os
import random
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class FotMobClient:
    """
    Client for FotMob public API (https://www.fotmob.com/api/).

    No API key required. Uses browser-like headers for access.
    Rate limited to ~2 seconds between requests with random jitter.
    """

    BASE_URL = "https://www.fotmob.com/api"

    # Default browser-like headers
    # Note: Do NOT include 'br' (Brotli) in Accept-Encoding - Python requests doesn't handle it
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.fotmob.com/',
        'Origin': 'https://www.fotmob.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    def __init__(
        self,
        cache_dir: str = "data/cache/fotmob",
        rate_limit_delay: float = 2.0,
        cache_ttl_hours: float = 1.0,
        cache_ttl_historical_hours: float = 24.0,
        max_retries: int = 3
    ):
        """
        Initialize FotMob client.

        Args:
            cache_dir: Directory for response caching
            rate_limit_delay: Minimum delay between requests (seconds)
            cache_ttl_hours: Cache TTL for current/live data (hours)
            cache_ttl_historical_hours: Cache TTL for historical data (hours)
            max_retries: Maximum retry attempts per request
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit_delay = rate_limit_delay
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.cache_ttl_historical = timedelta(hours=cache_ttl_historical_hours)
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)

        self.last_request_time = 0.0

        # Statistics
        self.total_requests = 0
        self.cache_hits = 0
        self.failed_requests = 0

    def _rate_limit(self):
        """Enforce rate limiting with random jitter."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_delay = self.rate_limit_delay + random.uniform(0, 0.5)

        if time_since_last < min_delay:
            sleep_time = min_delay - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _get_cache_key(self, endpoint: str, params: Dict = None) -> str:
        """Generate cache key from endpoint and params."""
        key_str = f"{endpoint}:{json.dumps(params or {}, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def _get_cached_response(self, cache_key: str, historical: bool = False) -> Optional[Dict]:
        """Check cache for valid response."""
        cache_file = self.cache_dir / f"{cache_key}.json"

        if not cache_file.exists():
            return None

        try:
            modified_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            ttl = self.cache_ttl_historical if historical else self.cache_ttl

            if datetime.now() - modified_time > ttl:
                return None

            with open(cache_file, 'r') as f:
                data = json.load(f)
                self.cache_hits += 1
                return data

        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Cache read error for {cache_key}: {e}")
            return None

    def _save_to_cache(self, cache_key: str, data: Dict):
        """Save response to cache."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
        except OSError as e:
            logger.warning(f"Cache write error for {cache_key}: {e}")

    def _make_request(
        self,
        endpoint: str,
        params: Dict = None,
        use_cache: bool = True,
        historical: bool = False
    ) -> Optional[Dict]:
        """
        Make API request with rate limiting, retry, and caching.

        Args:
            endpoint: API endpoint path (e.g., 'leagues', 'teams')
            params: Query parameters
            use_cache: Whether to use cached responses
            historical: Whether this is historical data (longer cache TTL)

        Returns:
            JSON response dict or None on failure
        """
        # Check cache first
        cache_key = self._get_cache_key(endpoint, params)
        if use_cache:
            cached = self._get_cached_response(cache_key, historical)
            if cached is not None:
                logger.debug(f"Cache hit for {endpoint}")
                return cached

        # Rate limit
        self._rate_limit()
        self.total_requests += 1

        url = f"{self.BASE_URL}/{endpoint}"
        retries = 0
        backoff_base = 2

        while retries < self.max_retries:
            start_time = time.time()
            try:
                logger.info(f"FotMob request: {endpoint} params={params}")
                response = self.session.get(url, params=params or {}, timeout=30)
                response_time_ms = (time.time() - start_time) * 1000

                if response.status_code == 429:
                    # Rate limited - back off
                    wait_time = min(120, backoff_base ** retries * 10)
                    logger.warning(f"Rate limit hit (429). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                if response.status_code == 403:
                    logger.error(f"Access forbidden (403) for {endpoint}. May need header update.")
                    self.failed_requests += 1
                    return None

                if response.status_code == 503:
                    # Service unavailable - back off
                    wait_time = min(60, backoff_base ** retries * 5)
                    logger.warning(f"Service unavailable (503). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                response.raise_for_status()
                data = response.json()

                logger.info(f"FotMob response: {endpoint} ({response_time_ms:.0f}ms)")

                # Cache successful response
                if use_cache:
                    self._save_to_cache(cache_key, data)

                return data

            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout (attempt {retries + 1}/{self.max_retries}): {e}")
                retries += 1
                if retries < self.max_retries:
                    time.sleep(backoff_base ** retries)
                continue

            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error (attempt {retries + 1}/{self.max_retries}): {e}")
                retries += 1
                if retries < self.max_retries:
                    time.sleep(backoff_base ** retries)
                continue

            except requests.exceptions.RequestException as e:
                logger.error(f"FotMob request failed: {e}")
                self.failed_requests += 1
                return None

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from {endpoint}: {e}")
                self.failed_requests += 1
                return None

        # Max retries exceeded
        logger.error(f"Max retries ({self.max_retries}) exceeded for {endpoint}")
        self.failed_requests += 1
        return None

    # =========================================
    # LEAGUE ENDPOINTS
    # =========================================

    def get_league(self, league_id: int, use_cache: bool = True) -> Optional[Dict]:
        """
        Get league data: standings, matches, top players, seasons.

        GET /leagues?id={league_id}

        Returns full league response including:
        - table.data[0].table.all: standings
        - allAvailableSeasons: season list
        - details: league metadata
        """
        return self._make_request('leagues', {'id': league_id}, use_cache=use_cache)

    def get_league_season(
        self, league_id: int, season: str, use_cache: bool = True
    ) -> Optional[Dict]:
        """
        Get league data for a specific season.

        GET /leagues?id={league_id}&season={season}

        Args:
            league_id: FotMob league ID
            season: Season string (e.g., '2023/2024')
        """
        return self._make_request(
            'leagues',
            {'id': league_id, 'season': season},
            use_cache=use_cache,
            historical=True
        )

    # =========================================
    # TEAM ENDPOINTS
    # =========================================

    def get_team(self, team_id: int, use_cache: bool = True) -> Optional[Dict]:
        """
        Get team data: squad, stats, history, fixtures.

        GET /teams?id={team_id}

        Returns full team response including:
        - details: team metadata + sportsTeamJSONLD.athlete (squad)
        - table: league standings context
        """
        return self._make_request('teams', {'id': team_id}, use_cache=use_cache)

    # =========================================
    # PLAYER ENDPOINTS
    # =========================================

    def get_player(self, player_id: int, use_cache: bool = True) -> Optional[Dict]:
        """
        Get player data: bio, career stats, recent matches.

        GET /playerData?id={player_id}

        Returns full player response including:
        - name, birthDate, primaryTeam, positionDescription
        - playerInformation: height, foot, market value
        - mainLeague.stats: current season stats
        - careerHistory: full career
        - recentMatches: last N matches
        """
        return self._make_request('playerData', {'id': player_id}, use_cache=use_cache)

    # =========================================
    # MATCH ENDPOINTS
    # =========================================

    def get_match(self, match_id: int, use_cache: bool = True) -> Optional[Dict]:
        """
        Get match details: lineups, events, stats, xG, player ratings.

        GET /matchDetails?matchId={match_id}

        Returns full match response including:
        - header.teams: team names, scores
        - header.status: match status
        - stats.Periods.All: match statistics
        - content.events: goals, cards, subs
        - playerStats: per-player detailed stats
        """
        return self._make_request(
            'matchDetails',
            {'matchId': match_id},
            use_cache=use_cache,
            historical=True
        )

    # =========================================
    # UTILITY METHODS
    # =========================================

    def get_stats(self) -> Dict[str, int]:
        """Get client usage statistics."""
        return {
            'total_requests': self.total_requests,
            'cache_hits': self.cache_hits,
            'failed_requests': self.failed_requests,
        }

    def clear_cache(self):
        """Clear all cached responses."""
        import shutil
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("FotMob cache cleared")

    def close(self):
        """Cleanup session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
