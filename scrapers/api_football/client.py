"""
API-Football client for fetching real football data.
Free tier: 100 requests/day

Features:
- Automatic retry with exponential backoff
- Rate limiting (1 req/sec)
- Structured logging
- Circuit breaker for fault tolerance
- API usage tracking
"""

import requests
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

# Import utilities
try:
    from utils.retry import api_retry, get_circuit_breaker, MaxRetriesExceeded
    from utils.logging_config import get_logger, APIRequestLogger
    from utils.monitoring import get_metrics_collector
    UTILS_AVAILABLE = True
except ImportError:
    UTILS_AVAILABLE = False

logger = logging.getLogger(__name__)


class APIFootballClient:
    """
    Client for API-Football (api-football-v1.p.rapidapi.com)

    Features:
    - Automatic retry with exponential backoff
    - Rate limiting (1 request per second)
    - Circuit breaker for fault tolerance
    - Structured logging with request/response metrics
    - API usage tracking
    """

    def __init__(self, api_key: Optional[str] = None, use_direct_api: bool = True):
        """
        Initialize API-Football client.

        Args:
            api_key: API Key
            use_direct_api: If True, use direct api-sports.io, else use rapidapi.com
        """
        self.api_key = api_key or "DEMO_KEY"
        self.use_direct_api = use_direct_api

        if self.use_direct_api:
            self.base_url = "https://v3.football.api-sports.io"
            self.headers = {
                "x-apisports-key": self.api_key
            }
        else:
            self.base_url = "https://api-football-v1.p.rapidapi.com/v3"
            self.headers = {
                "X-RapidAPI-Key": self.api_key,
                "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
            }

        self.last_request_time = 0
        self.rate_limit_delay = 1  # 1 second between requests

        # Initialize utilities if available
        if UTILS_AVAILABLE:
            self.circuit_breaker = get_circuit_breaker("api_football")
            self.api_logger = APIRequestLogger("api_football")
            self.metrics = get_metrics_collector()
        else:
            self.circuit_breaker = None
            self.api_logger = None
            self.metrics = None

        # Request statistics
        self.total_requests = 0
        self.failed_requests = 0
        
    def _rate_limit(self):
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)

        self.last_request_time = time.time()

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows request."""
        if self.circuit_breaker and not self.circuit_breaker.can_execute():
            logger.warning("Circuit breaker is open - rejecting request")
            return False
        return True

    def _record_success(self):
        """Record successful request for circuit breaker."""
        if self.circuit_breaker:
            self.circuit_breaker.record_success()

    def _record_failure(self):
        """Record failed request for circuit breaker."""
        self.failed_requests += 1
        if self.circuit_breaker:
            self.circuit_breaker.record_failure()

    def _log_request(self, endpoint: str, params: Dict, response_time_ms: float,
                     status_code: int, records: int = 0, error: str = None):
        """Log request details."""
        if self.api_logger:
            self.api_logger.log_request(
                endpoint=endpoint,
                params=params,
                response_status=status_code,
                response_time_ms=response_time_ms,
                records_returned=records,
                error=error
            )
        if self.metrics:
            self.metrics.record("api_football_response_time_ms", response_time_ms, endpoint=endpoint)
            self.metrics.record("api_football_requests", 1, endpoint=endpoint)

    def _make_request(self, endpoint: str, params: Dict = None, return_full: bool = False) -> Optional[Dict]:
        """
        Make API request with rate limiting, retry, and circuit breaker.

        Args:
            endpoint: API endpoint (e.g., 'leagues', 'teams')
            params: Query parameters
            return_full: Unused, kept for backwards compatibility

        Returns:
            Full response dict or None on failure
        """
        # Check circuit breaker first
        if not self._check_circuit_breaker():
            return None

        self._rate_limit()
        self.total_requests += 1

        url = f"{self.base_url}/{endpoint}"
        retries = 0
        max_retries = 3
        backoff_base = 2  # Exponential backoff base

        while retries < max_retries:
            start_time = time.time()
            try:
                logger.info(f"API request: {endpoint} with params {params}")
                response = requests.get(url, headers=self.headers, params=params or {}, timeout=30)
                response_time_ms = (time.time() - start_time) * 1000

                if response.status_code == 429:
                    # Rate limit - use exponential backoff
                    wait_time = min(60, backoff_base ** retries * 10)
                    logger.warning(f"Rate limit hit (429). Waiting {wait_time} seconds...")
                    self._log_request(endpoint, params, response_time_ms, 429, error="Rate limit")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                response.raise_for_status()
                data = response.json()

                # Check for functional errors in 200 OK response
                if data.get('errors'):
                    if isinstance(data['errors'], dict) and 'rateLimit' in data['errors']:
                        wait_time = min(60, backoff_base ** retries * 10)
                        logger.warning(f"Rate limit hit (body). Waiting {wait_time} seconds...")
                        self._log_request(endpoint, params, response_time_ms, 200, error="Rate limit in body")
                        time.sleep(wait_time)
                        retries += 1
                        continue

                    if data['errors']:
                        logger.error(f"API errors: {data['errors']}")
                        self._log_request(endpoint, params, response_time_ms, 200, error=str(data['errors']))
                        self._record_failure()
                        return None

                # Success
                records = len(data.get('response', [])) if isinstance(data.get('response'), list) else 1
                self._log_request(endpoint, params, response_time_ms, response.status_code, records)
                self._record_success()
                return data

            except requests.exceptions.Timeout as e:
                response_time_ms = (time.time() - start_time) * 1000
                logger.warning(f"Request timeout (attempt {retries + 1}/{max_retries}): {e}")
                self._log_request(endpoint, params, response_time_ms, 0, error="Timeout")
                retries += 1
                if retries < max_retries:
                    wait_time = backoff_base ** retries
                    time.sleep(wait_time)
                continue

            except requests.exceptions.ConnectionError as e:
                response_time_ms = (time.time() - start_time) * 1000
                logger.warning(f"Connection error (attempt {retries + 1}/{max_retries}): {e}")
                self._log_request(endpoint, params, response_time_ms, 0, error="Connection error")
                retries += 1
                if retries < max_retries:
                    wait_time = backoff_base ** retries
                    time.sleep(wait_time)
                continue

            except requests.exceptions.RequestException as e:
                response_time_ms = (time.time() - start_time) * 1000
                logger.error(f"API request failed: {e}")
                self._log_request(endpoint, params, response_time_ms, 0, error=str(e))
                self._record_failure()
                return None

        # Max retries exceeded
        logger.error(f"Max retries ({max_retries}) exceeded for {endpoint}")
        self._record_failure()
        return None

    def get_leagues(self, country: str = None) -> Optional[List[Dict]]:
        """Get available leagues."""
        params = {}
        if country:
            params['country'] = country
        
        data = self._make_request('leagues', params)
        return data.get('response', []) if data else None
    
    def get_teams(self, league_id: int, season: int) -> Optional[List[Dict]]:
        """
        Get teams for a league/season.
        
        Args:
            league_id: League ID (e.g., 39 for Premier League)
            season: Season year (e.g., 2024)
        """
        params = {
            'league': league_id,
            'season': season
        }
        
        data = self._make_request('teams', params)
        return data.get('response', []) if data else None
    
    def get_standings(self, league_id: int, season: int) -> Optional[List[Dict]]:
        """Get league standings."""
        params = {
            'league': league_id,
            'season': season
        }
        
        data = self._make_request('standings', params)
        return data.get('response', []) if data else None
    
    def get_fixtures(self, league_id: int, season: int) -> Optional[List[Dict]]:
        """Get fixtures/matches for a league/season."""
        params = {
            'league': league_id,
            'season': season
        }
        
        data = self._make_request('fixtures', params)
        return data.get('response', []) if data else None
    
    def get_fixture_statistics(self, fixture_id: int) -> Optional[Dict]:
        """Get statistics for a specific fixture."""
        params = {'fixture': fixture_id}
        
        data = self._make_request('fixtures/statistics', params)
        return data.get('response', []) if data else None

    def get_players(self, season: int, league_id: Optional[int] = None, team_id: Optional[int] = None, page: int = 1) -> Optional[Dict]:
        """
        Get players for a league OR team/season with pagination info.
        Return full response dict.
        """
        params = {
            'season': season,
            'page': page
        }
        if league_id:
            params['league'] = league_id
        if team_id:
            params['team'] = team_id
            
        # We need the full response to see 'paging'
        # This method duplicates some logic from _make_request to handle the full response directly.
        self._rate_limit()
        url = f"{self.base_url}/players"
        
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                logger.info(f"API request: players {params}")
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code == 429:
                    logger.warning("Rate limit hit (429). Waiting 60 seconds...")
                    time.sleep(60)
                    retries += 1
                    continue
                    
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('errors'):
                    if isinstance(data['errors'], dict) and 'rateLimit' in data['errors']:
                         logger.warning("Rate limit hit (body). Waiting 60 seconds...")
                         time.sleep(60)
                         retries += 1
                         continue
                    if data['errors']: 
                         logger.error(f"API errors: {data['errors']}")
                         return None
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                return None
        return None
    
    def get_top_scorers(self, league_id: int, season: int) -> Optional[List[Dict]]:
        """Get top scorers for a league/season."""
        params = {
            'league': league_id,
            'season': season
        }
        
        data = self._make_request('players/topscorers', params)
        return data.get('response', []) if data else None


# League IDs for top 5 European leagues
LEAGUE_IDS = {
    'premier-league': 39,
    'la-liga': 140,
    'serie-a': 135,
    'bundesliga': 78,
    'ligue-1': 61
}
