"""
API Usage Tracking Module

Tracks API requests to ensure we stay within rate limits.
Integrates with the database schema for persistent tracking.
"""

import logging
from datetime import datetime, date
from typing import Optional, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class APITracker:
    """
    Tracks API usage for rate limit management.

    Features:
    - In-memory tracking for current session
    - Database persistence for historical tracking
    - Quota checking before requests
    - Request logging for debugging
    """

    def __init__(
        self,
        source_name: str = 'api_football',
        daily_limit: int = 100,
        use_db: bool = True
    ):
        """
        Initialize API tracker.

        Args:
            source_name: Identifier for the API source
            daily_limit: Maximum requests allowed per day
            use_db: Whether to persist tracking to database
        """
        self.source_name = source_name
        self.daily_limit = daily_limit
        self.use_db = use_db

        # In-memory tracking for current session
        self._session_requests = 0
        self._session_start = datetime.now()
        self._request_log = []

        # Database connection (lazy loaded)
        self._db = None

    @property
    def db(self):
        """Lazy load database connection."""
        if self._db is None and self.use_db:
            try:
                from database.connection import get_db
                self._db = get_db()
            except Exception as e:
                logger.warning(f"Could not connect to database: {e}")
                self.use_db = False
        return self._db

    def get_usage_today(self) -> int:
        """
        Get total requests made today.

        Returns:
            Number of requests made today
        """
        if self.use_db and self.db:
            try:
                result = self.db.execute_query(
                    """
                    SELECT COALESCE(requests_count, 0)
                    FROM api_usage_tracking
                    WHERE tracking_date = CURRENT_DATE
                    AND source_name = :source
                    """,
                    {'source': self.source_name}
                )
                if result:
                    return result[0][0]
            except Exception as e:
                logger.warning(f"Could not get usage from database: {e}")

        return self._session_requests

    def get_remaining_requests(self) -> int:
        """
        Get remaining requests for today.

        Returns:
            Number of requests remaining
        """
        used = self.get_usage_today()
        return max(0, self.daily_limit - used)

    def get_requests_today(self) -> int:
        """
        Alias for get_usage_today() for consistency.

        Returns:
            Number of requests made today
        """
        return self.get_usage_today()

    def can_make_request(self, count: int = 1) -> bool:
        """
        Check if we can make the specified number of requests.

        Args:
            count: Number of requests to check

        Returns:
            True if requests can be made within limit
        """
        return self.get_remaining_requests() >= count

    def record_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        response_status: Optional[int] = None,
        response_time_ms: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> int:
        """
        Record an API request.

        Args:
            endpoint: API endpoint called
            params: Request parameters
            response_status: HTTP status code
            response_time_ms: Response time in milliseconds
            error_message: Error message if request failed

        Returns:
            New total request count for today
        """
        self._session_requests += 1

        # Log to in-memory list
        self._request_log.append({
            'timestamp': datetime.now(),
            'endpoint': endpoint,
            'params': params,
            'status': response_status,
            'time_ms': response_time_ms,
            'error': error_message
        })

        # Persist to database
        if self.use_db and self.db:
            try:
                # Update daily count
                self.db.execute_query(
                    """
                    INSERT INTO api_usage_tracking (tracking_date, source_name, endpoint, requests_count)
                    VALUES (CURRENT_DATE, :source, :endpoint, 1)
                    ON CONFLICT (tracking_date, source_name)
                    DO UPDATE SET
                        requests_count = api_usage_tracking.requests_count + 1,
                        last_request_time = CURRENT_TIMESTAMP,
                        endpoint = COALESCE(EXCLUDED.endpoint, api_usage_tracking.endpoint)
                    """,
                    {'source': self.source_name, 'endpoint': endpoint},
                    fetch=False
                )

                # Log detailed request
                import json
                self.db.execute_query(
                    """
                    INSERT INTO api_request_log
                    (source_name, endpoint, params, response_status, response_time_ms, error_message)
                    VALUES (:source, :endpoint, :params, :status, :time_ms, :error)
                    """,
                    {
                        'source': self.source_name,
                        'endpoint': endpoint,
                        'params': json.dumps(params) if params else None,
                        'status': response_status,
                        'time_ms': response_time_ms,
                        'error': error_message
                    },
                    fetch=False
                )
            except Exception as e:
                logger.warning(f"Could not log request to database: {e}")

        return self.get_usage_today()

    def get_session_stats(self) -> Dict[str, Any]:
        """
        Get statistics for current session.

        Returns:
            Dictionary with session statistics
        """
        return {
            'session_requests': self._session_requests,
            'session_start': self._session_start.isoformat(),
            'session_duration_minutes': (datetime.now() - self._session_start).total_seconds() / 60,
            'total_today': self.get_usage_today(),
            'remaining': self.get_remaining_requests(),
            'daily_limit': self.daily_limit,
            'quota_percentage_used': (self.get_usage_today() / self.daily_limit) * 100
        }

    def get_request_log(self, limit: int = 50) -> list:
        """
        Get recent request log.

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of recent request entries
        """
        return self._request_log[-limit:]

    def estimate_cost(
        self,
        leagues: int = 1,
        include_fixtures: bool = True,
        include_players: bool = False,
        include_standings: bool = True
    ) -> int:
        """
        Estimate the number of API requests needed.

        Args:
            leagues: Number of leagues to process
            include_fixtures: Whether fixtures will be collected
            include_players: Whether player data will be collected
            include_standings: Whether standings will be collected

        Returns:
            Estimated number of API requests
        """
        cost = 0

        # Per league costs
        if include_standings:
            cost += 1  # Standings endpoint (includes teams)

        if include_fixtures:
            cost += 1  # Fixtures endpoint

        if include_players:
            # Player endpoint is paginated, ~2 requests per team
            # Assume 20 teams per league
            cost += 40

        return cost * leagues

    @contextmanager
    def track_request(self, endpoint: str, params: Optional[Dict] = None):
        """
        Context manager for tracking a request with timing.

        Usage:
            with tracker.track_request('standings', {'league': 39}) as req:
                response = client.get_standings(39, 2024)
                req['status'] = 200
        """
        import time

        request_info = {
            'endpoint': endpoint,
            'params': params,
            'status': None,
            'error': None
        }

        start_time = time.time()

        try:
            yield request_info
        except Exception as e:
            request_info['error'] = str(e)
            raise
        finally:
            elapsed_ms = int((time.time() - start_time) * 1000)
            self.record_request(
                endpoint=endpoint,
                params=params,
                response_status=request_info.get('status'),
                response_time_ms=elapsed_ms,
                error_message=request_info.get('error')
            )

    def check_and_warn(self, required_requests: int = 1) -> bool:
        """
        Check quota and log warning if low.

        Args:
            required_requests: Number of requests needed

        Returns:
            True if quota is sufficient, False otherwise
        """
        remaining = self.get_remaining_requests()

        if remaining < required_requests:
            logger.error(
                f"Insufficient API quota: need {required_requests}, have {remaining}"
            )
            return False

        if remaining <= 10:
            logger.warning(f"API quota critically low: {remaining} requests remaining")
        elif remaining <= 30:
            logger.warning(f"API quota getting low: {remaining} requests remaining")

        return True

    def reset_session(self):
        """Reset session tracking (doesn't affect database totals)."""
        self._session_requests = 0
        self._session_start = datetime.now()
        self._request_log = []


# Global tracker instance
_tracker_instance = None


def get_tracker(
    source_name: str = 'api_football',
    daily_limit: int = 100
) -> APITracker:
    """
    Get or create global API tracker instance.

    Args:
        source_name: API source identifier
        daily_limit: Daily request limit

    Returns:
        APITracker instance
    """
    global _tracker_instance

    if _tracker_instance is None:
        _tracker_instance = APITracker(
            source_name=source_name,
            daily_limit=daily_limit
        )

    return _tracker_instance
