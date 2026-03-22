"""
Retry Mechanism with Exponential Backoff for Football Data Pipeline.

Features:
- Configurable retry policies
- Exponential backoff with jitter
- Circuit breaker pattern
- Retry-specific exceptions
- Logging and metrics
"""

import time
import random
import logging
from functools import wraps
from typing import Callable, Tuple, Type, Optional, Any, Union, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import requests


logger = logging.getLogger(__name__)


# ============================================
# RETRY EXCEPTIONS
# ============================================

class RetryError(Exception):
    """Base retry error."""
    pass


class MaxRetriesExceeded(RetryError):
    """Raised when max retries have been exceeded."""

    def __init__(self, message: str, last_exception: Optional[Exception] = None):
        self.last_exception = last_exception
        super().__init__(message)


class CircuitBreakerOpen(RetryError):
    """Raised when circuit breaker is open."""
    pass


# ============================================
# RETRY CONFIGURATION
# ============================================

class BackoffStrategy(Enum):
    """Backoff strategies for retry."""
    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_factor: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1  # 10% jitter

    # Exceptions to retry on
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    )

    # HTTP status codes to retry on
    retryable_status_codes: Tuple[int, ...] = (
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    )

    # Whether to retry on rate limit
    retry_on_rate_limit: bool = True
    rate_limit_retry_delay: float = 60.0  # Wait for rate limit reset


# ============================================
# BACKOFF CALCULATION
# ============================================

def calculate_delay(
    attempt: int,
    config: RetryConfig
) -> float:
    """
    Calculate delay for the given attempt number.

    Args:
        attempt: Current attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    if config.backoff_strategy == BackoffStrategy.CONSTANT:
        delay = config.base_delay

    elif config.backoff_strategy == BackoffStrategy.LINEAR:
        delay = config.base_delay * (attempt + 1)

    elif config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
        delay = config.base_delay * (config.backoff_factor ** attempt)

    elif config.backoff_strategy == BackoffStrategy.FIBONACCI:
        # Fibonacci sequence for delay
        fib = [1, 1]
        for _ in range(attempt):
            fib.append(fib[-1] + fib[-2])
        delay = config.base_delay * fib[attempt]

    else:
        delay = config.base_delay

    # Apply max delay cap
    delay = min(delay, config.max_delay)

    # Apply jitter
    if config.jitter:
        jitter_amount = delay * config.jitter_factor
        delay = delay + random.uniform(-jitter_amount, jitter_amount)

    return max(0, delay)


# ============================================
# CIRCUIT BREAKER
# ============================================

class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    When errors exceed threshold, the circuit opens and
    rejects requests for a cooldown period.
    """
    failure_threshold: int = 5
    success_threshold: int = 2
    cooldown_seconds: float = 30.0

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: Optional[datetime] = field(default=None, init=False)

    @property
    def state(self) -> CircuitState:
        """Get current state, checking for cooldown expiry."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = (datetime.now() - self._last_failure_time).total_seconds()
                if elapsed >= self.cooldown_seconds:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    logger.info("Circuit breaker entering half-open state")
        return self._state

    def record_success(self):
        """Record a successful call."""
        self._failure_count = 0
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                logger.info("Circuit breaker closed after successful recovery")

    def record_failure(self):
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = datetime.now()
        self._success_count = 0

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning("Circuit breaker opened after half-open failure")
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                f"Circuit breaker opened after {self._failure_count} failures"
            )

    def can_execute(self) -> bool:
        """Check if request can be executed."""
        return self.state != CircuitState.OPEN

    def reset(self):
        """Reset circuit breaker state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None


# ============================================
# RETRY DECORATOR
# ============================================

def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    backoff_factor: float = 2.0,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
    circuit_breaker: Optional[CircuitBreaker] = None
):
    """
    Decorator to add retry logic to a function.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries
        max_delay: Maximum delay between retries
        backoff_strategy: Strategy for increasing delay
        backoff_factor: Factor for backoff calculation
        retryable_exceptions: Tuple of exceptions to retry on
        on_retry: Callback called on each retry (attempt, exception, delay)
        circuit_breaker: Optional circuit breaker instance

    Usage:
        @retry(max_retries=3, base_delay=1.0)
        def fetch_data():
            ...

        @retry(
            max_retries=5,
            retryable_exceptions=(requests.exceptions.Timeout,),
            on_retry=lambda a, e, d: print(f"Retry {a}: {e}")
        )
        def api_call():
            ...
    """
    if retryable_exceptions is None:
        retryable_exceptions = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            ConnectionError,
            TimeoutError,
        )

    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_strategy=backoff_strategy,
        backoff_factor=backoff_factor,
        retryable_exceptions=retryable_exceptions
    )

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Check circuit breaker
            if circuit_breaker and not circuit_breaker.can_execute():
                raise CircuitBreakerOpen(
                    f"Circuit breaker is open for {func.__name__}"
                )

            last_exception: Optional[Exception] = None

            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)

                    # Record success for circuit breaker
                    if circuit_breaker:
                        circuit_breaker.record_success()

                    return result

                except retryable_exceptions as e:
                    last_exception = e

                    # Record failure for circuit breaker
                    if circuit_breaker:
                        circuit_breaker.record_failure()

                    # Check if we have retries left
                    if attempt >= max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}",
                            extra={
                                "function": func.__name__,
                                "attempts": attempt + 1,
                                "last_error": str(e)
                            }
                        )
                        raise MaxRetriesExceeded(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {e}",
                            last_exception=e
                        )

                    # Calculate delay
                    delay = calculate_delay(attempt, config)

                    # Check for rate limit (429)
                    if (
                        config.retry_on_rate_limit
                        and isinstance(e, requests.exceptions.HTTPError)
                        and hasattr(e, 'response')
                        and e.response is not None
                        and e.response.status_code == 429
                    ):
                        # Try to get retry-after header
                        retry_after = e.response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = config.rate_limit_retry_delay
                        else:
                            delay = config.rate_limit_retry_delay

                        logger.warning(
                            f"Rate limited, waiting {delay}s before retry",
                            extra={"delay": delay}
                        )

                    # Log retry
                    logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                        f"after {delay:.2f}s: {type(e).__name__}: {e}",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "delay": delay,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        }
                    )

                    # Call on_retry callback
                    if on_retry:
                        on_retry(attempt + 1, e, delay)

                    # Wait before retry
                    time.sleep(delay)

            # Should not reach here, but just in case
            raise MaxRetriesExceeded(
                f"Max retries exceeded for {func.__name__}",
                last_exception=last_exception
            )

        return wrapper
    return decorator


# ============================================
# RETRY CONTEXT MANAGER
# ============================================

class RetryContext:
    """
    Context manager for retry logic.

    Usage:
        with RetryContext(max_retries=3) as ctx:
            for attempt in ctx:
                try:
                    result = risky_operation()
                    break
                except Exception as e:
                    ctx.record_failure(e)
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
        retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None
    ):
        self.config = RetryConfig(
            max_retries=max_retries,
            base_delay=base_delay,
            backoff_strategy=backoff_strategy,
            retryable_exceptions=retryable_exceptions or (Exception,)
        )
        self._attempt = 0
        self._last_exception: Optional[Exception] = None
        self._should_continue = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __iter__(self):
        return self

    def __next__(self) -> int:
        if not self._should_continue or self._attempt > self.config.max_retries:
            raise StopIteration

        if self._attempt > 0 and self._last_exception:
            delay = calculate_delay(self._attempt - 1, self.config)
            logger.info(f"Waiting {delay:.2f}s before retry {self._attempt}")
            time.sleep(delay)

        current_attempt = self._attempt
        self._attempt += 1
        return current_attempt

    def record_failure(self, exception: Exception):
        """Record a failure for retry consideration."""
        if isinstance(exception, self.config.retryable_exceptions):
            self._last_exception = exception
            if self._attempt > self.config.max_retries:
                raise MaxRetriesExceeded(
                    f"Max retries ({self.config.max_retries}) exceeded",
                    last_exception=exception
                )
        else:
            self._should_continue = False
            raise exception

    def success(self):
        """Mark the operation as successful, stopping retries."""
        self._should_continue = False


# ============================================
# API-SPECIFIC RETRY HELPERS
# ============================================

# Global circuit breakers for different APIs
_circuit_breakers: dict = {}


def get_circuit_breaker(api_name: str) -> CircuitBreaker:
    """Get or create circuit breaker for an API."""
    if api_name not in _circuit_breakers:
        _circuit_breakers[api_name] = CircuitBreaker(
            failure_threshold=5,
            success_threshold=2,
            cooldown_seconds=60.0
        )
    return _circuit_breakers[api_name]


def api_retry(
    api_name: str = "default",
    max_retries: int = 3,
    base_delay: float = 1.0,
    use_circuit_breaker: bool = True
):
    """
    Decorator specifically for API calls with sensible defaults.

    Args:
        api_name: Name of the API (for circuit breaker isolation)
        max_retries: Maximum retry attempts
        base_delay: Base delay between retries
        use_circuit_breaker: Whether to use circuit breaker

    Usage:
        @api_retry(api_name="api_football")
        def fetch_teams():
            ...
    """
    circuit_breaker = get_circuit_breaker(api_name) if use_circuit_breaker else None

    return retry(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=120.0,
        backoff_strategy=BackoffStrategy.EXPONENTIAL,
        backoff_factor=2.0,
        retryable_exceptions=(
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            ConnectionError,
            TimeoutError,
        ),
        circuit_breaker=circuit_breaker
    )


def retry_api_call(
    func: Callable,
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    **kwargs
) -> Any:
    """
    Utility function to retry an API call.

    Args:
        func: Function to call
        *args: Positional arguments for func
        max_retries: Maximum retry attempts
        base_delay: Base delay between retries
        **kwargs: Keyword arguments for func

    Returns:
        Result of the function call
    """
    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        backoff_strategy=BackoffStrategy.EXPONENTIAL
    )

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except config.retryable_exceptions as e:
            last_exception = e

            if attempt >= max_retries:
                raise MaxRetriesExceeded(
                    f"Max retries ({max_retries}) exceeded",
                    last_exception=e
                )

            delay = calculate_delay(attempt, config)
            logger.warning(f"Retry {attempt + 1}/{max_retries} after {delay:.2f}s: {e}")
            time.sleep(delay)

    raise MaxRetriesExceeded(
        f"Max retries ({max_retries}) exceeded",
        last_exception=last_exception
    )
