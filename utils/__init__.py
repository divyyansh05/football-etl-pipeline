"""
Utility modules for the Football Data ETL Pipeline.

Includes:
- api_tracker: API usage tracking and quota management
- logging_config: Production-grade structured logging
- validators: Data validation framework
- retry: Retry mechanism with exponential backoff
- monitoring: Health checks and alerting system
"""

from .api_tracker import APITracker, get_tracker
from .logging_config import (
    setup_logging,
    get_logger,
    log_execution_time,
    log_etl_operation,
    ETLJobLogger,
    APIRequestLogger,
    LogContext
)
from .validators import (
    ValidationError,
    ValidationResult,
    validate_entity,
    validate_batch,
    clean_and_validate,
    TeamSchema,
    PlayerSchema,
    MatchSchema,
    PlayerStatsSchema
)
from .retry import (
    retry,
    api_retry,
    retry_api_call,
    RetryConfig,
    RetryContext,
    BackoffStrategy,
    CircuitBreaker,
    MaxRetriesExceeded,
    CircuitBreakerOpen,
    get_circuit_breaker
)
from .monitoring import (
    AlertManager,
    AlertSeverity,
    Alert,
    HealthMonitor,
    HealthStatus,
    HealthCheckResult,
    get_alert_manager,
    get_health_monitor,
    get_metrics_collector,
    MetricsCollector
)

__all__ = [
    # API Tracker
    'APITracker',
    'get_tracker',

    # Logging
    'setup_logging',
    'get_logger',
    'log_execution_time',
    'log_etl_operation',
    'ETLJobLogger',
    'APIRequestLogger',
    'LogContext',

    # Validators
    'ValidationError',
    'ValidationResult',
    'validate_entity',
    'validate_batch',
    'clean_and_validate',
    'TeamSchema',
    'PlayerSchema',
    'MatchSchema',
    'PlayerStatsSchema',

    # Retry
    'retry',
    'api_retry',
    'retry_api_call',
    'RetryConfig',
    'RetryContext',
    'BackoffStrategy',
    'CircuitBreaker',
    'MaxRetriesExceeded',
    'CircuitBreakerOpen',
    'get_circuit_breaker',

    # Monitoring
    'AlertManager',
    'AlertSeverity',
    'Alert',
    'HealthMonitor',
    'HealthStatus',
    'HealthCheckResult',
    'get_alert_manager',
    'get_health_monitor',
    'get_metrics_collector',
    'MetricsCollector'
]
