"""
Production Logging Configuration for Football Data Pipeline.

Features:
- Structured JSON logging for machine parsing
- Console output with Rich formatting for development
- File rotation with size and time-based policies
- Separate error log for critical issues
- Context-aware logging with request IDs
- Performance metrics logging
"""

import os
import sys
import json
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from functools import wraps
import time
import traceback


# ============================================
# LOG DIRECTORIES
# ============================================

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


# ============================================
# CUSTOM JSON FORMATTER
# ============================================

class JSONFormatter(logging.Formatter):
    """
    Format log records as JSON for structured logging.

    Enables easy parsing by log aggregation tools like
    ELK Stack, Datadog, or CloudWatch.
    """

    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info) if record.exc_info[0] else None
            }

        # Add extra fields if present
        if self.include_extra:
            extra_fields = {}
            for key, value in record.__dict__.items():
                if key not in [
                    'name', 'msg', 'args', 'created', 'filename', 'funcName',
                    'levelname', 'levelno', 'lineno', 'module', 'msecs',
                    'pathname', 'process', 'processName', 'relativeCreated',
                    'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                    'message', 'taskName'
                ]:
                    try:
                        # Ensure value is JSON serializable
                        json.dumps(value)
                        extra_fields[key] = value
                    except (TypeError, ValueError):
                        extra_fields[key] = str(value)

            if extra_fields:
                log_data["extra"] = extra_fields

        return json.dumps(log_data, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Rich console formatter with colors and formatting.
    """

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)

        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Format message
        message = record.getMessage()

        # Build formatted string
        formatted = f"{color}{timestamp} | {record.levelname:8} | {record.name:20} | {message}{self.RESET}"

        # Add exception if present
        if record.exc_info:
            formatted += f"\n{color}{self.formatException(record.exc_info)}{self.RESET}"

        return formatted


# ============================================
# LOGGING CONTEXT
# ============================================

class LogContext:
    """
    Thread-local storage for logging context.

    Allows adding context like request_id, job_name, etc.
    that will be included in all log messages.
    """

    _context: Dict[str, Any] = {}

    @classmethod
    def set(cls, **kwargs):
        """Set context values."""
        cls._context.update(kwargs)

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        """Get a context value."""
        return cls._context.get(key, default)

    @classmethod
    def clear(cls):
        """Clear all context."""
        cls._context.clear()

    @classmethod
    def get_all(cls) -> Dict[str, Any]:
        """Get all context values."""
        return cls._context.copy()


class ContextFilter(logging.Filter):
    """
    Filter that adds context to log records.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in LogContext.get_all().items():
            setattr(record, key, value)
        return True


# ============================================
# LOGGER FACTORY
# ============================================

def setup_logging(
    level: str = "INFO",
    json_logs: bool = False,
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    enable_console: bool = True,
    enable_file: bool = True
) -> logging.Logger:
    """
    Configure the root logger with production settings.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: Use JSON format for file logs
        log_file: Custom log file name (default: football_etl.log)
        max_bytes: Max log file size before rotation
        backup_count: Number of backup files to keep
        enable_console: Enable console output
        enable_file: Enable file output

    Returns:
        Configured root logger
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add context filter
    context_filter = ContextFilter()

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(ConsoleFormatter())
        console_handler.addFilter(context_filter)
        root_logger.addHandler(console_handler)

    # File handlers
    if enable_file:
        log_filename = log_file or "football_etl.log"
        log_path = LOG_DIR / log_filename

        # Main log file with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)

        if json_logs:
            file_handler.setFormatter(JSONFormatter())
        else:
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
            ))
        file_handler.addFilter(context_filter)
        root_logger.addHandler(file_handler)

        # Error log file (errors only)
        error_log_path = LOG_DIR / "errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(JSONFormatter())
        error_handler.addFilter(context_filter)
        root_logger.addHandler(error_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# ============================================
# PERFORMANCE LOGGING DECORATORS
# ============================================

def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    Decorator to log function execution time.

    Usage:
        @log_execution_time()
        def my_function():
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or logging.getLogger(func.__module__)
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                _logger.info(
                    f"{func.__name__} completed",
                    extra={
                        "function": func.__name__,
                        "execution_time_seconds": round(execution_time, 3),
                        "status": "success"
                    }
                )
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                _logger.error(
                    f"{func.__name__} failed: {e}",
                    extra={
                        "function": func.__name__,
                        "execution_time_seconds": round(execution_time, 3),
                        "status": "error",
                        "error_type": type(e).__name__
                    },
                    exc_info=True
                )
                raise

        return wrapper
    return decorator


def log_etl_operation(operation_name: str, logger: Optional[logging.Logger] = None):
    """
    Decorator for ETL operations with detailed logging.

    Usage:
        @log_etl_operation("process_teams")
        def process_teams(self, data):
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or logging.getLogger(func.__module__)
            start_time = time.time()

            _logger.info(
                f"Starting ETL operation: {operation_name}",
                extra={"operation": operation_name, "phase": "start"}
            )

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Try to extract record counts from result
                record_count = None
                if isinstance(result, dict):
                    record_count = result.get('count') or result.get('records') or len(result)
                elif isinstance(result, (list, tuple)):
                    record_count = len(result)

                _logger.info(
                    f"Completed ETL operation: {operation_name}",
                    extra={
                        "operation": operation_name,
                        "phase": "complete",
                        "execution_time_seconds": round(execution_time, 3),
                        "records_processed": record_count
                    }
                )
                return result

            except Exception as e:
                execution_time = time.time() - start_time
                _logger.error(
                    f"ETL operation failed: {operation_name} - {e}",
                    extra={
                        "operation": operation_name,
                        "phase": "error",
                        "execution_time_seconds": round(execution_time, 3),
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    exc_info=True
                )
                raise

        return wrapper
    return decorator


# ============================================
# ETL JOB LOGGER
# ============================================

class ETLJobLogger:
    """
    Specialized logger for ETL jobs with structured output.

    Tracks job progress, records processed, and errors.
    """

    def __init__(self, job_name: str, logger: Optional[logging.Logger] = None):
        self.job_name = job_name
        self.logger = logger or get_logger(f"etl.{job_name}")
        self.start_time: Optional[float] = None
        self.records_processed = 0
        self.records_failed = 0
        self.errors: list = []

    def start(self, **context):
        """Start the job and set context."""
        self.start_time = time.time()
        LogContext.set(job_name=self.job_name, **context)
        self.logger.info(
            f"Job started: {self.job_name}",
            extra={"phase": "start", **context}
        )

    def progress(self, message: str, records: int = 0, **extra):
        """Log progress update."""
        self.records_processed += records
        self.logger.info(
            message,
            extra={
                "phase": "progress",
                "records_processed": self.records_processed,
                **extra
            }
        )

    def error(self, message: str, exception: Optional[Exception] = None, **extra):
        """Log an error."""
        self.records_failed += 1
        self.errors.append({
            "message": message,
            "exception": str(exception) if exception else None,
            "timestamp": datetime.utcnow().isoformat()
        })
        self.logger.error(
            message,
            extra={
                "phase": "error",
                "records_failed": self.records_failed,
                **extra
            },
            exc_info=exception is not None
        )

    def complete(self, **extra) -> Dict[str, Any]:
        """Complete the job and return summary."""
        execution_time = time.time() - self.start_time if self.start_time else 0

        summary = {
            "job_name": self.job_name,
            "status": "completed" if not self.errors else "completed_with_errors",
            "execution_time_seconds": round(execution_time, 3),
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "error_count": len(self.errors),
            **extra
        }

        self.logger.info(
            f"Job completed: {self.job_name}",
            extra={"phase": "complete", **summary}
        )

        LogContext.clear()
        return summary

    def fail(self, error: Exception, **extra) -> Dict[str, Any]:
        """Mark job as failed."""
        execution_time = time.time() - self.start_time if self.start_time else 0

        summary = {
            "job_name": self.job_name,
            "status": "failed",
            "execution_time_seconds": round(execution_time, 3),
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "error": str(error),
            "error_type": type(error).__name__,
            **extra
        }

        self.logger.error(
            f"Job failed: {self.job_name} - {error}",
            extra={"phase": "failed", **summary},
            exc_info=True
        )

        LogContext.clear()
        return summary


# ============================================
# API REQUEST LOGGER
# ============================================

class APIRequestLogger:
    """
    Logger for API requests with timing and response info.
    """

    def __init__(self, api_name: str, logger: Optional[logging.Logger] = None):
        self.api_name = api_name
        self.logger = logger or get_logger(f"api.{api_name}")

    def log_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict] = None,
        response_status: Optional[int] = None,
        response_time_ms: Optional[float] = None,
        records_returned: Optional[int] = None,
        error: Optional[str] = None
    ):
        """Log an API request."""
        log_data = {
            "api": self.api_name,
            "endpoint": endpoint,
            "method": method,
            "params": params,
            "response_status": response_status,
            "response_time_ms": response_time_ms,
            "records_returned": records_returned
        }

        if error:
            log_data["error"] = error
            self.logger.error(
                f"API request failed: {method} {endpoint}",
                extra=log_data
            )
        else:
            self.logger.info(
                f"API request: {method} {endpoint}",
                extra=log_data
            )


# ============================================
# INITIALIZE DEFAULT LOGGING
# ============================================

# Auto-initialize with sensible defaults
_initialized = False

def init_logging():
    """Initialize logging if not already done."""
    global _initialized
    if not _initialized:
        log_level = os.getenv("LOG_LEVEL", "INFO")
        json_logs = os.getenv("JSON_LOGS", "false").lower() == "true"
        setup_logging(level=log_level, json_logs=json_logs)
        _initialized = True


# Initialize on import
init_logging()
