"""
Monitoring and Alerting System for Football Data Pipeline.

Features:
- Health checks for all components
- Job status monitoring
- Alert dispatching (console, file, webhook)
- Metrics collection and reporting
- Scheduled health monitoring
"""

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import requests

logger = logging.getLogger(__name__)


# ============================================
# ALERT SEVERITY
# ============================================

class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents an alert."""
    severity: AlertSeverity
    title: str
    message: str
    component: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "component": self.component,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }


# ============================================
# HEALTH CHECK RESULTS
# ============================================

class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    component: str
    status: HealthStatus
    message: str
    response_time_ms: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "response_time_ms": self.response_time_ms,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


# ============================================
# ALERT HANDLERS
# ============================================

class AlertHandler:
    """Base class for alert handlers."""

    def send(self, alert: Alert):
        """Send an alert."""
        raise NotImplementedError


class ConsoleAlertHandler(AlertHandler):
    """Handler that prints alerts to console."""

    COLORS = {
        AlertSeverity.INFO: "\033[36m",      # Cyan
        AlertSeverity.WARNING: "\033[33m",   # Yellow
        AlertSeverity.ERROR: "\033[31m",     # Red
        AlertSeverity.CRITICAL: "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def send(self, alert: Alert):
        color = self.COLORS.get(alert.severity, self.RESET)
        print(f"{color}[{alert.severity.value.upper()}] {alert.title}{self.RESET}")
        print(f"  Component: {alert.component}")
        print(f"  Message: {alert.message}")
        print(f"  Time: {alert.timestamp.isoformat()}")
        if alert.metadata:
            print(f"  Details: {json.dumps(alert.metadata, indent=2)}")
        print()


class FileAlertHandler(AlertHandler):
    """Handler that writes alerts to a file."""

    def __init__(self, filepath: str = "logs/alerts.log"):
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def send(self, alert: Alert):
        with open(self.filepath, "a") as f:
            f.write(json.dumps(alert.to_dict()) + "\n")


class WebhookAlertHandler(AlertHandler):
    """Handler that sends alerts to a webhook URL."""

    def __init__(self, webhook_url: str, timeout: int = 10):
        self.webhook_url = webhook_url
        self.timeout = timeout

    def send(self, alert: Alert):
        try:
            payload = alert.to_dict()
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.debug(f"Alert sent to webhook: {alert.title}")
        except Exception as e:
            logger.error(f"Failed to send alert to webhook: {e}")


class SlackAlertHandler(AlertHandler):
    """Handler that sends alerts to Slack."""

    SEVERITY_EMOJIS = {
        AlertSeverity.INFO: ":information_source:",
        AlertSeverity.WARNING: ":warning:",
        AlertSeverity.ERROR: ":x:",
        AlertSeverity.CRITICAL: ":rotating_light:"
    }

    def __init__(self, webhook_url: str, channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel

    def send(self, alert: Alert):
        emoji = self.SEVERITY_EMOJIS.get(alert.severity, ":bell:")

        payload = {
            "text": f"{emoji} *{alert.title}*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {alert.title}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:* {alert.severity.value.upper()}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Component:* {alert.component}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": alert.message
                    }
                }
            ]
        }

        if self.channel:
            payload["channel"] = self.channel

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")


class EmailAlertHandler(AlertHandler):
    """Handler that sends alerts via email."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        sender: str,
        recipients: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.sender = sender
        self.recipients = recipients
        self.username = username
        self.password = password
        self.use_tls = use_tls

    def send(self, alert: Alert):
        msg = MIMEMultipart()
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg["Subject"] = f"[{alert.severity.value.upper()}] {alert.title}"

        body = f"""
Football Data Pipeline Alert

Severity: {alert.severity.value.upper()}
Component: {alert.component}
Time: {alert.timestamp.isoformat()}

Message:
{alert.message}

Details:
{json.dumps(alert.metadata, indent=2) if alert.metadata else 'N/A'}
        """
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.send_message(msg)
            logger.debug(f"Email alert sent: {alert.title}")
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")


# ============================================
# ALERT MANAGER
# ============================================

class AlertManager:
    """
    Central manager for alerts.

    Collects alerts and dispatches them to configured handlers.
    """

    def __init__(self):
        self.handlers: List[AlertHandler] = []
        self.alert_history: List[Alert] = []
        self.max_history = 1000
        self.severity_filter: Optional[AlertSeverity] = None

    def add_handler(self, handler: AlertHandler):
        """Add an alert handler."""
        self.handlers.append(handler)

    def set_severity_filter(self, min_severity: AlertSeverity):
        """Set minimum severity for alerts to be dispatched."""
        self.severity_filter = min_severity

    def alert(
        self,
        severity: AlertSeverity,
        title: str,
        message: str,
        component: str,
        **metadata
    ):
        """
        Send an alert.

        Args:
            severity: Alert severity level
            title: Short title for the alert
            message: Detailed message
            component: Component that generated the alert
            **metadata: Additional metadata
        """
        alert = Alert(
            severity=severity,
            title=title,
            message=message,
            component=component,
            metadata=metadata
        )

        # Add to history
        self.alert_history.append(alert)
        if len(self.alert_history) > self.max_history:
            self.alert_history = self.alert_history[-self.max_history:]

        # Check severity filter
        severity_order = [
            AlertSeverity.INFO,
            AlertSeverity.WARNING,
            AlertSeverity.ERROR,
            AlertSeverity.CRITICAL
        ]

        if self.severity_filter:
            filter_index = severity_order.index(self.severity_filter)
            alert_index = severity_order.index(severity)
            if alert_index < filter_index:
                return  # Skip alerts below filter

        # Dispatch to handlers
        for handler in self.handlers:
            try:
                handler.send(alert)
            except Exception as e:
                logger.error(f"Failed to send alert via {type(handler).__name__}: {e}")

    def info(self, title: str, message: str, component: str, **metadata):
        """Send an info alert."""
        self.alert(AlertSeverity.INFO, title, message, component, **metadata)

    def warning(self, title: str, message: str, component: str, **metadata):
        """Send a warning alert."""
        self.alert(AlertSeverity.WARNING, title, message, component, **metadata)

    def error(self, title: str, message: str, component: str, **metadata):
        """Send an error alert."""
        self.alert(AlertSeverity.ERROR, title, message, component, **metadata)

    def critical(self, title: str, message: str, component: str, **metadata):
        """Send a critical alert."""
        self.alert(AlertSeverity.CRITICAL, title, message, component, **metadata)

    def get_recent_alerts(
        self,
        limit: int = 10,
        severity: Optional[AlertSeverity] = None,
        component: Optional[str] = None
    ) -> List[Alert]:
        """Get recent alerts with optional filtering."""
        alerts = self.alert_history

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        if component:
            alerts = [a for a in alerts if a.component == component]

        return alerts[-limit:]


# ============================================
# HEALTH MONITOR
# ============================================

class HealthMonitor:
    """
    Monitor health of system components.

    Performs health checks and generates alerts for unhealthy components.
    """

    def __init__(self, alert_manager: Optional[AlertManager] = None):
        self.alert_manager = alert_manager or AlertManager()
        self.checks: Dict[str, Callable[[], HealthCheckResult]] = {}
        self.last_results: Dict[str, HealthCheckResult] = {}

    def register_check(self, name: str, check_func: Callable[[], HealthCheckResult]):
        """Register a health check function."""
        self.checks[name] = check_func

    def run_check(self, name: str) -> HealthCheckResult:
        """Run a single health check."""
        if name not in self.checks:
            return HealthCheckResult(
                component=name,
                status=HealthStatus.UNKNOWN,
                message=f"No health check registered for {name}"
            )

        import time
        start_time = time.time()

        try:
            result = self.checks[name]()
            result.response_time_ms = (time.time() - start_time) * 1000
        except Exception as e:
            result = HealthCheckResult(
                component=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000
            )
            logger.error(f"Health check failed for {name}: {e}")

        self.last_results[name] = result

        # Generate alerts for unhealthy components
        if result.status == HealthStatus.UNHEALTHY:
            self.alert_manager.error(
                title=f"Health Check Failed: {name}",
                message=result.message,
                component=name,
                **result.details
            )
        elif result.status == HealthStatus.DEGRADED:
            self.alert_manager.warning(
                title=f"Health Check Degraded: {name}",
                message=result.message,
                component=name,
                **result.details
            )

        return result

    def run_all_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all registered health checks."""
        results = {}
        for name in self.checks:
            results[name] = self.run_check(name)
        return results

    def get_overall_status(self) -> HealthStatus:
        """Get overall system health status."""
        if not self.last_results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in self.last_results.values()]

        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        if any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        if any(s == HealthStatus.UNKNOWN for s in statuses):
            return HealthStatus.UNKNOWN

        return HealthStatus.HEALTHY

    def get_health_report(self) -> Dict[str, Any]:
        """Get a comprehensive health report."""
        return {
            "overall_status": self.get_overall_status().value,
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                name: result.to_dict()
                for name, result in self.last_results.items()
            }
        }


# ============================================
# BUILT-IN HEALTH CHECKS
# ============================================

def check_database_health() -> HealthCheckResult:
    """Check database connectivity and performance."""
    try:
        from database.connection import get_db
        db = get_db()

        # Test query
        result = db.execute_query("SELECT 1 as health_check")

        if result and result[0][0] == 1:
            # Get additional stats
            table_counts = {}
            for table in ['teams', 'players', 'matches', 'leagues']:
                try:
                    count_result = db.execute_query(f"SELECT COUNT(*) FROM {table}")
                    table_counts[table] = count_result[0][0] if count_result else 0
                except:
                    table_counts[table] = -1

            return HealthCheckResult(
                component="database",
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                details={"table_counts": table_counts}
            )
        else:
            return HealthCheckResult(
                component="database",
                status=HealthStatus.UNHEALTHY,
                message="Database query returned unexpected result"
            )

    except Exception as e:
        return HealthCheckResult(
            component="database",
            status=HealthStatus.UNHEALTHY,
            message=f"Database connection failed: {str(e)}"
        )


def check_api_football_health() -> HealthCheckResult:
    """Check API-Football connectivity."""
    api_key = os.getenv('API_FOOTBALL_KEY')

    if not api_key:
        return HealthCheckResult(
            component="api_football",
            status=HealthStatus.UNHEALTHY,
            message="API_FOOTBALL_KEY not configured"
        )

    try:
        from scrapers.api_football.client import APIFootballClient
        client = APIFootballClient(api_key=api_key)

        # Make a simple request
        leagues = client.get_leagues(country='England')

        if leagues:
            return HealthCheckResult(
                component="api_football",
                status=HealthStatus.HEALTHY,
                message="API-Football connection successful",
                details={"leagues_returned": len(leagues)}
            )
        else:
            return HealthCheckResult(
                component="api_football",
                status=HealthStatus.DEGRADED,
                message="API-Football returned empty response"
            )

    except Exception as e:
        return HealthCheckResult(
            component="api_football",
            status=HealthStatus.UNHEALTHY,
            message=f"API-Football connection failed: {str(e)}"
        )


def check_api_quota_health() -> HealthCheckResult:
    """Check API quota status."""
    try:
        from utils.api_tracker import get_tracker
        tracker = get_tracker()

        remaining = tracker.get_remaining_requests()
        used = tracker.get_usage_today()

        if remaining <= 0:
            return HealthCheckResult(
                component="api_quota",
                status=HealthStatus.UNHEALTHY,
                message="API quota exhausted",
                details={"used": used, "remaining": remaining}
            )
        elif remaining < 10:
            return HealthCheckResult(
                component="api_quota",
                status=HealthStatus.DEGRADED,
                message=f"API quota low: {remaining} requests remaining",
                details={"used": used, "remaining": remaining}
            )
        else:
            return HealthCheckResult(
                component="api_quota",
                status=HealthStatus.HEALTHY,
                message=f"API quota OK: {remaining} requests remaining",
                details={"used": used, "remaining": remaining}
            )

    except Exception as e:
        return HealthCheckResult(
            component="api_quota",
            status=HealthStatus.UNKNOWN,
            message=f"Could not check quota: {str(e)}"
        )


def check_scheduler_health() -> HealthCheckResult:
    """Check scheduler status."""
    try:
        from scheduler.job_scheduler import get_scheduler
        scheduler = get_scheduler()

        if scheduler.is_running():
            jobs = scheduler.list_jobs()
            return HealthCheckResult(
                component="scheduler",
                status=HealthStatus.HEALTHY,
                message=f"Scheduler running with {len(jobs)} jobs",
                details={"jobs": [j['id'] for j in jobs]}
            )
        else:
            return HealthCheckResult(
                component="scheduler",
                status=HealthStatus.DEGRADED,
                message="Scheduler is not running"
            )

    except Exception as e:
        return HealthCheckResult(
            component="scheduler",
            status=HealthStatus.UNKNOWN,
            message=f"Could not check scheduler: {str(e)}"
        )


def check_disk_space_health(min_free_gb: float = 1.0) -> HealthCheckResult:
    """Check available disk space."""
    import shutil

    try:
        usage = shutil.disk_usage("/")
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        used_percent = (usage.used / usage.total) * 100

        if free_gb < min_free_gb:
            return HealthCheckResult(
                component="disk_space",
                status=HealthStatus.UNHEALTHY,
                message=f"Low disk space: {free_gb:.1f}GB free",
                details={
                    "free_gb": round(free_gb, 2),
                    "total_gb": round(total_gb, 2),
                    "used_percent": round(used_percent, 1)
                }
            )
        elif used_percent > 90:
            return HealthCheckResult(
                component="disk_space",
                status=HealthStatus.DEGRADED,
                message=f"High disk usage: {used_percent:.1f}%",
                details={
                    "free_gb": round(free_gb, 2),
                    "total_gb": round(total_gb, 2),
                    "used_percent": round(used_percent, 1)
                }
            )
        else:
            return HealthCheckResult(
                component="disk_space",
                status=HealthStatus.HEALTHY,
                message=f"Disk space OK: {free_gb:.1f}GB free",
                details={
                    "free_gb": round(free_gb, 2),
                    "total_gb": round(total_gb, 2),
                    "used_percent": round(used_percent, 1)
                }
            )

    except Exception as e:
        return HealthCheckResult(
            component="disk_space",
            status=HealthStatus.UNKNOWN,
            message=f"Could not check disk space: {str(e)}"
        )


# ============================================
# SINGLETON INSTANCES
# ============================================

_alert_manager: Optional[AlertManager] = None
_health_monitor: Optional[HealthMonitor] = None


def get_alert_manager() -> AlertManager:
    """Get the global alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
        # Add default handlers
        _alert_manager.add_handler(ConsoleAlertHandler())
        _alert_manager.add_handler(FileAlertHandler())
    return _alert_manager


def get_health_monitor() -> HealthMonitor:
    """Get the global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor(get_alert_manager())
        # Register default health checks
        _health_monitor.register_check("database", check_database_health)
        _health_monitor.register_check("api_quota", check_api_quota_health)
        _health_monitor.register_check("disk_space", check_disk_space_health)
    return _health_monitor


# ============================================
# METRICS COLLECTOR
# ============================================

@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """
    Collect and store metrics for monitoring.

    Simple in-memory metrics collection for observability.
    """

    def __init__(self, max_points: int = 10000):
        self.max_points = max_points
        self.metrics: Dict[str, List[MetricPoint]] = {}

    def record(self, name: str, value: float, **tags):
        """Record a metric value."""
        point = MetricPoint(name=name, value=value, tags=tags)

        if name not in self.metrics:
            self.metrics[name] = []

        self.metrics[name].append(point)

        # Trim old data
        if len(self.metrics[name]) > self.max_points:
            self.metrics[name] = self.metrics[name][-self.max_points:]

    def get_metric(
        self,
        name: str,
        since: Optional[datetime] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> List[MetricPoint]:
        """Get metric points with optional filtering."""
        if name not in self.metrics:
            return []

        points = self.metrics[name]

        if since:
            points = [p for p in points if p.timestamp >= since]

        if tags:
            points = [
                p for p in points
                if all(p.tags.get(k) == v for k, v in tags.items())
            ]

        return points

    def get_latest(self, name: str) -> Optional[MetricPoint]:
        """Get the latest value for a metric."""
        if name not in self.metrics or not self.metrics[name]:
            return None
        return self.metrics[name][-1]

    def get_average(
        self,
        name: str,
        duration: timedelta = timedelta(hours=1)
    ) -> Optional[float]:
        """Get average value over duration."""
        since = datetime.utcnow() - duration
        points = self.get_metric(name, since=since)

        if not points:
            return None

        return sum(p.value for p in points) / len(points)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics."""
        return {
            name: {
                "count": len(points),
                "latest": points[-1].value if points else None,
                "latest_timestamp": points[-1].timestamp.isoformat() if points else None
            }
            for name, points in self.metrics.items()
        }


# Global metrics collector
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector
