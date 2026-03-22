"""
ETL Run Tracker for Bronze/Silver/Gold Pattern.

Provides tracking and logging for ETL runs with quality metrics.
Part of ISSUE-027 fix.

Features:
- ETL run lifecycle management (start, update, complete)
- Raw payload storage (Bronze layer)
- Quality metrics tracking
- Incremental load watermarks
"""

import logging
import hashlib
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================
# ETL RUN TRACKING
# ============================================

@dataclass
class ETLRunStats:
    """Statistics for an ETL run."""
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    quality_score: float = 100.0
    completeness_score: float = 100.0
    validity_score: float = 100.0
    anomaly_count: int = 0


class ETLRunTracker:
    """
    Tracks ETL run lifecycle and metrics.

    Usage:
        tracker = ETLRunTracker(db)

        # Start a run
        run_id = tracker.start_run(
            run_type='fotmob_daily',
            source_name='fotmob',
            parameters={'leagues': ['premier-league']}
        )

        # Update stats during run
        tracker.update_stats(run_id, records_fetched=100, records_inserted=95)

        # Complete the run
        tracker.complete_run(run_id, status='completed')

        # Or mark as failed
        tracker.fail_run(run_id, error_message='Connection timeout')
    """

    def __init__(self, db):
        self.db = db
        self._current_run_id: Optional[int] = None
        self._run_stats: Dict[int, ETLRunStats] = {}

    def start_run(
        self,
        run_type: str,
        source_name: str,
        parameters: Dict = None,
    ) -> int:
        """
        Start a new ETL run and return its ID.

        Args:
            run_type: Type of run (e.g., 'fotmob_daily', 'api_football_weekly')
            source_name: Data source name
            parameters: Run parameters (leagues, seasons, etc.)

        Returns:
            run_id for tracking
        """
        try:
            result = self.db.execute_query(
                """
                INSERT INTO etl_runs (run_type, source_name, parameters, status)
                VALUES (:type, :source, :params, 'running')
                RETURNING run_id
                """,
                params={
                    'type': run_type,
                    'source': source_name,
                    'params': json.dumps(parameters) if parameters else None,
                },
                fetch=True
            )

            if result:
                run_id = result[0][0]
                self._current_run_id = run_id
                self._run_stats[run_id] = ETLRunStats()
                logger.info(f"Started ETL run {run_id}: {run_type} from {source_name}")
                return run_id

        except Exception as e:
            logger.error(f"Failed to start ETL run: {e}")

        return 0

    def update_stats(
        self,
        run_id: int,
        records_fetched: int = None,
        records_inserted: int = None,
        records_updated: int = None,
        records_skipped: int = None,
        records_failed: int = None,
        quality_score: float = None,
        completeness_score: float = None,
        validity_score: float = None,
        anomaly_count: int = None,
    ):
        """
        Update statistics for an ETL run.

        Stats are accumulated (added to existing values).
        """
        if run_id not in self._run_stats:
            self._run_stats[run_id] = ETLRunStats()

        stats = self._run_stats[run_id]

        if records_fetched:
            stats.records_fetched += records_fetched
        if records_inserted:
            stats.records_inserted += records_inserted
        if records_updated:
            stats.records_updated += records_updated
        if records_skipped:
            stats.records_skipped += records_skipped
        if records_failed:
            stats.records_failed += records_failed
        if quality_score is not None:
            stats.quality_score = quality_score
        if completeness_score is not None:
            stats.completeness_score = completeness_score
        if validity_score is not None:
            stats.validity_score = validity_score
        if anomaly_count:
            stats.anomaly_count += anomaly_count

    def complete_run(
        self,
        run_id: int,
        status: str = 'completed',
        error_summary: str = None,
    ):
        """
        Mark an ETL run as complete.

        Args:
            run_id: Run ID
            status: Final status ('completed', 'partial', 'failed')
            error_summary: Optional error summary
        """
        stats = self._run_stats.get(run_id, ETLRunStats())

        try:
            self.db.execute_query(
                """
                UPDATE etl_runs SET
                    completed_at = CURRENT_TIMESTAMP,
                    duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)),
                    records_fetched = :fetched,
                    records_inserted = :inserted,
                    records_updated = :updated,
                    records_skipped = :skipped,
                    records_failed = :failed,
                    quality_score = :quality,
                    completeness_score = :completeness,
                    validity_score = :validity,
                    anomaly_count = :anomalies,
                    error_summary = :errors,
                    status = :status
                WHERE run_id = :run_id
                """,
                params={
                    'run_id': run_id,
                    'fetched': stats.records_fetched,
                    'inserted': stats.records_inserted,
                    'updated': stats.records_updated,
                    'skipped': stats.records_skipped,
                    'failed': stats.records_failed,
                    'quality': stats.quality_score,
                    'completeness': stats.completeness_score,
                    'validity': stats.validity_score,
                    'anomalies': stats.anomaly_count,
                    'errors': error_summary,
                    'status': status,
                },
                fetch=False
            )

            logger.info(
                f"ETL run {run_id} {status}: "
                f"{stats.records_inserted} inserted, "
                f"{stats.records_updated} updated, "
                f"{stats.records_failed} failed"
            )

        except Exception as e:
            logger.error(f"Failed to complete ETL run {run_id}: {e}")

        # Cleanup
        if run_id in self._run_stats:
            del self._run_stats[run_id]
        if self._current_run_id == run_id:
            self._current_run_id = None

    def fail_run(self, run_id: int, error_message: str):
        """Mark an ETL run as failed."""
        self.complete_run(run_id, status='failed', error_summary=error_message)

    def get_current_run_id(self) -> Optional[int]:
        """Get the current run ID."""
        return self._current_run_id


# ============================================
# RAW PAYLOAD STORAGE (BRONZE LAYER)
# ============================================

class RawPayloadStore:
    """
    Stores raw API payloads for the Bronze layer.

    Enables:
    - Reproducibility (reprocess from raw data)
    - Debugging (inspect original responses)
    - Deduplication (avoid refetching same data)
    """

    def __init__(self, db):
        self.db = db

    def store_payload(
        self,
        source_name: str,
        endpoint: str,
        payload: Any,
        request_params: Dict = None,
        response_status: int = 200,
        etl_run_id: int = None,
    ) -> int:
        """
        Store a raw API payload.

        Args:
            source_name: Data source name
            endpoint: API endpoint called
            payload: Raw response data
            request_params: Request parameters
            response_status: HTTP status code
            etl_run_id: Associated ETL run ID

        Returns:
            payload_id
        """
        try:
            # Calculate hash for deduplication
            payload_json = json.dumps(payload, sort_keys=True, default=str)
            payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()

            # Check for existing payload
            result = self.db.execute_query(
                """
                SELECT payload_id FROM raw_payloads
                WHERE payload_hash = :hash
                  AND source_name = :source
                  AND endpoint = :endpoint
                  AND fetched_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
                """,
                params={
                    'hash': payload_hash,
                    'source': source_name,
                    'endpoint': endpoint,
                },
                fetch=True
            )

            if result:
                # Payload already exists
                return result[0][0]

            # Store new payload
            result = self.db.execute_query(
                """
                INSERT INTO raw_payloads (
                    source_name, endpoint, payload, request_params,
                    response_status, etl_run_id, payload_hash
                ) VALUES (
                    :source, :endpoint, :payload, :params,
                    :status, :run_id, :hash
                )
                RETURNING payload_id
                """,
                params={
                    'source': source_name,
                    'endpoint': endpoint,
                    'payload': payload_json,
                    'params': json.dumps(request_params) if request_params else None,
                    'status': response_status,
                    'run_id': etl_run_id,
                    'hash': payload_hash,
                },
                fetch=True
            )

            return result[0][0] if result else 0

        except Exception as e:
            logger.error(f"Failed to store raw payload: {e}")
            return 0

    def get_payload(self, payload_id: int) -> Optional[Dict]:
        """Retrieve a stored payload."""
        result = self.db.execute_query(
            "SELECT payload, request_params FROM raw_payloads WHERE payload_id = :pid",
            params={'pid': payload_id},
            fetch=True
        )

        if result:
            payload_json = result[0][0]
            return json.loads(payload_json) if payload_json else None

        return None

    def mark_processed(self, payload_id: int, status: str = 'processed'):
        """Mark a payload as processed."""
        self.db.execute_query(
            """
            UPDATE raw_payloads SET
                processed_at = CURRENT_TIMESTAMP,
                processing_status = :status
            WHERE payload_id = :pid
            """,
            params={'pid': payload_id, 'status': status},
            fetch=False
        )

    def get_unprocessed_payloads(
        self,
        source_name: str = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Get payloads that haven't been processed yet."""
        params = {'limit': limit}
        where_clause = "processing_status = 'pending'"

        if source_name:
            where_clause += " AND source_name = :source"
            params['source'] = source_name

        result = self.db.execute_query(
            f"""
            SELECT payload_id, source_name, endpoint, payload
            FROM raw_payloads
            WHERE {where_clause}
            ORDER BY fetched_at
            LIMIT :limit
            """,
            params=params,
            fetch=True
        )

        return [
            {
                'payload_id': row[0],
                'source_name': row[1],
                'endpoint': row[2],
                'payload': json.loads(row[3]) if row[3] else None,
            }
            for row in result
        ]


# ============================================
# INCREMENTAL LOAD WATERMARKS
# ============================================

class WatermarkManager:
    """
    Manages watermarks for incremental loading.

    Tracks the last processed timestamp/ID for each entity type
    to enable efficient delta loads.
    """

    def __init__(self, db):
        self.db = db

    def get_watermark(
        self,
        source_name: str,
        entity_type: str,
    ) -> Optional[datetime]:
        """
        Get the watermark (last processed timestamp) for an entity.

        Returns:
            Last processed timestamp, or None if no watermark exists
        """
        result = self.db.execute_query(
            """
            SELECT last_processed_at, last_processed_id
            FROM etl_watermarks
            WHERE source_name = :source AND entity_type = :entity
            """,
            params={'source': source_name, 'entity': entity_type},
            fetch=True
        )

        if result:
            return result[0][0]  # last_processed_at

        return None

    def set_watermark(
        self,
        source_name: str,
        entity_type: str,
        processed_at: datetime = None,
        processed_id: str = None,
    ):
        """
        Update the watermark for an entity.

        Args:
            source_name: Data source name
            entity_type: Entity type (e.g., 'player', 'match')
            processed_at: Timestamp of last processed record
            processed_id: ID of last processed record
        """
        try:
            self.db.execute_query(
                """
                INSERT INTO etl_watermarks (
                    source_name, entity_type, last_processed_at, last_processed_id
                ) VALUES (
                    :source, :entity, :processed_at, :processed_id
                )
                ON CONFLICT (source_name, entity_type) DO UPDATE SET
                    last_processed_at = COALESCE(EXCLUDED.last_processed_at, etl_watermarks.last_processed_at),
                    last_processed_id = COALESCE(EXCLUDED.last_processed_id, etl_watermarks.last_processed_id),
                    updated_at = CURRENT_TIMESTAMP
                """,
                params={
                    'source': source_name,
                    'entity': entity_type,
                    'processed_at': processed_at or datetime.now(),
                    'processed_id': processed_id,
                },
                fetch=False
            )

        except Exception as e:
            logger.error(f"Failed to set watermark: {e}")

    def reset_watermark(self, source_name: str, entity_type: str):
        """Reset a watermark to enable full reload."""
        self.db.execute_query(
            """
            DELETE FROM etl_watermarks
            WHERE source_name = :source AND entity_type = :entity
            """,
            params={'source': source_name, 'entity': entity_type},
            fetch=False
        )


# ============================================
# RUN HISTORY QUERIES
# ============================================

def get_recent_runs(db, limit: int = 10, source_name: str = None) -> List[Dict]:
    """Get recent ETL runs."""
    params = {'limit': limit}
    where_clause = ""

    if source_name:
        where_clause = "WHERE source_name = :source"
        params['source'] = source_name

    result = db.execute_query(
        f"""
        SELECT
            run_id, run_type, source_name, started_at, completed_at,
            duration_seconds, records_inserted, records_failed,
            quality_score, status
        FROM etl_runs
        {where_clause}
        ORDER BY started_at DESC
        LIMIT :limit
        """,
        params=params,
        fetch=True
    )

    return [
        {
            'run_id': row[0],
            'run_type': row[1],
            'source_name': row[2],
            'started_at': row[3],
            'completed_at': row[4],
            'duration_seconds': row[5],
            'records_inserted': row[6],
            'records_failed': row[7],
            'quality_score': float(row[8]) if row[8] else None,
            'status': row[9],
        }
        for row in result
    ]


def get_run_summary(db, days: int = 7) -> Dict:
    """Get summary of ETL runs over the past N days."""
    result = db.execute_query(
        """
        SELECT
            source_name,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(records_inserted) as total_inserted,
            SUM(records_failed) as total_failed,
            AVG(quality_score) as avg_quality
        FROM etl_runs
        WHERE started_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
        GROUP BY source_name
        """ % days,
        fetch=True
    )

    return {
        row[0]: {
            'total_runs': row[1],
            'completed': row[2],
            'failed': row[3],
            'total_inserted': row[4],
            'total_failed': row[5],
            'avg_quality': float(row[6]) if row[6] else None,
        }
        for row in result
    }
