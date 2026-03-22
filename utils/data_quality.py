"""
Data Quality Gates for Football Data Pipeline.

Provides pre-load validation and post-load quality checks.
Part of ISSUE-026 fix.

Features:
- Schema validation before database load
- Value range validation (e.g., xG 0-10)
- Completeness checks
- Anomaly detection
- Quality scoring and reporting
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, date
import json

logger = logging.getLogger(__name__)


# ============================================
# VALIDATION RULES
# ============================================

@dataclass
class ValidationRule:
    """A single validation rule."""
    field: str
    rule_type: str  # 'required', 'range', 'enum', 'type', 'regex', 'custom'
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = 'medium'  # 'critical', 'high', 'medium', 'low'
    message: str = ''


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    quality_score: float = 100.0


# Field-level validation rules by entity type
PLAYER_VALIDATION_RULES = [
    ValidationRule('player_name', 'required', severity='critical', message='Player name is required'),
    ValidationRule('player_name', 'type', {'expected': str}, severity='critical'),
    ValidationRule('player_name', 'length', {'min': 2, 'max': 100}, severity='high'),

    ValidationRule('date_of_birth', 'type', {'expected': (date, str, type(None))}, severity='low'),
    ValidationRule('date_of_birth', 'range', {'min': date(1970, 1, 1), 'max': date(2010, 1, 1)}, severity='medium'),

    ValidationRule('nationality', 'type', {'expected': (str, type(None))}, severity='low'),
    ValidationRule('nationality', 'length', {'max': 50}, severity='medium'),

    ValidationRule('position', 'enum', {'values': [
        None, 'Goalkeeper', 'Defender', 'Midfielder', 'Forward', 'Attacker',
        'GK', 'CB', 'LB', 'RB', 'LWB', 'RWB', 'CDM', 'CM', 'CAM', 'LM', 'RM',
        'LW', 'RW', 'CF', 'ST', 'DEF', 'MID', 'FWD'
    ]}, severity='low'),

    ValidationRule('height_cm', 'range', {'min': 150, 'max': 220}, severity='low'),
    ValidationRule('preferred_foot', 'enum', {'values': [None, 'Left', 'Right', 'Both', 'left', 'right', 'both']}, severity='low'),
    ValidationRule('preferred_foot', 'length', {'max': 10}, severity='high'),  # Prevent dict serialization issues
]

PLAYER_SEASON_STATS_RULES = [
    ValidationRule('player_id', 'required', severity='critical'),
    ValidationRule('season_id', 'required', severity='critical'),
    ValidationRule('league_id', 'required', severity='critical'),

    # xG validation (most common issues)
    ValidationRule('xg', 'range', {'min': 0, 'max': 50}, severity='high', message='xG out of reasonable range'),
    ValidationRule('xag', 'range', {'min': 0, 'max': 30}, severity='high', message='xA out of reasonable range'),
    ValidationRule('npxg', 'range', {'min': 0, 'max': 50}, severity='high'),

    # Basic stats
    ValidationRule('goals', 'range', {'min': 0, 'max': 100}, severity='medium'),
    ValidationRule('assists', 'range', {'min': 0, 'max': 100}, severity='medium'),
    ValidationRule('matches_played', 'range', {'min': 0, 'max': 60}, severity='medium'),
    ValidationRule('minutes', 'range', {'min': 0, 'max': 6000}, severity='medium'),

    # Shots
    ValidationRule('shots', 'range', {'min': 0, 'max': 500}, severity='medium'),
    ValidationRule('shots_on_target', 'range', {'min': 0, 'max': 300}, severity='medium'),

    # Cards
    ValidationRule('yellow_cards', 'range', {'min': 0, 'max': 20}, severity='low'),
    ValidationRule('red_cards', 'range', {'min': 0, 'max': 5}, severity='low'),
]

TEAM_SEASON_STATS_RULES = [
    ValidationRule('team_id', 'required', severity='critical'),
    ValidationRule('season_id', 'required', severity='critical'),

    ValidationRule('matches_played', 'range', {'min': 0, 'max': 50}, severity='medium'),
    ValidationRule('wins', 'range', {'min': 0, 'max': 50}, severity='medium'),
    ValidationRule('draws', 'range', {'min': 0, 'max': 50}, severity='medium'),
    ValidationRule('losses', 'range', {'min': 0, 'max': 50}, severity='medium'),
    ValidationRule('points', 'range', {'min': 0, 'max': 150}, severity='medium'),

    ValidationRule('goals_for', 'range', {'min': 0, 'max': 200}, severity='medium'),
    ValidationRule('goals_against', 'range', {'min': 0, 'max': 200}, severity='medium'),

    ValidationRule('xg_for', 'range', {'min': 0, 'max': 200}, severity='high'),
    ValidationRule('xg_against', 'range', {'min': 0, 'max': 200}, severity='high'),
]

MATCH_RULES = [
    ValidationRule('match_date', 'required', severity='critical'),
    ValidationRule('home_team_id', 'required', severity='critical'),
    ValidationRule('away_team_id', 'required', severity='critical'),

    ValidationRule('home_score', 'range', {'min': 0, 'max': 20}, severity='medium'),
    ValidationRule('away_score', 'range', {'min': 0, 'max': 20}, severity='medium'),
    ValidationRule('home_xg', 'range', {'min': 0, 'max': 10}, severity='high'),
    ValidationRule('away_xg', 'range', {'min': 0, 'max': 10}, severity='high'),
]


# ============================================
# VALIDATION ENGINE
# ============================================

class DataQualityValidator:
    """
    Validates data before and after loading.

    Usage:
        validator = DataQualityValidator()
        result = validator.validate_player(player_data)
        if not result.is_valid:
            for error in result.errors:
                print(f"Error: {error}")
    """

    def __init__(self, db=None):
        self.db = db
        self.rules_by_entity = {
            'player': PLAYER_VALIDATION_RULES,
            'player_season_stats': PLAYER_SEASON_STATS_RULES,
            'team_season_stats': TEAM_SEASON_STATS_RULES,
            'match': MATCH_RULES,
        }

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        rule: ValidationRule
    ) -> Optional[Dict[str, Any]]:
        """
        Validate a single field against a rule.

        Returns error dict if validation fails, None if passes.
        """
        if rule.rule_type == 'required':
            if value is None or value == '':
                return {
                    'field': field_name,
                    'rule': 'required',
                    'severity': rule.severity,
                    'message': rule.message or f'{field_name} is required',
                    'value': value,
                }

        elif rule.rule_type == 'type':
            expected = rule.params.get('expected', str)
            if value is not None:
                if isinstance(expected, tuple):
                    if not isinstance(value, expected):
                        return {
                            'field': field_name,
                            'rule': 'type',
                            'severity': rule.severity,
                            'message': f'{field_name} must be one of {expected}',
                            'value': type(value).__name__,
                            'expected': str(expected),
                        }
                elif not isinstance(value, expected):
                    return {
                        'field': field_name,
                        'rule': 'type',
                        'severity': rule.severity,
                        'message': f'{field_name} must be {expected.__name__}',
                        'value': type(value).__name__,
                        'expected': expected.__name__,
                    }

        elif rule.rule_type == 'range':
            if value is not None:
                min_val = rule.params.get('min')
                max_val = rule.params.get('max')

                # Handle date comparisons
                if isinstance(value, str) and isinstance(min_val, date):
                    try:
                        value = datetime.fromisoformat(value).date()
                    except ValueError:
                        pass

                try:
                    if min_val is not None and value < min_val:
                        return {
                            'field': field_name,
                            'rule': 'range',
                            'severity': rule.severity,
                            'message': rule.message or f'{field_name} below minimum {min_val}',
                            'value': value,
                            'min': min_val,
                        }
                    if max_val is not None and value > max_val:
                        return {
                            'field': field_name,
                            'rule': 'range',
                            'severity': rule.severity,
                            'message': rule.message or f'{field_name} above maximum {max_val}',
                            'value': value,
                            'max': max_val,
                        }
                except TypeError:
                    pass  # Skip if types aren't comparable

        elif rule.rule_type == 'length':
            if value is not None and isinstance(value, str):
                min_len = rule.params.get('min', 0)
                max_len = rule.params.get('max', float('inf'))

                if len(value) < min_len:
                    return {
                        'field': field_name,
                        'rule': 'length',
                        'severity': rule.severity,
                        'message': f'{field_name} too short (min {min_len})',
                        'value': len(value),
                    }
                if len(value) > max_len:
                    return {
                        'field': field_name,
                        'rule': 'length',
                        'severity': rule.severity,
                        'message': f'{field_name} too long (max {max_len})',
                        'value': f'{value[:20]}... (len={len(value)})',
                    }

        elif rule.rule_type == 'enum':
            if value is not None:
                allowed = rule.params.get('values', [])
                # Case-insensitive comparison for strings
                if isinstance(value, str):
                    if value not in allowed and value.lower() not in [v.lower() if isinstance(v, str) else v for v in allowed if v]:
                        return {
                            'field': field_name,
                            'rule': 'enum',
                            'severity': rule.severity,
                            'message': f'{field_name} has invalid value',
                            'value': value,
                            'allowed': allowed[:10],  # Truncate for readability
                        }
                elif value not in allowed:
                    return {
                        'field': field_name,
                        'rule': 'enum',
                        'severity': rule.severity,
                        'message': f'{field_name} has invalid value',
                        'value': value,
                        'allowed': allowed[:10],
                    }

        return None

    def validate_record(
        self,
        entity_type: str,
        record: Dict[str, Any],
        custom_rules: List[ValidationRule] = None
    ) -> ValidationResult:
        """
        Validate a single record against entity rules.

        Args:
            entity_type: Type of entity ('player', 'player_season_stats', etc.)
            record: Data dictionary to validate
            custom_rules: Additional rules to apply

        Returns:
            ValidationResult with errors, warnings, and quality score
        """
        rules = self.rules_by_entity.get(entity_type, [])
        if custom_rules:
            rules = rules + custom_rules

        errors = []
        warnings = []

        for rule in rules:
            field_name = rule.field
            value = record.get(field_name)

            error = self._validate_field(field_name, value, rule)
            if error:
                if rule.severity in ('critical', 'high'):
                    errors.append(error)
                else:
                    warnings.append(error)

        # Calculate quality score
        total_rules = len(rules)
        failed_rules = len(errors) + len(warnings) * 0.5
        quality_score = max(0, (1 - failed_rules / max(total_rules, 1)) * 100)

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            quality_score=round(quality_score, 2)
        )

    def validate_batch(
        self,
        entity_type: str,
        records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict], float]:
        """
        Validate a batch of records.

        Returns:
            Tuple of (valid_records, invalid_records, overall_quality_score)
        """
        valid = []
        invalid = []
        total_score = 0

        for record in records:
            result = self.validate_record(entity_type, record)
            total_score += result.quality_score

            if result.is_valid:
                valid.append(record)
            else:
                invalid.append({
                    'record': record,
                    'errors': result.errors,
                    'warnings': result.warnings,
                })

        avg_score = total_score / len(records) if records else 100
        return valid, invalid, round(avg_score, 2)

    def validate_player(self, player_data: Dict) -> ValidationResult:
        """Validate player record."""
        return self.validate_record('player', player_data)

    def validate_player_stats(self, stats_data: Dict) -> ValidationResult:
        """Validate player season stats record."""
        return self.validate_record('player_season_stats', stats_data)

    def validate_team_stats(self, stats_data: Dict) -> ValidationResult:
        """Validate team season stats record."""
        return self.validate_record('team_season_stats', stats_data)

    def validate_match(self, match_data: Dict) -> ValidationResult:
        """Validate match record."""
        return self.validate_record('match', match_data)


# ============================================
# ANOMALY DETECTION
# ============================================

class AnomalyDetector:
    """
    Detects statistical anomalies in football data.

    Uses simple z-score based detection for numeric fields.
    """

    # Expected ranges for key metrics (mean, std)
    EXPECTED_RANGES = {
        'xg': {'mean': 5.0, 'std': 5.0, 'max_zscore': 4},  # xG > 25 is suspicious
        'xag': {'mean': 3.0, 'std': 3.0, 'max_zscore': 4},
        'goals': {'mean': 5.0, 'std': 8.0, 'max_zscore': 4},
        'assists': {'mean': 3.0, 'std': 5.0, 'max_zscore': 4},
        'rating': {'mean': 6.8, 'std': 0.5, 'max_zscore': 4},
    }

    def detect_anomalies(
        self,
        records: List[Dict[str, Any]],
        fields: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies in a batch of records.

        Args:
            records: List of data dictionaries
            fields: Fields to check (defaults to EXPECTED_RANGES keys)

        Returns:
            List of anomaly reports
        """
        anomalies = []
        fields = fields or list(self.EXPECTED_RANGES.keys())

        for i, record in enumerate(records):
            record_anomalies = []

            for field in fields:
                value = record.get(field)
                if value is None:
                    continue

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    continue

                params = self.EXPECTED_RANGES.get(field, {})
                mean = params.get('mean', 0)
                std = params.get('std', 1)
                max_zscore = params.get('max_zscore', 3)

                if std > 0:
                    zscore = abs(value - mean) / std
                    if zscore > max_zscore:
                        record_anomalies.append({
                            'field': field,
                            'value': value,
                            'zscore': round(zscore, 2),
                            'expected_mean': mean,
                            'expected_std': std,
                        })

            if record_anomalies:
                anomalies.append({
                    'record_index': i,
                    'record_id': record.get('player_id') or record.get('id'),
                    'record_name': record.get('player_name') or record.get('name'),
                    'anomalies': record_anomalies,
                })

        return anomalies


# ============================================
# QUALITY REPORTER
# ============================================

class QualityReporter:
    """
    Generates quality reports for ETL runs.
    """

    def __init__(self, db=None):
        self.db = db
        self.validator = DataQualityValidator(db)
        self.anomaly_detector = AnomalyDetector()

    def generate_run_report(
        self,
        run_id: int = None,
        entity_type: str = None,
        records: List[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Generate a quality report for an ETL run.

        Args:
            run_id: ETL run ID (if using database tracking)
            entity_type: Type of entity being processed
            records: Records to validate

        Returns:
            Quality report dictionary
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'run_id': run_id,
            'entity_type': entity_type,
            'total_records': len(records) if records else 0,
            'valid_records': 0,
            'invalid_records': 0,
            'quality_score': 100.0,
            'errors_by_severity': {'critical': 0, 'high': 0, 'medium': 0, 'low': 0},
            'errors_by_field': {},
            'anomalies': [],
            'sample_errors': [],
        }

        if not records:
            return report

        # Validate records
        if entity_type:
            valid, invalid, quality_score = self.validator.validate_batch(entity_type, records)
            report['valid_records'] = len(valid)
            report['invalid_records'] = len(invalid)
            report['quality_score'] = quality_score

            # Aggregate errors
            for item in invalid:
                for error in item.get('errors', []):
                    severity = error.get('severity', 'medium')
                    report['errors_by_severity'][severity] = report['errors_by_severity'].get(severity, 0) + 1

                    field = error.get('field')
                    report['errors_by_field'][field] = report['errors_by_field'].get(field, 0) + 1

            # Sample errors (first 5)
            report['sample_errors'] = [item['errors'][:3] for item in invalid[:5]]

        # Detect anomalies
        anomalies = self.anomaly_detector.detect_anomalies(records)
        report['anomalies'] = anomalies[:10]  # Top 10 anomalies
        report['anomaly_count'] = len(anomalies)

        return report

    def log_quality_issue(
        self,
        etl_run_id: int,
        severity: str,
        category: str,
        description: str,
        table_name: str = None,
        column_name: str = None,
        record_id: int = None,
        expected_value: str = None,
        actual_value: str = None,
    ):
        """Log a quality issue to the database."""
        if not self.db:
            logger.warning(f"Quality issue (no DB): {severity} - {description}")
            return

        try:
            self.db.execute_query(
                """
                INSERT INTO data_quality_issues (
                    etl_run_id, severity, category, table_name, column_name,
                    record_id, issue_description, expected_value, actual_value
                ) VALUES (
                    :run_id, :severity, :category, :table_name, :column_name,
                    :record_id, :description, :expected, :actual
                )
                """,
                params={
                    'run_id': etl_run_id,
                    'severity': severity,
                    'category': category,
                    'table_name': table_name,
                    'column_name': column_name,
                    'record_id': record_id,
                    'description': description,
                    'expected': expected_value,
                    'actual': actual_value,
                },
                fetch=False
            )
        except Exception as e:
            logger.error(f"Failed to log quality issue: {e}")


# ============================================
# SAFE VALUE EXTRACTION
# ============================================

def safe_extract_scalar(value: Any, default: Any = None, max_length: int = None) -> Any:
    """
    Safely extract a scalar value from potentially dict-like objects.

    This addresses ISSUE-019 where FotMob returns dicts like:
    {'key': 'right', 'fallback': 'Right'}

    Args:
        value: The value to extract from
        default: Default value if extraction fails
        max_length: Maximum string length (truncates if exceeded)

    Returns:
        Scalar value suitable for database insertion
    """
    if value is None:
        return default

    # Handle dict format from FotMob
    if isinstance(value, dict):
        # Try common keys
        extracted = value.get('fallback') or value.get('key') or value.get('value')
        if extracted is not None:
            value = extracted
        else:
            # Try first non-None value
            for v in value.values():
                if v is not None:
                    value = v
                    break
            else:
                return default

    # Convert to string if still dict-like
    if hasattr(value, 'get') and not isinstance(value, str):
        value = str(value)

    # Check for stringified dict
    if isinstance(value, str):
        val_str = str(value)
        if val_str.startswith('{') and ('fallback' in val_str or 'key' in val_str):
            # This is a stringified dict - don't use it
            return default

        # Truncate if needed
        if max_length and len(val_str) > max_length:
            logger.warning(f"Truncating value from {len(val_str)} to {max_length} chars")
            return val_str[:max_length]

    return value


def safe_extract_numeric(value: Any, default: Any = None) -> Optional[float]:
    """
    Safely extract a numeric value.

    Args:
        value: The value to extract from
        default: Default value if extraction fails

    Returns:
        Numeric value or default
    """
    if value is None:
        return default

    # Handle dict format
    if isinstance(value, dict):
        value = value.get('fallback') or value.get('key') or value.get('value')

    if value is None:
        return default

    try:
        if isinstance(value, str):
            # Remove common suffixes
            value = value.replace('%', '').replace('cm', '').replace('kg', '').strip()
        return float(value)
    except (ValueError, TypeError):
        return default
