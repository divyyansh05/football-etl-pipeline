"""
Data Validation Framework for Football Data Pipeline.

Features:
- Schema validation for all entities (teams, players, matches, etc.)
- Type checking and coercion
- Range validation for numeric fields
- Reference integrity checks
- Custom validation rules
- Detailed error reporting
"""

import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Callable, Union, Type
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


# ============================================
# VALIDATION ERRORS
# ============================================

class ValidationError(Exception):
    """Base validation error."""

    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        self.message = message
        self.field = field
        self.value = value
        super().__init__(self.format_message())

    def format_message(self) -> str:
        if self.field:
            return f"Validation error on field '{self.field}': {self.message}"
        return f"Validation error: {self.message}"


class SchemaValidationError(ValidationError):
    """Error for schema validation failures."""
    pass


class TypeValidationError(ValidationError):
    """Error for type validation failures."""
    pass


class RangeValidationError(ValidationError):
    """Error for range validation failures."""
    pass


class ReferenceValidationError(ValidationError):
    """Error for reference integrity failures."""
    pass


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    cleaned_data: Optional[Dict[str, Any]] = None

    def add_error(self, error: ValidationError):
        """Add an error and mark as invalid."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str):
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(warning)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "errors": [
                {
                    "message": e.message,
                    "field": e.field,
                    "value": str(e.value) if e.value is not None else None
                }
                for e in self.errors
            ],
            "warnings": self.warnings
        }


# ============================================
# FIELD VALIDATORS
# ============================================

class FieldValidator:
    """Base class for field validators."""

    def validate(self, value: Any, field_name: str) -> Any:
        """Validate and return cleaned value."""
        raise NotImplementedError


class RequiredValidator(FieldValidator):
    """Validates that a field is not None or empty."""

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            raise ValidationError(f"Field is required", field_name, value)
        if isinstance(value, str) and not value.strip():
            raise ValidationError(f"Field cannot be empty", field_name, value)
        return value


class TypeValidator(FieldValidator):
    """Validates and coerces field type."""

    def __init__(self, expected_type: Type, coerce: bool = True):
        self.expected_type = expected_type
        self.coerce = coerce

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if isinstance(value, self.expected_type):
            return value

        if self.coerce:
            try:
                if self.expected_type == int:
                    # Handle string numbers
                    if isinstance(value, str):
                        value = value.strip()
                        if not value:
                            return None
                    return int(float(value))

                elif self.expected_type == float:
                    if isinstance(value, str):
                        value = value.strip()
                        if not value:
                            return None
                    return float(value)

                elif self.expected_type == str:
                    return str(value).strip()

                elif self.expected_type == bool:
                    if isinstance(value, str):
                        return value.lower() in ('true', '1', 'yes', 'on')
                    return bool(value)

                elif self.expected_type == date:
                    if isinstance(value, datetime):
                        return value.date()
                    if isinstance(value, str):
                        # Try common date formats
                        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                            try:
                                return datetime.strptime(value, fmt).date()
                            except ValueError:
                                continue
                    raise ValueError(f"Cannot parse date: {value}")

                elif self.expected_type == datetime:
                    if isinstance(value, str):
                        # Try common datetime formats
                        for fmt in [
                            '%Y-%m-%dT%H:%M:%S',
                            '%Y-%m-%dT%H:%M:%SZ',
                            '%Y-%m-%d %H:%M:%S',
                            '%Y-%m-%d'
                        ]:
                            try:
                                return datetime.strptime(value, fmt)
                            except ValueError:
                                continue
                    raise ValueError(f"Cannot parse datetime: {value}")

                return self.expected_type(value)

            except (ValueError, TypeError) as e:
                raise TypeValidationError(
                    f"Cannot convert to {self.expected_type.__name__}: {e}",
                    field_name,
                    value
                )

        raise TypeValidationError(
            f"Expected {self.expected_type.__name__}, got {type(value).__name__}",
            field_name,
            value
        )


class RangeValidator(FieldValidator):
    """Validates numeric range."""

    def __init__(
        self,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None
    ):
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if self.min_value is not None and value < self.min_value:
            raise RangeValidationError(
                f"Value {value} is less than minimum {self.min_value}",
                field_name,
                value
            )

        if self.max_value is not None and value > self.max_value:
            raise RangeValidationError(
                f"Value {value} is greater than maximum {self.max_value}",
                field_name,
                value
            )

        return value


class LengthValidator(FieldValidator):
    """Validates string length."""

    def __init__(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None
    ):
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if not isinstance(value, str):
            return value

        if self.min_length is not None and len(value) < self.min_length:
            raise ValidationError(
                f"Length {len(value)} is less than minimum {self.min_length}",
                field_name,
                value
            )

        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError(
                f"Length {len(value)} is greater than maximum {self.max_length}",
                field_name,
                value
            )

        return value


class RegexValidator(FieldValidator):
    """Validates string against regex pattern."""

    def __init__(self, pattern: str, message: Optional[str] = None):
        self.pattern = re.compile(pattern)
        self.message = message or f"Value does not match pattern: {pattern}"

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if not isinstance(value, str):
            value = str(value)

        if not self.pattern.match(value):
            raise ValidationError(self.message, field_name, value)

        return value


class EnumValidator(FieldValidator):
    """Validates value is in allowed set."""

    def __init__(self, allowed_values: List[Any], case_insensitive: bool = False):
        self.allowed_values = allowed_values
        self.case_insensitive = case_insensitive

        if case_insensitive:
            self.lookup = {str(v).lower(): v for v in allowed_values}
        else:
            self.lookup = {v: v for v in allowed_values}

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        lookup_key = str(value).lower() if self.case_insensitive else value

        if lookup_key not in self.lookup:
            raise ValidationError(
                f"Value must be one of: {', '.join(str(v) for v in self.allowed_values)}",
                field_name,
                value
            )

        return self.lookup[lookup_key]


class URLValidator(FieldValidator):
    """Validates URL format."""

    URL_PATTERN = re.compile(
        r'^https?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )

    def validate(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if not isinstance(value, str):
            raise ValidationError("URL must be a string", field_name, value)

        if not self.URL_PATTERN.match(value):
            raise ValidationError("Invalid URL format", field_name, value)

        return value


# ============================================
# SCHEMA DEFINITIONS
# ============================================

@dataclass
class FieldSchema:
    """Schema definition for a single field."""
    name: str
    validators: List[FieldValidator] = field(default_factory=list)
    required: bool = False
    default: Any = None
    transform: Optional[Callable[[Any], Any]] = None


class EntitySchema:
    """Base class for entity schemas."""

    def __init__(self):
        self.fields: Dict[str, FieldSchema] = {}

    def add_field(
        self,
        name: str,
        validators: Optional[List[FieldValidator]] = None,
        required: bool = False,
        default: Any = None,
        transform: Optional[Callable] = None
    ):
        """Add a field to the schema."""
        self.fields[name] = FieldSchema(
            name=name,
            validators=validators or [],
            required=required,
            default=default,
            transform=transform
        )

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against schema."""
        result = ValidationResult(is_valid=True, cleaned_data={})

        for field_name, field_schema in self.fields.items():
            value = data.get(field_name, field_schema.default)

            # Check required
            if field_schema.required and (value is None or value == ''):
                result.add_error(ValidationError(
                    "Field is required",
                    field_name,
                    value
                ))
                continue

            # Skip validation if None and not required
            if value is None:
                result.cleaned_data[field_name] = None
                continue

            # Apply transform first
            if field_schema.transform:
                try:
                    value = field_schema.transform(value)
                except Exception as e:
                    result.add_error(ValidationError(
                        f"Transform failed: {e}",
                        field_name,
                        value
                    ))
                    continue

            # Run validators
            for validator in field_schema.validators:
                try:
                    value = validator.validate(value, field_name)
                except ValidationError as e:
                    result.add_error(e)
                    break
            else:
                # All validators passed
                result.cleaned_data[field_name] = value

        return result


# ============================================
# FOOTBALL ENTITY SCHEMAS
# ============================================

class TeamSchema(EntitySchema):
    """Validation schema for team entities."""

    # Valid positions for position validation
    VALID_POSITIONS = [
        'GK', 'CB', 'LB', 'RB', 'WB', 'LWB', 'RWB',
        'CDM', 'CM', 'CAM', 'LM', 'RM',
        'LW', 'RW', 'CF', 'ST',
        'DEF', 'MID', 'FWD', 'Defender', 'Midfielder', 'Attacker', 'Goalkeeper'
    ]

    def __init__(self):
        super().__init__()

        self.add_field('name', [
            TypeValidator(str),
            LengthValidator(min_length=1, max_length=100)
        ], required=True)

        self.add_field('short_name', [
            TypeValidator(str),
            LengthValidator(max_length=50)
        ])

        self.add_field('code', [
            TypeValidator(str),
            LengthValidator(max_length=10)
        ])

        self.add_field('country', [
            TypeValidator(str),
            LengthValidator(min_length=1, max_length=100)
        ], required=True)

        self.add_field('city', [
            TypeValidator(str),
            LengthValidator(max_length=100)
        ])

        self.add_field('founded', [
            TypeValidator(int),
            RangeValidator(min_value=1800, max_value=datetime.now().year)
        ])

        self.add_field('stadium', [
            TypeValidator(str),
            LengthValidator(max_length=200)
        ])

        self.add_field('stadium_capacity', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=200000)
        ])

        self.add_field('logo_url', [URLValidator()])

        self.add_field('api_football_id', [
            TypeValidator(int),
            RangeValidator(min_value=1)
        ])


class PlayerSchema(EntitySchema):
    """Validation schema for player entities."""

    VALID_POSITIONS = [
        'GK', 'CB', 'LB', 'RB', 'WB', 'LWB', 'RWB',
        'CDM', 'CM', 'CAM', 'LM', 'RM',
        'LW', 'RW', 'CF', 'ST',
        'DEF', 'MID', 'FWD', 'Defender', 'Midfielder', 'Attacker', 'Goalkeeper'
    ]

    VALID_FEET = ['left', 'right', 'both', 'Left', 'Right', 'Both']

    def __init__(self):
        super().__init__()

        self.add_field('name', [
            TypeValidator(str),
            LengthValidator(min_length=1, max_length=150)
        ], required=True)

        self.add_field('first_name', [
            TypeValidator(str),
            LengthValidator(max_length=100)
        ])

        self.add_field('last_name', [
            TypeValidator(str),
            LengthValidator(max_length=100)
        ])

        self.add_field('date_of_birth', [
            TypeValidator(date)
        ])

        self.add_field('nationality', [
            TypeValidator(str),
            LengthValidator(min_length=1, max_length=100)
        ])

        self.add_field('height_cm', [
            TypeValidator(int),
            RangeValidator(min_value=140, max_value=230)
        ], transform=self._parse_height)

        self.add_field('weight_kg', [
            TypeValidator(int),
            RangeValidator(min_value=40, max_value=150)
        ], transform=self._parse_weight)

        self.add_field('position', [
            TypeValidator(str),
            EnumValidator(self.VALID_POSITIONS, case_insensitive=True)
        ])

        self.add_field('preferred_foot', [
            TypeValidator(str),
            EnumValidator(self.VALID_FEET, case_insensitive=True)
        ], transform=lambda x: x.lower() if x else None)

        self.add_field('jersey_number', [
            TypeValidator(int),
            RangeValidator(min_value=1, max_value=99)
        ])

        self.add_field('photo_url', [URLValidator()])

        self.add_field('api_football_id', [
            TypeValidator(int),
            RangeValidator(min_value=1)
        ])

    @staticmethod
    def _parse_height(value: Any) -> Optional[int]:
        """Parse height from various formats (e.g., '180 cm', '1.80 m')."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value) if value > 100 else int(value * 100)
        if isinstance(value, str):
            value = value.lower().strip()
            if 'cm' in value:
                return int(float(value.replace('cm', '').strip()))
            if 'm' in value:
                return int(float(value.replace('m', '').strip()) * 100)
            try:
                num = float(value)
                return int(num) if num > 100 else int(num * 100)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_weight(value: Any) -> Optional[int]:
        """Parse weight from various formats (e.g., '75 kg', '165 lbs')."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.lower().strip()
            if 'kg' in value:
                return int(float(value.replace('kg', '').strip()))
            if 'lb' in value or 'lbs' in value:
                lbs = float(value.replace('lbs', '').replace('lb', '').strip())
                return int(lbs * 0.453592)
            try:
                return int(float(value))
            except ValueError:
                return None
        return None


class MatchSchema(EntitySchema):
    """Validation schema for match entities."""

    VALID_STATUSES = [
        'scheduled', 'live', 'finished', 'postponed',
        'cancelled', 'abandoned', 'suspended',
        'TBD', 'NS', 'FT', 'AET', 'PEN', '1H', '2H', 'HT', 'ET', 'BT'
    ]

    def __init__(self):
        super().__init__()

        self.add_field('league_id', [TypeValidator(str)], required=True)

        self.add_field('season', [
            TypeValidator(str),
            RegexValidator(r'^\d{4}(-\d{2})?$', "Season must be YYYY or YYYY-YY format")
        ], required=True)

        self.add_field('matchday', [
            TypeValidator(int),
            RangeValidator(min_value=1, max_value=50)
        ])

        self.add_field('date', [TypeValidator(datetime)], required=True)

        self.add_field('status', [
            TypeValidator(str),
            EnumValidator(self.VALID_STATUSES, case_insensitive=True)
        ], default='scheduled')

        self.add_field('home_team_id', [TypeValidator(str)], required=True)
        self.add_field('away_team_id', [TypeValidator(str)], required=True)

        self.add_field('home_score', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=50)
        ])

        self.add_field('away_score', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=50)
        ])

        self.add_field('home_score_ht', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=30)
        ])

        self.add_field('away_score_ht', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=30)
        ])

        self.add_field('venue', [
            TypeValidator(str),
            LengthValidator(max_length=200)
        ])

        self.add_field('referee', [
            TypeValidator(str),
            LengthValidator(max_length=100)
        ])

        self.add_field('attendance', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=200000)
        ])

        self.add_field('api_football_id', [
            TypeValidator(int),
            RangeValidator(min_value=1)
        ])


class PlayerStatsSchema(EntitySchema):
    """Validation schema for player season statistics."""

    def __init__(self):
        super().__init__()

        self.add_field('player_id', [TypeValidator(str)], required=True)
        self.add_field('team_id', [TypeValidator(str)], required=True)
        self.add_field('league_id', [TypeValidator(str)], required=True)
        self.add_field('season', [TypeValidator(str)], required=True)

        # Appearance stats
        self.add_field('appearances', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=100)
        ], default=0)

        self.add_field('starts', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=100)
        ], default=0)

        self.add_field('minutes_played', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=10000)
        ], default=0)

        # Attack stats
        self.add_field('goals', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=100)
        ], default=0)

        self.add_field('assists', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=100)
        ], default=0)

        self.add_field('shots', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=500)
        ], default=0)

        self.add_field('shots_on_target', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=300)
        ], default=0)

        # Passing stats
        self.add_field('passes', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=10000)
        ], default=0)

        self.add_field('pass_accuracy', [
            TypeValidator(float),
            RangeValidator(min_value=0, max_value=100)
        ])

        self.add_field('key_passes', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=500)
        ], default=0)

        # Discipline
        self.add_field('yellow_cards', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=30)
        ], default=0)

        self.add_field('red_cards', [
            TypeValidator(int),
            RangeValidator(min_value=0, max_value=10)
        ], default=0)


# ============================================
# VALIDATION UTILITIES
# ============================================

# Pre-instantiated schemas for convenience
SCHEMAS = {
    'team': TeamSchema(),
    'player': PlayerSchema(),
    'match': MatchSchema(),
    'player_stats': PlayerStatsSchema()
}


def validate_entity(entity_type: str, data: Dict[str, Any]) -> ValidationResult:
    """
    Validate an entity against its schema.

    Args:
        entity_type: Type of entity ('team', 'player', 'match', 'player_stats')
        data: Data to validate

    Returns:
        ValidationResult with cleaned data or errors
    """
    schema = SCHEMAS.get(entity_type)
    if not schema:
        raise ValueError(f"Unknown entity type: {entity_type}")

    return schema.validate(data)


def validate_batch(
    entity_type: str,
    data_list: List[Dict[str, Any]],
    fail_fast: bool = False
) -> Dict[str, Any]:
    """
    Validate a batch of entities.

    Args:
        entity_type: Type of entity
        data_list: List of data dicts to validate
        fail_fast: Stop on first error

    Returns:
        Dict with valid_records, invalid_records, and summary
    """
    valid_records = []
    invalid_records = []

    for i, data in enumerate(data_list):
        result = validate_entity(entity_type, data)

        if result.is_valid:
            valid_records.append(result.cleaned_data)
        else:
            invalid_records.append({
                'index': i,
                'data': data,
                'errors': result.to_dict()['errors']
            })

            if fail_fast:
                break

    return {
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'summary': {
            'total': len(data_list),
            'valid': len(valid_records),
            'invalid': len(invalid_records),
            'validation_rate': len(valid_records) / len(data_list) * 100 if data_list else 0
        }
    }


def clean_and_validate(
    entity_type: str,
    data: Dict[str, Any],
    raise_on_error: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Clean and validate a single entity.

    Args:
        entity_type: Type of entity
        data: Data to validate
        raise_on_error: Raise exception on validation error

    Returns:
        Cleaned data or None if invalid
    """
    result = validate_entity(entity_type, data)

    if not result.is_valid:
        if raise_on_error:
            raise SchemaValidationError(
                f"Validation failed with {len(result.errors)} errors: "
                f"{[e.message for e in result.errors]}"
            )
        logger.warning(
            f"Validation failed for {entity_type}",
            extra={
                'entity_type': entity_type,
                'errors': [e.message for e in result.errors],
                'data_sample': str(data)[:200]
            }
        )
        return None

    return result.cleaned_data
