"""
Data Deduplication Logic for Football Data Pipeline.

Features:
- Player deduplication based on multiple matching criteria
- Fuzzy name matching with configurable thresholds
- Cross-source entity resolution
- Merge strategies for duplicate records
- Audit trail for merged records
"""

import logging
import re
from datetime import date
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from collections import defaultdict

logger = logging.getLogger(__name__)


# ============================================
# SIMILARITY FUNCTIONS
# ============================================

def normalize_name(name: str) -> str:
    """
    Normalize a player name for comparison.

    - Lowercase
    - Remove diacritics/accents
    - Remove common prefixes/suffixes
    - Standardize whitespace
    """
    if not name:
        return ""

    # Lowercase
    normalized = name.lower().strip()

    # Remove common prefixes
    prefixes = ['jr.', 'jr', 'sr.', 'sr', 'ii', 'iii', 'iv']
    for prefix in prefixes:
        if normalized.endswith(f' {prefix}'):
            normalized = normalized[:-len(prefix)-1]

    # Remove extra whitespace
    normalized = ' '.join(normalized.split())

    # Simple accent removal (basic Latin)
    accent_map = {
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'å': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
        'ý': 'y', 'ÿ': 'y',
        'ñ': 'n', 'ç': 'c',
        'ß': 'ss', 'æ': 'ae', 'œ': 'oe',
        'ø': 'o', 'ð': 'd', 'þ': 'th'
    }

    for accented, plain in accent_map.items():
        normalized = normalized.replace(accented, plain)

    return normalized


def string_similarity(s1: str, s2: str) -> float:
    """
    Calculate similarity ratio between two strings.

    Uses SequenceMatcher for fuzzy matching.
    Returns value between 0 and 1.
    """
    if not s1 or not s2:
        return 0.0

    s1_norm = normalize_name(s1)
    s2_norm = normalize_name(s2)

    if s1_norm == s2_norm:
        return 1.0

    return SequenceMatcher(None, s1_norm, s2_norm).ratio()


def name_parts_match(name1: str, name2: str) -> float:
    """
    Compare names by their parts (first name, last name).

    Handles cases like:
    - "Bruno Fernandes" vs "Fernandes, Bruno"
    - "B. Fernandes" vs "Bruno Fernandes"
    - "Mohamed Salah" vs "M. Salah"
    """
    if not name1 or not name2:
        return 0.0

    parts1 = set(normalize_name(name1).split())
    parts2 = set(normalize_name(name2).split())

    if not parts1 or not parts2:
        return 0.0

    # Check for exact match of parts
    if parts1 == parts2:
        return 1.0

    # Check overlap
    intersection = parts1 & parts2
    union = parts1 | parts2

    if not union:
        return 0.0

    jaccard = len(intersection) / len(union)

    # Bonus for matching last name (assuming last word is surname)
    list1 = normalize_name(name1).split()
    list2 = normalize_name(name2).split()

    if list1 and list2 and list1[-1] == list2[-1]:
        jaccard = min(1.0, jaccard + 0.3)

    return jaccard


def calculate_age_at_date(dob: date, reference_date: date) -> int:
    """Calculate age at a given date."""
    age = reference_date.year - dob.year
    if reference_date.month < dob.month or (
        reference_date.month == dob.month and reference_date.day < dob.day
    ):
        age -= 1
    return age


# ============================================
# MATCH SCORING
# ============================================

@dataclass
class MatchScore:
    """Score for a potential duplicate match."""
    score: float
    confidence: str  # 'high', 'medium', 'low'
    matching_fields: List[str]
    mismatching_fields: List[str]
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_likely_duplicate(self) -> bool:
        return self.score >= 0.8

    @property
    def is_possible_duplicate(self) -> bool:
        return 0.6 <= self.score < 0.8


@dataclass
class PlayerRecord:
    """Simplified player record for deduplication."""
    id: str
    name: str
    date_of_birth: Optional[date] = None
    nationality: Optional[str] = None
    position: Optional[str] = None
    current_team_id: Optional[str] = None
    api_football_id: Optional[int] = None
    fotmob_id: Optional[int] = None
    height_cm: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PlayerRecord':
        """Create from dictionary."""
        dob = data.get('date_of_birth')
        if isinstance(dob, str):
            try:
                dob = date.fromisoformat(dob)
            except:
                dob = None

        return cls(
            id=str(data.get('player_id') or data.get('id', '')),
            name=data.get('player_name') or data.get('name', ''),
            date_of_birth=dob,
            nationality=data.get('nationality'),
            position=data.get('position'),
            current_team_id=str(data.get('current_team_id', '')) if data.get('current_team_id') else None,
            api_football_id=data.get('api_football_id'),
            fotmob_id=data.get('fotmob_id'),
            height_cm=data.get('height_cm')
        )


class PlayerDeduplicator:
    """
    Deduplication logic for player entities.

    Uses multiple signals to identify duplicates:
    1. External IDs (api_football_id, fotmob_id) - exact match
    2. Name similarity - fuzzy match
    3. Date of birth - exact match
    4. Nationality - exact match
    5. Team context - supporting signal
    """

    # Weight for each matching criteria
    WEIGHTS = {
        'external_id': 1.0,      # Definitive match
        'name_exact': 0.35,
        'name_fuzzy': 0.25,
        'dob': 0.25,
        'nationality': 0.10,
        'position': 0.05,
        'team': 0.05
    }

    # Thresholds
    NAME_SIMILARITY_THRESHOLD = 0.85
    HIGH_CONFIDENCE_THRESHOLD = 0.9
    MEDIUM_CONFIDENCE_THRESHOLD = 0.75
    LOW_CONFIDENCE_THRESHOLD = 0.6

    def calculate_match_score(
        self,
        player1: PlayerRecord,
        player2: PlayerRecord
    ) -> MatchScore:
        """
        Calculate match score between two player records.

        Returns MatchScore with overall score and details.
        """
        score = 0.0
        matching = []
        mismatching = []
        details = {}

        # 1. Check external IDs first (definitive match)
        if player1.api_football_id and player2.api_football_id:
            if player1.api_football_id == player2.api_football_id:
                return MatchScore(
                    score=1.0,
                    confidence='high',
                    matching_fields=['api_football_id'],
                    mismatching_fields=[],
                    details={'match_type': 'external_id_exact'}
                )
            else:
                # Different IDs = different players
                return MatchScore(
                    score=0.0,
                    confidence='high',
                    matching_fields=[],
                    mismatching_fields=['api_football_id'],
                    details={'match_type': 'external_id_mismatch'}
                )

        if player1.fotmob_id and player2.fotmob_id:
            if player1.fotmob_id == player2.fotmob_id:
                return MatchScore(
                    score=1.0,
                    confidence='high',
                    matching_fields=['fotmob_id'],
                    mismatching_fields=[],
                    details={'match_type': 'external_id_exact'}
                )
            else:
                return MatchScore(
                    score=0.0,
                    confidence='high',
                    matching_fields=[],
                    mismatching_fields=['fotmob_id'],
                    details={'match_type': 'external_id_mismatch'}
                )

        # 2. Name similarity
        name_sim = string_similarity(player1.name, player2.name)
        name_parts_sim = name_parts_match(player1.name, player2.name)
        best_name_sim = max(name_sim, name_parts_sim)

        details['name_similarity'] = round(best_name_sim, 3)

        if best_name_sim >= 0.95:
            score += self.WEIGHTS['name_exact']
            matching.append('name (exact)')
        elif best_name_sim >= self.NAME_SIMILARITY_THRESHOLD:
            score += self.WEIGHTS['name_fuzzy']
            matching.append(f'name (fuzzy: {best_name_sim:.2f})')
        elif best_name_sim < 0.5:
            mismatching.append('name')

        # 3. Date of birth
        if player1.date_of_birth and player2.date_of_birth:
            if player1.date_of_birth == player2.date_of_birth:
                score += self.WEIGHTS['dob']
                matching.append('date_of_birth')
            else:
                # Check if within 1 year (data entry errors)
                diff_days = abs((player1.date_of_birth - player2.date_of_birth).days)
                if diff_days <= 365:
                    score += self.WEIGHTS['dob'] * 0.5
                    matching.append(f'date_of_birth (within 1 year)')
                else:
                    mismatching.append('date_of_birth')

        # 4. Nationality
        if player1.nationality and player2.nationality:
            nat1 = player1.nationality.lower().strip()
            nat2 = player2.nationality.lower().strip()
            if nat1 == nat2:
                score += self.WEIGHTS['nationality']
                matching.append('nationality')
            else:
                mismatching.append('nationality')

        # 5. Position
        if player1.position and player2.position:
            pos1 = player1.position.upper()
            pos2 = player2.position.upper()

            # Position groups for fuzzy matching
            position_groups = {
                'GK': ['GK', 'Goalkeeper'],
                'DEF': ['CB', 'LB', 'RB', 'LWB', 'RWB', 'WB', 'DEF', 'Defender'],
                'MID': ['CM', 'CDM', 'CAM', 'LM', 'RM', 'MID', 'Midfielder'],
                'FWD': ['ST', 'CF', 'LW', 'RW', 'FWD', 'Attacker', 'Forward']
            }

            def get_position_group(pos):
                for group, positions in position_groups.items():
                    if pos in positions or pos.title() in positions:
                        return group
                return None

            if pos1 == pos2:
                score += self.WEIGHTS['position']
                matching.append('position')
            elif get_position_group(pos1) == get_position_group(pos2):
                score += self.WEIGHTS['position'] * 0.5
                matching.append('position (same group)')
            else:
                mismatching.append('position')

        # 6. Team context (weak signal)
        if player1.current_team_id and player2.current_team_id:
            if player1.current_team_id == player2.current_team_id:
                score += self.WEIGHTS['team']
                matching.append('current_team')

        # Determine confidence
        if score >= self.HIGH_CONFIDENCE_THRESHOLD:
            confidence = 'high'
        elif score >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            confidence = 'medium'
        elif score >= self.LOW_CONFIDENCE_THRESHOLD:
            confidence = 'low'
        else:
            confidence = 'none'

        return MatchScore(
            score=score,
            confidence=confidence,
            matching_fields=matching,
            mismatching_fields=mismatching,
            details=details
        )

    def find_duplicates(
        self,
        players: List[PlayerRecord],
        threshold: float = 0.75
    ) -> List[Tuple[PlayerRecord, PlayerRecord, MatchScore]]:
        """
        Find potential duplicates in a list of players.

        Returns list of (player1, player2, score) tuples.
        """
        duplicates = []

        # Build index by normalized last name for efficiency
        by_last_name: Dict[str, List[PlayerRecord]] = defaultdict(list)
        for player in players:
            if player.name:
                parts = normalize_name(player.name).split()
                if parts:
                    by_last_name[parts[-1]].append(player)

        # Compare within name groups
        checked_pairs: Set[Tuple[str, str]] = set()

        for name_key, group in by_last_name.items():
            for i, player1 in enumerate(group):
                for player2 in group[i+1:]:
                    pair_key = tuple(sorted([player1.id, player2.id]))
                    if pair_key in checked_pairs:
                        continue
                    checked_pairs.add(pair_key)

                    score = self.calculate_match_score(player1, player2)
                    if score.score >= threshold:
                        duplicates.append((player1, player2, score))

        # Sort by score descending
        duplicates.sort(key=lambda x: x[2].score, reverse=True)

        return duplicates

    def find_duplicates_for_new_record(
        self,
        new_player: PlayerRecord,
        existing_players: List[PlayerRecord],
        threshold: float = 0.75
    ) -> List[Tuple[PlayerRecord, MatchScore]]:
        """
        Find potential duplicates for a new player record.

        Returns list of (existing_player, score) tuples.
        """
        matches = []

        for existing in existing_players:
            score = self.calculate_match_score(new_player, existing)
            if score.score >= threshold:
                matches.append((existing, score))

        # Sort by score descending
        matches.sort(key=lambda x: x[1].score, reverse=True)

        return matches


# ============================================
# MERGE STRATEGIES
# ============================================

class MergeStrategy:
    """Base class for merge strategies."""

    def merge(
        self,
        primary: Dict[str, Any],
        secondary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge two records, returning merged result."""
        raise NotImplementedError


class PreferPrimaryMergeStrategy(MergeStrategy):
    """
    Merge strategy that prefers primary record values.

    Only fills in missing values from secondary.
    """

    def merge(
        self,
        primary: Dict[str, Any],
        secondary: Dict[str, Any]
    ) -> Dict[str, Any]:
        result = primary.copy()

        for key, value in secondary.items():
            if key not in result or result[key] is None:
                result[key] = value

        return result


class PreferNonNullMergeStrategy(MergeStrategy):
    """
    Merge strategy that prefers non-null values.

    Takes first non-null value for each field.
    """

    def merge(
        self,
        primary: Dict[str, Any],
        secondary: Dict[str, Any]
    ) -> Dict[str, Any]:
        result = {}
        all_keys = set(primary.keys()) | set(secondary.keys())

        for key in all_keys:
            primary_val = primary.get(key)
            secondary_val = secondary.get(key)

            if primary_val is not None:
                result[key] = primary_val
            elif secondary_val is not None:
                result[key] = secondary_val
            else:
                result[key] = None

        return result


class SourcePriorityMergeStrategy(MergeStrategy):
    """
    Merge strategy based on data source priority.

    Uses source reliability to determine preference.
    """

    SOURCE_PRIORITY = {
        'api_football': 1,
        'fotmob': 2,
        'statsbomb': 3
    }

    def __init__(self, primary_source: str, secondary_source: str):
        self.primary_source = primary_source
        self.secondary_source = secondary_source

    def merge(
        self,
        primary: Dict[str, Any],
        secondary: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Determine which source has priority
        primary_priority = self.SOURCE_PRIORITY.get(self.primary_source, 99)
        secondary_priority = self.SOURCE_PRIORITY.get(self.secondary_source, 99)

        if primary_priority <= secondary_priority:
            base = primary
            supplement = secondary
        else:
            base = secondary
            supplement = primary

        result = base.copy()
        for key, value in supplement.items():
            if key not in result or result[key] is None:
                result[key] = value

        return result


# ============================================
# DATABASE DEDUPLICATION QUERIES
# ============================================

def get_duplicate_detection_query() -> str:
    """
    SQL query to find potential duplicate players.

    Returns players with similar names and matching DOB or nationality.
    """
    return """
    WITH normalized_players AS (
        SELECT
            player_id,
            player_name,
            LOWER(TRIM(player_name)) as name_normalized,
            date_of_birth,
            nationality,
            position,
            current_team_id,
            api_football_id,
            fotmob_id
        FROM players
    )
    SELECT
        p1.player_id as player1_id,
        p1.player_name as player1_name,
        p2.player_id as player2_id,
        p2.player_name as player2_name,
        p1.date_of_birth as dob1,
        p2.date_of_birth as dob2,
        p1.nationality as nat1,
        p2.nationality as nat2,
        SIMILARITY(p1.name_normalized, p2.name_normalized) as name_similarity
    FROM normalized_players p1
    JOIN normalized_players p2
        ON p1.player_id < p2.player_id
        AND SIMILARITY(p1.name_normalized, p2.name_normalized) > 0.7
    WHERE
        -- Different players (by ID)
        p1.player_id != p2.player_id
        -- And at least one strong matching signal
        AND (
            -- Same DOB
            (p1.date_of_birth IS NOT NULL AND p1.date_of_birth = p2.date_of_birth)
            -- Or same nationality with very similar name
            OR (p1.nationality = p2.nationality AND SIMILARITY(p1.name_normalized, p2.name_normalized) > 0.85)
            -- Or matching external ID from different source
            OR (p1.api_football_id IS NOT NULL AND p1.api_football_id = p2.api_football_id)
            OR (p1.fotmob_id IS NOT NULL AND p1.fotmob_id = p2.fotmob_id)
        )
    ORDER BY name_similarity DESC
    LIMIT 100;
    """


def get_merge_players_query() -> str:
    """
    SQL query to merge duplicate player records.

    Updates references and deletes duplicate.
    """
    return """
    -- Merge player_id_secondary into player_id_primary
    -- This is a template - replace IDs as needed

    -- 1. Update player_season_stats references
    UPDATE player_season_stats
    SET player_id = %(primary_id)s
    WHERE player_id = %(secondary_id)s;

    -- 2. Update any other referencing tables
    -- (Add more UPDATE statements as needed for your schema)

    -- 3. Merge any additional data from secondary to primary
    UPDATE players
    SET
        api_football_id = COALESCE(api_football_id, (SELECT api_football_id FROM players WHERE player_id = %(secondary_id)s)),
        fotmob_id = COALESCE(fotmob_id, (SELECT fotmob_id FROM players WHERE player_id = %(secondary_id)s)),
        height_cm = COALESCE(height_cm, (SELECT height_cm FROM players WHERE player_id = %(secondary_id)s)),
        weight_kg = COALESCE(weight_kg, (SELECT weight_kg FROM players WHERE player_id = %(secondary_id)s)),
        photo_url = COALESCE(photo_url, (SELECT photo_url FROM players WHERE player_id = %(secondary_id)s))
    WHERE player_id = %(primary_id)s;

    -- 4. Delete the duplicate record
    DELETE FROM players WHERE player_id = %(secondary_id)s;
    """


# ============================================
# DEDUPLICATION REPORT
# ============================================

@dataclass
class DeduplicationReport:
    """Report of deduplication results."""
    total_records: int
    duplicates_found: int
    duplicates_merged: int
    duplicates_skipped: int
    errors: List[str] = field(default_factory=list)
    merged_pairs: List[Tuple[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_records': self.total_records,
            'duplicates_found': self.duplicates_found,
            'duplicates_merged': self.duplicates_merged,
            'duplicates_skipped': self.duplicates_skipped,
            'merge_rate': self.duplicates_merged / self.duplicates_found if self.duplicates_found > 0 else 0,
            'errors': self.errors,
            'merged_pairs': self.merged_pairs
        }
