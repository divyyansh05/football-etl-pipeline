"""
Player Identity Resolution Layer.

Provides cross-source entity matching and deduplication.
Part of ISSUE-025 fix.

Features:
- External ID-based matching (FotMob, API-Football, Transfermarkt, StatsBomb)
- Fuzzy name matching with confidence scores
- Mapping table management
- Manual override support
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, date
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


# ============================================
# DATA CLASSES
# ============================================

@dataclass
class PlayerIdentity:
    """Represents a player's cross-source identity."""
    player_id: int  # Internal database ID

    # External source IDs
    fotmob_id: Optional[int] = None
    api_football_id: Optional[int] = None
    transfermarkt_id: Optional[str] = None
    statsbomb_id: Optional[int] = None
    understat_id: Optional[int] = None
    whoscored_id: Optional[int] = None

    # Matching metadata
    confidence_score: float = 0.0
    match_method: str = 'unknown'
    verified: bool = False

    # Player info for matching
    name: Optional[str] = None
    date_of_birth: Optional[date] = None
    nationality: Optional[str] = None


@dataclass
class MatchCandidate:
    """A potential match between two player records."""
    source_id: Any  # External ID from source
    source_name: str
    player_id: int  # Database player ID
    player_name: str
    confidence: float
    match_method: str
    matching_fields: List[str]
    mismatching_fields: List[str]


# ============================================
# IDENTITY RESOLVER
# ============================================

class PlayerIdentityResolver:
    """
    Resolves player identities across multiple data sources.

    Usage:
        resolver = PlayerIdentityResolver(db)

        # Find existing player by external ID
        player_id = resolver.find_player_by_external_id(
            fotmob_id=12345
        )

        # Find or create with fuzzy matching
        player_id, is_new = resolver.resolve_player(
            source_name='fotmob',
            external_id=12345,
            player_name='Bruno Fernandes',
            date_of_birth=date(1994, 9, 8),
            nationality='Portugal'
        )

        # Update mapping after manual verification
        resolver.update_mapping(
            player_id=100,
            api_football_id=7890,
            verified=True
        )
    """

    # Confidence thresholds
    HIGH_CONFIDENCE = 0.95
    MEDIUM_CONFIDENCE = 0.80
    LOW_CONFIDENCE = 0.60

    # Source priority for merging
    SOURCE_PRIORITY = {
        'fotmob': 1,
        'api_football': 2,
        'transfermarkt': 3,
        'statsbomb': 4,
        'understat': 5,
        'whoscored': 6,
    }

    def __init__(self, db):
        self.db = db

    # =========================================
    # EXTERNAL ID LOOKUP
    # =========================================

    def find_player_by_external_id(
        self,
        fotmob_id: int = None,
        api_football_id: int = None,
        transfermarkt_id: str = None,
        statsbomb_id: int = None,
        understat_id: int = None,
    ) -> Optional[int]:
        """
        Find player ID by any external source ID.

        Checks mapping table first, then falls back to players table.

        Returns:
            player_id if found, None otherwise
        """
        # Try mapping table first
        conditions = []
        params = {}

        if fotmob_id:
            conditions.append("fotmob_id = :fotmob_id")
            params['fotmob_id'] = fotmob_id
        if api_football_id:
            conditions.append("api_football_id = :api_football_id")
            params['api_football_id'] = api_football_id
        if transfermarkt_id:
            conditions.append("transfermarkt_id = :transfermarkt_id")
            params['transfermarkt_id'] = transfermarkt_id
        if statsbomb_id:
            conditions.append("statsbomb_id = :statsbomb_id")
            params['statsbomb_id'] = statsbomb_id
        if understat_id:
            conditions.append("understat_id = :understat_id")
            params['understat_id'] = understat_id

        if not conditions:
            return None

        where_clause = " OR ".join(conditions)

        # Check mapping table
        result = self.db.execute_query(
            f"""
            SELECT player_id, confidence_score
            FROM player_id_mappings
            WHERE {where_clause}
            ORDER BY confidence_score DESC
            LIMIT 1
            """,
            params=params,
            fetch=True
        )

        if result:
            return result[0][0]

        # Fallback: check players table for fotmob_id
        if fotmob_id:
            result = self.db.execute_query(
                "SELECT player_id FROM players WHERE fotmob_id = :fid",
                params={'fid': fotmob_id},
                fetch=True
            )
            if result:
                return result[0][0]

        return None

    # =========================================
    # FUZZY MATCHING
    # =========================================

    def find_candidates_by_name(
        self,
        player_name: str,
        date_of_birth: date = None,
        nationality: str = None,
        team_id: int = None,
        max_candidates: int = 5,
    ) -> List[MatchCandidate]:
        """
        Find potential matching players by name similarity.

        Args:
            player_name: Player name to match
            date_of_birth: Optional DOB for filtering
            nationality: Optional nationality for filtering
            team_id: Optional team for filtering
            max_candidates: Maximum candidates to return

        Returns:
            List of MatchCandidate objects sorted by confidence
        """
        if not player_name:
            return []

        # Normalize name for comparison
        normalized = self._normalize_name(player_name)
        name_parts = normalized.split()
        last_name = name_parts[-1] if name_parts else normalized

        # Query potential matches
        params = {'name_pattern': f'%{last_name}%'}
        where_clauses = ["LOWER(player_name) LIKE LOWER(:name_pattern)"]

        if date_of_birth:
            where_clauses.append("(date_of_birth IS NULL OR date_of_birth = :dob)")
            params['dob'] = date_of_birth

        if nationality:
            where_clauses.append("(nationality IS NULL OR LOWER(nationality) = LOWER(:nat))")
            params['nat'] = nationality

        query = f"""
            SELECT player_id, player_name, date_of_birth, nationality, fotmob_id
            FROM players
            WHERE {' AND '.join(where_clauses)}
            LIMIT 50
        """

        result = self.db.execute_query(query, params=params, fetch=True)
        candidates = []

        for row in result:
            db_player_id, db_name, db_dob, db_nat, db_fotmob_id = row

            # Calculate match score
            confidence, matching, mismatching = self._calculate_match_confidence(
                player_name=player_name,
                db_name=db_name,
                date_of_birth=date_of_birth,
                db_dob=db_dob,
                nationality=nationality,
                db_nationality=db_nat,
            )

            if confidence >= self.LOW_CONFIDENCE:
                candidates.append(MatchCandidate(
                    source_id=db_fotmob_id,
                    source_name='database',
                    player_id=db_player_id,
                    player_name=db_name,
                    confidence=confidence,
                    match_method='fuzzy_name',
                    matching_fields=matching,
                    mismatching_fields=mismatching,
                ))

        # Sort by confidence descending
        candidates.sort(key=lambda x: x.confidence, reverse=True)

        return candidates[:max_candidates]

    def _calculate_match_confidence(
        self,
        player_name: str,
        db_name: str,
        date_of_birth: date = None,
        db_dob: date = None,
        nationality: str = None,
        db_nationality: str = None,
    ) -> Tuple[float, List[str], List[str]]:
        """
        Calculate confidence score for a potential match.

        Returns:
            Tuple of (confidence, matching_fields, mismatching_fields)
        """
        score = 0.0
        matching = []
        mismatching = []

        # Name similarity (weight: 0.5)
        name_sim = self._name_similarity(player_name, db_name)
        if name_sim >= 0.95:
            score += 0.5
            matching.append('name (exact)')
        elif name_sim >= 0.85:
            score += 0.4
            matching.append(f'name ({name_sim:.0%})')
        elif name_sim >= 0.70:
            score += 0.25
            matching.append(f'name ({name_sim:.0%})')
        else:
            mismatching.append('name')

        # Date of birth (weight: 0.3)
        if date_of_birth and db_dob:
            if date_of_birth == db_dob:
                score += 0.3
                matching.append('dob')
            else:
                mismatching.append('dob')
        elif date_of_birth or db_dob:
            # One is missing - neutral
            pass

        # Nationality (weight: 0.2)
        if nationality and db_nationality:
            if self._normalize_name(nationality) == self._normalize_name(db_nationality):
                score += 0.2
                matching.append('nationality')
            else:
                mismatching.append('nationality')

        return round(score, 3), matching, mismatching

    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name similarity using multiple methods."""
        if not name1 or not name2:
            return 0.0

        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)

        if n1 == n2:
            return 1.0

        # SequenceMatcher similarity
        seq_sim = SequenceMatcher(None, n1, n2).ratio()

        # Name parts overlap (Jaccard)
        parts1 = set(n1.split())
        parts2 = set(n2.split())
        if parts1 and parts2:
            jaccard = len(parts1 & parts2) / len(parts1 | parts2)
        else:
            jaccard = 0.0

        # Last name match bonus
        list1 = n1.split()
        list2 = n2.split()
        last_name_bonus = 0.1 if (list1 and list2 and list1[-1] == list2[-1]) else 0.0

        return min(1.0, max(seq_sim, jaccard) + last_name_bonus)

    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        if not name:
            return ""

        normalized = name.lower().strip()

        # Remove common suffixes
        for suffix in [' jr.', ' jr', ' sr.', ' sr', ' ii', ' iii', ' iv']:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]

        # Simple accent removal
        accent_map = {
            'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
            'ñ': 'n', 'ç': 'c',
        }
        for accented, plain in accent_map.items():
            normalized = normalized.replace(accented, plain)

        return ' '.join(normalized.split())

    # =========================================
    # RESOLUTION
    # =========================================

    def resolve_player(
        self,
        source_name: str,
        external_id: Any,
        player_name: str,
        date_of_birth: date = None,
        nationality: str = None,
        auto_create: bool = True,
    ) -> Tuple[Optional[int], bool]:
        """
        Resolve a player identity from an external source.

        Steps:
        1. Try exact ID match
        2. Try fuzzy name match
        3. Create new player if auto_create=True

        Args:
            source_name: Source system name
            external_id: ID from the source system
            player_name: Player name
            date_of_birth: Optional DOB
            nationality: Optional nationality
            auto_create: Whether to create new player if not found

        Returns:
            Tuple of (player_id, is_new)
        """
        is_new = False

        # 1. Try exact ID match
        id_params = {f'{source_name}_id': external_id} if source_name in ['fotmob', 'api_football', 'statsbomb', 'understat'] else {}
        if source_name == 'fotmob':
            id_params = {'fotmob_id': external_id}
        elif source_name == 'api_football':
            id_params = {'api_football_id': external_id}
        elif source_name == 'transfermarkt':
            id_params = {'transfermarkt_id': external_id}
        elif source_name == 'statsbomb':
            id_params = {'statsbomb_id': external_id}
        elif source_name == 'understat':
            id_params = {'understat_id': external_id}

        player_id = self.find_player_by_external_id(**id_params)
        if player_id:
            return (player_id, False)

        # 2. Try fuzzy name match
        candidates = self.find_candidates_by_name(
            player_name=player_name,
            date_of_birth=date_of_birth,
            nationality=nationality,
        )

        if candidates:
            best = candidates[0]
            if best.confidence >= self.HIGH_CONFIDENCE:
                # High confidence - auto-link
                self.update_mapping(
                    player_id=best.player_id,
                    confidence_score=best.confidence,
                    match_method='fuzzy_auto',
                    **{f'{source_name}_id': external_id}
                )
                return (best.player_id, False)

            elif best.confidence >= self.MEDIUM_CONFIDENCE:
                # Medium confidence - link but flag for review
                logger.info(
                    f"Medium confidence match: {player_name} -> {best.player_name} "
                    f"({best.confidence:.0%})"
                )
                self.update_mapping(
                    player_id=best.player_id,
                    confidence_score=best.confidence,
                    match_method='fuzzy_review',
                    **{f'{source_name}_id': external_id}
                )
                return (best.player_id, False)

        # 3. Create new player if auto_create
        if auto_create:
            player_id = self._create_player(
                player_name=player_name,
                date_of_birth=date_of_birth,
                nationality=nationality,
                source_name=source_name,
                external_id=external_id,
            )
            is_new = True
            return (player_id, is_new)

        return (None, False)

    def _create_player(
        self,
        player_name: str,
        date_of_birth: date = None,
        nationality: str = None,
        source_name: str = None,
        external_id: Any = None,
    ) -> Optional[int]:
        """Create a new player and mapping record."""
        try:
            # Build insert based on source
            extra_col = ''
            extra_val = ''
            extra_param = {}

            if source_name == 'fotmob':
                extra_col = ', fotmob_id'
                extra_val = ', :ext_id'
                extra_param['ext_id'] = external_id

            result = self.db.execute_query(
                f"""
                INSERT INTO players (player_name, date_of_birth, nationality{extra_col})
                VALUES (:name, :dob, :nat{extra_val})
                RETURNING player_id
                """,
                params={
                    'name': player_name,
                    'dob': date_of_birth,
                    'nat': nationality,
                    **extra_param,
                },
                fetch=True
            )

            if result:
                player_id = result[0][0]

                # Create mapping record
                self.update_mapping(
                    player_id=player_id,
                    confidence_score=1.0,
                    match_method='created',
                    **({f'{source_name}_id': external_id} if source_name and external_id else {})
                )

                return player_id

        except Exception as e:
            logger.error(f"Error creating player {player_name}: {e}")

        return None

    # =========================================
    # MAPPING MANAGEMENT
    # =========================================

    def update_mapping(
        self,
        player_id: int,
        fotmob_id: int = None,
        api_football_id: int = None,
        transfermarkt_id: str = None,
        statsbomb_id: int = None,
        understat_id: int = None,
        whoscored_id: int = None,
        confidence_score: float = None,
        match_method: str = None,
        verified: bool = False,
    ):
        """
        Update or create a player ID mapping.

        Args:
            player_id: Database player ID
            *_id: External source IDs to update
            confidence_score: Match confidence (0-1)
            match_method: How the match was made
            verified: Whether manually verified
        """
        try:
            # Build dynamic update
            set_clauses = []
            params = {'player_id': player_id}

            if fotmob_id is not None:
                set_clauses.append("fotmob_id = :fotmob_id")
                params['fotmob_id'] = fotmob_id
            if api_football_id is not None:
                set_clauses.append("api_football_id = :api_football_id")
                params['api_football_id'] = api_football_id
            if transfermarkt_id is not None:
                set_clauses.append("transfermarkt_id = :transfermarkt_id")
                params['transfermarkt_id'] = transfermarkt_id
            if statsbomb_id is not None:
                set_clauses.append("statsbomb_id = :statsbomb_id")
                params['statsbomb_id'] = statsbomb_id
            if understat_id is not None:
                set_clauses.append("understat_id = :understat_id")
                params['understat_id'] = understat_id
            if whoscored_id is not None:
                set_clauses.append("whoscored_id = :whoscored_id")
                params['whoscored_id'] = whoscored_id
            if confidence_score is not None:
                set_clauses.append("confidence_score = :confidence")
                params['confidence'] = confidence_score
            if match_method is not None:
                set_clauses.append("match_method = :method")
                params['method'] = match_method
            if verified:
                set_clauses.append("verified_by = 'manual'")
                set_clauses.append("verified_at = CURRENT_TIMESTAMP")

            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

            if not set_clauses:
                return

            # Upsert mapping
            self.db.execute_query(
                f"""
                INSERT INTO player_id_mappings (player_id, {', '.join(k for k in params.keys() if k != 'player_id')})
                VALUES (:player_id, {', '.join(':' + k for k in params.keys() if k != 'player_id')})
                ON CONFLICT (player_id) DO UPDATE SET
                    {', '.join(set_clauses)}
                """,
                params=params,
                fetch=False
            )

        except Exception as e:
            logger.error(f"Error updating mapping for player {player_id}: {e}")

    def get_mapping(self, player_id: int) -> Optional[PlayerIdentity]:
        """Get the identity mapping for a player."""
        result = self.db.execute_query(
            """
            SELECT
                m.player_id, m.fotmob_id, m.api_football_id, m.transfermarkt_id,
                m.statsbomb_id, m.understat_id, m.whoscored_id,
                m.confidence_score, m.match_method,
                m.verified_by IS NOT NULL as verified,
                p.player_name, p.date_of_birth, p.nationality
            FROM player_id_mappings m
            JOIN players p ON m.player_id = p.player_id
            WHERE m.player_id = :pid
            """,
            params={'pid': player_id},
            fetch=True
        )

        if result:
            row = result[0]
            return PlayerIdentity(
                player_id=row[0],
                fotmob_id=row[1],
                api_football_id=row[2],
                transfermarkt_id=row[3],
                statsbomb_id=row[4],
                understat_id=row[5],
                whoscored_id=row[6],
                confidence_score=float(row[7]) if row[7] else 0.0,
                match_method=row[8],
                verified=row[9],
                name=row[10],
                date_of_birth=row[11],
                nationality=row[12],
            )

        return None

    def find_unverified_mappings(
        self,
        min_confidence: float = 0.6,
        max_confidence: float = 0.95,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Find mappings that need manual verification.

        Returns mappings with confidence between min and max that
        haven't been verified.
        """
        result = self.db.execute_query(
            """
            SELECT
                m.player_id, p.player_name,
                m.fotmob_id, m.api_football_id,
                m.confidence_score, m.match_method
            FROM player_id_mappings m
            JOIN players p ON m.player_id = p.player_id
            WHERE m.verified_by IS NULL
              AND m.confidence_score >= :min_conf
              AND m.confidence_score < :max_conf
            ORDER BY m.confidence_score DESC
            LIMIT :limit
            """,
            params={
                'min_conf': min_confidence,
                'max_conf': max_confidence,
                'limit': limit,
            },
            fetch=True
        )

        return [
            {
                'player_id': row[0],
                'player_name': row[1],
                'fotmob_id': row[2],
                'api_football_id': row[3],
                'confidence_score': float(row[4]) if row[4] else 0.0,
                'match_method': row[5],
            }
            for row in result
        ]


# ============================================
# TEAM IDENTITY RESOLVER
# ============================================

class TeamIdentityResolver:
    """
    Resolves team identities across multiple data sources.

    Similar to PlayerIdentityResolver but for teams.
    """

    def __init__(self, db):
        self.db = db

    def find_team_by_external_id(
        self,
        fotmob_id: int = None,
        api_football_id: int = None,
        league_id: int = None,
    ) -> Optional[int]:
        """Find team ID by any external source ID."""
        if fotmob_id:
            result = self.db.execute_query(
                """
                SELECT team_id FROM teams
                WHERE fotmob_id = :fid
                  AND (:lid IS NULL OR league_id = :lid)
                """,
                params={'fid': fotmob_id, 'lid': league_id},
                fetch=True
            )
            if result:
                return result[0][0]

            # Try mapping table
            result = self.db.execute_query(
                "SELECT team_id FROM team_id_mappings WHERE fotmob_id = :fid",
                params={'fid': fotmob_id},
                fetch=True
            )
            if result:
                return result[0][0]

        if api_football_id:
            result = self.db.execute_query(
                "SELECT team_id FROM team_id_mappings WHERE api_football_id = :aid",
                params={'aid': api_football_id},
                fetch=True
            )
            if result:
                return result[0][0]

        return None

    def find_team_by_name(
        self,
        team_name: str,
        league_id: int = None,
    ) -> Optional[int]:
        """Find team by name (exact or fuzzy)."""
        params = {'name': team_name}
        where_clause = "team_name = :name"

        if league_id:
            where_clause += " AND league_id = :lid"
            params['lid'] = league_id

        result = self.db.execute_query(
            f"SELECT team_id FROM teams WHERE {where_clause}",
            params=params,
            fetch=True
        )

        if result:
            return result[0][0]

        # Try fuzzy match with LIKE
        params['name_pattern'] = f'%{team_name}%'
        result = self.db.execute_query(
            f"""
            SELECT team_id, team_name FROM teams
            WHERE team_name ILIKE :name_pattern
              {'AND league_id = :lid' if league_id else ''}
            LIMIT 5
            """,
            params=params,
            fetch=True
        )

        if result:
            # Find best match
            best_id = None
            best_sim = 0
            for row in result:
                sim = SequenceMatcher(None, team_name.lower(), row[1].lower()).ratio()
                if sim > best_sim and sim >= 0.8:
                    best_sim = sim
                    best_id = row[0]
            return best_id

        return None

    def update_mapping(
        self,
        team_id: int,
        fotmob_id: int = None,
        api_football_id: int = None,
        confidence_score: float = 1.0,
    ):
        """Update or create a team ID mapping."""
        try:
            params = {'team_id': team_id, 'confidence': confidence_score}
            set_clauses = ["updated_at = CURRENT_TIMESTAMP"]

            if fotmob_id is not None:
                set_clauses.append("fotmob_id = :fotmob_id")
                params['fotmob_id'] = fotmob_id
            if api_football_id is not None:
                set_clauses.append("api_football_id = :api_football_id")
                params['api_football_id'] = api_football_id

            self.db.execute_query(
                f"""
                INSERT INTO team_id_mappings (team_id, fotmob_id, api_football_id, confidence_score)
                VALUES (:team_id, :fotmob_id, :api_football_id, :confidence)
                ON CONFLICT (team_id) DO UPDATE SET
                    {', '.join(set_clauses)}
                """,
                params={
                    **params,
                    'fotmob_id': fotmob_id,
                    'api_football_id': api_football_id,
                },
                fetch=False
            )

        except Exception as e:
            logger.error(f"Error updating team mapping for {team_id}: {e}")
