"""
BaseETL — abstract base class for all ETL pipelines.

DatabaseConnection API (confirmed from database/connection.py):
  Constructor: DatabaseConnection()  — zero-arg singleton
  db.execute_query(sql, params={}, fetch=True) → list[Row] | result
  db.engine — SQLAlchemy engine (used by BatchLoader)

All ETL subclasses must accept a DatabaseConnection instance.
"""
import json
import logging
import os
import unicodedata
from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BRONZE_BASE = Path("data/raw")


class BaseETL(ABC):
    """
    Abstract base for SofaScoreETL, UnderstatETL, ClubEloETL.

    Provides:
      - Bronze layer file saving (deterministic paths)
      - ETL run logging (etl_run_log table)
      - Common DB lookups (league_id, season_id, team_id by sofascore_id)
      - get_current_season: DB primary, date fallback, warn if neither
      - log_unmatched: writes to unmatched_players_log
      - normalise_name: NFKD unicode → ascii → lower
    """

    SOURCE_NAME: str = "base"  # override in subclass

    def __init__(self, db=None):
        """
        Args:
            db: DatabaseConnection instance. If None, creates a new singleton.
        """
        if db is None:
            from database.connection import DatabaseConnection
            db = DatabaseConnection()
        self.db = db
        self._league_id_cache: Dict[str, int] = {}
        self._season_id_cache: Dict[str, int] = {}
        self._team_id_cache: Dict[int, int] = {}  # sofascore_id → team_id
        self._run_id: Optional[int] = None

    # ── Bronze ────────────────────────────────────────────────────────────────

    def save_bronze(
        self,
        data: Any,
        source: str,
        league: str,
        season: str,
        filename: str,
    ) -> Path:
        """
        Persist raw data to Bronze layer.

        Path: data/raw/{source}/{league}/{season}/{filename}

        DataFrames are serialised via reset_index().to_dict(orient='records').
        All other objects are JSON-serialised directly.

        Returns the path written.
        """
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                data = data.reset_index().to_dict(orient="records")
        except ImportError:
            pass

        path = BRONZE_BASE / source / league / season / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, default=str, indent=2)
        logger.debug(f"Bronze saved: {path}")
        return path

    # ── ETL run logging ───────────────────────────────────────────────────────

    def start_run(self, league_name: str, season_name: str) -> int:
        """Insert etl_run_log row, return run_id."""
        rows = self.db.execute_query(
            """
            INSERT INTO etl_run_log (source, league_name, season_name, status)
            VALUES (:src, :league, :season, 'running')
            RETURNING run_id
            """,
            {"src": self.SOURCE_NAME, "league": league_name, "season": season_name},
            fetch=True,
        )
        self._run_id = rows[0][0]
        return self._run_id

    def finish_run(
        self,
        run_id: int,
        *,
        status: str = "success",
        processed: int = 0,
        enriched: int = 0,
        skipped: int = 0,
        unmatched: int = 0,
        errors: int = 0,
        notes: str = "",
    ) -> None:
        """Update etl_run_log row on completion."""
        self.db.execute_query(
            """
            UPDATE etl_run_log
               SET run_completed      = NOW(),
                   status             = :status,
                   players_processed  = :processed,
                   players_enriched   = :enriched,
                   players_skipped    = :skipped,
                   players_unmatched  = :unmatched,
                   errors_count       = :errors,
                   notes              = :notes
             WHERE run_id = :run_id
            """,
            {
                "run_id": run_id,
                "status": status,
                "processed": processed,
                "enriched": enriched,
                "skipped": skipped,
                "unmatched": unmatched,
                "errors": errors,
                "notes": notes[:500] if notes else "",
            },
            fetch=False,
        )

    def log_unmatched(
        self,
        source: str,
        player_name: str,
        team_name: str = "",
        league_name: str = "",
        season_name: str = "",
        reason: str = "no_match",
        best_candidate: str = "",
        best_score: float = 0.0,
    ) -> None:
        """Write unmatched player entry to unmatched_players_log."""
        norm = self.normalise_name(player_name)
        self.db.execute_query(
            """
            INSERT INTO unmatched_players_log
                (source, player_name, player_name_norm, team_name,
                 league_name, season_name, reason, best_candidate, best_score)
            VALUES
                (:src, :name, :norm, :team, :league, :season,
                 :reason, :best, :score)
            """,
            {
                "src": source,
                "name": player_name,
                "norm": norm,
                "team": team_name,
                "league": league_name,
                "season": season_name,
                "reason": reason,
                "best": best_candidate,
                "score": best_score,
            },
            fetch=False,
        )

    # ── DB lookups ────────────────────────────────────────────────────────────

    def get_league_id(self, league_name: str) -> Optional[int]:
        if league_name not in self._league_id_cache:
            rows = self.db.execute_query(
                "SELECT league_id FROM leagues WHERE league_name = :name",
                {"name": league_name},
                fetch=True,
            )
            self._league_id_cache[league_name] = rows[0][0] if rows else None
        return self._league_id_cache[league_name]

    def get_season_id(self, season_name: str) -> Optional[int]:
        if season_name not in self._season_id_cache:
            rows = self.db.execute_query(
                "SELECT season_id FROM seasons WHERE season_name = :name",
                {"name": season_name},
                fetch=True,
            )
            self._season_id_cache[season_name] = rows[0][0] if rows else None
        return self._season_id_cache[season_name]

    def get_team_id_by_sofascore(self, sofascore_id: int) -> Optional[int]:
        """Look up team_id by SofaScore team ID."""
        if sofascore_id not in self._team_id_cache:
            rows = self.db.execute_query(
                "SELECT team_id FROM teams WHERE sofascore_id = :sid",
                {"sid": sofascore_id},
                fetch=True,
            )
            self._team_id_cache[sofascore_id] = rows[0][0] if rows else None
        return self._team_id_cache[sofascore_id]

    def get_current_season(self) -> str:
        """
        Return current season name (e.g. '2025-26').

        Priority:
          1. DB: SELECT season_name WHERE is_current=TRUE
          2. Date fallback via season_utils (August cutoff)
          3. Warn if neither available
        """
        rows = self.db.execute_query(
            "SELECT season_name FROM seasons WHERE is_current = TRUE LIMIT 1",
            fetch=True,
        )
        if rows:
            return rows[0][0]

        logger.warning(
            "No current season in DB — using date-based fallback. "
            "Run: UPDATE seasons SET is_current=TRUE WHERE season_name='<name>'"
        )
        from utils.season_utils import SeasonUtils
        return SeasonUtils.get_current_season()

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def normalise_name(name: str) -> str:
        """
        Normalise player name for matching.
        Removes accents, lowercases, strips whitespace.
        e.g. 'Ángel Di María' → 'angel di maria'
        """
        if not name:
            return ""
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
        return ascii_name.lower().strip()

    # ── Abstract ──────────────────────────────────────────────────────────────

    @abstractmethod
    def run(self, league: str, season: str) -> Dict[str, Any]:
        """
        Run the ETL for one league/season.

        Args:
            league: League name as stored in DB ('Premier League', etc.)
            season: Season name as stored in DB ('2025-26', etc.)

        Returns:
            Stats dict with at minimum:
            {processed, enriched, skipped, errors}
        """
