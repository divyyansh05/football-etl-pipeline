"""
ClubEloETL — Team ELO rating snapshots.

Reads historical and current ELO ratings from clubelo.com via the
soccerdata library (ClubElo class).

This ETL is INDEPENDENT of SofaScore/Understat.  It writes only to the
team_elo table and does NOT create or modify player or team identity
records.  Team matching is done by clubelo_name stored on the teams table,
or by fuzzy name match.

Bronze layer:
  data/raw/clubelo/{date}/elo.json

Design:
  - run(date_str)  — snapshot for a specific date, e.g. '2025-01-01'
  - run_date_range(start, end, step_days)  — multiple snapshots
  - Upsert: ON CONFLICT (team_id, elo_date) DO UPDATE
"""
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from etl.base_etl import BaseETL

logger = logging.getLogger(__name__)

# Leagues we care about — ClubElo league strings
TARGET_LEAGUES = {
    "ENG1",  # Premier League
    "ESP1",  # La Liga
    "ITA1",  # Serie A
    "GER1",  # Bundesliga
    "FRA1",  # Ligue 1
}

# ClubElo league string → canonical DB league name
CLUBELO_TO_LEAGUE = {
    "ENG1": "Premier League",
    "ESP1": "La Liga",
    "ITA1": "Serie A",
    "GER1": "Bundesliga",
    "FRA1": "Ligue 1",
}


class ClubEloETL(BaseETL):
    """
    Inserts ELO snapshots for top-5 EU league clubs into team_elo.

    Usage:
        etl = ClubEloETL()
        etl.run_date("2025-01-01")          # single snapshot
        etl.run("Premier League", "2024-25") # whole season weekly snapshots
    """

    SOURCE_NAME = "clubelo"

    def __init__(self, db=None):
        super().__init__(db)
        # Cache: clubelo_name (lowercase) → team_id
        self._name_cache: Dict[str, Optional[int]] = {}

    # ── Abstract implementation (BaseETL.run) ─────────────────────────────────

    def run(self, league: str, season: str) -> Dict[str, Any]:
        """
        Collect weekly ELO snapshots for the full season date range.

        For '2024-25' this covers roughly Aug 2024 → Jun 2025 in weekly steps.
        Returns aggregated counts.
        """
        start_year, _ = _parse_season(season)
        # Approximate season bounds: 1 Aug start_year → 31 May end_year
        season_start = date(start_year, 8, 1)
        season_end   = date(start_year + 1, 5, 31)

        totals: Dict[str, Any] = {
            "processed": 0, "enriched": 0,
            "skipped":   0, "errors":   0, "unmatched": 0,
        }

        current = season_start
        while current <= season_end:
            result = self.run_date(current.isoformat())
            for k in totals:
                totals[k] += result.get(k, 0)
            current += timedelta(days=7)

        return totals

    def run_date(self, date_str: str) -> Dict[str, Any]:
        """
        Collect ELO snapshot for a single date and upsert into team_elo.

        Args:
            date_str: ISO date string, e.g. '2025-01-01'

        Returns:
            Stats dict.
        """
        run_id = self.start_run("all-leagues", date_str)
        logger.info(f"[run={run_id}] ClubEloETL date={date_str}")

        counts: Dict[str, Any] = {
            "processed": 0, "enriched": 0,
            "skipped":   0, "errors":   0, "unmatched": 0,
        }

        try:
            rows = self._fetch(date_str)
        except Exception as exc:
            logger.error(f"ClubElo fetch failed for {date_str}: {exc}", exc_info=True)
            counts["errors"] += 1
            self.finish_run(run_id, status="error", **counts)
            return counts

        if not rows:
            logger.warning(f"No ClubElo data for {date_str}")
            self.finish_run(run_id, status="success", **counts)
            return counts

        # Filter to target leagues only
        target_rows = [
            r for r in rows
            if r.get("league") in TARGET_LEAGUES
        ]

        self.save_bronze(target_rows, self.SOURCE_NAME, "snapshots", date_str[:7], f"{date_str}.json")

        for row in target_rows:
            try:
                result = self._process_row(row, date_str)
                counts["processed"] += 1
                if result == "enriched":
                    counts["enriched"] += 1
                elif result == "unmatched":
                    counts["unmatched"] += 1
                else:
                    counts["skipped"] += 1
            except Exception as exc:
                logger.error(
                    f"ClubElo row failed (team={row.get('team')!r}): {exc}",
                    exc_info=True,
                )
                counts["errors"] += 1

        status = "success" if counts["errors"] == 0 else "partial"
        self.finish_run(run_id, status=status, **counts)
        logger.info(
            f"[run={run_id}] Done {date_str}: "
            f"{counts['enriched']} upserted, "
            f"{counts['unmatched']} unmatched"
        )
        return counts

    # ── Data fetch ────────────────────────────────────────────────────────────

    def _fetch(self, date_str: str) -> List[Dict]:
        """
        Use soccerdata ClubElo to read ratings as of date_str.

        Returns list of dicts with keys: team, league, elo, rank.
        """
        import soccerdata as sd

        ce = sd.ClubElo()
        df = ce.read_by_date(date_str)
        if df is None or df.empty:
            return []

        rows = []
        for idx, row in df.iterrows():
            club = str(idx) if not isinstance(idx, str) else idx
            rows.append({
                "team":   club,
                "league": row.get("league") or row.get("League") or "",
                "elo":    float(row.get("elo") or row.get("Elo") or 0),
                "rank":   int(row.get("rank") or row.get("Rank") or 0) if (row.get("rank") or row.get("Rank")) else None,
            })
        return rows

    # ── Per-row processing ────────────────────────────────────────────────────

    def _process_row(self, row: Dict, date_str: str) -> str:
        clubelo_name = row.get("team", "").strip()
        elo_rating   = row.get("elo")
        elo_rank     = row.get("rank")
        league_str   = row.get("league", "")

        if not clubelo_name or not elo_rating:
            return "skipped"

        elo_date = date.fromisoformat(date_str)
        db_team_id = self._resolve_team(clubelo_name, league_str)

        self._upsert_elo(
            team_id=db_team_id,           # may be None — recorded as unmatched
            team_name_clubelo=clubelo_name,
            elo_date=elo_date,
            elo_rating=elo_rating,
            elo_rank=elo_rank,
            league=league_str,
        )

        if db_team_id is None:
            logger.debug(f"No DB team match for ClubElo team: {clubelo_name!r}")
            return "unmatched"

        return "enriched"

    # ── Team matching ─────────────────────────────────────────────────────────

    def _resolve_team(self, clubelo_name: str, league_str: str) -> Optional[int]:
        """
        Match ClubElo team name → DB team_id.

        Priority:
          1. teams.clubelo_name exact match
          2. teams.team_name exact match (after normalisation)
          3. pg_trgm similarity > 0.85 within the league
        """
        key = clubelo_name.lower()
        if key in self._name_cache:
            return self._name_cache[key]

        league_name = CLUBELO_TO_LEAGUE.get(league_str)
        league_id   = self.get_league_id(league_name) if league_name else None

        # 1. clubelo_name column exact
        rows = self.db.execute_query(
            """
            SELECT team_id FROM teams
             WHERE lower(clubelo_name) = lower(:name)
             LIMIT 1
            """,
            {"name": clubelo_name},
            fetch=True,
        )
        if rows:
            self._name_cache[key] = rows[0][0]
            return rows[0][0]

        # 2. team_name exact (normalised)
        params: Dict = {"name": clubelo_name}
        extra = ""
        if league_id:
            params["lid"] = league_id
            extra = " AND league_id = :lid"

        rows = self.db.execute_query(
            f"""
            SELECT team_id FROM teams
             WHERE immutable_unaccent(lower(team_name))
                   = immutable_unaccent(lower(:name))
             {extra}
             LIMIT 1
            """,
            params,
            fetch=True,
        )
        if rows:
            # Cache the mapping and stamp clubelo_name for future runs
            tid = rows[0][0]
            self._name_cache[key] = tid
            self._stamp_clubelo_name(tid, clubelo_name)
            return tid

        # 3. Fuzzy pg_trgm
        rows = self.db.execute_query(
            f"""
            SELECT team_id,
                   similarity(lower(team_name), lower(:name)) AS sim
              FROM teams
             WHERE similarity(lower(team_name), lower(:name)) > 0.85
             {extra}
             ORDER BY sim DESC
             LIMIT 1
            """,
            params,
            fetch=True,
        )
        if rows:
            tid = rows[0][0]
            self._name_cache[key] = tid
            self._stamp_clubelo_name(tid, clubelo_name)
            logger.debug(
                f"ClubElo fuzzy match: {clubelo_name!r} → team_id={tid} "
                f"(sim={rows[0][1]:.3f})"
            )
            return tid

        self._name_cache[key] = None
        return None

    def _stamp_clubelo_name(self, team_id: int, clubelo_name: str) -> None:
        """Persist clubelo_name on the teams row so future lookups are instant."""
        self.db.execute_query(
            """
            UPDATE teams
               SET clubelo_name = :name,
                   updated_at   = NOW()
             WHERE team_id = :tid
               AND clubelo_name IS NULL
            """,
            {"name": clubelo_name, "tid": team_id},
            fetch=False,
        )

    # ── DB upsert ─────────────────────────────────────────────────────────────

    def _upsert_elo(
        self,
        team_id: Optional[int],
        team_name_clubelo: str,
        elo_date: date,
        elo_rating: float,
        elo_rank: Optional[int],
        league: str,
    ) -> None:
        """
        Insert or update a team_elo row.

        team_id may be None for unmatched teams — row is still persisted so
        it can be manually linked later.
        """
        self.db.execute_query(
            """
            INSERT INTO team_elo
                (team_id, team_name_clubelo, elo_date, elo_rating, elo_rank, league)
            VALUES
                (:tid, :name, :dt, :elo, :rank, :league)
            ON CONFLICT (team_name_clubelo, elo_date)
            DO UPDATE SET
                team_id    = COALESCE(EXCLUDED.team_id, team_elo.team_id),
                elo_rating = EXCLUDED.elo_rating,
                elo_rank   = EXCLUDED.elo_rank,
                league     = EXCLUDED.league
            """,
            {
                "tid":    team_id,
                "name":   team_name_clubelo,
                "dt":     elo_date,
                "elo":    elo_rating,
                "rank":   elo_rank,
                "league": league,
            },
            fetch=False,
        )


# ── Utilities ──────────────────────────────────────────────────────────────────

def _parse_season(season_name: str):
    """'2024-25' → (2024, 2025)"""
    parts = season_name.split("-")
    start_year = int(parts[0])
    end_suffix = int(parts[1])
    end_year = int(str(start_year)[:2] + str(end_suffix).zfill(2))
    return start_year, end_year
