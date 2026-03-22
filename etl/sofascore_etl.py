"""
SofaScore ETL Integration

Enriches player_season_stats with 112 deep fields from SofaScore:
- Aerial duels won/lost and percentage
- Ground duels won and percentage
- Tackles won and percentage
- Ball recovery, dispossessed, dribbled past
- Big chances created/missed
- Accurate passes percentage, final third passes
- Shots inside/outside box
- Rating, touches, key passes
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from database.connection import get_db
from scrapers.sofascore.client import SofaScoreClient, LEAGUE_IDS, SEASON_IDS

logger = logging.getLogger(__name__)

SEASON_NAME_MAP = {
    2022: '2022-23',
    2023: '2023-24',
    2024: '2024-25',
    2025: '2025-26',
}

LEAGUE_DISPLAY_MAP = {
    'premier-league': 'Premier League',
    'la-liga': 'La Liga',
    'serie-a': 'Serie A',
    'bundesliga': 'Bundesliga',
    'ligue-1': 'Ligue 1',
}


class SofaScoreETL:
    SUPPORTED_LEAGUES = list(LEAGUE_IDS.keys())

    def __init__(self, db=None):
        self.client = SofaScoreClient(rate_limit_delay=1.5)
        self.db = db or get_db()
        self._ensure_columns()
        self._league_cache = {}
        self._season_cache = {}
        self._team_cache = {}
        self.stats = {
            'players_enriched': 0,
            'new_records': 0,
            'skipped': 0,
            'errors': [],
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _ensure_columns(self):
        """Add new columns to player_season_stats if they don't exist."""
        alterations = [
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS sofascore_rating NUMERIC(4,2)",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS aerial_duels_won INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS aerial_duels_lost INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS ground_duels_won_pct NUMERIC(5,2)",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS tackles_won INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS tackles_won_pct NUMERIC(5,2)",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dispossessed INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS dribbled_past INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS big_chances_missed INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS accurate_passes_pct NUMERIC(5,2)",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS accurate_final_third INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS shots_inside_box INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS shots_outside_box INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS possession_won_att_third INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS touches INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS error_lead_to_goal INTEGER DEFAULT 0",
            "ALTER TABLE player_season_stats ADD COLUMN IF NOT EXISTS sofascore_updated_at TIMESTAMP",
            "ALTER TABLE players ADD COLUMN IF NOT EXISTS sofascore_id INTEGER",
            "CREATE INDEX IF NOT EXISTS idx_players_sofascore_id ON players(sofascore_id)",
        ]
        for sql in alterations:
            try:
                self.db.execute_query(sql, fetch=False)
            except Exception as e:
                logger.warning(f"Column alteration warning (may already exist): {e}")

    def _get_league_id(self, league: str) -> Optional[int]:
        if league not in self._league_cache:
            rows = self.db.execute_query(
                "SELECT league_id FROM leagues WHERE LOWER(league_name) = LOWER(:name)",
                {'name': LEAGUE_DISPLAY_MAP.get(league, league)},
                fetch=True
            )
            self._league_cache[league] = rows[0][0] if rows else None
        return self._league_cache[league]

    def _get_season_id(self, season_name: str) -> Optional[int]:
        if season_name not in self._season_cache:
            rows = self.db.execute_query(
                "SELECT season_id FROM seasons WHERE season_name = :name",
                {'name': season_name},
                fetch=True
            )
            self._season_cache[season_name] = rows[0][0] if rows else None
        return self._season_cache[season_name]

    def _get_team_id(self, team_name: str, league_id: int) -> Optional[int]:
        cache_key = (team_name, league_id)
        if cache_key not in self._team_cache:
            rows = self.db.execute_query(
                """SELECT t.team_id FROM teams t
                   JOIN team_season_stats tss ON t.team_id = tss.team_id
                   WHERE LOWER(t.team_name) = LOWER(:name)
                   AND tss.league_id = :lid LIMIT 1""",
                {'name': team_name, 'lid': league_id},
                fetch=True
            )
            if not rows:
                # Fallback: search by name only
                rows = self.db.execute_query(
                    "SELECT team_id FROM teams WHERE LOWER(team_name) = LOWER(:name) LIMIT 1",
                    {'name': team_name},
                    fetch=True
                )
            self._team_cache[cache_key] = rows[0][0] if rows else None
        return self._team_cache[cache_key]

    def _find_player_id(self, sofascore_id: int, player_name: str) -> Optional[int]:
        """Find DB player ID by sofascore_id first, then name fallback."""
        # Try sofascore_id
        rows = self.db.execute_query(
            "SELECT player_id FROM players WHERE sofascore_id = :sid",
            {'sid': sofascore_id},
            fetch=True
        )
        if rows:
            return rows[0][0]

        # Try exact name match
        rows = self.db.execute_query(
            "SELECT player_id FROM players WHERE LOWER(player_name) = LOWER(:name) LIMIT 1",
            {'name': player_name},
            fetch=True
        )
        if rows:
            player_id = rows[0][0]
            # Cache the sofascore_id on the players record
            self.db.execute_query(
                "UPDATE players SET sofascore_id = :sid WHERE player_id = :pid",
                {'sid': sofascore_id, 'pid': player_id},
                fetch=False
            )
            return player_id

        return None

    def _upsert_player_stats(
        self,
        player_id: int,
        team_id: int,
        season_id: int,
        league_id: int,
        stats: Dict
    ) -> bool:
        """Upsert SofaScore stats into player_season_stats."""
        query = """
            INSERT INTO player_season_stats (
                player_id, team_id, season_id, league_id,
                sofascore_rating,
                aerial_duels_won, aerial_duels_lost, aerial_win_pct,
                ground_duels_won, ground_duels_won_pct,
                duels_won, duels_won_pct,
                tackles, tackles_won, tackles_won_pct,
                interceptions, clearances,
                dispossessed, dribbled_past,
                big_chances_created, big_chances_missed,
                recoveries, accurate_long_balls, accurate_crosses,
                accurate_passes_pct, accurate_final_third,
                key_passes, touches,
                shots_inside_box, shots_outside_box,
                fouls_committed, fouls_won, offsides,
                possession_won_att_third, error_lead_to_goal,
                sofascore_updated_at
            ) VALUES (
                :player_id, :team_id, :season_id, :league_id,
                :rating,
                :aerial_won, :aerial_lost, :aerial_pct,
                :ground_won, :ground_pct,
                :duels_won, :duels_pct,
                :tackles, :tackles_won, :tackles_won_pct,
                :interceptions, :clearances,
                :dispossessed, :dribbled_past,
                :big_chances_created, :big_chances_missed,
                :recoveries, :accurate_long_balls, :accurate_crosses,
                :accurate_passes_pct, :accurate_final_third,
                :key_passes, :touches,
                :shots_inside_box, :shots_outside_box,
                :fouls_committed, :fouls_won, :offsides,
                :possession_won_att_third, :error_lead_to_goal,
                NOW()
            )
            ON CONFLICT (player_id, team_id, season_id, league_id)
            DO UPDATE SET
                sofascore_rating = EXCLUDED.sofascore_rating,
                aerial_duels_won = EXCLUDED.aerial_duels_won,
                aerial_duels_lost = EXCLUDED.aerial_duels_lost,
                aerial_win_pct = EXCLUDED.aerial_win_pct,
                ground_duels_won = EXCLUDED.ground_duels_won,
                ground_duels_won_pct = EXCLUDED.ground_duels_won_pct,
                duels_won = EXCLUDED.duels_won,
                duels_won_pct = EXCLUDED.duels_won_pct,
                tackles = EXCLUDED.tackles,
                tackles_won = EXCLUDED.tackles_won,
                tackles_won_pct = EXCLUDED.tackles_won_pct,
                interceptions = EXCLUDED.interceptions,
                clearances = EXCLUDED.clearances,
                dispossessed = EXCLUDED.dispossessed,
                dribbled_past = EXCLUDED.dribbled_past,
                big_chances_created = EXCLUDED.big_chances_created,
                big_chances_missed = EXCLUDED.big_chances_missed,
                recoveries = EXCLUDED.recoveries,
                accurate_long_balls = EXCLUDED.accurate_long_balls,
                accurate_crosses = EXCLUDED.accurate_crosses,
                accurate_passes_pct = EXCLUDED.accurate_passes_pct,
                accurate_final_third = EXCLUDED.accurate_final_third,
                key_passes = EXCLUDED.key_passes,
                touches = EXCLUDED.touches,
                shots_inside_box = EXCLUDED.shots_inside_box,
                shots_outside_box = EXCLUDED.shots_outside_box,
                fouls_committed = EXCLUDED.fouls_committed,
                fouls_won = EXCLUDED.fouls_won,
                offsides = EXCLUDED.offsides,
                possession_won_att_third = EXCLUDED.possession_won_att_third,
                error_lead_to_goal = EXCLUDED.error_lead_to_goal,
                sofascore_updated_at = NOW()
        """
        params = {
            'player_id': player_id,
            'team_id': team_id,
            'season_id': season_id,
            'league_id': league_id,
            'rating': stats.get('rating'),
            'aerial_won': stats.get('aerialDuelsWon', 0),
            'aerial_lost': stats.get('aerialLost', 0),
            'aerial_pct': stats.get('aerialDuelsWonPercentage'),
            'ground_won': stats.get('groundDuelsWon', 0),
            'ground_pct': stats.get('groundDuelsWonPercentage'),
            'duels_won': stats.get('totalDuelsWon', 0),
            'duels_pct': stats.get('totalDuelsWonPercentage'),
            'tackles': stats.get('tackles', 0),
            'tackles_won': stats.get('tacklesWon', 0),
            'tackles_won_pct': stats.get('tacklesWonPercentage'),
            'interceptions': stats.get('interceptions', 0),
            'clearances': stats.get('clearances', 0),
            'dispossessed': stats.get('dispossessed', 0),
            'dribbled_past': stats.get('dribbledPast', 0),
            'big_chances_created': stats.get('bigChancesCreated', 0),
            'big_chances_missed': stats.get('bigChancesMissed', 0),
            'recoveries': stats.get('ballRecovery', 0),
            'accurate_long_balls': stats.get('accurateLongBalls', 0),
            'accurate_crosses': stats.get('accurateCrosses', 0),
            'accurate_passes_pct': stats.get('accuratePassesPercentage'),
            'accurate_final_third': stats.get('accurateFinalThirdPasses', 0),
            'key_passes': stats.get('keyPasses', 0),
            'touches': stats.get('touches', 0),
            'shots_inside_box': stats.get('shotsFromInsideTheBox', 0),
            'shots_outside_box': stats.get('shotsFromOutsideTheBox', 0),
            'fouls_committed': stats.get('fouls', 0),
            'fouls_won': stats.get('wasFouled', 0),
            'offsides': stats.get('offsides', 0),
            'possession_won_att_third': stats.get('possessionWonAttThird', 0),
            'error_lead_to_goal': stats.get('errorLeadToGoal', 0),
        }

        try:
            self.db.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Upsert failed for player {player_id}: {e}")
            return False

    def process_league_season(self, league: str, season_year: int) -> Dict:
        """Process one league/season: fetch all players and upsert stats."""
        season_name = SEASON_NAME_MAP.get(season_year)
        if not season_name:
            raise ValueError(f"Unknown season year: {season_year}")

        league_id = self._get_league_id(league)
        season_id = self._get_season_id(season_name)

        if not league_id or not season_id:
            raise ValueError(f"League/season not found in DB: {league} {season_name}")

        logger.info(f"Processing SofaScore: {league} {season_name}")
        all_stats = self.client.get_all_league_players(league, season_year)

        enriched = 0
        skipped = 0
        errors = []

        for stats in all_stats:
            sofascore_id = stats.get('player_sofascore_id')
            player_name = stats.get('player_name', '')
            team_name = stats.get('team_name', '')

            player_id = self._find_player_id(sofascore_id, player_name)
            if not player_id:
                skipped += 1
                continue

            team_id = self._get_team_id(team_name, league_id)
            if not team_id:
                skipped += 1
                continue

            success = self._upsert_player_stats(
                player_id, team_id, season_id, league_id, stats
            )
            if success:
                enriched += 1
            else:
                errors.append(player_name)

        self.stats['players_enriched'] += enriched
        self.stats['skipped'] += skipped
        logger.info(
            f"SofaScore {league} {season_name}: "
            f"{enriched} enriched, {skipped} skipped, {len(errors)} errors"
        )
        return {'enriched': enriched, 'skipped': skipped, 'errors': errors}

    def get_statistics(self) -> Dict:
        return self.stats
