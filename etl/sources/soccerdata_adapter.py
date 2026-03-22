# SoccerData source adapters

import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

LEAGUE_MAP = {
    "premier-league": "ENG-Premier League",
    "la-liga": "ESP-La Liga",
    "serie-a": "ITA-Serie A",
    "bundesliga": "GER-Bundesliga",
    "ligue-1": "FRA-Ligue 1",
}


class SoccerDataAdapter:

    def __init__(self, data_dir=None, no_cache=False):
        # Lazy import — avoids hard dependency if soccerdata not installed
        import soccerdata as sd
        self._sd = sd
        self.data_dir = data_dir or Path("data/soccerdata")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.no_cache = no_cache

    def _make_fotmob(self, league: str):
        """Instantiate soccerdata.FotMob for the given league."""
        return self._sd.FotMob(
            leagues=LEAGUE_MAP[league],
            data_dir=self.data_dir,
            no_cache=self.no_cache,
        )

    def get_match_events(self, league: str, season: int) -> pd.DataFrame:
        if league not in LEAGUE_MAP:
            logger.warning(
                f"League '{league}' not in LEAGUE_MAP, returning empty DataFrame. "
                f"Known leagues: {list(LEAGUE_MAP)}"
            )
            return pd.DataFrame()
        try:
            fm = self._make_fotmob(league)
            return fm.read_events(season=season)
        except Exception as e:
            logger.error(f"get_match_events failed for {league} {season}: {e}")
            return pd.DataFrame()

    def get_shot_events(self, league: str, season: int) -> pd.DataFrame:
        if league not in LEAGUE_MAP:
            logger.warning(
                f"League '{league}' not in LEAGUE_MAP, returning empty DataFrame. "
                f"Known leagues: {list(LEAGUE_MAP)}"
            )
            return pd.DataFrame()
        try:
            fm = self._make_fotmob(league)
            return fm.read_shotmap(season=season)
        except Exception as e:
            logger.error(f"get_shot_events failed for {league} {season}: {e}")
            return pd.DataFrame()

    def get_lineups(self, league: str, season: int) -> pd.DataFrame:
        if league not in LEAGUE_MAP:
            logger.warning(
                f"League '{league}' not in LEAGUE_MAP, returning empty DataFrame. "
                f"Known leagues: {list(LEAGUE_MAP)}"
            )
            return pd.DataFrame()
        try:
            fm = self._make_fotmob(league)
            return fm.read_lineup(season=season)
        except Exception as e:
            logger.error(f"get_lineups failed for {league} {season}: {e}")
            return pd.DataFrame()

    def test_connection(self, league: str = "premier-league", season: int = 2024) -> dict:
        try:
            fm = self._make_fotmob(league)
            # Fetch the schedule first, then pull just one game's events
            schedule = fm.read_schedule(season=season)
            if schedule.empty:
                return {"status": "error", "message": "No matches found in schedule"}
            game_id = schedule.index.get_level_values("game_id")[0]
            df = fm.read_events(game_id=[game_id], season=season)
            return {
                "status": "ok",
                "rows": len(df),
                "columns": list(df.columns),
            }
        except Exception as e:
            logger.error(f"test_connection failed: {e}")
            return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    adapter = SoccerDataAdapter(no_cache=False)
    result = adapter.test_connection()
    print(result)
