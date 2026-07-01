from pydantic import BaseModel
from typing import Optional
from datetime import date

class PlatformOverview(BaseModel):
    players: int
    teams: int
    player_match_rows: int
    team_match_rows: int
    files_loaded: int
    earliest_match: Optional[date] = None
    latest_match: Optional[date] = None
