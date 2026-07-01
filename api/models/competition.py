from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Competition(BaseModel):
    competition_id: int
    wyscout_id: Optional[int] = None
    name: str
    country: Optional[str] = None
    competition_type: Optional[str] = None

class CompetitionSummary(BaseModel):
    competition_name: str
    matches: int
    players: int
    first_match: Optional[datetime] = None
    last_match: Optional[datetime] = None

class Season(BaseModel):
    season_id: int
    wyscout_id: Optional[int] = None
    competition_id: int
    season_name: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    is_current: bool = False
