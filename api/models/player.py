from pydantic import BaseModel
from typing import Optional
from datetime import date


class PlayerSummary(BaseModel):
    player_id: int
    wyscout_id: int
    name: str
    position_group: Optional[str] = None
    primary_position: Optional[str] = None
    nationality: Optional[str] = None
    matches: Optional[int] = None
    total_goals: Optional[int] = None
    total_assists: Optional[int] = None
    avg_xg: Optional[float] = None


class PlayerDetail(BaseModel):
    player_id: int
    wyscout_id: int
    name: str
    normalised_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    nationality: Optional[str] = None
    position_group: Optional[str] = None
    primary_position: Optional[str] = None
    height_cm: Optional[int] = None
    preferred_foot: Optional[str] = None


class PlayerMatchStat(BaseModel):
    match_label: Optional[str] = None
    competition_name: Optional[str] = None
    match_date: Optional[date] = None
    position_played: Optional[str] = None
    minutes_played: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    xg: Optional[float] = None
    xa: Optional[float] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    passes: Optional[int] = None
    passes_accurate: Optional[int] = None
    dribbles: Optional[int] = None
    dribbles_successful: Optional[int] = None
    duels: Optional[int] = None
    duels_won: Optional[int] = None
    interceptions: Optional[int] = None
    progressive_runs: Optional[int] = None
    touches_in_box: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None


class PlayerSeasonAgg(BaseModel):
    competition_name: str
    matches: int
    minutes: int
    goals: int
    assists: int
    xg: Optional[float] = None
    xa: Optional[float] = None
    shots: Optional[int] = None
    passes_per90: Optional[float] = None
    goals_per90: Optional[float] = None
    xg_per90: Optional[float] = None
