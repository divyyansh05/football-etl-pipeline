from pydantic import BaseModel
from typing import Optional
from datetime import date


class TeamSummary(BaseModel):
    team_id: int
    wyscout_id: int
    name: str
    country: Optional[str] = None
    matches: Optional[int] = None


class TeamMatchStat(BaseModel):
    match_label: Optional[str] = None
    competition_name: Optional[str] = None
    match_date: Optional[date] = None
    is_home: Optional[bool] = None
    formation: Optional[str] = None
    result: Optional[str] = None
    goals: Optional[int] = None
    xg: Optional[float] = None
    shots: Optional[int] = None
    possession_pct: Optional[float] = None
    passes: Optional[int] = None
    passes_accurate: Optional[int] = None
    ppda: Optional[float] = None
    duels: Optional[int] = None
    duels_won: Optional[int] = None


class TeamSeasonAgg(BaseModel):
    competition_name: str
    matches: int
    wins: int
    draws: int
    losses: int
    goals_scored: int
    goals_conceded: int
    avg_xg: float
    avg_possession: float
    avg_ppda: float
    pass_acc_pct: float
