from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Tuple, TypedDict


class TileProfile(TypedDict):
    highways: Optional[Tuple[str, ...]]
    simplify_tolerance: int


class TileQueryParams(TypedDict, total=False):
    z: int
    x: int
    y: int
    extent: int
    buffer: int
    risk_length_floor_m: float
    crime_weight: float
    collision_weight: float
    slight_casualty_weight: float
    serious_casualty_weight: float
    fatal_casualty_weight: float
    user_report_weight: float
    anonymous_report_weight: float
    repeat_authenticated_report_weight: float
    user_report_signal_cap: float
    month_date: date
    start_month_date: date
    end_month_date: date
    crime_type: str


@dataclass(frozen=True)
class TileMonthFilter:
    clause: Optional[str]
    params: Dict[str, date]
