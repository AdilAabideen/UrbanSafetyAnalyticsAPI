from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Tuple, TypedDict

MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
PBF_MEDIA_TYPE = "application/x-protobuf"
TILE_CACHE_CONTROL = "public, max-age=60"
TILE_EXTENT = 4096
TILE_BUFFER = 64
RISK_LENGTH_FLOOR_M = 100.0
CRIME_WEIGHT = 0.55
COLLISION_WEIGHT = 0.45
SLIGHT_CASUALTY_WEIGHT = 0.5
SERIOUS_CASUALTY_WEIGHT = 2.0
FATAL_CASUALTY_WEIGHT = 5.0
USER_REPORTED_CRIME_WEIGHT = 0.10
ANONYMOUS_USER_REPORT_WEIGHT = 0.5
REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25
USER_REPORTED_SIGNAL_CAP = 3.0

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
