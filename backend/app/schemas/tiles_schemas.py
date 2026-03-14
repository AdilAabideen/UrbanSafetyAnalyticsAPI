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
CRIME_ALPHA = 0.70
CRIME_DECAY_LAMBDA = 0.1625
COLLISION_DECAY_LAMBDA = 0.08
USER_REPORT_DECAY_LAMBDA = 0.51
CRIME_COMPONENT_WEIGHT = 0.50
COLLISION_COMPONENT_WEIGHT = 0.35
USER_REPORT_COMPONENT_WEIGHT = 0.15
JUNCTION_DISTANCE_M = 15.0
JUNCTION_DEGREE_WEIGHT = 0.03
JUNCTION_FACTOR_CAP = 0.20
CURVE_WEIGHT = 0.50
CURVE_FACTOR_CAP = 0.25
ADJACENT_SEGMENT_DISTANCE_M = 15.0
USER_REPORT_DISTINCT_AUTH_WEIGHT = 1.0
USER_REPORT_ANONYMOUS_WEIGHT = 0.5
USER_REPORT_REPEAT_WEIGHT = 0.25
USER_REPORT_CLUSTER_CAP = 3.0
ROAD_CLASS_HIGH_FACTOR = 1.20
ROAD_CLASS_MEDIUM_FACTOR = 1.10
ROAD_CLASS_DEFAULT_FACTOR = 1.00
RISK_BAND_ORANGE_THRESHOLD = 0.50
RISK_BAND_RED_THRESHOLD = 1.00

CRIME_HARM_WEIGHTS: Dict[str, float] = {
    "Anti-social behaviour": 0.35,
    "Bicycle theft": 0.40,
    "Burglary": 0.75,
    "Criminal damage and arson": 0.70,
    "Drugs": 0.55,
    "Other crime": 0.45,
    "Other theft": 0.50,
    "Possession of weapons": 0.80,
    "Public order": 0.50,
    "Robbery": 0.90,
    "Shoplifting": 0.45,
    "Theft from the person": 0.70,
    "Vehicle crime": 0.65,
    "Violence and sexual offences": 1.00,
}
DEFAULT_CRIME_HARM_WEIGHT = 0.50

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
    include_crime: bool
    include_collisions: bool
    include_user_reports: bool
    crime_alpha: float
    crime_decay_lambda: float
    collision_decay_lambda: float
    user_report_decay_lambda: float
    crime_component_weight: float
    collision_component_weight: float
    user_report_component_weight: float
    junction_distance_m: float
    junction_degree_weight: float
    junction_factor_cap: float
    curve_weight: float
    curve_factor_cap: float
    adjacent_segment_distance_m: float
    user_report_distinct_auth_weight: float
    user_report_anonymous_weight: float
    user_report_repeat_weight: float
    user_report_cluster_cap: float
    road_class_high_factor: float
    road_class_medium_factor: float
    road_class_default_factor: float
    harm_anti_social_behaviour: float
    harm_bicycle_theft: float
    harm_burglary: float
    harm_criminal_damage_and_arson: float
    harm_drugs: float
    harm_other_crime: float
    harm_other_theft: float
    harm_possession_of_weapons: float
    harm_public_order: float
    harm_robbery: float
    harm_shoplifting: float
    harm_theft_from_the_person: float
    harm_vehicle_crime: float
    harm_violence_and_sexual_offences: float
    harm_default: float
    risk_band_orange_threshold: float
    risk_band_red_threshold: float


@dataclass(frozen=True)
class TileMonthFilter:
    clause: Optional[str]
    params: Dict[str, date]
