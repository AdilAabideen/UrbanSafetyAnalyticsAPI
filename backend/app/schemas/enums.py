from enum import Enum


class CrimeType(str, Enum):
    ANTI_SOCIAL_BEHAVIOUR = "Anti-social behaviour"
    BICYCLE_THEFT = "Bicycle theft"
    BURGLARY = "Burglary"
    CRIMINAL_DAMAGE_AND_ARSON = "Criminal damage and arson"
    DRUGS = "Drugs"
    OTHER_CRIME = "Other crime"
    OTHER_THEFT = "Other theft"
    POSSESSION_OF_WEAPONS = "Possession of weapons"
    PUBLIC_ORDER = "Public order"
    ROBBERY = "Robbery"
    SHOPLIFTING = "Shoplifting"
    THEFT_FROM_THE_PERSON = "Theft from the person"
    VEHICLE_CRIME = "Vehicle crime"
    VIOLENCE_AND_SEXUAL_OFFENCES = "Violence and sexual offences"


class CrimeOutcome(str, Enum):
    ACTION_TO_BE_TAKEN_BY_ANOTHER_ORG = "Action to be taken by another organisation"
    AWAITING_COURT_OUTCOME = "Awaiting court outcome"
    COURT_RESULT_UNAVAILABLE = "Court result unavailable"
    FORMAL_ACTION_NOT_IN_PUBLIC_INTEREST = "Formal action is not in the public interest"
    FURTHER_ACTION_NOT_IN_PUBLIC_INTEREST = "Further action is not in the public interest"
    FURTHER_INVESTIGATION_NOT_IN_PUBLIC_INTEREST = "Further investigation is not in the public interest"
    INVESTIGATION_COMPLETE_NO_SUSPECT = "Investigation complete; no suspect identified"
    LOCAL_RESOLUTION = "Local resolution"
    OFFENDER_GIVEN_A_CAUTION = "Offender given a caution"
    OFFENDER_GIVEN_DRUGS_POSSESSION_WARNING = "Offender given a drugs possession warning"
    OFFENDER_GIVEN_PENALTY_NOTICE = "Offender given penalty notice"
    STATUS_UPDATE_UNAVAILABLE = "Status update unavailable"
    SUSPECT_CHARGED_AS_PART_OF_ANOTHER_CASE = "Suspect charged as part of another case"
    UNABLE_TO_PROSECUTE_SUSPECT = "Unable to prosecute suspect"
    UNDER_INVESTIGATION = "Under investigation"


class CollisionSeverity(str, Enum):
    FATAL = "Fatal"
    SERIOUS = "Serious"
    SLIGHT = "Slight"


class WeatherCondition(str, Enum):
    FINE_NO_HIGH_WINDS = "Fine no high winds"
    FINE_HIGH_WINDS = "Fine + high winds"
    RAINING_NO_HIGH_WINDS = "Raining no high winds"
    RAINING_HIGH_WINDS = "Raining + high winds"
    SNOWING_NO_HIGH_WINDS = "Snowing no high winds"
    OTHER = "Other"
    UNKNOWN = "Unknown"


class LightCondition(str, Enum):
    DAYLIGHT = "Daylight"
    DARKNESS_LIGHTS_LIT = "Darkness - lights lit"
    DARKNESS_LIGHTS_UNLIT = "Darkness - lights unlit"
    DARKNESS_NO_LIGHTING = "Darkness - no lighting"
    DARKNESS_LIGHTING_UNKNOWN = "Darkness - lighting unknown"


class RoadSurfaceCondition(str, Enum):
    DRY = "Dry"
    FLOOD_OVER_3CM_DEEP = "Flood over 3cm. deep"
    FROST_OR_ICE = "Frost or ice"
    SNOW = "Snow"
    WET_OR_DAMP = "Wet or damp"


class HighwayClass(str, Enum):
    BRIDLEWAY = "bridleway"
    BUS_GUIDEWAY = "bus_guideway"
    BUSWAY = "busway"
    CONSTRUCTION = "construction"
    CORRIDOR = "corridor"
    CYCLEWAY = "cycleway"
    ELEVATOR = "elevator"
    FOOTWAY = "footway"
    LIVING_STREET = "living_street"
    MOTORWAY = "motorway"
    MOTORWAY_LINK = "motorway_link"
    NO = "no"
    PATH = "path"
    PEDESTRIAN = "pedestrian"
    PLATFORM = "platform"
    PRIMARY = "primary"
    PRIMARY_LINK = "primary_link"
    PROPOSED = "proposed"
    RACEWAY = "raceway"
    RESIDENTIAL = "residential"
    REST_AREA = "rest_area"
    ROAD = "road"
    SECONDARY = "secondary"
    SECONDARY_LINK = "secondary_link"
    SERVICE = "service"
    SERVICES = "services"
    STEPS = "steps"
    TERTIARY = "tertiary"
    TERTIARY_LINK = "tertiary_link"
    TRACK = "track"
    TRUNK = "trunk"
    TRUNK_LINK = "trunk_link"
    UNCLASSIFIED = "unclassified"

