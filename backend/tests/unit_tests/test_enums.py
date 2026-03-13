from app.schemas.enums import (
    CollisionSeverity,
    CrimeOutcome,
    CrimeType,
    HighwayClass,
    LightCondition,
    RoadSurfaceCondition,
    WeatherCondition,
)


def test_crime_type_enum_values():
    assert CrimeType.BURGLARY.value == "Burglary"
    assert CrimeType.VIOLENCE_AND_SEXUAL_OFFENCES.value == "Violence and sexual offences"


def test_crime_outcome_enum_values():
    assert CrimeOutcome.UNDER_INVESTIGATION.value == "Under investigation"
    assert CrimeOutcome.INVESTIGATION_COMPLETE_NO_SUSPECT.value == "Investigation complete; no suspect identified"


def test_collision_severity_enum_values():
    assert {s.value for s in CollisionSeverity} == {"Fatal", "Serious", "Slight"}


def test_weather_light_and_surface_enums_have_expected_members():
    assert "Fine no high winds" in {w.value for w in WeatherCondition}
    assert "Daylight" in {l.value for l in LightCondition}
    assert "Dry" in {r.value for r in RoadSurfaceCondition}


def test_highway_class_includes_common_types():
    values = {h.value for h in HighwayClass}
    assert "motorway" in values
    assert "residential" in values
    assert "tertiary" in values

