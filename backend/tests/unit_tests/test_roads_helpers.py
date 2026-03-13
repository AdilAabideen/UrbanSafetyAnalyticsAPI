from app.api import roads_utils


def test_safe_pct_change_handles_zero_and_non_zero_baselines():
    assert roads_utils._safe_pct_change(120, 100) == 20.0
    assert roads_utils._safe_pct_change(120, 0) is None


def test_highway_message_flags_over_indexed_groups():
    message = roads_utils._highway_message(
        {"highway": "residential", "incident_count": 90, "length_m": 1000},
        total_incidents=100,
        total_length_m=10000,
    )

    assert "over-indexing" in message


def test_risk_item_message_includes_crime_type_and_period_change():
    message = roads_utils._risk_item_message(
        {
            "incident_count": 12,
            "dominant_crime_type": "Shoplifting",
            "previous_period_change_pct": 25.0,
        }
    )

    assert "12 incidents" in message
    assert "Shoplifting" in message
    assert "25.0% up" in message
