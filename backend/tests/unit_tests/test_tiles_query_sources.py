from app.api import tiles


def test_risk_tile_query_uses_segment_month_type_stats_instead_of_raw_crimes():
    query = str(tiles._roads_with_risk_tile_query(10, "c.month = :month_date", False))

    assert "FROM segment_month_type_stats c" in query
    assert "SUM(c.crime_count)::numeric AS crimes" in query
    assert "FROM user_reported_events ure" in query
    assert "user_reported_crime_signal" in query
    assert "FROM crime_events c" not in query
    assert "FROM segment_month_collision_stats c" in query
    assert "FROM collision_events c" not in query
    assert "collision_severity_points" in query
    assert "safety_score" in query
    assert "FROM active_roads" in query
    assert "raw_safety_score" in query
    assert "LEFT JOIN normalized_scores" in query


def test_low_zoom_tile_query_filters_minor_roads():
    query = str(tiles._roads_only_tile_query(8))

    assert "rs.highway IN ('motorway', 'trunk', 'primary')" in query
    assert "ST_Simplify(rs.geom, 80)" in query


def test_high_zoom_tile_query_keeps_full_geometry_without_filter():
    query = str(tiles._roads_only_tile_query(15))

    assert "rs.highway IN" not in query
    assert "ST_AsMVTGeom(rs.geom, bounds.geom, :extent, :buffer, true)" in query
