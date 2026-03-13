from typing import Optional

from fastapi import HTTPException
from sqlalchemy import bindparam

from .crime_utils import (
    _normalize_filter_values,
    _optional_bbox,
    _parse_month,
    _shift_month,
)


def _resolve_from_to_filter(from_month, to_month, required=False):
    """Validate and normalize a `from`/`to` YYYY-MM month range."""
    if from_month or to_month:
        if not (from_month and to_month):
            raise HTTPException(status_code=400, detail="from and to must be provided together")

        from_month_date = _parse_month(from_month, "from")
        to_month_date = _parse_month(to_month, "to")
        if from_month_date > to_month_date:
            raise HTTPException(status_code=400, detail="from must be less than or equal to to")

        return {
            "from_date": from_month_date,
            "to_date": to_month_date,
            "from": from_month_date.strftime("%Y-%m"),
            "to": to_month_date.strftime("%Y-%m"),
        }

    if required:
        raise HTTPException(status_code=400, detail="from and to are required")

    return {
        "from_date": None,
        "to_date": None,
        "from": None,
        "to": None,
    }


def _where_sql(where_clauses):
    """Format a WHERE clause block from a list of conditions."""
    if not where_clauses:
        return ""
    return "WHERE " + " AND ".join(where_clauses)


def _roads_scope_filters(bbox, highways):
    """Build road-segment scope filters (bbox and highway classes)."""
    where_clauses = []
    query_params = {}

    if bbox:
        where_clauses.extend(
            [
                "rs.geom && ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857)",
                "ST_Intersects(rs.geom, ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857))",
            ]
        )
        query_params.update(bbox)

    if highways:
        where_clauses.append("COALESCE(NULLIF(rs.highway, ''), 'unknown') IN :highways")
        query_params["highways"] = highways

    return where_clauses, query_params


def _incident_count_filters(range_filter, crime_types, last_outcome_categories=None, alias="c"):
    """Build event-level filters for crimes linked to roads."""
    where_clauses = []
    query_params = {}

    if range_filter["from_date"] is not None:
        where_clauses.append(f"{alias}.month BETWEEN :from_month_date AND :to_month_date")
        query_params["from_month_date"] = range_filter["from_date"]
        query_params["to_month_date"] = range_filter["to_date"]

    if crime_types:
        where_clauses.append(f"COALESCE(NULLIF({alias}.crime_type, ''), 'unknown') IN :crime_types")
        query_params["crime_types"] = crime_types

    if last_outcome_categories:
        where_clauses.append(
            f"COALESCE(NULLIF({alias}.last_outcome_category, ''), 'unknown') IN :last_outcome_categories"
        )
        query_params["last_outcome_categories"] = last_outcome_categories

    return where_clauses, query_params


def _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories=None):
    """Attach expanding bind params only when placeholders are present."""
    sql = str(query)

    if highways and ":highways" in sql:
        query = query.bindparams(bindparam("highways", expanding=True))
    if crime_types and ":crime_types" in sql:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories and ":last_outcome_categories" in sql:
        query = query.bindparams(bindparam("last_outcome_categories", expanding=True))
    return query


def _roads_analytics_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    crime_type,
    last_outcome_category,
    highway,
    required_range,
):
    """Normalize shared roads analytics query parameters."""
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    last_outcome_categories = _normalize_filter_values(last_outcome_category, "lastOutcomeCategory")
    highways = _normalize_filter_values(highway, "highway")
    return range_filter, bbox, crime_types, last_outcome_categories, highways


def _risk_band_expression(score_alias="risk_score"):
    """Return SQL expression mapping score values into risk bands."""
    return f"""
    CASE
        WHEN {score_alias} >= 90 THEN 'red'
        WHEN {score_alias} >= 70 THEN 'orange'
        ELSE 'green'
    END
    """


def _sort_expression(sort):
    """Return SQL ORDER BY expression for risk endpoint sort modes."""
    if sort == "incident_count":
        return "incident_count DESC, risk_score DESC, incidents_per_km DESC, segment_id ASC"
    if sort == "incidents_per_km":
        return "incidents_per_km DESC, risk_score DESC, incident_count DESC, segment_id ASC"
    return "risk_score DESC, incident_count DESC, incidents_per_km DESC, segment_id ASC"


def _matched_previous_period(range_filter):
    """Return the previous matched period for period-over-period comparison."""
    month_span = (
        (range_filter["to_date"].year - range_filter["from_date"].year) * 12
        + (range_filter["to_date"].month - range_filter["from_date"].month)
    )
    previous_to_date = _shift_month(range_filter["from_date"], -1)
    previous_from_date = _shift_month(range_filter["from_date"], -(month_span + 1))
    return {
        "from_date": previous_from_date,
        "to_date": previous_to_date,
        "from": previous_from_date.strftime("%Y-%m"),
        "to": previous_to_date.strftime("%Y-%m"),
    }


def _safe_pct_change(current_value: Optional[float], previous_value: Optional[float]):
    """Compute safe percent change, handling empty/zero previous values."""
    if previous_value in (None, 0):
        return None
    return round(((current_value - previous_value) / previous_value) * 100.0, 2)


def _road_events_cte(road_where_sql, event_where_sql):
    """Build the base CTE used by roads analytics queries."""
    return f"""
        WITH roads_scope AS (
            SELECT
                rs.id,
                COALESCE(NULLIF(rs.name, ''), '(unnamed road)') AS name,
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        ),
        events_scope AS (
            SELECT
                ce.segment_id,
                ce.month,
                rs.highway,
                COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
                COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS outcome
            FROM crime_events ce
            JOIN roads_scope rs ON rs.id = ce.segment_id
            {event_where_sql}
        )
    """


def _road_insight_messages(total_incidents, top_highway, top_crime_type, top_outcome, current_vs_previous_pct):
    """Generate overview insights for the roads analytics overview endpoint."""
    if total_incidents == 0:
        return ["No road-linked incidents matched the current filter selection."]

    insights = []
    if top_highway:
        insights.append(
            f"{top_highway['highway']} roads drive the largest incident volume in this selection."
        )
    if top_crime_type:
        insights.append(
            f"{top_crime_type['crime_type']} is the dominant road-linked crime type here."
        )
    if top_outcome:
        insights.append(
            f"Most linked incidents currently end with '{top_outcome['outcome']}'."
        )
    if current_vs_previous_pct is not None:
        direction = "above" if current_vs_previous_pct >= 0 else "below"
        insights.append(
            f"This period is {abs(current_vs_previous_pct):.1f}% {direction} the previous matched period."
        )
    return insights[:4]


def _highway_message(item, total_incidents, total_length_m):
    """Generate a short explanation for a highway breakdown row."""
    if item["incident_count"] == 0:
        return f"{item['highway']} roads are present here but have no linked incidents in this selection."

    share_of_incidents = 0 if total_incidents == 0 else (item["incident_count"] / total_incidents) * 100.0
    share_of_length = 0 if total_length_m == 0 else (item["length_m"] / total_length_m) * 100.0
    if share_of_incidents > share_of_length * 1.5:
        return f"{item['highway']} roads are over-indexing for incidents relative to their length."
    if share_of_incidents > 25:
        return f"{item['highway']} roads account for a large share of incidents in this view."
    return f"{item['highway']} roads contribute steady incident volume without dominating the area."


def _risk_item_message(item):
    """Generate the per-road narrative shown in risk results."""
    parts = [f"{item['incident_count']} incidents"]
    if item.get("dominant_crime_type"):
        parts.append(f"driven mainly by {item['dominant_crime_type']}")
    if item.get("previous_period_change_pct") is not None:
        direction = "up" if item["previous_period_change_pct"] >= 0 else "down"
        parts.append(f"{abs(item['previous_period_change_pct']):.1f}% {direction} vs previous period")
    return ", ".join(parts)
