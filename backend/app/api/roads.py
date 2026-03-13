from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from .roads_utils import (
    _bind_roads_analytics_params,
    _execute,
    _highway_message,
    _incident_count_filters,
    _matched_previous_period,
    _resolve_from_to_filter,
    _risk_band_expression,
    _risk_item_message,
    _road_events_cte,
    _road_insight_messages,
    _roads_analytics_filters,
    _roads_scope_filters,
    _safe_pct_change,
    _sort_expression,
    _where_sql,
)
from ..db import get_db


router = APIRouter(tags=["roads"])


@router.get("/roads/analytics/meta")
def get_road_analytics_meta(db: Session = Depends(get_db)):
    months_query = text(
        """
        SELECT
            to_char(MIN(ce.month), 'YYYY-MM') AS min_month,
            to_char(MAX(ce.month), 'YYYY-MM') AS max_month
        FROM crime_events ce
        WHERE ce.segment_id IS NOT NULL
        """
    )
    highways_query = text(
        """
        SELECT DISTINCT COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway
        FROM road_segments rs
        ORDER BY highway ASC
        """
    )
    crime_types_query = text(
        """
        SELECT DISTINCT COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type
        FROM crime_events ce
        WHERE ce.segment_id IS NOT NULL
        ORDER BY crime_type ASC
        """
    )
    outcomes_query = text(
        """
        SELECT DISTINCT COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS outcome
        FROM crime_events ce
        WHERE ce.segment_id IS NOT NULL
        ORDER BY outcome ASC
        """
    )
    counts_query = text(
        """
        SELECT
            COUNT(*)::bigint AS road_segments_total,
            COUNT(*) FILTER (WHERE rs.name IS NOT NULL AND NULLIF(rs.name, '') IS NOT NULL)::bigint AS named_roads_total,
            COALESCE(SUM(rs.length_m), 0)::float AS total_length_m,
            (
                SELECT COUNT(DISTINCT ce.segment_id)::bigint
                FROM crime_events ce
                WHERE ce.segment_id IS NOT NULL
            ) AS roads_with_incidents,
            (
                SELECT COUNT(*)::bigint
                FROM crime_events ce
                WHERE ce.segment_id IS NOT NULL
            ) AS incidents_total
        FROM road_segments rs
        """
    )

    months_row = _execute(db, months_query, {}).mappings().first() or {}
    highway_rows = _execute(db, highways_query, {}).mappings().all()
    crime_type_rows = _execute(db, crime_types_query, {}).mappings().all()
    outcome_rows = _execute(db, outcomes_query, {}).mappings().all()
    counts_row = _execute(db, counts_query, {}).mappings().first() or {}

    return {
        "months": {
            "min": months_row.get("min_month"),
            "max": months_row.get("max_month"),
        },
        "highways": [row["highway"] for row in highway_rows],
        "crime_types": [row["crime_type"] for row in crime_type_rows],
        "outcomes": [row["outcome"] for row in outcome_rows],
        "counts": {
            "road_segments_total": counts_row.get("road_segments_total", 0),
            "named_roads_total": counts_row.get("named_roads_total", 0),
            "total_length_m": counts_row.get("total_length_m", 0),
            "roads_with_incidents": counts_row.get("roads_with_incidents", 0),
            "incidents_total": counts_row.get("incidents_total", 0),
        },
    }


@router.get("/roads/analytics/overview")
def get_road_analytics_overview(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    summary_query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        )
        SELECT
            COUNT(*)::bigint AS total_segments,
            COALESCE(SUM(length_m), 0)::float AS total_length_m,
            COUNT(DISTINCT highway)::bigint AS unique_highway_types,
            COUNT(*) FILTER (WHERE incident_count > 0)::bigint AS roads_with_incidents,
            COALESCE(SUM(incident_count), 0)::bigint AS total_incidents,
            COALESCE(SUM(incident_count) / NULLIF(SUM(length_m) / 1000.0, 0), 0)::float AS avg_incidents_per_km
        FROM scored
        """
    )
    top_highway_query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS incident_count,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)) / NULLIF(SUM(rs.length_m) / 1000.0, 0), 0)::float AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC
        LIMIT 1
        """
    )
    top_crime_type_query = text(
        base_cte
        + """
        SELECT
            crime_type,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY crime_type
        ORDER BY count DESC, crime_type ASC
        LIMIT 1
        """
    )
    top_outcome_query = text(
        base_cte
        + """
        SELECT
            outcome,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY outcome
        ORDER BY count DESC, outcome ASC
        LIMIT 1
        """
    )
    top_road_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density
            FROM scored
        ),
        final_ranked AS (
            SELECT
                *,
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM ranked
        )
        SELECT
            segment_id,
            name,
            highway,
            length_m,
            incident_count,
            incidents_per_km,
            risk_score,
            {_risk_band_expression()} AS band
        FROM final_ranked
        ORDER BY risk_score DESC, incident_count DESC, incidents_per_km DESC
        LIMIT 1
        """
    )
    band_breakdown_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density
            FROM scored
        ),
        final_ranked AS (
            SELECT
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM ranked
        )
        SELECT
            {_risk_band_expression()} AS band,
            COUNT(*)::bigint AS count
        FROM final_ranked
        GROUP BY band
        """
    )

    summary_query = _bind_roads_analytics_params(summary_query, highways, crime_types, last_outcome_categories)
    top_highway_query = _bind_roads_analytics_params(top_highway_query, highways, crime_types, last_outcome_categories)
    top_crime_type_query = _bind_roads_analytics_params(top_crime_type_query, highways, crime_types, last_outcome_categories)
    top_outcome_query = _bind_roads_analytics_params(top_outcome_query, highways, crime_types, last_outcome_categories)
    top_road_query = _bind_roads_analytics_params(top_road_query, highways, crime_types, last_outcome_categories)
    band_breakdown_query = _bind_roads_analytics_params(band_breakdown_query, highways, crime_types, last_outcome_categories)

    summary_row = _execute(db, summary_query, query_params).mappings().first() or {}
    top_highway_row = _execute(db, top_highway_query, query_params).mappings().first()
    top_crime_type_row = _execute(db, top_crime_type_query, query_params).mappings().first()
    top_outcome_row = _execute(db, top_outcome_query, query_params).mappings().first()
    top_road_row = _execute(db, top_road_query, query_params).mappings().first()
    band_rows = _execute(db, band_breakdown_query, query_params).mappings().all()

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COUNT(*)::bigint AS incident_count
        FROM crime_events ce
        JOIN roads_scope rs ON rs.id = ce.segment_id
        {_where_sql(previous_where_clauses)}
        """
    )
    previous_query = _bind_roads_analytics_params(previous_query, highways, crime_types, last_outcome_categories)
    previous_query_params = dict(query_params)
    previous_query_params.update(previous_params)
    previous_row = _execute(db, previous_query, previous_query_params).mappings().first() or {}

    total_incidents = summary_row.get("total_incidents", 0)
    previous_incidents = previous_row.get("incident_count", 0)
    current_vs_previous_pct = _safe_pct_change(total_incidents, previous_incidents)
    total_segments = summary_row.get("total_segments", 0)
    roads_with_incidents = summary_row.get("roads_with_incidents", 0)
    band_breakdown = {"red": 0, "orange": 0, "green": 0}
    band_breakdown.update({row["band"]: row["count"] for row in band_rows})

    return {
        "filters": {
            "from": range_filter["from"],
            "to": range_filter["to"],
            "crimeType": crime_types,
            "lastOutcomeCategory": last_outcome_categories,
            "highway": highways,
            "bbox": None if not bbox else {
                "minLon": bbox["min_lon"],
                "minLat": bbox["min_lat"],
                "maxLon": bbox["max_lon"],
                "maxLat": bbox["max_lat"],
            },
        },
        "total_segments": total_segments,
        "total_length_m": summary_row.get("total_length_m", 0),
        "roads_with_incidents": roads_with_incidents,
        "roads_without_incidents": max(total_segments - roads_with_incidents, 0),
        "road_coverage_pct": 0 if total_segments == 0 else round((roads_with_incidents / total_segments) * 100.0, 2),
        "unique_highway_types": summary_row.get("unique_highway_types", 0),
        "total_incidents": total_incidents,
        "avg_incidents_per_km": summary_row.get("avg_incidents_per_km", 0),
        "top_road": None if not top_road_row else {
            "segment_id": top_road_row["segment_id"],
            "name": top_road_row["name"],
            "highway": top_road_row["highway"],
            "length_m": top_road_row["length_m"],
            "incident_count": top_road_row["incident_count"],
            "incidents_per_km": top_road_row["incidents_per_km"],
            "risk_score": top_road_row["risk_score"],
            "band": top_road_row["band"],
        },
        "top_highway": None if not top_highway_row else {
            "highway": top_highway_row["highway"],
            "segment_count": top_highway_row["segment_count"],
            "length_m": top_highway_row["length_m"],
            "incident_count": top_highway_row["incident_count"],
            "incidents_per_km": top_highway_row["incidents_per_km"],
        },
        "top_crime_type": top_crime_type_row,
        "top_outcome": top_outcome_row,
        "current_period": {"from": range_filter["from"], "to": range_filter["to"], "incident_count": total_incidents},
        "previous_period": {
            "from": previous_filter["from"],
            "to": previous_filter["to"],
            "incident_count": previous_incidents,
        },
        "current_vs_previous_pct": current_vs_previous_pct,
        "band_breakdown": band_breakdown,
        "insights": _road_insight_messages(
            total_incidents,
            top_highway_row,
            top_crime_type_row,
            top_outcome_row,
            current_vs_previous_pct,
        ),
    }


@router.get("/roads/analytics/charts")
def get_road_analytics_charts(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    timeseriesGroupBy: str = Query("overall"),
    groupLimit: int = Query(5, ge=1, le=10),
    limit: int = Query(10, ge=1, le=25),
    db: Session = Depends(get_db),
):
    if timeseriesGroupBy not in {"overall", "highway", "crime_type", "outcome"}:
        raise HTTPException(status_code=400, detail="timeseriesGroupBy must be overall, highway, crime_type, or outcome")

    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    group_expr = {
        "overall": "'overall'",
        "highway": "highway",
        "crime_type": "crime_type",
        "outcome": "outcome",
    }[timeseriesGroupBy]

    if timeseriesGroupBy == "overall":
        timeseries_query = text(
            base_cte
            + """
            ,
            months AS (
                SELECT generate_series(
                    CAST(:from_month_date AS date),
                    CAST(:to_month_date AS date),
                    interval '1 month'
                )::date AS month
            ),
            counts AS (
                SELECT
                    month,
                    COUNT(*)::bigint AS count
                FROM events_scope
                GROUP BY month
            )
            SELECT
                'overall' AS group_key,
                to_char(months.month, 'YYYY-MM') AS month,
                COALESCE(counts.count, 0)::bigint AS count
            FROM months
            LEFT JOIN counts ON counts.month = months.month
            ORDER BY months.month ASC
            """
        )
        timeseries_rows = _execute(
            db,
            _bind_roads_analytics_params(timeseries_query, highways, crime_types, last_outcome_categories),
            query_params,
        ).mappings().all()
        series = [{"key": "overall", "points": [{"month": row["month"], "count": row["count"]} for row in timeseries_rows]}]
    else:
        query_params["group_limit"] = groupLimit
        timeseries_query = text(
            base_cte
            + f"""
            ,
            months AS (
                SELECT generate_series(
                    CAST(:from_month_date AS date),
                    CAST(:to_month_date AS date),
                    interval '1 month'
                )::date AS month
            ),
            top_groups AS (
                SELECT
                    {group_expr} AS group_key,
                    COUNT(*)::bigint AS total
                FROM events_scope
                GROUP BY group_key
                ORDER BY total DESC, group_key ASC
                LIMIT :group_limit
            ),
            counts AS (
                SELECT
                    month,
                    {group_expr} AS group_key,
                    COUNT(*)::bigint AS count
                FROM events_scope
                GROUP BY month, group_key
            )
            SELECT
                top_groups.group_key,
                top_groups.total,
                to_char(months.month, 'YYYY-MM') AS month,
                COALESCE(counts.count, 0)::bigint AS count
            FROM top_groups
            CROSS JOIN months
            LEFT JOIN counts
              ON counts.group_key = top_groups.group_key
             AND counts.month = months.month
            ORDER BY top_groups.total DESC, top_groups.group_key ASC, months.month ASC
            """
        )
        timeseries_rows = _execute(
            db,
            _bind_roads_analytics_params(timeseries_query, highways, crime_types, last_outcome_categories),
            query_params,
        ).mappings().all()
        grouped = {}
        group_totals = {}
        for row in timeseries_rows:
            grouped.setdefault(row["group_key"], [])
            grouped[row["group_key"]].append({"month": row["month"], "count": row["count"]})
            group_totals[row["group_key"]] = row["total"]
        series = [
            {"key": key, "total": group_totals.get(key, 0), "points": points}
            for key, points in grouped.items()
        ]

    highway_query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS count,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)) / NULLIF(SUM(rs.length_m) / 1000.0, 0), 0)::float AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY count DESC, rs.highway ASC
        """
    )
    crime_type_query = text(
        base_cte
        + """
        SELECT
            crime_type,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY crime_type
        ORDER BY count DESC, crime_type ASC
        """
    )
    outcome_query = text(
        base_cte
        + """
        SELECT
            outcome,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY outcome
        ORDER BY count DESC, outcome ASC
        """
    )
    total_query = text(base_cte + "SELECT COUNT(*)::bigint AS total_incidents FROM events_scope")
    band_breakdown_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density
            FROM scored
        ),
        final_ranked AS (
            SELECT
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM ranked
        )
        SELECT
            {_risk_band_expression()} AS band,
            COUNT(*)::bigint AS count
        FROM final_ranked
        GROUP BY band
        """
    )

    highway_query = _bind_roads_analytics_params(highway_query, highways, crime_types, last_outcome_categories)
    crime_type_query = _bind_roads_analytics_params(crime_type_query, highways, crime_types, last_outcome_categories)
    outcome_query = _bind_roads_analytics_params(outcome_query, highways, crime_types, last_outcome_categories)
    total_query = _bind_roads_analytics_params(total_query, highways, crime_types, last_outcome_categories)
    band_breakdown_query = _bind_roads_analytics_params(band_breakdown_query, highways, crime_types, last_outcome_categories)

    highway_rows = _execute(db, highway_query, query_params).mappings().all()
    crime_type_rows = _execute(db, crime_type_query, query_params).mappings().all()
    outcome_rows = _execute(db, outcome_query, query_params).mappings().all()
    total_incidents = (_execute(db, total_query, query_params).mappings().first() or {}).get("total_incidents", 0)
    band_rows = _execute(db, band_breakdown_query, query_params).mappings().all()

    total_length_m = sum(row["length_m"] for row in highway_rows)
    by_highway = [
        {
            "highway": row["highway"],
            "segment_count": row["segment_count"],
            "length_m": row["length_m"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
            "incidents_per_km": row["incidents_per_km"],
            "message": _highway_message(
                {"highway": row["highway"], "incident_count": row["count"], "length_m": row["length_m"]},
                total_incidents,
                total_length_m,
            ),
        }
        for row in highway_rows[:limit]
    ]
    by_crime_type = [
        {
            "crime_type": row["crime_type"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
        }
        for row in crime_type_rows[:limit]
    ]
    by_outcome = [
        {
            "outcome": row["outcome"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
        }
        for row in outcome_rows[:limit]
    ]

    overall_total = (
        sum(point["count"] for item in series for point in item["points"])
        if timeseriesGroupBy != "overall"
        else sum(point["count"] for point in series[0]["points"]) if series else 0
    )
    overall_points = (
        series[0]["points"]
        if timeseriesGroupBy == "overall"
        else [
            {
                "month": month,
                "count": sum(item["points"][idx]["count"] for item in series),
            }
            for idx, month in enumerate([point["month"] for point in series[0]["points"]])
        ]
        if series
        else []
    )
    peak_point = max(overall_points, key=lambda point: point["count"], default=None)

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COUNT(*)::bigint AS incident_count
        FROM crime_events ce
        JOIN roads_scope rs ON rs.id = ce.segment_id
        {_where_sql(previous_where_clauses)}
        """
    )
    previous_query = _bind_roads_analytics_params(previous_query, highways, crime_types, last_outcome_categories)
    previous_query_params = {key: value for key, value in query_params.items() if key != "group_limit"}
    previous_query_params.update(previous_params)
    previous_total = (_execute(db, previous_query, previous_query_params).mappings().first() or {}).get("incident_count", 0)
    current_vs_previous_pct = _safe_pct_change(overall_total, previous_total)

    band_breakdown = {"red": 0, "orange": 0, "green": 0}
    band_breakdown.update({row["band"]: row["count"] for row in band_rows})

    insights = []
    if peak_point:
        insights.append(f"Peak incident month in this selection is {peak_point['month']}.")
    if by_highway:
        insights.append(f"{by_highway[0]['highway']} roads dominate the current highway breakdown.")
    if by_crime_type:
        insights.append(f"{by_crime_type[0]['crime_type']} is the largest road-linked crime type in this view.")
    if current_vs_previous_pct is not None:
        direction = "above" if current_vs_previous_pct >= 0 else "below"
        insights.append(
            f"The current period is {abs(current_vs_previous_pct):.1f}% {direction} the previous matched period."
        )

    return {
        "filters": {
            "from": range_filter["from"],
            "to": range_filter["to"],
            "crimeType": crime_types,
            "lastOutcomeCategory": last_outcome_categories,
            "highway": highways,
            "bbox": None if not bbox else {
                "minLon": bbox["min_lon"],
                "minLat": bbox["min_lat"],
                "maxLon": bbox["max_lon"],
                "maxLat": bbox["max_lat"],
            },
        },
        "timeseries": {
            "groupBy": timeseriesGroupBy,
            "series": series,
            "total": overall_total,
            "peak": peak_point,
            "current_vs_previous_pct": current_vs_previous_pct,
        },
        "by_highway": by_highway,
        "by_crime_type": by_crime_type,
        "by_outcome": by_outcome,
        "band_breakdown": band_breakdown,
        "insights": insights,
    }


@router.get("/roads/analytics/risk")
def get_road_analytics_risk(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    sort: str = Query("risk_score"),
    db: Session = Depends(get_db),
):
    if sort not in {"risk_score", "incidents_per_km", "incident_count"}:
        raise HTTPException(status_code=400, detail="sort must be risk_score, incidents_per_km, or incident_count")

    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    query_params["row_limit"] = limit
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_where_sql = _where_sql(previous_where_clauses)

    query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        dominant_crime_types AS (
            SELECT
                segment_id,
                crime_type,
                COUNT(*)::bigint AS type_count,
                ROW_NUMBER() OVER (
                    PARTITION BY segment_id
                    ORDER BY COUNT(*) DESC, crime_type ASC
                ) AS rn
            FROM events_scope
            GROUP BY segment_id, crime_type
        ),
        dominant_outcomes AS (
            SELECT
                segment_id,
                outcome,
                COUNT(*)::bigint AS outcome_count,
                ROW_NUMBER() OVER (
                    PARTITION BY segment_id
                    ORDER BY COUNT(*) DESC, outcome ASC
                ) AS rn
            FROM events_scope
            GROUP BY segment_id, outcome
        ),
        previous_events_scope AS (
            SELECT
                ce.segment_id
            FROM crime_events ce
            JOIN roads_scope rs ON rs.id = ce.segment_id
            {previous_where_sql}
        ),
        previous_incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS previous_incident_count
            FROM previous_events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km,
                COALESCE(pic.previous_incident_count, 0)::bigint AS previous_incident_count,
                dct.crime_type AS dominant_crime_type,
                dout.outcome AS dominant_outcome
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            LEFT JOIN previous_incident_counts pic ON pic.segment_id = rs.id
            LEFT JOIN dominant_crime_types dct ON dct.segment_id = rs.id AND dct.rn = 1
            LEFT JOIN dominant_outcomes dout ON dout.segment_id = rs.id AND dout.rn = 1
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        metrics AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density,
                SUM(incident_count) OVER ()::bigint AS total_incidents_in_scope
            FROM scored
        ),
        ranked AS (
            SELECT
                *,
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM metrics
        )
        SELECT
            segment_id,
            name,
            highway,
            length_m,
            incident_count,
            incidents_per_km,
            dominant_crime_type,
            dominant_outcome,
            previous_incident_count,
            total_incidents_in_scope,
            risk_score,
            {_risk_band_expression()} AS band
        FROM ranked
        ORDER BY {_sort_expression(sort)}
        LIMIT :row_limit
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories)

    execution_params = dict(query_params)
    execution_params.update(previous_params)
    rows = _execute(db, query, execution_params).mappings().all()

    return {
        "filters": {
            "from": range_filter["from"],
            "to": range_filter["to"],
            "crimeType": crime_types,
            "lastOutcomeCategory": last_outcome_categories,
            "highway": highways,
            "bbox": None if not bbox else {
                "minLon": bbox["min_lon"],
                "minLat": bbox["min_lat"],
                "maxLon": bbox["max_lon"],
                "maxLat": bbox["max_lat"],
            },
        },
        "items": [
            {
                "segment_id": row["segment_id"],
                "name": row["name"],
                "highway": row["highway"],
                "length_m": row["length_m"],
                "incident_count": row["incident_count"],
                "incidents_per_km": row["incidents_per_km"],
                "dominant_crime_type": row["dominant_crime_type"],
                "dominant_outcome": row["dominant_outcome"],
                "share_of_incidents": 0
                if row["total_incidents_in_scope"] in (None, 0)
                else round((row["incident_count"] / row["total_incidents_in_scope"]) * 100.0, 2),
                "previous_period_change_pct": _safe_pct_change(
                    row["incident_count"],
                    row["previous_incident_count"],
                ),
                "risk_score": row["risk_score"],
                "band": row["band"],
                "message": _risk_item_message(
                    {
                        "incident_count": row["incident_count"],
                        "dominant_crime_type": row["dominant_crime_type"],
                        "previous_period_change_pct": _safe_pct_change(
                            row["incident_count"],
                            row["previous_incident_count"],
                        ),
                    }
                ),
            }
            for row in rows
        ],
        "meta": {
            "returned": len(rows),
            "limit": limit,
            "sort": sort,
        },
    }
