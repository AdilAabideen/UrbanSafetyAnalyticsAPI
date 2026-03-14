import { WEST_YORKSHIRE_BBOX } from "./crimeUtils";

export const DEFAULT_ROAD_FILTERS = {
  monthFrom: toMonthValue(new Date(new Date().getFullYear(), new Date().getMonth() - 2, 1)),
  monthTo: toMonthValue(new Date()),
  crimeType: "",
  outcomeCategory: "",
  highway: "",
  bbox: { ...WEST_YORKSHIRE_BBOX },
};

export const ROAD_RISK_FETCH_LIMIT = 100;
export const ROAD_RISK_PAGE_SIZE = 25;
export const ROAD_CHART_LIMIT = 8;

export const ROAD_WORKSPACE_TABS = [
  { id: "risk", label: "Risk Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "highways", label: "Highways" },
  { id: "crime-types", label: "Crime Types" },
  { id: "outcomes", label: "Outcomes" },
];

export const RISK_SORT_OPTIONS = [
  { value: "risk_score", label: "Risk Score" },
  { value: "incident_count", label: "Incident Count" },
  { value: "incidents_per_km", label: "Incidents / km" },
];

export function normalizeRoadOverview(payload) {
  return {
    totalSegments: getRoadNumber(payload, "total_segments"),
    totalLengthKm: getRoadLengthKm(payload),
    roadsWithIncidents: getRoadNumber(payload, "roads_with_incidents"),
    roadsWithoutIncidents: getRoadNumber(payload, "roads_without_incidents"),
    roadCoveragePct: getRoadNumber(payload, "road_coverage_pct"),
    uniqueHighwayTypes: getRoadNumber(payload, "unique_highway_types"),
    totalIncidents: getRoadNumber(payload, "total_incidents"),
    averageIncidentsPerKm: getRoadNumber(payload, "avg_incidents_per_km"),
    currentVsPreviousPct: getRoadNumber(payload, "current_vs_previous_pct"),
    topRoad: payload?.top_road ? toRoadRecord(payload.top_road) : null,
    topHighway: payload?.top_highway
      ? normalizeRoadChartHighwayItem(payload.top_highway, 0)
      : null,
    topCrimeType: payload?.top_crime_type
      ? {
          crimeType: getRoadProperty(payload.top_crime_type, "crime_type"),
          count: getRoadNumber(payload.top_crime_type, "count"),
        }
      : null,
    topOutcome: payload?.top_outcome
      ? {
          outcome: getRoadProperty(payload.top_outcome, "outcome"),
          count: getRoadNumber(payload.top_outcome, "count"),
        }
      : null,
    bandBreakdown: normalizeBandBreakdown(payload?.band_breakdown),
    insights: normalizeInsights(payload?.insights),
  };
}

export function normalizeRoadCharts(payload) {
  const timeseries = payload?.timeseries || {};

  return {
    timeseries: {
      groupBy: getRoadProperty(timeseries, "groupBy", "group_by") || "overall",
      series: Array.isArray(timeseries?.series)
        ? timeseries.series.map((seriesItem, index) => ({
            key: getRoadProperty(seriesItem, "key") || `series-${index + 1}`,
            total: getRoadNumber(seriesItem, "total"),
            points: Array.isArray(seriesItem?.points)
              ? seriesItem.points
                  .map((point) => ({
                    month: getRoadProperty(point, "month"),
                    count: getRoadNumber(point, "count"),
                  }))
                  .filter((point) => point.month)
              : [],
          }))
        : [],
      total: getRoadNumber(timeseries, "total"),
      peak: timeseries?.peak
        ? {
            month: getRoadProperty(timeseries.peak, "month"),
            count: getRoadNumber(timeseries.peak, "count"),
          }
        : null,
      currentVsPreviousPct: getRoadNumber(timeseries, "current_vs_previous_pct"),
    },
    byHighway: resolveItems(payload?.by_highway).map((item, index) =>
      normalizeRoadChartHighwayItem(item, index),
    ),
    byCrimeType: resolveItems(payload?.by_crime_type).map((item, index) =>
      normalizeBreakdownItem(item, "crime_type", index),
    ),
    byOutcome: resolveItems(payload?.by_outcome).map((item, index) =>
      normalizeBreakdownItem(item, "outcome", index),
    ),
    bandBreakdown: normalizeBandBreakdown(payload?.band_breakdown),
    insights: normalizeInsights(payload?.insights),
  };
}

export function normalizeRoadRiskResponse(payload) {
  const items = resolveItems(payload?.items).map((item, index) => normalizeRoadRiskItem(item, index));

  return {
    items,
    meta: {
      returned: getRoadNumber(payload?.meta, "returned") || items.length,
      limit: getRoadNumber(payload?.meta, "limit") || ROAD_RISK_FETCH_LIMIT,
      sort: getRoadProperty(payload?.meta, "sort") || "risk_score",
    },
  };
}

export function normalizeRoadRiskItem(item, index) {
  const roadRecord = toRoadRecord(item);

  return {
    ...roadRecord,
    sourceType: "risk",
    selectionKey: createRoadSelectionKey("risk", item, index),
    message: getRoadProperty(item, "message"),
    shareOfIncidents: getRoadNumber(item, "share_of_incidents", "share"),
    previousPeriodChangePct: getRoadNumber(item, "previous_period_change_pct"),
    dominantCrimeType: getRoadProperty(item, "dominant_crime_type", "dominantCrimeType"),
    dominantOutcome: getRoadProperty(item, "dominant_outcome", "dominantOutcome"),
  };
}

export function normalizeRoadChartHighwayItem(item, index) {
  return {
    ...toRoadRecord(item),
    sourceType: "highway",
    selectionKey: createRoadSelectionKey("highway", item, index),
    count: getRoadNumber(item, "count", "incident_count"),
    segmentCount: getRoadNumber(item, "segment_count"),
    share: getRoadNumber(item, "share", "share_of_incidents"),
    message: getRoadProperty(item, "message"),
  };
}

export function normalizeBreakdownItem(item, key, index) {
  return {
    key: `${key}-${getRoadProperty(item, key) || index}`,
    label: getRoadProperty(item, key) || `Item ${index + 1}`,
    count: getRoadNumber(item, "count"),
    share: getRoadNumber(item, "share"),
  };
}

export function normalizeBandBreakdown(breakdown) {
  return {
    red: getRoadNumber(breakdown, "red"),
    orange: getRoadNumber(breakdown, "orange"),
    green: getRoadNumber(breakdown, "green"),
  };
}

export function normalizeBandRows(bandBreakdown) {
  return [
    { label: "Red", count: getRoadNumber(bandBreakdown, "red"), fillClass: "bg-[#ef4444]" },
    { label: "Orange", count: getRoadNumber(bandBreakdown, "orange"), fillClass: "bg-[#f97316]" },
    { label: "Green", count: getRoadNumber(bandBreakdown, "green"), fillClass: "bg-[#22c55e]" },
  ];
}

export function normalizeInsights(items) {
  return Array.isArray(items) ? items.filter(Boolean) : [];
}

export function hasBandBreakdown(bandBreakdown) {
  return Object.values(bandBreakdown || {}).some((value) => Number(value) > 0);
}

export function resolveItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }

  return [];
}

export function toRoadRecord(item) {
  const record = item?.properties || item || {};

  return {
    ...record,
    roadId: getRoadProperty(record, "segment_id", "segmentId", "road_id", "roadId", "id"),
    name: getRoadProperty(record, "name", "road_name", "roadName", "label", "title"),
    highway:
      getRoadProperty(record, "highway", "highway_type", "road_class", "classification") ||
      "Unclassified",
    incidents: getRoadNumber(record, "incident_count", "count", "incidents"),
    incidentsPerKm: getRoadNumber(record, "incidents_per_km", "incidentsPerKm"),
    lengthKm: getRoadLengthKm(record),
    score: getRoadNumber(record, "risk_score", "riskScore", "score"),
    riskBand:
      getRoadProperty(record, "band", "risk_band", "riskBand") ||
      deriveRiskBand(getRoadNumber(record, "incidents_per_km", "incidentsPerKm")),
    dominantCrimeType: getRoadProperty(record, "dominant_crime_type", "dominantCrimeType"),
    dominantOutcome: getRoadProperty(record, "dominant_outcome", "dominantOutcome"),
    message: getRoadProperty(record, "message"),
    shareOfIncidents: getRoadNumber(record, "share_of_incidents", "share"),
    previousPeriodChangePct: getRoadNumber(record, "previous_period_change_pct"),
    count: getRoadNumber(record, "count", "incident_count", "incidents"),
    segmentCount: getRoadNumber(record, "segment_count"),
  };
}

export function mergeRoadSelection(current, next) {
  return {
    ...current,
    ...next,
    selectionKey: next?.selectionKey || current?.selectionKey,
    sourceType: next?.sourceType || current?.sourceType,
  };
}

export function getRoadProperty(source, ...keys) {
  const record = source?.properties || source || {};

  for (const key of keys) {
    const value = record?.[key];

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return null;
}

export function getRoadNumber(source, ...keys) {
  const value = getRoadProperty(source, ...keys);
  const numericValue = Number(value);

  return Number.isFinite(numericValue) ? numericValue : 0;
}

export function getRoadLengthKm(source) {
  const lengthKm = getRoadNumber(source, "length_km", "lengthKm", "total_length_km");

  if (lengthKm > 0) {
    return lengthKm;
  }

  const lengthM = getRoadNumber(source, "length_m", "lengthM", "total_length_m");

  if (lengthM > 0) {
    return lengthM / 1000;
  }

  return 0;
}

export function createRoadSelectionKey(prefix, item, index) {
  return [
    prefix,
    getRoadProperty(item, "segment_id", "segmentId", "road_id", "roadId", "id", "highway"),
    getRoadProperty(item, "name", "road_name", "roadName", "label"),
    index,
  ]
    .filter(Boolean)
    .join("-");
}

export function cloneRoadFilters(filters) {
  return {
    ...filters,
    bbox: filters?.bbox ? { ...filters.bbox } : null,
  };
}

export function createDefaultFiltersFromMeta(months) {
  if (!months?.min || !months?.max) {
    return {
      ...DEFAULT_ROAD_FILTERS,
      bbox: { ...WEST_YORKSHIRE_BBOX },
    };
  }

  const maxIndex = monthToIndex(months.max);
  const minIndex = monthToIndex(months.min);
  const fromIndex = Math.max(minIndex, maxIndex - 2);

  return {
    ...DEFAULT_ROAD_FILTERS,
    monthFrom: indexToMonth(fromIndex),
    monthTo: months.max,
    bbox: { ...WEST_YORKSHIRE_BBOX },
  };
}

export function areRoadFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.highway === right?.highway &&
    areBboxesEqual(left?.bbox, right?.bbox)
  );
}

export function areBboxesEqual(left, right) {
  if (!left && !right) {
    return true;
  }

  if (!left || !right) {
    return false;
  }

  return (
    left.minLon === right.minLon &&
    left.minLat === right.minLat &&
    left.maxLon === right.maxLon &&
    left.maxLat === right.maxLat
  );
}

export function monthToIndex(month) {
  const [year, value] = month.split("-").map(Number);
  return year * 12 + (value - 1);
}

export function indexToMonth(index) {
  const year = Math.floor(index / 12);
  const month = (index % 12) + 1;
  return `${year}-${String(month).padStart(2, "0")}`;
}

export function deriveRiskBand(incidentsPerKm) {
  const value = Number(incidentsPerKm);

  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }

  if (value >= 8) {
    return "red";
  }

  if (value >= 4) {
    return "orange";
  }

  return "green";
}

export function createEmptyRoadChartsData() {
  return {
    timeseries: {
      groupBy: "overall",
      series: [],
      total: 0,
      peak: null,
      currentVsPreviousPct: null,
    },
    byHighway: [],
    byCrimeType: [],
    byOutcome: [],
    bandBreakdown: { red: 0, orange: 0, green: 0 },
    insights: [],
  };
}

function toMonthValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}
