import { API_BASE_URL } from "../config/env";
import { fetchJson, toQueryString } from "./serviceUtils";

function toBboxQuery(bbox) {
  if (!bbox) {
    return {};
  }

  return {
    minLon: bbox.minLon,
    minLat: bbox.minLat,
    maxLon: bbox.maxLon,
    maxLat: bbox.maxLat,
  };
}

function buildRoadAnalyticsQuery({
  from,
  to,
  bbox,
  crimeTypes,
  lastOutcomeCategories,
  highways,
  limit,
  sort,
  timeseriesGroupBy,
  groupLimit,
}) {
  return toQueryString({
    from,
    to,
    ...toBboxQuery(bbox),
    crimeType: crimeTypes,
    lastOutcomeCategory: lastOutcomeCategories,
    highway: highways,
    limit,
    sort,
    timeseriesGroupBy,
    groupLimit,
  });
}

export const roadsService = {
  getVectorTilesUrl({
    includeRisk = true,
    month,
    startMonth,
    endMonth,
    crimeTypes,
  } = {}) {
    const params = new URLSearchParams();

    params.set("includeRisk", includeRisk ? "true" : "false");

    if (month) {
      params.set("month", month);
    }

    if (startMonth) {
      params.set("startMonth", startMonth);
    }

    if (endMonth) {
      params.set("endMonth", endMonth);
    }

    if (Array.isArray(crimeTypes)) {
      crimeTypes.filter(Boolean).forEach((crimeType) => params.append("crimeType", crimeType));
    }

    return `${API_BASE_URL}/tiles/roads/{z}/{x}/{y}.mvt?${params.toString()}`;
  },

  async getRoadAnalyticsMeta(requestOptions = {}) {
    return fetchJson(
      `${API_BASE_URL}/roads/analytics/meta`,
      "Failed to fetch road analytics metadata",
      requestOptions,
    );
  },

  async getRoadAnalyticsOverview(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, highways },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      highways,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/overview?${query}`,
      "Failed to fetch road overview analytics",
      requestOptions,
    );
  },

  async getRoadAnalyticsCharts(
    {
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      highways,
      timeseriesGroupBy = "overall",
      groupLimit = 5,
      limit = 8,
    },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      highways,
      timeseriesGroupBy,
      groupLimit,
      limit,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/charts?${query}`,
      "Failed to fetch road chart analytics",
      requestOptions,
    );
  },

  async getRoadAnalyticsRisk(
    {
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      highways,
      sort = "risk_score",
      limit = 25,
    },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      highways,
      sort,
      limit,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/risk?${query}`,
      "Failed to fetch road risk analytics",
      requestOptions,
    );
  },
};
