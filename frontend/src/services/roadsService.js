import { API_BASE_URL } from "../config/env";
import { fetchJson, toFeatureCollection, toQueryString } from "./serviceUtils";

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
  target,
  baselineMonths,
  bbox,
  crimeTypes,
  lastOutcomeCategories,
  lsoaNames,
  limit,
  sort,
}) {
  return toQueryString({
    from,
    to,
    target,
    baselineMonths,
    ...toBboxQuery(bbox),
    crimeType: crimeTypes,
    lastOutcomeCategory: lastOutcomeCategories,
    lsoaName: lsoaNames,
    limit,
    sort,
  });
}

export const roadsService = {
  getVectorTilesUrl({ startMonth, endMonth } = {}) {
    const params = new URLSearchParams({ includeRisk: "true" });
    if (startMonth) params.set("startMonth", startMonth);
    if (endMonth) params.set("endMonth", endMonth);
    return `${API_BASE_URL}/tiles/roads/{z}/{x}/{y}.mvt?${params}`;
  },

  async getRoadsInBoundingBox(
    { minLon, minLat, maxLon, maxLat, limit = 1200 },
    requestOptions = {},
  ) {
    const query = toQueryString({ minLon, minLat, maxLon, maxLat, limit });
    const data = await fetchJson(
      `${API_BASE_URL}/roads?${query}`,
      "Failed to fetch roads in viewport",
      requestOptions,
    );

    return toFeatureCollection(data);
  },

  async getNearestRoad({ lon, lat }, requestOptions = {}) {
    const query = toQueryString({ lon, lat });
    return fetchJson(`${API_BASE_URL}/roads/nearest?${query}`, "Failed to fetch nearest road", requestOptions);
  },

  async getRoadStats({ minLon, minLat, maxLon, maxLat }, requestOptions = {}) {
    const query = toQueryString({ minLon, minLat, maxLon, maxLat });
    return fetchJson(`${API_BASE_URL}/roads/stats?${query}`, "Failed to fetch road stats", requestOptions);
  },

  async getRoadById(id, requestOptions = {}) {
    return fetchJson(`${API_BASE_URL}/roads/${id}`, "Failed to fetch road details", requestOptions);
  },

  async getRoadByIdGeoJson(id, requestOptions = {}) {
    return fetchJson(
      `${API_BASE_URL}/roads/${id}/geojson`,
      "Failed to fetch road GeoJSON",
      requestOptions,
    );
  },

  async getRoadAnalyticsMeta(requestOptions = {}) {
    return fetchJson(
      `${API_BASE_URL}/roads/analytics/meta`,
      "Failed to fetch road analytics metadata",
      requestOptions,
    );
  },

  async getRoadAnalyticsSummary(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/summary?${query}`,
      "Failed to fetch road summary analytics",
      requestOptions,
    );
  },

  async getRoadAnalyticsTimeseries(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/timeseries?${query}`,
      "Failed to fetch road time series analytics",
      requestOptions,
    );
  },

  async getRoadAnalyticsHighways(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames, limit = 10 },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
      limit,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/highways?${query}`,
      "Failed to fetch road highway analytics",
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
      lsoaNames,
      sort = "incidents_per_km",
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
      lsoaNames,
      sort,
      limit,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/risk?${query}`,
      "Failed to fetch road risk analytics",
      requestOptions,
    );
  },

  async getRoadAnalyticsAnomaly(
    { target, baselineMonths = 6, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildRoadAnalyticsQuery({
      target,
      baselineMonths,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${API_BASE_URL}/roads/analytics/anomaly?${query}`,
      "Failed to fetch road anomaly analytics",
      requestOptions,
    );
  },
};
