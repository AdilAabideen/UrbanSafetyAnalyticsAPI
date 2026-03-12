import { config } from "../config/env";
import { fetchJson, toFeatureCollection, toQueryString } from "./serviceUtils";

function clampZoom(zoom) {
  return Math.max(0, Math.min(22, Math.round(zoom)));
}

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

function buildSharedCrimeQuery({
  from,
  to,
  bbox,
  crimeTypes,
  lastOutcomeCategories,
  lsoaNames,
  limit,
  cursor,
  target,
  baselineMonths,
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
    cursor,
  });
}

function resolveFeatureCollection(payload) {
  const candidates = [
    payload,
    payload?.data,
    payload?.geojson,
    payload?.featureCollection,
    payload?.collection,
    payload?.results,
  ];

  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }

    if (candidate.type === "FeatureCollection" && Array.isArray(candidate.features)) {
      return candidate;
    }

    if (Array.isArray(candidate.features)) {
      return {
        type: "FeatureCollection",
        features: candidate.features,
      };
    }
  }

  return toFeatureCollection(payload);
}

function resolveMode(payload, featureCollection) {
  const explicitMode =
    payload?.mode ||
    payload?.resolved_mode ||
    payload?.meta?.mode ||
    payload?.metadata?.mode;

  if (explicitMode) {
    return explicitMode;
  }

  const hasClusterProperties = featureCollection.features.some((feature) => {
    const properties = feature?.properties || {};
    return (
      Number.isFinite(properties.count) ||
      Number.isFinite(properties.point_count) ||
      Number.isFinite(properties.cluster_count)
    );
  });

  return hasClusterProperties ? "clusters" : "points";
}

function resolveCursor(payload) {
  return (
    payload?.next_cursor ||
    payload?.nextCursor ||
    payload?.cursor ||
    payload?.pagination?.next_cursor ||
    null
  );
}

export const crimeService = {
  async getAnalyticsMeta(requestOptions = {}) {
    return fetchJson(
      `${config.apiBaseUrl}/analytics/meta`,
      "Failed to fetch crime analytics metadata",
      requestOptions,
    );
  },

  async getCrimeIncidents(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames, limit, cursor },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
      limit,
      cursor,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/incidents?${query}`,
      "Failed to fetch crime incidents",
      requestOptions,
    );
  },

  async getCrimeAnalyticsSummary(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/analytics/summary?${query}`,
      "Failed to fetch crime summary analytics",
      requestOptions,
    );
  },

  async getCrimeAnalyticsTimeseries(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/analytics/timeseries?${query}`,
      "Failed to fetch crime time series analytics",
      requestOptions,
    );
  },

  async getCrimeAnalyticsTypes(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames, limit = 10 },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
      limit,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/analytics/types?${query}`,
      "Failed to fetch crime type analytics",
      requestOptions,
    );
  },

  async getCrimeAnalyticsOutcomes(
    { from, to, bbox, crimeTypes, lastOutcomeCategories, lsoaNames, limit = 10 },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      from,
      to,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
      limit,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/analytics/outcomes?${query}`,
      "Failed to fetch crime outcome analytics",
      requestOptions,
    );
  },

  async getCrimeAnalyticsAnomaly(
    { target, baselineMonths = 6, bbox, crimeTypes, lastOutcomeCategories, lsoaNames },
    requestOptions = {},
  ) {
    const query = buildSharedCrimeQuery({
      target,
      baselineMonths,
      bbox,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
    });

    return fetchJson(
      `${config.apiBaseUrl}/crimes/analytics/anomaly?${query}`,
      "Failed to fetch crime anomaly analytics",
      requestOptions,
    );
  },

  async getCrimesForViewport(
    {
      minLon,
      minLat,
      maxLon,
      maxLat,
      zoom,
      startMonth,
      endMonth,
      crimeTypes,
      lastOutcomeCategories,
      lsoaNames,
      limit,
      mode = "auto",
      cursor,
    },
    requestOptions = {},
  ) {
    const query = toQueryString({
      minLon,
      minLat,
      maxLon,
      maxLat,
      zoom: clampZoom(zoom),
      startMonth,
      endMonth,
      crimeType: crimeTypes,
      lastOutcomeCategory: lastOutcomeCategories,
      lsoaName: lsoaNames,
      limit,
      mode,
      cursor,
    });

    const payload = await fetchJson(
      `${config.crimeApiUrl}?${query}`,
      "Failed to fetch crimes for the current viewport",
      requestOptions,
    );

    const data = resolveFeatureCollection(payload);

    return {
      data,
      mode: resolveMode(payload, data),
      nextCursor: resolveCursor(payload),
      featureCount: data.features.length,
      sourceLabel: "crime API",
    };
  },

  async getCrimeById(crimeId, requestOptions = {}) {
    return fetchJson(
      `${config.apiBaseUrl}/crimes/${crimeId}`,
      "Failed to fetch the selected crime",
      requestOptions,
    );
  },
};
