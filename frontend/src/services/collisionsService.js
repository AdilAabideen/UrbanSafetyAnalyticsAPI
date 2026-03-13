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

function buildSharedCollisionQuery({
  from,
  to,
  bbox,
  collisionSeverities,
  roadTypes,
  lsoaCodes,
  weatherConditions,
  lightConditions,
  roadSurfaceConditions,
  limit,
  cursor,
}) {
  return toQueryString({
    from,
    to,
    ...toBboxQuery(bbox),
    collisionSeverity: collisionSeverities,
    roadType: roadTypes,
    lsoaCode: lsoaCodes,
    weatherCondition: weatherConditions,
    lightCondition: lightConditions,
    roadSurfaceCondition: roadSurfaceConditions,
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

export const collisionsService = {
  async getCollisionIncidents(
    {
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
      limit,
      cursor,
    },
    requestOptions = {},
  ) {
    const query = buildSharedCollisionQuery({
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
      limit,
      cursor,
    });

    return fetchJson(
      `${config.apiBaseUrl}/collisions/incidents?${query}`,
      "Failed to fetch collision incidents",
      requestOptions,
    );
  },

  async getCollisionAnalyticsSummary(
    {
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
    },
    requestOptions = {},
  ) {
    const query = buildSharedCollisionQuery({
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
    });

    return fetchJson(
      `${config.apiBaseUrl}/collisions/analytics/summary?${query}`,
      "Failed to fetch collision summary analytics",
      requestOptions,
    );
  },

  async getCollisionAnalyticsTimeseries(
    {
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
    },
    requestOptions = {},
  ) {
    const query = buildSharedCollisionQuery({
      from,
      to,
      bbox,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
    });

    return fetchJson(
      `${config.apiBaseUrl}/collisions/analytics/timeseries?${query}`,
      "Failed to fetch collision time series analytics",
      requestOptions,
    );
  },

  async getCollisionsForViewport(
    {
      minLon,
      minLat,
      maxLon,
      maxLat,
      zoom,
      month,
      startMonth,
      endMonth,
      collisionSeverities,
      roadTypes,
      lsoaCodes,
      weatherConditions,
      lightConditions,
      roadSurfaceConditions,
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
      month,
      startMonth,
      endMonth,
      collisionSeverity: collisionSeverities,
      roadType: roadTypes,
      lsoaCode: lsoaCodes,
      weatherCondition: weatherConditions,
      lightCondition: lightConditions,
      roadSurfaceCondition: roadSurfaceConditions,
      limit,
      mode,
      cursor,
    });

    const payload = await fetchJson(
      `${config.apiBaseUrl}/collisions/map?${query}`,
      "Failed to fetch collisions for the current viewport",
      requestOptions,
    );

    const data = resolveFeatureCollection(payload);

    return {
      data,
      mode: resolveMode(payload, data),
      nextCursor: resolveCursor(payload),
      featureCount: data.features.length,
      sourceLabel: "collisions API",
    };
  },
};
