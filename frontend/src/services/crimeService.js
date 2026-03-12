import { config } from "../config/env";
import { fetchJson, toFeatureCollection, toQueryString } from "./serviceUtils";

function clampZoom(zoom) {
  return Math.max(0, Math.min(22, Math.round(zoom)));
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
  async getCrimesForViewport(
    {
      minLon,
      minLat,
      maxLon,
      maxLat,
      zoom,
      month,
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
      month,
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
};
