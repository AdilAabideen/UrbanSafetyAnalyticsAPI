import { API_BASE_URL } from "../config/env";

function toQueryString(params) {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return;
    }
    searchParams.append(key, String(value));
  });

  return searchParams.toString();
}

async function parseJsonOrThrow(response, fallbackMessage) {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || fallbackMessage);
  }

  return response.json();
}

function toFeatureCollection(payload) {
  if (!payload) {
    return { type: "FeatureCollection", features: [] };
  }

  if (payload.type === "FeatureCollection" && Array.isArray(payload.features)) {
    return payload;
  }

  if (Array.isArray(payload)) {
    return { type: "FeatureCollection", features: payload };
  }

  if (Array.isArray(payload.features)) {
    return { type: "FeatureCollection", features: payload.features };
  }

  return { type: "FeatureCollection", features: [] };
}

async function fetchJson(url, fallbackMessage, requestOptions = {}) {
  try {
    const response = await fetch(url, { signal: requestOptions.signal });
    return parseJsonOrThrow(response, fallbackMessage);
  } catch (error) {
    if (error?.name === "AbortError") {
      throw error;
    }

    if (error instanceof TypeError) {
      throw new Error(`Cannot reach API at ${API_BASE_URL}. Check VITE_API_BASE_URL and backend status.`);
    }

    throw error;
  }
}

export const roadsService = {
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
};
