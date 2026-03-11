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

export const roadsService = {
  async getRoadsInBoundingBox(
    { minLon, minLat, maxLon, maxLat, limit = 2000 },
    requestOptions = {},
  ) {
    const query = toQueryString({ minLon, minLat, maxLon, maxLat, limit });
    const response = await fetch(`${API_BASE_URL}/roads?${query}`, {
      signal: requestOptions.signal,
    });
    return parseJsonOrThrow(response, "Failed to fetch roads in viewport");
  },

  async getNearestRoad({ lon, lat }, requestOptions = {}) {
    const query = toQueryString({ lon, lat });
    const response = await fetch(`${API_BASE_URL}/roads/nearest?${query}`, {
      signal: requestOptions.signal,
    });
    return parseJsonOrThrow(response, "Failed to fetch nearest road");
  },

  async getRoadStats({ minLon, minLat, maxLon, maxLat }, requestOptions = {}) {
    const query = toQueryString({ minLon, minLat, maxLon, maxLat });
    const response = await fetch(`${API_BASE_URL}/roads/stats?${query}`, {
      signal: requestOptions.signal,
    });
    return parseJsonOrThrow(response, "Failed to fetch road stats");
  },

  async getRoadById(id, requestOptions = {}) {
    const response = await fetch(`${API_BASE_URL}/roads/${id}`, {
      signal: requestOptions.signal,
    });
    return parseJsonOrThrow(response, "Failed to fetch road details");
  },
};
