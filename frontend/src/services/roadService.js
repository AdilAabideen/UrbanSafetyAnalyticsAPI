import { API_BASE_URL } from "../config/env";
import { fetchJson, toFeatureCollection, toQueryString } from "./serviceUtils";

export const roadsService = {
  getVectorTilesUrl({ startMonth, endMonth } = {}) {
    const params = new URLSearchParams({ includeRisk: "true"});
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
};
