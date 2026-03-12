const apiBaseUrl = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

export const config = {
  apiBaseUrl,
  mapboxAccessToken: import.meta.env.VITE_MAPBOX_ACCESS_TOKEN || "",
  crimeApiUrl: import.meta.env.VITE_CRIME_API_URL || `${apiBaseUrl}/crimes/map`,
};

export const API_BASE_URL = config.apiBaseUrl;
