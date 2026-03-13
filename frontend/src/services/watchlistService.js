import { API_BASE_URL } from "../config/env";
import { parseJsonOrThrow, toQueryString } from "./serviceUtils";

function buildHeaders(accessToken, headers = {}) {
  const nextHeaders = new Headers(headers);

  if (accessToken) {
    nextHeaders.set("Authorization", `Bearer ${accessToken}`);
  }

  return nextHeaders;
}

async function requestWatchlist(path, fallbackMessage, { accessToken, headers, ...requestOptions } = {}) {
  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...requestOptions,
      headers: buildHeaders(accessToken, headers),
    });

    return parseJsonOrThrow(response, fallbackMessage);
  } catch (error) {
    if (error?.name === "AbortError") {
      throw error;
    }

    if (error instanceof TypeError) {
      throw new Error(`Cannot reach ${API_BASE_URL}. Check the configured endpoint and server status.`);
    }

    throw error;
  }
}

function normalizePreference(rawPreference) {
  if (!rawPreference || typeof rawPreference !== "object") {
    return null;
  }

  return {
    id: Number(rawPreference.id) || null,
    watchlistId: Number(rawPreference.watchlist_id ?? rawPreference.watchlistId) || null,
    windowMonths: Number(rawPreference.window_months ?? rawPreference.windowMonths) || 0,
    crimeTypes: Array.isArray(rawPreference.crime_types)
      ? rawPreference.crime_types
      : Array.isArray(rawPreference.crimeTypes)
        ? rawPreference.crimeTypes
        : [],
    travelMode:
      rawPreference.travel_mode ||
      rawPreference.travelMode ||
      "",
    createdAt: rawPreference.created_at || rawPreference.createdAt || "",
  };
}

function normalizeWatchlist(rawWatchlist) {
  if (!rawWatchlist || typeof rawWatchlist !== "object") {
    return null;
  }

  return {
    id: Number(rawWatchlist.id) || null,
    userId: Number(rawWatchlist.user_id ?? rawWatchlist.userId) || null,
    name: rawWatchlist.name || "Untitled watchlist",
    minLon: Number(rawWatchlist.min_lon ?? rawWatchlist.minLon) || 0,
    minLat: Number(rawWatchlist.min_lat ?? rawWatchlist.minLat) || 0,
    maxLon: Number(rawWatchlist.max_lon ?? rawWatchlist.maxLon) || 0,
    maxLat: Number(rawWatchlist.max_lat ?? rawWatchlist.maxLat) || 0,
    createdAt: rawWatchlist.created_at || rawWatchlist.createdAt || "",
    preference: normalizePreference(rawWatchlist.preference),
  };
}

function extractWatchlists(payload) {
  const candidates = [
    payload?.watchlists,
    payload?.items,
    payload?.data,
    Array.isArray(payload) ? payload : null,
  ];

  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    return candidate.map(normalizeWatchlist).filter(Boolean);
  }

  if (payload?.watchlist) {
    const normalizedWatchlist = normalizeWatchlist(payload.watchlist);
    return normalizedWatchlist ? [normalizedWatchlist] : [];
  }

  return [];
}

function extractWatchlist(payload) {
  if (payload?.watchlist) {
    return normalizeWatchlist(payload.watchlist);
  }

  return normalizeWatchlist(payload);
}

export const watchlistService = {
  async createWatchlist(payload, accessToken, requestOptions = {}) {
    const response = await requestWatchlist("/watchlists", "Failed to create watchlist.", {
      ...requestOptions,
      method: "POST",
      accessToken,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    return extractWatchlist(response);
  },

  async getWatchlists(accessToken, requestOptions = {}) {
    const response = await requestWatchlist("/watchlists", "Failed to fetch watchlists.", {
      ...requestOptions,
      accessToken,
    });

    return extractWatchlists(response);
  },

  async getWatchlistById(watchlistId, accessToken, requestOptions = {}) {
    const query = toQueryString({ watchlist_id: watchlistId });
    const response = await requestWatchlist(
      `/watchlists?${query}`,
      "Failed to fetch the selected watchlist.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return extractWatchlist(response);
  },
};
