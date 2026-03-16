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
    startMonth: rawPreference.start_month ?? rawPreference.startMonth ?? "",
    endMonth: rawPreference.end_month ?? rawPreference.endMonth ?? "",
    crimeTypes: Array.isArray(rawPreference.crime_types)
      ? rawPreference.crime_types
      : Array.isArray(rawPreference.crimeTypes)
        ? rawPreference.crimeTypes
        : [],
    travelMode: rawPreference.travel_mode || rawPreference.travelMode || "",
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
    payload?.results,
    Array.isArray(payload) ? payload : null,
  ];

  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    return candidate.map(normalizeWatchlist).filter(Boolean);
  }

  const singleItem = payload?.watchlist || payload?.item;
  if (singleItem) {
    const normalized = normalizeWatchlist(singleItem);
    return normalized ? [normalized] : [];
  }

  const normalized = normalizeWatchlist(payload);
  return normalized ? [normalized] : [];
}

function extractWatchlist(payload) {
  const rawWatchlist = payload?.watchlist || payload?.item || payload?.data || payload;
  return normalizeWatchlist(rawWatchlist);
}

function extractRiskScoreResponse(payload) {
  return payload?.result || payload?.data || payload;
}

function extractBasicMetricsResponse(payload) {
  return payload?.result || payload?.data || payload;
}

function normalizeMapFeatureCollection(rawCollection) {
  const rawFeatures = Array.isArray(rawCollection?.features) ? rawCollection.features : [];
  return {
    type: "FeatureCollection",
    features: rawFeatures.filter((feature) => feature && feature.type === "Feature"),
  };
}

function extractMapEventsResponse(payload) {
  const raw = payload?.result || payload?.data || payload || {};
  return {
    crimes: normalizeMapFeatureCollection(raw?.crimes),
    collisions: normalizeMapFeatureCollection(raw?.collisions),
    user_reported_events: normalizeMapFeatureCollection(
      raw?.user_reported_events ?? raw?.userReportedEvents,
    ),
  };
}

function normalizeRiskRun(rawRun) {
  if (!rawRun || typeof rawRun !== "object") {
    return null;
  }

  const inferredScore = Number(
    rawRun?.risk_result?.risk_score ??
    rawRun?.riskResult?.riskScore ??
    rawRun?.risk_result?.score ??
    rawRun.risk_score ??
    rawRun.riskScore ??
    rawRun.score ??
    rawRun?.risk?.risk_score ??
    rawRun?.risk?.riskScore ??
    rawRun?.risk?.score,
  );

  return {
    id: Number(rawRun.id ?? rawRun.run_id ?? rawRun.watchlist_run_id ?? rawRun.watchlistRunId) || null,
    createdAt:
      rawRun.created_at ??
      rawRun.createdAt ??
      rawRun.stored_at ??
      rawRun.storedAt ??
      rawRun.generated_at ??
      rawRun.generatedAt ??
      "",
    score: Number.isFinite(inferredScore) ? inferredScore : null,
    data: rawRun,
  };
}

function extractRiskRuns(payload) {
  const candidates = [
    payload?.items,
    payload?.runs,
    payload?.data,
    payload?.results,
    Array.isArray(payload) ? payload : null,
  ];

  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    return candidate.map(normalizeRiskRun).filter(Boolean);
  }

  return [];
}

export const watchlistService = {
  async createWatchlist(payload, accessToken, requestOptions = {}) {
    console.log(payload);
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
      `/watchlists${query ? `?${query}` : ""}`,
      "Failed to fetch the selected watchlist.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return extractWatchlist(response);
  },

  async updateWatchlist(watchlistId, payload, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}`,
      "Failed to update the selected watchlist.",
      {
        ...requestOptions,
        method: "PATCH",
        accessToken,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      },
    );

    return extractWatchlist(response);
  },

  async deleteWatchlist(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}`,
      "Failed to delete the selected watchlist.",
      {
        ...requestOptions,
        method: "DELETE",
        accessToken,
      },
    );

    return {
      ok: true,
      watchlistId: Number(
        response?.watchlist_id ?? response?.watchlistId ?? response?.id ?? watchlistId,
      ) || Number(watchlistId),
      message: response?.message || response?.detail || "Watchlist deleted.",
    };
  },

  async computeWatchlistRiskScore(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/analytics/risk-score`,
      "Failed to compute watchlist risk score.",
      {
        ...requestOptions,
        method: "POST",
        accessToken,
      },
    );

    return extractRiskScoreResponse(response);
  },

  async getWatchlistBasicMetrics(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/analytics/basic-metrics`,
      "Failed to load watchlist basic metrics.",
      {
        ...requestOptions,
        method: "GET",
        accessToken,
      },
    );

    return extractBasicMetricsResponse(response);
  },

  async getWatchlistMapEvents(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/analytics/map-events`,
      "Failed to load watchlist map events.",
      {
        ...requestOptions,
        method: "GET",
        accessToken,
      },
    );

    return extractMapEventsResponse(response);
  },

  async forecastWatchlistNextMonth(watchlistId, payload, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/analytics/forecast`,
      "Failed to compute watchlist forecast.",
      {
        ...requestOptions,
        method: "POST",
        accessToken,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      },
    );

    return response?.forecast ? response : response?.data || response;
  },

  async getWatchlistRiskScoreRuns(
    watchlistId,
    accessToken,
    { limit = 50 } = {},
    requestOptions = {},
  ) {
    const query = toQueryString({ limit });
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/analytics/risk-score/runs${query ? `?${query}` : ""}`,
      "Failed to load watchlist risk-score runs.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return extractRiskRuns(response);
  },
};
