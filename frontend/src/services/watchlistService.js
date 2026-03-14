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
    includeCollisions: Boolean(
      rawPreference.include_collisions ?? rawPreference.includeCollisions,
    ),
    baselineMonths: Number(rawPreference.baseline_months ?? rawPreference.baselineMonths) || 0,
    hotspotK: Number(rawPreference.hotspot_k ?? rawPreference.hotspotK) || 0,
    includeHotspotStability: Boolean(
      rawPreference.include_hotspot_stability ?? rawPreference.includeHotspotStability,
    ),
    includeForecast: Boolean(
      rawPreference.include_forecast ?? rawPreference.includeForecast,
    ),
    weightCrime: Number(rawPreference.weight_crime ?? rawPreference.weightCrime) || 0,
    weightCollision: Number(rawPreference.weight_collision ?? rawPreference.weightCollision) || 0,
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

function normalizeReportListItem(rawReport) {
  if (!rawReport || typeof rawReport !== "object") {
    return null;
  }

  return {
    id: Number(rawReport.id) || null,
    watchlistId: Number(rawReport.watchlist_id ?? rawReport.watchlistId) || null,
    from: rawReport.from || "",
    to: rawReport.to || "",
    forecastTarget: rawReport.forecast_target ?? rawReport.forecastTarget ?? "",
    crimeType: rawReport.crime_type ?? rawReport.crimeType ?? "",
    mode: rawReport.mode || "",
    includeCollisions: Boolean(rawReport.include_collisions ?? rawReport.includeCollisions),
    createdAt: rawReport.created_at ?? rawReport.createdAt ?? "",
  };
}

function normalizeReport(payload) {
  const rawReport = payload?.report || payload;

  if (!rawReport || typeof rawReport !== "object") {
    return null;
  }

  return {
    id: Number(rawReport.id) || null,
    snapshotId: Number(rawReport.snapshot_id ?? rawReport.snapshotId) || null,
    storedAt: rawReport.stored_at ?? rawReport.storedAt ?? "",
    generatedAt: rawReport.generated_at ?? rawReport.generatedAt ?? "",
    watchlist: rawReport.watchlist
      ? {
          id: Number(rawReport.watchlist.id) || null,
          name: rawReport.watchlist.name || "Watchlist",
          bbox: {
            minLon: Number(rawReport.watchlist?.bbox?.minLon) || 0,
            minLat: Number(rawReport.watchlist?.bbox?.minLat) || 0,
            maxLon: Number(rawReport.watchlist?.bbox?.maxLon) || 0,
            maxLat: Number(rawReport.watchlist?.bbox?.maxLat) || 0,
          },
        }
      : null,
    scope: rawReport.scope || null,
    preferencesUsed: rawReport.preferences_used ?? rawReport.preferencesUsed ?? null,
    summary: rawReport.summary || null,
    risk: rawReport.risk || null,
    forecast: rawReport.forecast || null,
    hotspots: rawReport.hotspots || null,
    topRiskyRoads: Array.isArray(rawReport.top_risky_roads)
      ? rawReport.top_risky_roads
      : Array.isArray(rawReport.topRiskyRoads)
        ? rawReport.topRiskyRoads
        : [],
  };
}

function normalizeAnalyticsRun(rawRun) {
  if (!rawRun || typeof rawRun !== "object") {
    return null;
  }

  return {
    id: Number(rawRun.id ?? rawRun.watchlist_run_id) || null,
    watchlistId: Number(rawRun.watchlist_id ?? rawRun.watchlistId) || null,
    reportType: rawRun.report_type ?? rawRun.reportType ?? "",
    request: rawRun.request || null,
    result: rawRun.result || null,
    createdAt: rawRun.created_at ?? rawRun.createdAt ?? rawRun.stored_at ?? rawRun.storedAt ?? "",
    storedAt: rawRun.stored_at ?? rawRun.storedAt ?? "",
  };
}

function normalizeRunResponse(payload) {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  return {
    watchlistId: Number(payload.watchlist_id ?? payload.watchlistId) || null,
    reportType: payload.report_type ?? payload.reportType ?? "",
    watchlistRunId: Number(payload.watchlist_run_id ?? payload.watchlistRunId) || null,
    storedAt: payload.stored_at ?? payload.storedAt ?? "",
    request: payload.request || null,
    result: payload.result || null,
  };
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

  async getWatchlistReports(watchlistId, accessToken, { limit = 20 } = {}, requestOptions = {}) {
    const query = toQueryString({ limit });
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/reports${query ? `?${query}` : ""}`,
      "Failed to fetch saved watchlist reports.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return Array.isArray(response?.items)
      ? response.items.map(normalizeReportListItem).filter(Boolean)
      : [];
  },

  async getWatchlistReportById(watchlistId, reportId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/reports/${reportId}`,
      "Failed to fetch the selected saved report.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return normalizeReport(response);
  },

  async generateWatchlistReport(watchlistId, payload, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/report`,
      "Failed to generate the watchlist report.",
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

    return normalizeReport(response);
  },

  async runRiskScore(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/risk-score/run`,
      "Failed to run risk score analytics.",
      {
        ...requestOptions,
        method: "POST",
        accessToken,
      },
    );

    console.log(response)

    return normalizeRunResponse(response);
  },

  async runRiskForecast(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/risk-forecast/run`,
      "Failed to run risk forecast analytics.",
      {
        ...requestOptions,
        method: "POST",
        accessToken,
      },
    );

    console.log(response)

    return normalizeRunResponse(response);
  },

  async runHotspotStability(watchlistId, accessToken, requestOptions = {}) {
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/hotspot-stability/run`,
      "Failed to run hotspot stability analytics.",
      {
        ...requestOptions,
        method: "POST",
        accessToken,
      },
    );

    return normalizeRunResponse(response);
  },

  async getRiskScoreResults(watchlistId, accessToken, { runId, limit = 20 } = {}, requestOptions = {}) {
    const query = toQueryString({ run_id: runId, limit });
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/risk-score/results${query ? `?${query}` : ""}`,
      "Failed to load stored risk score results.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    console.log(response)

    return Array.isArray(response?.items)
      ? response.items.map(normalizeAnalyticsRun).filter(Boolean)
      : [];
  },

  async getRiskForecastResults(watchlistId, accessToken, { runId, limit = 20 } = {}, requestOptions = {}) {
    const query = toQueryString({ run_id: runId, limit });
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/risk-forecast/results${query ? `?${query}` : ""}`,
      "Failed to load stored risk forecast results.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return Array.isArray(response?.items)
      ? response.items.map(normalizeAnalyticsRun).filter(Boolean)
      : [];
  },

  async getHotspotStabilityResults(watchlistId, accessToken, { runId, limit = 20 } = {}, requestOptions = {}) {
    const query = toQueryString({ run_id: runId, limit });
    const response = await requestWatchlist(
      `/watchlists/${watchlistId}/hotspot-stability/results${query ? `?${query}` : ""}`,
      "Failed to load stored hotspot stability results.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return Array.isArray(response?.items)
      ? response.items.map(normalizeAnalyticsRun).filter(Boolean)
      : [];
  },
};
