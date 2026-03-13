import { API_BASE_URL } from "../config/env";
import { parseJsonOrThrow, toQueryString } from "./serviceUtils";

function buildHeaders(accessToken, headers = {}) {
  const nextHeaders = new Headers(headers);

  if (accessToken) {
    nextHeaders.set("Authorization", `Bearer ${accessToken}`);
  }

  return nextHeaders;
}

async function requestReportedEvent(path, fallbackMessage, { accessToken, headers, ...requestOptions } = {}) {
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

function normalizeReportedDetails(eventKind, details) {
  if (!details || typeof details !== "object") {
    return null;
  }

  if (eventKind === "crime") {
    return {
      crimeType: details.crime_type || details.crimeType || "",
    };
  }

  return {
    weatherCondition:
      details.weather_condition ||
      details.weatherCondition ||
      "",
    lightCondition:
      details.light_condition ||
      details.lightCondition ||
      "",
    numberOfVehicles: Number(details.number_of_vehicles ?? details.numberOfVehicles) || 0,
  };
}

function normalizeReportedEvent(rawReport) {
  if (!rawReport || typeof rawReport !== "object") {
    return null;
  }

  const eventKind = rawReport.event_kind || rawReport.eventKind || "crime";

  return {
    id: Number(rawReport.id) || null,
    eventKind,
    reporterType: rawReport.reporter_type || rawReport.reporterType || "anonymous",
    month: rawReport.month || "",
    eventDate: rawReport.event_date || rawReport.eventDate || "",
    eventTime: rawReport.event_time || rawReport.eventTime || "",
    longitude: Number(rawReport.longitude) || 0,
    latitude: Number(rawReport.latitude) || 0,
    userId: Number(rawReport.user_id ?? rawReport.userId) || null,
    reporterEmail: rawReport.reporter_email || rawReport.reporterEmail || "",
    segmentId: Number(rawReport.segment_id ?? rawReport.segmentId) || null,
    snapDistanceM: Number(rawReport.snap_distance_m ?? rawReport.snapDistanceM) || null,
    description: rawReport.description || "",
    adminApproved: Boolean(rawReport.admin_approved ?? rawReport.adminApproved),
    moderationStatus:
      rawReport.moderation_status ||
      rawReport.moderationStatus ||
      "pending",
    moderationNotes: rawReport.moderation_notes || rawReport.moderationNotes || "",
    moderatedBy: Number(rawReport.moderated_by ?? rawReport.moderatedBy) || null,
    moderatedAt: rawReport.moderated_at || rawReport.moderatedAt || "",
    createdAt: rawReport.created_at || rawReport.createdAt || "",
    details: normalizeReportedDetails(eventKind, rawReport.details),
  };
}

export const reportedEventsService = {
  async createReportedEvent(payload, accessToken, requestOptions = {}) {
    const response = await requestReportedEvent("/reported-events", "Failed to submit the report.", {
      ...requestOptions,
      method: "POST",
      accessToken,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    return normalizeReportedEvent(response?.report || response);
  },

  async getMyReportedEvents(
    { status, eventKind, limit = 20, cursor } = {},
    accessToken,
    requestOptions = {},
  ) {
    const query = toQueryString({
      status,
      event_kind: eventKind,
      limit,
      cursor,
    });

    const response = await requestReportedEvent(
      `/reported-events/mine${query ? `?${query}` : ""}`,
      "Failed to load your reported events.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return {
      items: Array.isArray(response?.items)
        ? response.items.map(normalizeReportedEvent).filter(Boolean)
        : [],
      meta: response?.meta || null,
    };
  },

  async getAdminReportedEvents(
    { status, eventKind, reporterType, from, to, limit = 50, cursor } = {},
    accessToken,
    requestOptions = {},
  ) {
    const query = toQueryString({
      status,
      event_kind: eventKind,
      reporter_type: reporterType,
      from,
      to,
      limit,
      cursor,
    });

    const response = await requestReportedEvent(
      `/admin/reported-events${query ? `?${query}` : ""}`,
      "Failed to load the admin moderation queue.",
      {
        ...requestOptions,
        accessToken,
      },
    );

    return {
      items: Array.isArray(response?.items)
        ? response.items.map(normalizeReportedEvent).filter(Boolean)
        : [],
      meta: response?.meta || null,
    };
  },

  async moderateReportedEvent(
    reportId,
    payload,
    accessToken,
    requestOptions = {},
  ) {
    const response = await requestReportedEvent(
      `/admin/reported-events/${reportId}/moderation`,
      "Failed to update the report moderation status.",
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

    return normalizeReportedEvent(response?.report || response);
  },
};
