import { API_BASE_URL } from "../config/env";

const AUTH_STORAGE_KEY = "data-map.auth.session";

function emptySession() {
  return { accessToken: "", user: null };
}

function normalizeStoredSession(payload) {
  if (!payload || typeof payload !== "object") {
    return emptySession();
  }

  return {
    accessToken: typeof payload.accessToken === "string" ? payload.accessToken : "",
    user: payload.user && typeof payload.user === "object" ? payload.user : null,
  };
}

async function requestJson(path, fallbackMessage, { accessToken, ...requestOptions } = {}) {
  const headers = new Headers(requestOptions.headers || {});

  if (requestOptions.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  if (accessToken) {
    headers.set("Authorization", `Bearer ${accessToken}`);
  }

  let response;

  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      ...requestOptions,
      headers,
    });
  } catch (error) {
    if (error instanceof TypeError) {
      throw new Error(`Cannot reach ${API_BASE_URL}. Check the configured endpoint and server status.`);
    }

    throw error;
  }

  const contentType = response.headers.get("content-type") || "";
  let payload = null;

  if (contentType.includes("application/json")) {
    payload = await response.json().catch(() => null);
  } else {
    const message = await response.text();
    payload = message ? { message } : null;
  }

  if (!response.ok) {
    const error = new Error(
      payload?.detail || payload?.message || payload?.error || fallbackMessage,
    );
    error.status = response.status;
    error.payload = payload;
    throw error;
  }

  return payload;
}

export const authService = {
  getStoredSession() {
    if (typeof window === "undefined") {
      return emptySession();
    }

    try {
      const rawSession = window.localStorage.getItem(AUTH_STORAGE_KEY);

      if (!rawSession) {
        return emptySession();
      }

      return normalizeStoredSession(JSON.parse(rawSession));
    } catch {
      return emptySession();
    }
  },

  storeSession(session) {
    if (typeof window === "undefined") {
      return;
    }

    const normalizedSession = normalizeStoredSession(session);

    if (!normalizedSession.accessToken) {
      window.localStorage.removeItem(AUTH_STORAGE_KEY);
      return;
    }

    window.localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(normalizedSession));
  },

  clearStoredSession() {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.removeItem(AUTH_STORAGE_KEY);
  },

  login(credentials) {
    console.log(credentials)
    return requestJson("/auth/login", "Login failed.", {
      method: "POST",
      body: JSON.stringify(credentials),
    });
  },

  register(credentials) {
    return requestJson("/auth/register", "Registration failed.", {
      method: "POST",
      body: JSON.stringify(credentials),
    });
  },

  getCurrentUser(accessToken) {
    return requestJson("/me", "Failed to load the current user.", {
      accessToken,
    });
  },

  updateProfile(updates, accessToken) {
    return requestJson("/me", "Failed to update the profile.", {
      method: "PATCH",
      accessToken,
      body: JSON.stringify(updates),
    });
  },
};
