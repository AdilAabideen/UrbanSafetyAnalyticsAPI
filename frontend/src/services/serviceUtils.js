export function toQueryString(params) {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return;
    }

    if (Array.isArray(value)) {
      value.forEach((item) => {
        if (item === undefined || item === null) {
          return;
        }

        searchParams.append(key, String(item));
      });
      return;
    }

    searchParams.append(key, String(value));
  });

  return searchParams.toString();
}

export async function parseJsonOrThrow(response, fallbackMessage) {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || fallbackMessage);
  }

  return response.json();
}

export function toFeatureCollection(payload) {
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

export async function fetchJson(url, fallbackMessage, requestOptions = {}) {
  try {
    const response = await fetch(url, { signal: requestOptions.signal });
    return parseJsonOrThrow(response, fallbackMessage);
  } catch (error) {
    if (error?.name === "AbortError") {
      throw error;
    }

    if (error instanceof TypeError) {
      const origin = new URL(url, window.location.origin).origin;
      throw new Error(`Cannot reach ${origin}. Check the configured endpoint and server status.`);
    }

    throw error;
  }
}
