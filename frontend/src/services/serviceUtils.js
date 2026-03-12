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

    if (message) {
      try {
        const parsed = JSON.parse(message);
        const detail =
          typeof parsed?.detail === "string"
            ? parsed.detail
            : Array.isArray(parsed?.detail)
              ? parsed.detail.map((item) => item?.msg || item?.message || String(item)).join(", ")
              : typeof parsed?.message === "string"
                ? parsed.message
                : typeof parsed?.error === "string"
                  ? parsed.error
                  : message;

        throw new Error(detail || fallbackMessage);
      } catch (error) {
        if (error instanceof SyntaxError) {
          throw new Error(message || fallbackMessage);
        }

        throw error;
      }
    }

    throw new Error(fallbackMessage);
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
