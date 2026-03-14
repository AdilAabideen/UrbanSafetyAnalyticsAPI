export function formatMonthLabel(month) {
  if (!month) {
    return "Unknown month";
  }

  const [year, value] = month.slice(0, 7).split("-").map(Number);

  if (!year || !value) {
    return month;
  }

  return new Intl.DateTimeFormat("en-GB", {
    month: "short",
    year: "numeric",
  }).format(new Date(Date.UTC(year, value - 1, 1)));
}

export function formatCount(value) {
  return new Intl.NumberFormat("en-GB").format(Number(value) || 0);
}

export function formatDistanceKm(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "No data";
  }

  return `${numericValue.toFixed(numericValue >= 10 ? 0 : 1)} km`;
}

export function formatMetricValue(value, suffix = "") {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "No data";
  }

  return `${numericValue.toFixed(numericValue >= 10 ? 0 : 2)}${suffix ? ` ${suffix}` : ""}`;
}

export function formatPercent(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "0.0%";
  }

  return `${numericValue.toFixed(1)}%`;
}

export function formatSignedPercent(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue)) {
    return "No data";
  }

  const prefix = numericValue > 0 ? "+" : "";
  return `${prefix}${numericValue.toFixed(1)}%`;
}

export function formatBandLabel(value) {
  const label = String(value || "").trim().toLowerCase();

  if (!label) {
    return "No band";
  }

  return label.charAt(0).toUpperCase() + label.slice(1);
}
