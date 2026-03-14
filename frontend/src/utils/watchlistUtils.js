const EMPTY_FEATURE_COLLECTION = { type: "FeatureCollection", features: [] };

export const WATCHLIST_CRIME_TYPE_OPTIONS = [
  { value: "Violence and sexual offences", label: "Violence and sexual offences", count: 387354 },
  { value: "Public order", label: "Public order", count: 86244 },
  { value: "Anti-social behaviour", label: "Anti-social behaviour", count: 83299 },
  { value: "Criminal damage and arson", label: "Criminal damage and arson", count: 69343 },
  { value: "Shoplifting", label: "Shoplifting", count: 65850 },
  { value: "Other theft", label: "Other theft", count: 61197 },
  { value: "Vehicle crime", label: "Vehicle crime", count: 50566 },
  { value: "Burglary", label: "Burglary", count: 45396 },
  { value: "Drugs", label: "Drugs", count: 29074 },
  { value: "Other crime", label: "Other crime", count: 27602 },
  { value: "Robbery", label: "Robbery", count: 10311 },
  { value: "Theft from the person", label: "Theft from the person", count: 8448 },
  { value: "Possession of weapons", label: "Possession of weapons", count: 7786 },
  { value: "Bicycle theft", label: "Bicycle theft", count: 5352 },
];

export const WATCHLIST_MODE_OPTIONS = ["Walking", "Cycling", "Driving"];

function toMonthInputValue(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

export function createDefaultWatchlistForm() {
  const now = new Date();
  const currentMonth = new Date(now.getFullYear(), now.getMonth(), 1);
  const sixMonthsAgo = new Date(currentMonth.getFullYear(), currentMonth.getMonth() - 5, 1);

  return {
    name: "",
    startMonth: toMonthInputValue(sixMonthsAgo),
    endMonth: toMonthInputValue(currentMonth),
    crimeTypes: [],
    mode: "",
    minLon: "",
    minLat: "",
    maxLon: "",
    maxLat: "",
  };
}

export function toCrimeTypePayloadValue(value) {
  return String(value || "").trim();
}

export function monthValueToApiDateSecond(value) {
  if (!value) {
    return "";
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return value;
  }

  if (/^\d{4}-\d{2}$/.test(value)) {
    return `${value}-01`;
  }

  return value;
}

export function monthValueToApiDate(value) {
  if (!value) {
    return "";
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return value;
  }

  if (/^\d{4}-\d{2}$/.test(value)) {
    return `${value}`;
  }

  return value;
}

export function apiDateToMonthValue(value) {
  const text = String(value || "");
  return text.length >= 7 ? text.slice(0, 7) : "";
}

export function parseBboxFromForm(form) {
  const minLon = parseCoordinate(form.minLon);
  const minLat = parseCoordinate(form.minLat);
  const maxLon = parseCoordinate(form.maxLon);
  const maxLat = parseCoordinate(form.maxLat);

  if (
    minLon === null ||
    minLat === null ||
    maxLon === null ||
    maxLat === null ||
    minLon >= maxLon ||
    minLat >= maxLat
  ) {
    return null;
  }

  return { minLon, minLat, maxLon, maxLat };
}

export function parseCoordinate(value) {
  if (value === "" || value === null || value === undefined) {
    return null;
  }

  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

export function buildDrawFeatureCollection(points, isClosed) {
  if (!points.length) {
    return EMPTY_FEATURE_COLLECTION;
  }

  const features = points.map((point, index) => ({
    type: "Feature",
    geometry: {
      type: "Point",
      coordinates: point,
    },
    properties: {
      vertex: index + 1,
    },
  }));

  if (points.length >= 2) {
    features.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: isClosed ? toClosedPolygonCoordinates(points) : points,
      },
      properties: {},
    });
  }

  if (isClosed && points.length >= 3) {
    features.push({
      type: "Feature",
      geometry: {
        type: "Polygon",
        coordinates: [toClosedPolygonCoordinates(points)],
      },
      properties: {},
    });
  }

  return {
    type: "FeatureCollection",
    features,
  };
}

export function toClosedPolygonCoordinates(points) {
  if (!points.length) {
    return [];
  }

  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];

  if (firstPoint[0] === lastPoint[0] && firstPoint[1] === lastPoint[1]) {
    return points;
  }

  return [...points, firstPoint];
}

export function toBboxFromPoints(points) {
  if (!points.length) {
    return null;
  }

  const longitudes = points.map((point) => point[0]);
  const latitudes = points.map((point) => point[1]);

  const minLon = Math.min(...longitudes);
  const minLat = Math.min(...latitudes);
  const maxLon = Math.max(...longitudes);
  const maxLat = Math.max(...latitudes);

  if (!Number.isFinite(minLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLon) || !Number.isFinite(maxLat)) {
    return null;
  }

  return { minLon, minLat, maxLon, maxLat };
}

export function formatCoordinateInput(value) {
  return Number(value).toFixed(6);
}
