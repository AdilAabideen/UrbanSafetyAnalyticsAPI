import { toMonthValue } from "../constants/crimeFilterOptions";

export const WEST_YORKSHIRE_CENTER = [-1.5491, 53.8008];
export const WEST_YORKSHIRE_BBOX = {
  minLon: -2.25,
  minLat: 53.55,
  maxLon: -1.1,
  maxLat: 54.05,
};
export const DEFAULT_MONTH_FROM = toMonthValue(
  new Date(new Date().getFullYear(), new Date().getMonth() - 2, 1),
);
export const DEFAULT_MONTH_TO = toMonthValue(new Date());
export const DEFAULT_CRIME_FILTERS = {
  monthFrom: DEFAULT_MONTH_FROM,
  monthTo: DEFAULT_MONTH_TO,
  crimeType: "",
  outcomeCategory: "",
  lsoaName: "",
};
export const MAX_POINT_FILTER_PAGES = 8;

export function getCrimeProperty(properties, ...keys) {
  for (const key of keys) {
    const value = properties?.[key];

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return null;
}

export function hasClusterCount(properties = {}) {
  return ["count", "point_count", "cluster_count"].some((key) => {
    const value = Number(properties?.[key]);
    return Number.isFinite(value) && value > 0;
  });
}

export function normalizeCrimeFeature(feature) {
  const properties = feature?.properties || feature || {};
  const coordinates = feature?.geometry?.type === "Point"
    ? feature.geometry.coordinates
    : Number.isFinite(properties?.lon) && Number.isFinite(properties?.lat)
      ? [properties.lon, properties.lat]
      : null;
  const lonValue = getCrimeProperty(properties, "lon", "Lon");
  const latValue = getCrimeProperty(properties, "lat", "Lat");
  const longitude = lonValue !== null ? Number(lonValue) : null;
  const latitude = latValue !== null ? Number(latValue) : null;
  const locationLabel =
    getCrimeProperty(
      properties,
      "location",
      "location_text",
      "Location",
      "street_name",
      "street",
      "display_location",
    ) || (Array.isArray(coordinates) ? `${coordinates[1]}, ${coordinates[0]}` : null);

  return {
    ...properties,
    geometry: feature?.geometry || null,
    lon: Number.isFinite(longitude) ? longitude : null,
    lat: Number.isFinite(latitude) ? latitude : null,
    recordId: getCrimeProperty(properties, "recordId", "record_id", "id", "ID"),
    crimeId: getCrimeProperty(properties, "crimeId", "crime_id", "Crime ID"),
    month: getCrimeProperty(properties, "month", "Month"),
    crimeType: getCrimeProperty(
      properties,
      "crimeType",
      "crime_type",
      "crime-type",
      "Crime type",
      "category",
    ),
    reportedBy: getCrimeProperty(
      properties,
      "reportedBy",
      "reported_by",
      "reported-by",
      "Reported by",
    ),
    fallsWithin: getCrimeProperty(
      properties,
      "fallsWithin",
      "falls_within",
      "falls-within",
      "Falls within",
    ),
    location: locationLabel,
    lsoaCode: getCrimeProperty(properties, "lsoaCode", "lsoa_code", "lsoa-code", "LSOA code"),
    lsoaName: getCrimeProperty(properties, "lsoaName", "lsoa_name", "lsoa-name", "LSOA name"),
    outcomeCategory: getCrimeProperty(
      properties,
      "outcomeCategory",
      "outcome_category",
      "outcome-category",
      "last_outcome_category",
      "last-outcome-category",
      "Last outcome category",
      "lastOutcomeCategory",
    ),
    context: getCrimeProperty(properties, "context", "Context"),
  };
}

export function toSearchOptions(values, selectedValue = "") {
  const uniqueValues = [...new Set(values.filter(Boolean))].sort((left, right) =>
    left.localeCompare(right),
  );

  if (selectedValue && !uniqueValues.includes(selectedValue)) {
    uniqueValues.unshift(selectedValue);
  }

  return uniqueValues.map((value) => ({ value, label: value }));
}

export function normalizeLsoaCategories(payload) {
  const items = Array.isArray(payload?.items) ? payload.items : Array.isArray(payload) ? payload : [];

  return items
    .map((item) => {
      const lsoaName = getCrimeProperty(item, "lsoaName", "lsoa_name", "LSOA name");
      const lsoaCode = getCrimeProperty(item, "lsoaCode", "lsoa_code", "LSOA code");
      const minLon = Number(item?.minLon);
      const minLat = Number(item?.minLat);
      const maxLon = Number(item?.maxLon);
      const maxLat = Number(item?.maxLat);
      const hasBbox = [minLon, minLat, maxLon, maxLat].every((value) => Number.isFinite(value));

      return {
        lsoaCode,
        lsoaName,
        count: Number(item?.count) || 0,
        bbox: hasBbox
          ? {
              minLon,
              minLat,
              maxLon,
              maxLat,
            }
          : null,
      };
    })
    .filter((item) => item.lsoaName);
}

export function findLsoaCategory(categories, lsoaName) {
  if (!lsoaName) {
    return null;
  }

  return categories.find((item) => item.lsoaName === lsoaName) || null;
}
