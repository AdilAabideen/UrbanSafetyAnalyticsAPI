import { DEFAULT_CRIME_FILTERS } from "./crimeUtils";

export function areCrimeFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.lsoaName === right?.lsoaName
  );
}

export function resolveItems(items) {
  return Array.isArray(items) ? items : [];
}

export function normalizeBreakdownItems(items, labelKey) {
  if (!Array.isArray(items)) {
    return [];
  }

  return items.map((item) => ({
    label: item?.[labelKey] || "Unknown",
    count: Number(item?.count) || 0,
  }));
}

export function createDefaultFiltersFromMeta() {
  return { ...DEFAULT_CRIME_FILTERS };
}
