const CRIME_TYPES = [
  "Violence and sexual offences",
  "Public order",
  "Anti-social behaviour",
  "Criminal damage and arson",
  "Shoplifting",
  "Other theft",
  "Vehicle crime",
  "Burglary",
  "Drugs",
  "Other crime",
  "Robbery",
  "Theft from the person",
  "Possession of weapons",
  "Bicycle theft",
];

const OUTCOME_CATEGORIES = [
  "Unable to prosecute suspect",
  "Investigation complete; no suspect identified",
  "Court result unavailable",
  "Status update unavailable",
  "Under investigation",
  "Local resolution",
  "Awaiting court outcome",
  "Action to be taken by another organisation",
  "Further action is not in the public interest",
  "Offender given a caution",
  "Further investigation is not in the public interest",
  "Formal action is not in the public interest",
  "Suspect charged as part of another case",
  "Offender given a drugs possession warning",
  "Offender given penalty notice",
];

const monthFormatter = new Intl.DateTimeFormat("en-GB", {
  month: "short",
  year: "numeric",
});

function toOption(value) {
  return { value, label: value };
}

export function toMonthValue(date) {
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${date.getFullYear()}-${month}`;
}

export const CRIME_TYPE_OPTIONS = CRIME_TYPES.map(toOption);
export const OUTCOME_CATEGORY_OPTIONS = OUTCOME_CATEGORIES.map(toOption);

export function createMonthOptions(totalMonths = 48) {
  const current = new Date();
  const start = new Date(current.getFullYear(), current.getMonth(), 1);
  const options = [];

  for (let index = 0; index < totalMonths; index += 1) {
    const monthDate = new Date(start.getFullYear(), start.getMonth() - index, 1);

    options.push({
      value: toMonthValue(monthDate),
      label: monthFormatter.format(monthDate),
    });
  }

  return options;
}

export function createMonthOptionsFromRange(minMonth, maxMonth) {
  if (!minMonth || !maxMonth) {
    return [];
  }

  const [minYear, minValue] = minMonth.split("-").map(Number);
  const [maxYear, maxValue] = maxMonth.split("-").map(Number);

  if (!minYear || !minValue || !maxYear || !maxValue) {
    return [];
  }

  const options = [];
  const minIndex = minYear * 12 + (minValue - 1);
  const maxIndex = maxYear * 12 + (maxValue - 1);

  for (let index = maxIndex; index >= minIndex; index -= 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    const monthDate = new Date(year, month - 1, 1);

    options.push({
      value: toMonthValue(monthDate),
      label: monthFormatter.format(monthDate),
    });
  }

  return options;
}
