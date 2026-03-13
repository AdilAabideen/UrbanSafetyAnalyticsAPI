import { useDeferredValue, useMemo, useState } from "react";

function FilterComponent({
  filters,
  monthOptions,
  crimeTypeOptions,
  outcomeOptions,
  lsoaOptions,
  visibleCrimeCount,
  mode,
  onChange,
  onClear,
  layout = "overlay",
  title = "Crime Filters",
  visibleLabel = "Visible crimes",
  categorySectionTitle = "Crime Filters",
  crimeTypeLabel = "Crime Type",
  outcomeLabel = "Last Outcome Category",
  lsoaLabel = "LSOA Name",
  lsoaPlaceholder = "Search LSOA",
  lsoaEmptyMessage = "No LSOA names available for this view yet.",
}) {
  const hasActiveFilters = Object.values(filters).some(Boolean);
  const isPanelLayout = layout === "panel";
  const containerClassName = isPanelLayout
    ? "flex h-full min-h-0 w-full flex-col gap-3 overflow-y-auto rounded-[24px] border border-cyan-200/10 bg-[#030b0e]/90 p-4 shadow-2xl"
    : "pointer-events-auto flex w-[340px] max-h-[35%] max-w-[calc(100vw-2rem)] flex-col gap-3 overflow-y-auto rounded-xl border border-cyan-200/10 bg-[#071316]/85 p-4 shadow-2xl backdrop-blur-md";

  return (
    <div className={containerClassName}>
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-wide text-cyan-50">{title}</h2>
          <p className="mt-1 text-xs text-cyan-100/60">
            {visibleLabel}: {visibleCrimeCount} | Mode: {mode}
          </p>
        </div>

        <button
          type="button"
          onClick={onClear}
          disabled={!hasActiveFilters}
          className="rounded-md bg-cyan-100/5 px-3 py-1.5 text-xs font-medium uppercase tracking-wider text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-40"
        >
          Clear
        </button>
      </div>

      <section className="flex flex-col gap-3 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          Time Range
        </h3>
        <MonthRangeSlider
          monthOptions={monthOptions}
          fromValue={filters.monthFrom}
          toValue={filters.monthTo}
          onChangeFrom={(value) => onChange("monthFrom", value)}
          onChangeTo={(value) => onChange("monthTo", value)}
        />
      </section>

      <section className="flex flex-col gap-3 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          {categorySectionTitle}
        </h3>
        <SearchSelectField
          label={crimeTypeLabel}
          value={filters.crimeType}
          options={crimeTypeOptions}
          placeholder="All crime types"
          onChange={(value) => onChange("crimeType", value)}
        />
        <SearchSelectField
          label={outcomeLabel}
          value={filters.outcomeCategory}
          options={outcomeOptions}
          placeholder="All outcomes"
          onChange={(value) => onChange("outcomeCategory", value)}
        />
        <SearchSelectField
          label={lsoaLabel}
          value={filters.lsoaName}
          options={lsoaOptions}
          placeholder={lsoaPlaceholder}
          emptyMessage={lsoaEmptyMessage}
          allowCustomValue
          onChange={(value) => onChange("lsoaName", value)}
        />
      </section>
    </div>
  );
}

function SearchSelectField({
  label,
  value,
  options,
  placeholder,
  emptyMessage = "No matching options.",
  allowCustomValue = false,
  onChange,
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const selectedOption = useMemo(
    () => options.find((option) => option.value === value) || null,
    [options, value],
  );

  const filteredOptions = useMemo(() => {
    const normalizedQuery = deferredQuery.trim().toLowerCase();

    if (!normalizedQuery) {
      return options.slice(0, 100);
    }

    return options
      .filter((option) => option.label.toLowerCase().includes(normalizedQuery))
      .slice(0, 100);
  }, [options, deferredQuery]);

  const canUseCustomValue =
    allowCustomValue &&
    query.trim() &&
    !options.some((option) => option.label.toLowerCase() === query.trim().toLowerCase());

  const openField = () => {
    setQuery(selectedOption?.label || "");
    setIsOpen(true);
  };

  const closeField = () => {
    setQuery("");
    setIsOpen(false);
  };

  const handleSelect = (nextValue) => {
    onChange(nextValue);
    closeField();
  };

  return (
    <div className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>

      {!isOpen ? (
        <div className="flex items-center gap-2 rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2">
          <button
            type="button"
            onClick={openField}
            className="flex flex-1 items-center justify-between gap-2 text-left text-sm font-medium text-cyan-50"
          >
            <span className={selectedOption ? "text-cyan-50" : "text-cyan-100/40"}>
              {selectedOption?.label || value || placeholder}
            </span>
            <span className="text-cyan-100/40">Search</span>
          </button>

          {value ? (
            <button
              type="button"
              onClick={() => onChange("")}
              className="rounded-md px-1.5 py-1 text-xs uppercase tracking-wider text-cyan-100/50 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
            >
              Clear
            </button>
          ) : null}
        </div>
      ) : (
        <div className="flex flex-col gap-2 rounded-md border border-cyan-200/10 bg-[#071316]/70 p-2">
          <div className="flex items-center gap-2">
            <input
              autoFocus
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Escape") {
                  closeField();
                }

                if (event.key === "Enter" && canUseCustomValue) {
                  event.preventDefault();
                  handleSelect(query.trim());
                }
              }}
              placeholder={placeholder}
              className="w-full bg-transparent text-sm font-medium text-cyan-50 outline-none placeholder:text-cyan-100/30"
            />
            <button
              type="button"
              onClick={closeField}
              className="rounded-md px-1.5 py-1 text-xs uppercase tracking-wider text-cyan-100/50 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
            >
              Done
            </button>
          </div>

          <div className="flex max-h-40 flex-col gap-1 overflow-y-auto">
            {filteredOptions.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => handleSelect(option.value)}
                className="rounded-md px-2 py-2 text-left text-sm text-cyan-50 transition-colors hover:bg-cyan-100/10"
              >
                {option.label}
              </button>
            ))}

            {canUseCustomValue ? (
              <button
                type="button"
                onClick={() => handleSelect(query.trim())}
                className="rounded-md px-2 py-2 text-left text-sm text-cyan-50 transition-colors hover:bg-cyan-100/10"
              >
                Use "{query.trim()}"
              </button>
            ) : null}

            {!filteredOptions.length && !canUseCustomValue ? (
              <span className="px-2 py-2 text-xs text-cyan-100/40">{emptyMessage}</span>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}

function MonthRangeSlider({ monthOptions, fromValue, toValue, onChangeFrom, onChangeTo }) {
  const reversed = useMemo(() => [...monthOptions].reverse(), [monthOptions]);
  const maxIndex = reversed.length - 1;

  const fromIndex = useMemo(() => {
    if (!fromValue) return 0;
    const idx = reversed.findIndex((opt) => opt.value === fromValue);
    return idx >= 0 ? idx : 0;
  }, [reversed, fromValue]);

  const toIndex = useMemo(() => {
    if (!toValue) return maxIndex;
    const idx = reversed.findIndex((opt) => opt.value === toValue);
    return idx >= 0 ? idx : maxIndex;
  }, [reversed, toValue, maxIndex]);

  const leftPercent = maxIndex > 0 ? (fromIndex / maxIndex) * 100 : 0;
  const rightPercent = maxIndex > 0 ? ((maxIndex - toIndex) / maxIndex) * 100 : 0;

  const handleFromChange = (event) => {
    const nextIndex = Math.min(Number(event.target.value), toIndex);
    onChangeFrom(nextIndex === 0 ? "" : reversed[nextIndex].value);
  };

  const handleToChange = (event) => {
    const nextIndex = Math.max(Number(event.target.value), fromIndex);
    onChangeTo(nextIndex === maxIndex ? "" : reversed[nextIndex].value);
  };

  if (reversed.length < 2) return null;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between text-xs text-cyan-100/60">
        <span>{reversed[fromIndex]?.label}</span>
        <span className="text-cyan-100/30">to</span>
        <span>{reversed[toIndex]?.label}</span>
      </div>

      <div className="relative h-6">
        <div className="absolute top-1/2 h-1 w-full -translate-y-1/2 rounded-full bg-cyan-100/10" />
        <div
          className="absolute top-1/2 h-1 -translate-y-1/2 rounded-full bg-cyan-400/60"
          style={{ left: `${leftPercent}%`, right: `${rightPercent}%` }}
        />
        <input
          type="range"
          min={0}
          max={maxIndex}
          value={fromIndex}
          onChange={handleFromChange}
          className="range-thumb-cyan pointer-events-none absolute inset-0 m-0 h-full w-full cursor-pointer appearance-none bg-transparent"
          style={{ zIndex: fromIndex > maxIndex - 10 ? 5 : 3 }}
        />
        <input
          type="range"
          min={0}
          max={maxIndex}
          value={toIndex}
          onChange={handleToChange}
          className="range-thumb-cyan pointer-events-none absolute inset-0 m-0 h-full w-full cursor-pointer appearance-none bg-transparent"
          style={{ zIndex: 4 }}
        />
      </div>

      <style>{`
        .range-thumb-cyan::-webkit-slider-thumb {
          -webkit-appearance: none;
          pointer-events: auto;
          height: 16px;
          width: 16px;
          border-radius: 9999px;
          background: #22d3ee;
          border: 2px solid #071316;
          cursor: pointer;
          box-shadow: 0 0 4px rgba(34, 211, 238, 0.4);
        }
        .range-thumb-cyan::-moz-range-thumb {
          pointer-events: auto;
          height: 16px;
          width: 16px;
          border-radius: 9999px;
          background: #22d3ee;
          border: 2px solid #071316;
          cursor: pointer;
          box-shadow: 0 0 4px rgba(34, 211, 238, 0.4);
        }
      `}</style>
    </div>
  );
}

export default FilterComponent;
