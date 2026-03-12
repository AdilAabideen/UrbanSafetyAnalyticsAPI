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
}) {
  const hasActiveFilters = Object.values(filters).some(Boolean);

  return (
    <div className="pointer-events-auto flex w-[340px] max-w-[calc(100vw-2rem)] max-h-[35%] flex-col gap-3 overflow-y-auto rounded-xl border border-cyan-200/10 bg-[#071316]/85 p-4 shadow-2xl backdrop-blur-md">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-wide text-cyan-50">Crime Filters</h2>
          <p className="mt-1 text-xs text-cyan-100/60">
            Visible crimes: {visibleCrimeCount} | Mode: {mode}
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
        <SelectFilterField
          label="Month and Year"
          value={filters.month}
          options={monthOptions}
          placeholder="All months"
          onChange={(value) => onChange("month", value)}
        />
      </section>

      <section className="flex flex-col gap-3 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          Crime Filters
        </h3>
        <SearchSelectField
          label="Crime Type"
          value={filters.crimeType}
          options={crimeTypeOptions}
          placeholder="All crime types"
          onChange={(value) => onChange("crimeType", value)}
        />
        <SearchSelectField
          label="Last Outcome Category"
          value={filters.outcomeCategory}
          options={outcomeOptions}
          placeholder="All outcomes"
          onChange={(value) => onChange("outcomeCategory", value)}
        />
        <SearchSelectField
          label="LSOA Name"
          value={filters.lsoaName}
          options={lsoaOptions}
          placeholder="Search LSOA"
          emptyMessage="No LSOA names available for this map view yet."
          allowCustomValue
          onChange={(value) => onChange("lsoaName", value)}
        />
      </section>
    </div>
  );
}

function SelectFilterField({ label, value, options, placeholder, onChange }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>
      <div className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2">
        <select
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="w-full bg-transparent text-sm font-medium text-cyan-50 outline-none"
        >
          <option value="">{placeholder}</option>
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
    </label>
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

export default FilterComponent;
