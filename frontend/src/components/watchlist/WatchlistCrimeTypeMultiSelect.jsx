import { useDeferredValue, useMemo, useState } from "react";
import { formatCount } from "../../utils/formatters";

function WatchlistCrimeTypeMultiSelect({ label, values, options, onChange }) {
  const [isOpen, setIsOpen] = useState(false);
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);

  const filteredOptions = useMemo(() => {
    const normalizedQuery = deferredQuery.trim().toLowerCase();

    if (!normalizedQuery) {
      return options;
    }

    return options.filter((option) => option.label.toLowerCase().includes(normalizedQuery));
  }, [deferredQuery, options]);

  const selectedOptions = useMemo(
    () => options.filter((option) => values.includes(option.value)),
    [options, values],
  );

  const toggleValue = (nextValue) => {
    onChange(
      values.includes(nextValue)
        ? values.filter((value) => value !== nextValue)
        : [...values, nextValue],
    );
  };

  return (
    <div className="flex flex-col gap-3 rounded-[12px] border border-cyan-200/10 bg-[#071316]/70 px-4 py-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">{label}</p>
          <p className="mt-2 text-sm text-cyan-100/55">
            Search and select one or more crime types for this watchlist.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setIsOpen((current) => !current)}
          className="rounded-full border border-cyan-100/10 bg-[#030b0e]/80 px-3 py-1 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10"
        >
          {isOpen ? "Hide" : "Browse"}
        </button>
      </div>

      {selectedOptions.length ? (
        <div className="flex flex-wrap gap-2">
          {selectedOptions.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => toggleValue(option.value)}
              className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-3 py-1.5 text-xs font-medium text-cyan-50 transition-colors hover:bg-cyan-50/20"
            >
              {option.label}
            </button>
          ))}
        </div>
      ) : (
        <p className="text-sm text-cyan-100/35">No crime types selected yet.</p>
      )}

      {isOpen ? (
        <div className="flex flex-col gap-3 rounded-[16px] border border-cyan-100/10 bg-[#030b0e]/80 p-3">
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search crime types"
            className="rounded-md border border-cyan-200/10 bg-[#071316]/90 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
          />

          <div className="max-h-56 overflow-y-auto">
            <div className="flex flex-col gap-2">
              {filteredOptions.map((option) => {
                const isSelected = values.includes(option.value);

                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => toggleValue(option.value)}
                    className={`flex items-center justify-between gap-3 rounded-[14px] border px-3 py-3 text-left transition-colors ${
                      isSelected
                        ? "border-cyan-300/35 bg-cyan-50/10 text-cyan-50"
                        : "border-cyan-100/10 bg-[#071316]/80 text-cyan-100/75 hover:bg-cyan-100/10 hover:text-cyan-50"
                    }`}
                  >
                    <span className="text-sm font-medium">{option.label}</span>
                    <span className="text-xs uppercase tracking-[0.16em] text-cyan-100/45">
                      {formatCount(option.count)}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default WatchlistCrimeTypeMultiSelect;
