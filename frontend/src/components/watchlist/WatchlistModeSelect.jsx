function WatchlistModeSelect({ label, value, options, onChange }) {
  return (
    <div className="flex flex-col gap-3 rounded-[12px] border border-cyan-200/10 bg-[#071316]/70 px-4 py-4">
      <div>
        <p className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">{label}</p>
        <p className="mt-2 text-sm text-cyan-100/55">
          Choose the travel mode that this watchlist should track against.
        </p>
      </div>

      <div className="grid grid-cols-3 gap-2">
        {options.map((option) => {
          const isSelected = value === option;

          return (
            <button
              key={option}
              type="button"
              onClick={() => onChange(option)}
              className={`rounded-[14px] border px-3 py-3 text-sm font-medium transition-colors ${
                isSelected
                  ? "border-cyan-300/40 bg-cyan-50/12 text-cyan-50"
                  : "border-cyan-100/10 bg-[#030b0e]/80 text-cyan-100/70 hover:bg-cyan-100/10 hover:text-cyan-50"
              }`}
            >
              {option}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export default WatchlistModeSelect;
