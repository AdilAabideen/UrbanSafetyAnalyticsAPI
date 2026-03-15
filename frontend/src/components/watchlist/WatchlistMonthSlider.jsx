function WatchlistMonthSlider({ label, value, options, onChange }) {
  const totalOptions = options.length;
  const numericValue = Number(value) || totalOptions;
  const clampedValue = Math.min(Math.max(numericValue, 1), totalOptions);
  const selectedOption = options[clampedValue - 1] || options[0];

  return (
    <div className="flex flex-col gap-3 rounded-[12px] border border-cyan-200/10 bg-[#071316]/70 px-4 py-4 ">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">{label}</p>
          <p className="mt-2 text-base font-semibold text-cyan-50">
            {selectedOption?.label || "No window selected"}
          </p>
        </div>
        <div className="rounded-full border border-cyan-100/10 bg-[#030b0e]/80 px-3 py-1 text-xs uppercase tracking-[0.18em] text-cyan-100/60">
          {clampedValue} months
        </div>
      </div>

      <input
        type="range"
        min="1"
        max={String(totalOptions)}
        step="1"
        value={clampedValue}
        onChange={(event) => onChange(Number(event.target.value))}
        className="h-2 w-full cursor-pointer appearance-none rounded-full bg-cyan-100/10 accent-cyan-50"
      />

      <div className="flex items-center justify-between text-xs uppercase tracking-[0.18em] text-cyan-100/40">
        <span>{options[0]?.label}</span>
        <span>{options[totalOptions - 1]?.label}</span>
      </div>
    </div>
  );
}

export default WatchlistMonthSlider;
