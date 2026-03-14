function WatchlistField({ label, value, onChange, placeholder, type = "text", inputMode }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">{label}</span>
      <input
        type={type}
        value={value}
        inputMode={inputMode}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
      />
    </label>
  );
}

export default WatchlistField;
