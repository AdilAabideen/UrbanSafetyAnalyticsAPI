function RoadDetailField({ label, value, subtle = false }) {
  return (
    <div className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>
      <span className={subtle ? "text-sm font-medium break-all text-cyan-50" : "text-lg font-semibold text-cyan-50"}>
        {value || "—"}
      </span>
    </div>
  );
}

export default RoadDetailField;
