import { formatMonthLabel } from "../../utils/formatters";

function CrimeFeedRow({ crime, isSelected, onSelect }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.25fr),minmax(0,1.05fr),minmax(0,0.85fr),minmax(0,0.95fr)] ${
        isSelected ? "bg-cyan-100/10" : "bg-transparent hover:bg-white/[0.03]"
      }`}
    >
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Crime type</p>
        <p className="mt-2 truncate text-sm font-semibold text-cyan-50">
          {crime.crimeType || "Unknown"}
        </p>
        <p className="mt-1 text-xs text-cyan-100/55">
          Record {crime.recordId || "—"}
          {crime.crimeId ? ` / Crime ${crime.crimeId}` : ""}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Location</p>
        <p className="mt-2 truncate text-sm text-cyan-50">
          {crime.location || "Location unavailable"}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {crime.lsoaName || "No LSOA recorded"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Reported</p>
        <p className="mt-2 text-sm text-cyan-50">{formatMonthLabel(crime.month)}</p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {crime.reportedBy || "Unknown source"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Outcome</p>
        <p className="mt-2 text-sm text-cyan-50">
          {crime.outcomeCategory || "Pending or not recorded"}
        </p>
      </div>
    </button>
  );
}

export default CrimeFeedRow;
