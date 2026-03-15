import { formatCount, formatMetricValue, formatPercent, formatSignedPercent, formatBandLabel } from "../../utils/formatters";

function RoadRiskRow({ road, isSelected, onSelect }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.25fr),minmax(0,1fr),minmax(0,1fr),minmax(0,0.9fr)] ${
        isSelected ? "bg-cyan-100/10" : "bg-transparent hover:bg-white/[0.03]"
      }`}
    >
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Road</p>
        <p className="mt-2 truncate text-sm font-semibold text-cyan-50">
          {road.name || road.highway}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {road.highway}
          {road.roadId ? ` / Segment ${road.roadId}` : ""}
        </p>
        <p className="mt-2 text-xs text-cyan-100/50">{road.message || "No narrative supplied"}</p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Exposure</p>
        <p className="mt-2 text-sm text-cyan-50">{formatCount(road.incidents)} incidents</p>
        <p className="mt-1 text-xs text-cyan-100/55">
          {formatMetricValue(road.incidentsPerKm, "incidents/km")}
        </p>
        <p className="mt-2 text-xs text-cyan-100/50">{formatPercent(road.shareOfIncidents)} of scope incidents</p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Signals</p>
        <p className="mt-2 text-sm text-cyan-50">{road.dominantCrimeType || "No dominant crime"}</p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {road.dominantOutcome || "No dominant outcome"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Risk</p>
        <p className="mt-2 text-sm text-cyan-50">{formatBandLabel(road.riskBand)}</p>
        <p className="mt-1 text-xs text-cyan-100/55">{formatMetricValue(road.score, "score")}</p>
        <p className="mt-2 text-xs text-cyan-100/50">
          {formatSignedPercent(road.previousPeriodChangePct)}
        </p>
      </div>
    </button>
  );
}

export default RoadRiskRow;
