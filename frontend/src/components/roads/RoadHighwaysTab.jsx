import BarRow from "../shared/BarRow";
import MiniMetricCard from "../shared/MiniMetricCard";
import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import { formatCount, formatMetricValue } from "../../utils/formatters";

function RoadHighwaysTab({ highwayItems, selectedRoad, onSelectRoad, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading highway breakdown from `/roads/analytics/charts`." />;
  }

  if (!highwayItems.length) {
    return <EmptyAnalyticsState message="No highway breakdown is available for the current selection." />;
  }

  const topHighway = highwayItems[0] || null;

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Highways</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Highway mix</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Road type composition returned by `/roads/analytics/charts`.
        </p>

        <div className="mt-6 space-y-3">
          {highwayItems.map((item) => (
            <button
              key={item.selectionKey}
              type="button"
              onClick={() => onSelectRoad(item)}
              className={`w-full rounded-[18px] border px-4 py-4 text-left transition-colors ${
                item.selectionKey === selectedRoad?.selectionKey
                  ? "border-cyan-200/20 bg-cyan-100/10"
                  : "border-white/5 bg-[#030b0e]/55 hover:bg-white/[0.03]"
              }`}
            >
              <div className="mb-3 flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-cyan-50">{item.highway}</p>
                  <p className="mt-1 text-xs text-cyan-100/55">{item.message || "Highway breakdown row"}</p>
                </div>
                <span className="text-sm font-semibold text-cyan-50">{formatCount(item.count)}</span>
              </div>
              <BarRow
                item={{ label: item.highway, count: item.count }}
                maxValue={highwayItems[0]?.count || 1}
              />
            </button>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top highway"
          value={topHighway?.highway || "No data"}
          meta={topHighway ? `${formatCount(topHighway.count)} incidents` : ""}
        />
        <MiniMetricCard
          label="Visible groups"
          value={formatCount(highwayItems.length)}
          meta="Limited by the charts endpoint"
        />
        <MiniMetricCard
          label="Top density"
          value={formatMetricValue(topHighway?.incidentsPerKm, "incidents/km")}
          meta="Highest ranked highway group in the current response"
        />
      </section>
    </div>
  );
}

export default RoadHighwaysTab;
