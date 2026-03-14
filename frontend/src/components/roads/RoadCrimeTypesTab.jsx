import BarRow from "../shared/BarRow";
import MiniMetricCard from "../shared/MiniMetricCard";
import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import { formatCount, formatPercent } from "../../utils/formatters";

function RoadCrimeTypesTab({ items, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading crime type composition from `/roads/analytics/charts`." />;
  }

  if (!items.length) {
    return <EmptyAnalyticsState message="No crime type composition is available for the current selection." />;
  }

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Crime Types</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Road-linked crime mix</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Dominant linked crime categories returned by `/roads/analytics/charts`.
        </p>

        <div className="mt-6 space-y-4">
          {items.map((item) => (
            <div key={item.key} className="rounded-[18px] border border-white/5 bg-[#030b0e]/55 px-4 py-4">
              <BarRow item={{ label: item.label, count: item.count }} maxValue={items[0]?.count || 1} />
              <p className="mt-3 text-xs text-cyan-100/55">
                {formatPercent(item.share)} of linked incidents in the current selection
              </p>
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top crime type"
          value={items[0]?.label || "No data"}
          meta={items[0] ? `${formatCount(items[0].count)} incidents` : ""}
        />
        <MiniMetricCard
          label="Top share"
          value={formatPercent(items[0]?.share || 0)}
          meta="Largest contribution inside the selected road scope"
        />
      </section>
    </div>
  );
}

export default RoadCrimeTypesTab;
