import BarRow from "../shared/BarRow";
import MiniMetricCard from "../shared/MiniMetricCard";
import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import BandBreakdownRow from "../shared/BandBreakdownRow";
import { formatCount, formatPercent } from "../../utils/formatters";
import { normalizeBandRows, hasBandBreakdown } from "../../utils/roadUtils";

function RoadOutcomesTab({ outcomeItems, bandBreakdown, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading outcome and band charts from `/roads/analytics/charts`." />;
  }

  if (!outcomeItems.length && !hasBandBreakdown(bandBreakdown)) {
    return <EmptyAnalyticsState message="No outcome or band breakdown data is available for the current selection." />;
  }

  const orderedBands = normalizeBandRows(bandBreakdown);

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Outcomes</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Outcome mix and risk bands</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Outcome composition plus band distribution returned by `/roads/analytics/charts`.
        </p>

        {outcomeItems.length ? (
          <div className="mt-6 space-y-4">
            {outcomeItems.map((item) => (
              <div key={item.key} className="rounded-[18px] border border-white/5 bg-[#030b0e]/55 px-4 py-4">
                <BarRow item={{ label: item.label, count: item.count }} maxValue={outcomeItems[0]?.count || 1} />
                <p className="mt-3 text-xs text-cyan-100/55">
                  {formatPercent(item.share)} of linked incidents in the current selection
                </p>
              </div>
            ))}
          </div>
        ) : null}
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top outcome"
          value={outcomeItems[0]?.label || "No data"}
          meta={outcomeItems[0] ? `${formatCount(outcomeItems[0].count)} incidents` : ""}
        />

        <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Band Breakdown</p>
          <div className="mt-4 space-y-3">
            {orderedBands.map((item) => (
              <BandBreakdownRow key={item.label} item={item} maxValue={orderedBands[0]?.count || 1} />
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

export default RoadOutcomesTab;
