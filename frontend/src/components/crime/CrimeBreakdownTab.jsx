import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import BarRow from "../shared/BarRow";
import MiniMetricCard from "../shared/MiniMetricCard";
import { formatCount } from "../../utils/formatters";

const BREAKDOWN_LIMIT = 10;

function CrimeBreakdownTab({ title, subtitle, items, otherCount, isLoading, emptyMessage }) {
  if (isLoading) {
    return <EmptyAnalyticsState message={`Loading ${title.toLowerCase()} analytics.`} />;
  }

  if (!items.length) {
    return <EmptyAnalyticsState message={emptyMessage} />;
  }

  const topItems = items.slice(0, BREAKDOWN_LIMIT);

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Bar Chart</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">{title}</h3>
        <p className="mt-1 text-sm text-cyan-100/60">{subtitle}</p>

        <div className="mt-6 space-y-4">
          {topItems.map((item) => (
            <BarRow key={item.label} item={item} maxValue={topItems[0]?.count || 1} />
          ))}

          {otherCount ? (
            <BarRow
              item={{ label: "Other", count: otherCount }}
              maxValue={topItems[0]?.count || otherCount}
            />
          ) : null}
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top category"
          value={topItems[0]?.label || "No data"}
          meta={topItems[0] ? `${formatCount(topItems[0].count)} incidents` : ""}
        />
        <MiniMetricCard
          label="Visible categories"
          value={formatCount(items.length + (otherCount ? 1 : 0))}
          meta={otherCount ? "Includes grouped remainder" : "Direct ranked result"}
        />
        {otherCount ? (
          <MiniMetricCard
            label="Other count"
            value={formatCount(otherCount)}
            meta="Remaining categories outside the top list"
          />
        ) : null}
      </section>
    </div>
  );
}

export default CrimeBreakdownTab;
