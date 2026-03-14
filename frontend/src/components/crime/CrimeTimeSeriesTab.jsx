import TimeSeriesChart from "../shared/TimeSeriesChart";
import MiniMetricCard from "../shared/MiniMetricCard";
import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import { formatCount, formatMonthLabel } from "../../utils/formatters";

function CrimeTimeSeriesTab({ series, total, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading time series from `/crimes/analytics/timeseries`." />;
  }

  if (!series.length) {
    return (
      <EmptyAnalyticsState message="No monthly series is available for the current selection." />
    );
  }

  const peakMonth =
    [...series].sort(
      (left, right) => right.count - left.count || left.month.localeCompare(right.month),
    )[0] || null;
  const quietestMonth =
    [...series].sort(
      (left, right) => left.count - right.count || left.month.localeCompare(right.month),
    )[0] || null;

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.45fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Time Series</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Monthly incident curve</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Monthly counts returned by `/crimes/analytics/timeseries`.
        </p>
        <div className="mt-6">
          <TimeSeriesChart series={series} />
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard label="Total in series" value={formatCount(total)} />
        <MiniMetricCard
          label="Peak month"
          value={peakMonth ? formatMonthLabel(peakMonth.month) : "No data"}
          meta={peakMonth ? `${formatCount(peakMonth.count)} incidents` : ""}
        />
        <MiniMetricCard
          label="Quietest month"
          value={quietestMonth ? formatMonthLabel(quietestMonth.month) : "No data"}
          meta={quietestMonth ? `${formatCount(quietestMonth.count)} incidents` : ""}
        />
      </section>
    </div>
  );
}

export default CrimeTimeSeriesTab;
