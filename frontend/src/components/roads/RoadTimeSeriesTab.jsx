import TimeSeriesChart from "../shared/TimeSeriesChart";
import MiniMetricCard from "../shared/MiniMetricCard";
import EmptyAnalyticsState from "../shared/EmptyAnalyticsState";
import { formatCount, formatMonthLabel, formatSignedPercent } from "../../utils/formatters";

function RoadTimeSeriesTab({ timeseries, isLoading }) {
  const overallSeries = timeseries.series[0]?.points || [];

  if (isLoading) {
    return <EmptyAnalyticsState message="Loading time series from `/roads/analytics/charts`." />;
  }

  if (!overallSeries.length) {
    return <EmptyAnalyticsState message="No road time series is available for the current selection." />;
  }

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.45fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Time Series</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Monthly road incident curve</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Trend line returned from `/roads/analytics/charts` using `timeseriesGroupBy=overall`.
        </p>
        <div className="mt-6">
          <TimeSeriesChart series={overallSeries} />
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard label="Total in series" value={formatCount(timeseries.total)} />
        <MiniMetricCard
          label="Peak month"
          value={timeseries.peak ? formatMonthLabel(timeseries.peak.month) : "No data"}
          meta={timeseries.peak ? `${formatCount(timeseries.peak.count)} incidents` : ""}
        />
        <MiniMetricCard
          label="Previous period"
          value={formatSignedPercent(timeseries.currentVsPreviousPct)}
          meta="Compared with the matched previous period"
        />
      </section>
    </div>
  );
}

export default RoadTimeSeriesTab;
