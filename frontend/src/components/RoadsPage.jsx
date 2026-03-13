import { useEffect, useMemo, useState } from "react";
import FilterComponent from "./FilterComponent";
import TopBar from "./TopBar";
import {
  CRIME_TYPE_OPTIONS,
  OUTCOME_CATEGORY_OPTIONS,
  createMonthOptions,
  createMonthOptionsFromRange,
  toMonthValue,
} from "../constants/crimeFilterOptions";
import { config } from "../config/env";
import { roadsService } from "../services";
import { WEST_YORKSHIRE_BBOX, toSearchOptions } from "../utils/crimeUtils";

const DEFAULT_ROAD_FILTERS = {
  monthFrom: toMonthValue(new Date(new Date().getFullYear(), new Date().getMonth() - 2, 1)),
  monthTo: toMonthValue(new Date()),
  crimeType: "",
  outcomeCategory: "",
  lsoaName: "",
};

const FILTER_REQUEST_DEBOUNCE_MS = 450;
const ROAD_RISK_LIMIT = 25;
const ROAD_HIGHWAYS_LIMIT = 10;
const ROAD_WORKSPACE_TABS = [
  { id: "risk", label: "Risk Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "highways", label: "Highways" },
  { id: "anomaly", label: "Anomaly" },
];

function RoadsPage({ docsUrl }) {
  const [activeTab, setActiveTab] = useState("risk");
  const [roadFilters, setRoadFilters] = useState(DEFAULT_ROAD_FILTERS);
  const [appliedRoadFilters, setAppliedRoadFilters] = useState(DEFAULT_ROAD_FILTERS);
  const [analyticsMeta, setAnalyticsMeta] = useState(null);
  const [summaryData, setSummaryData] = useState(null);
  const [timeseriesData, setTimeseriesData] = useState({ series: [], total: 0 });
  const [highwayData, setHighwayData] = useState({ items: [], otherCount: 0 });
  const [riskRows, setRiskRows] = useState([]);
  const [anomalyData, setAnomalyData] = useState(null);
  const [lsoaOptions, setLsoaOptions] = useState([]);
  const [selectedRoad, setSelectedRoad] = useState(null);
  const [loadingMeta, setLoadingMeta] = useState(true);
  const [loadingRiskFeed, setLoadingRiskFeed] = useState(true);
  const [loadingAnalytics, setLoadingAnalytics] = useState(true);
  const [riskErrorMessage, setRiskErrorMessage] = useState("");
  const [analyticsErrorMessage, setAnalyticsErrorMessage] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    const loadMeta = async () => {
      setLoadingMeta(true);

      try {
        const payload = await roadsService.getRoadAnalyticsMeta({ signal: controller.signal });

        if (controller.signal.aborted) {
          return;
        }

        setAnalyticsMeta(payload);
        setRoadFilters((current) => {
          const defaultFilters = createDefaultFiltersFromMeta(payload?.months);
          setAppliedRoadFilters(defaultFilters);

          return {
            ...current,
            monthFrom: defaultFilters.monthFrom,
            monthTo: defaultFilters.monthTo,
          };
        });
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoadingMeta(false);
        }
      }
    };

    void loadMeta();

    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (loadingMeta) {
      return undefined;
    }

    const timerId = window.setTimeout(() => {
      setAppliedRoadFilters((current) =>
        areRoadFiltersEqual(current, roadFilters) ? current : roadFilters,
      );
    }, FILTER_REQUEST_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [loadingMeta, roadFilters]);

  const monthOptions = useMemo(() => {
    const rangedOptions = createMonthOptionsFromRange(
      analyticsMeta?.months?.min,
      analyticsMeta?.months?.max,
    );

    return rangedOptions.length ? rangedOptions : createMonthOptions(48);
  }, [analyticsMeta?.months?.max, analyticsMeta?.months?.min]);

  const crimeTypeOptions = useMemo(() => {
    const items =
      analyticsMeta?.crime_types || analyticsMeta?.crimeTypes || analyticsMeta?.filters?.crime_types;

    if (!Array.isArray(items) || !items.length) {
      return CRIME_TYPE_OPTIONS;
    }

    return items.map((item) => ({ value: item, label: item }));
  }, [analyticsMeta?.crimeTypes, analyticsMeta?.crime_types, analyticsMeta?.filters?.crime_types]);

  const effectiveDateRange = useMemo(
    () => ({
      from:
        appliedRoadFilters.monthFrom || analyticsMeta?.months?.min || DEFAULT_ROAD_FILTERS.monthFrom,
      to: appliedRoadFilters.monthTo || analyticsMeta?.months?.max || DEFAULT_ROAD_FILTERS.monthTo,
    }),
    [
      analyticsMeta?.months?.max,
      analyticsMeta?.months?.min,
      appliedRoadFilters.monthFrom,
      appliedRoadFilters.monthTo,
    ],
  );

  const sharedRoadQuery = useMemo(
    () => ({
      from: effectiveDateRange.from,
      to: effectiveDateRange.to,
      bbox: WEST_YORKSHIRE_BBOX,
      crimeTypes: appliedRoadFilters.crimeType ? [appliedRoadFilters.crimeType] : undefined,
      lastOutcomeCategories: appliedRoadFilters.outcomeCategory
        ? [appliedRoadFilters.outcomeCategory]
        : undefined,
      lsoaNames: appliedRoadFilters.lsoaName ? [appliedRoadFilters.lsoaName] : undefined,
    }),
    [
      appliedRoadFilters.crimeType,
      appliedRoadFilters.lsoaName,
      appliedRoadFilters.outcomeCategory,
      effectiveDateRange.from,
      effectiveDateRange.to,
    ],
  );

  useEffect(() => {
    if (loadingMeta || !sharedRoadQuery.from || !sharedRoadQuery.to) {
      return undefined;
    }

    const controller = new AbortController();

    const loadRoadWorkspace = async () => {
      setLoadingRiskFeed(true);
      setLoadingAnalytics(true);
      setRiskErrorMessage("");
      setAnalyticsErrorMessage("");

      try {
        const [summaryResult, timeseriesResult, highwaysResult, riskResult, anomalyResult] =
          await Promise.allSettled([
            roadsService.getRoadAnalyticsSummary(sharedRoadQuery, {
              signal: controller.signal,
            }),
            roadsService.getRoadAnalyticsTimeseries(sharedRoadQuery, {
              signal: controller.signal,
            }),
            roadsService.getRoadAnalyticsHighways(
              { ...sharedRoadQuery, limit: ROAD_HIGHWAYS_LIMIT },
              { signal: controller.signal },
            ),
            roadsService.getRoadAnalyticsRisk(
              {
                ...sharedRoadQuery,
                sort: "incidents_per_km",
                limit: ROAD_RISK_LIMIT,
              },
              { signal: controller.signal },
            ),
            roadsService.getRoadAnalyticsAnomaly(
              {
                target: effectiveDateRange.to,
                baselineMonths: 6,
                bbox: sharedRoadQuery.bbox,
                crimeTypes: sharedRoadQuery.crimeTypes,
                lastOutcomeCategories: sharedRoadQuery.lastOutcomeCategories,
                lsoaNames: sharedRoadQuery.lsoaNames,
              },
              { signal: controller.signal },
            ),
          ]);

        if (controller.signal.aborted) {
          return;
        }

        let nextRiskRows = [];
        let nextHighwayItems = [];
        const analyticsErrors = [];

        if (summaryResult.status === "fulfilled") {
          setSummaryData(summaryResult.value);
        } else {
          setSummaryData(null);
          analyticsErrors.push(summaryResult.reason?.message || "Road summary unavailable");
        }

        if (timeseriesResult.status === "fulfilled") {
          setTimeseriesData(normalizeRoadTimeseries(timeseriesResult.value));
        } else {
          setTimeseriesData({ series: [], total: 0 });
          analyticsErrors.push(timeseriesResult.reason?.message || "Road time series unavailable");
        }

        if (highwaysResult.status === "fulfilled") {
          const normalizedHighwayData = normalizeRoadHighwayData(highwaysResult.value);
          nextHighwayItems = normalizedHighwayData.items;
          setHighwayData(normalizedHighwayData);
        } else {
          setHighwayData({ items: [], otherCount: 0 });
          analyticsErrors.push(highwaysResult.reason?.message || "Highway ranking unavailable");
        }

        if (riskResult.status === "fulfilled") {
          nextRiskRows = normalizeRoadRiskItems(riskResult.value);
          setRiskRows(nextRiskRows);
        } else {
          setRiskRows([]);
          setRiskErrorMessage(riskResult.reason?.message || "Road risk feed unavailable");
        }

        if (anomalyResult.status === "fulfilled") {
          setAnomalyData(normalizeRoadAnomaly(anomalyResult.value));
        } else {
          setAnomalyData(null);
          analyticsErrors.push(anomalyResult.reason?.message || "Road anomaly unavailable");
        }

        const nextLsoaOptions = toSearchOptions(
          [...nextRiskRows, ...nextHighwayItems].map((item) => item.lsoaName),
          appliedRoadFilters.lsoaName,
        );

        setLsoaOptions(nextLsoaOptions);
        setSelectedRoad((current) => {
          if (!current?.selectionKey) {
            return null;
          }

          return (
            [...nextRiskRows, ...nextHighwayItems].find(
              (item) => item.selectionKey === current.selectionKey,
            ) || null
          );
        });
        setAnalyticsErrorMessage(analyticsErrors.join(" | "));
      } finally {
        if (!controller.signal.aborted) {
          setLoadingRiskFeed(false);
          setLoadingAnalytics(false);
        }
      }
    };

    void loadRoadWorkspace();

    return () => {
      controller.abort();
    };
  }, [appliedRoadFilters.lsoaName, effectiveDateRange.to, loadingMeta, sharedRoadQuery]);

  const summaryCards = useMemo(() => {
    const totalIncidents =
      getSummaryNumber(summaryData, "total_incidents", "incident_count", "incidents", "total_crimes") ||
      timeseriesData.total;
    const totalSegments =
      getSummaryNumber(
        summaryData,
        "road_segments_total",
        "total_road_segments",
        "total_segments",
        "segment_count",
      ) ||
      getSummaryNumber(analyticsMeta?.counts, "road_segments_total") ||
      riskRows.length;
    const totalLengthKm =
      getSummaryNumber(summaryData, "total_length_km", "length_km", "network_length_km") ||
      sumRoadLengthsKm(riskRows);
    const highRiskCount = riskRows.filter(isHighRiskRoad).length;
    const topHighway =
      summaryData?.top_highway?.highway ||
      summaryData?.top_highway?.name ||
      summaryData?.top_highway?.label ||
      highwayData.items[0]?.highway ||
      "No data";
    const topHighwayCount =
      Number(summaryData?.top_highway?.count) || Number(highwayData.items[0]?.count) || 0;
    const anomalyRatio = Number(anomalyData?.ratio);

    return [
      {
        label: "Visible Segments",
        value: formatCount(riskRows.length),
        meta: "Rows from `/roads/analytics/risk`",
        accent: "text-[#39ef7d]",
      },
      {
        label: "Total Incidents",
        value: formatCount(totalIncidents),
        meta: `${formatMonthLabel(effectiveDateRange.from)} to ${formatMonthLabel(effectiveDateRange.to)}`,
        accent: "text-cyan-50",
      },
      {
        label: "Road Segments",
        value: formatCount(totalSegments),
        meta: "Segments inside the shared analytics filter set",
        accent: "text-[#60a5fa]",
      },
      {
        label: "High Risk Segments",
        value: formatCount(highRiskCount),
        meta: "Rows currently flagged as high or elevated risk",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Top Highway",
        value: topHighway,
        meta: topHighwayCount ? `${formatCount(topHighwayCount)} incidents` : "No ranked highway data",
        accent: "text-[#ffb072]",
      },
      {
        label: "Network Length",
        value: formatDistanceKm(totalLengthKm),
        meta: anomalyData
          ? `${anomalyData.flag ? "Flagged" : "Stable"} anomaly at ${formatRatio(anomalyRatio)}`
          : "Length derived from current risk feed",
        accent: "text-[#22c55e]",
      },
    ];
  }, [
    analyticsMeta?.counts,
    anomalyData,
    effectiveDateRange.from,
    effectiveDateRange.to,
    highwayData.items,
    riskRows,
    summaryData,
    timeseriesData.total,
  ]);

  const isApplyingFilters = useMemo(
    () => !areRoadFiltersEqual(roadFilters, appliedRoadFilters),
    [appliedRoadFilters, roadFilters],
  );

  const roadStatusLabel = loadingMeta
    ? "Loading road metadata..."
    : isApplyingFilters
      ? "Applying filters..."
      : loadingRiskFeed
        ? "Loading road risk feed..."
        : riskErrorMessage
          ? "Road risk feed unavailable"
          : `Showing ${formatCount(riskRows.length)} road segments`;

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Roads Page"
        subtitle="Road analytics endpoints drive the risk feed, monthly series, highway ranking, and anomaly watch."
      />

      <div className="min-h-0 flex-1 overflow-hidden p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[320px,minmax(0,1.7fr)]">
          <div className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <FilterComponent
              filters={roadFilters}
              monthOptions={monthOptions}
              crimeTypeOptions={crimeTypeOptions}
              outcomeOptions={OUTCOME_CATEGORY_OPTIONS}
              lsoaOptions={lsoaOptions}
              visibleCrimeCount={riskRows.length}
              mode={isApplyingFilters ? "pending" : loadingRiskFeed ? "loading" : "risk desk"}
              layout="panel"
              title="Road Filters"
              visibleLabel="Visible segments"
              categorySectionTitle="Shared Filters"
              crimeTypeLabel="Related Crime Type"
              outcomeLabel="Outcome Category"
              lsoaLabel="LSOA Name"
              lsoaPlaceholder="Filter by LSOA"
              lsoaEmptyMessage="No LSOA names are available for the current road analytics response."
              onChange={(key, value) => {
                setRoadFilters((current) => ({
                  ...current,
                  [key]: value,
                }));
              }}
              onClear={() => {
                setRoadFilters(createDefaultFiltersFromMeta(analyticsMeta?.months));
              }}
            />
          </div>

          <section className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <div className="grid gap-2 md:grid-cols-3 xl:grid-cols-6">
              {summaryCards.map((card) => (
                <article
                  key={card.label}
                  className="rounded-[20px] border border-white/5 bg-[#030b0e]/90 p-3 shadow-2xl"
                >
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                    {card.label}
                  </p>
                  <p className={`mt-3 text-lg font-semibold leading-snug ${card.accent}`}>
                    {card.value}
                  </p>
                  <p className="mt-1 text-xs leading-5 text-cyan-100/60">{card.meta}</p>
                </article>
              ))}
            </div>

            {riskErrorMessage ? (
              <div className="rounded-[20px] border border-red-300/30 bg-[#480000b8] px-4 py-3 text-sm text-red-100">
                {riskErrorMessage}
              </div>
            ) : null}

            {analyticsErrorMessage ? (
              <div className="rounded-[20px] border border-amber-300/25 bg-amber-300/5 px-4 py-3 text-sm text-amber-100/85">
                {analyticsErrorMessage}
              </div>
            ) : null}

            <div className="flex min-h-0 flex-1 flex-col rounded-[24px] border border-white/5 bg-[#030b0e]/90 shadow-2xl">
              <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/5 px-4 py-4">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                    Intelligence Workspace
                  </p>
                  <h2 className="mt-2 text-xl font-semibold text-cyan-50">
                    Road risk feed and analytics
                  </h2>
                  <p className="mt-1 text-sm text-cyan-100/60">{roadStatusLabel}</p>
                </div>

                <div className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.25em] text-cyan-100/55">
                  {isApplyingFilters ? "Debounced filters" : "Analytics only"}
                </div>
              </div>

              <div className="flex flex-wrap gap-2 border-b border-white/5 px-4 py-3">
                {ROAD_WORKSPACE_TABS.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    onClick={() => setActiveTab(tab.id)}
                    className={`rounded-full px-3 py-2 text-sm transition-colors ${
                      activeTab === tab.id
                        ? "bg-cyan-100/10 text-cyan-50"
                        : "bg-transparent text-cyan-100/55 hover:bg-cyan-100/5 hover:text-cyan-50"
                    }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>

              <div className="min-h-0 flex-1 overflow-hidden">
                {activeTab === "risk" ? (
                  <RoadRiskTab
                    riskRows={riskRows}
                    selectedRoad={selectedRoad}
                    onSelectRoad={setSelectedRoad}
                    isLoading={loadingRiskFeed}
                  />
                ) : null}

                {activeTab === "timeseries" ? (
                  <RoadTimeSeriesTab
                    series={timeseriesData.series}
                    total={timeseriesData.total}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "highways" ? (
                  <RoadHighwaysTab
                    highwayItems={highwayData.items}
                    otherCount={highwayData.otherCount}
                    selectedRoad={selectedRoad}
                    onSelectRoad={setSelectedRoad}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "anomaly" ? (
                  <RoadAnomalyTab anomalyData={anomalyData} isLoading={loadingAnalytics} />
                ) : null}
              </div>
            </div>
          </section>
        </div>
      </div>

      <div className="flex shrink-0 items-center justify-between gap-3 border-t border-white/5 bg-[#030b0e] px-3 py-2">
        <div className="flex flex-1 flex-col gap-1">
          <span className="text-[11px] text-cyan-100/60">API: {config.apiBaseUrl}</span>
          <span className="text-[11px] text-cyan-100/45">
            Meta: `/roads/analytics/meta` | Risk: `/roads/analytics/risk`
          </span>
        </div>

        <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
          <span>{roadStatusLabel}</span>
          <strong className="text-[#39ef7d]">Road Analytics</strong>
          <span className="text-cyan-100/60">Drawer + tabs</span>
        </div>
      </div>

      <SlidingRoadDrawer road={selectedRoad} onClose={() => setSelectedRoad(null)} />
    </div>
  );
}

function RoadRiskTab({ riskRows, selectedRoad, onSelectRoad, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading road segments from `/roads/analytics/risk`." />;
  }

  if (!riskRows.length) {
    return (
      <EmptyAnalyticsState message="No road segments match the current filter set. Adjust the filters to repopulate the risk feed." />
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="border-b border-white/5 px-4 py-3 text-sm text-cyan-100/70">
        Sorted by incidents per kilometre from `/roads/analytics/risk`.
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {riskRows.map((road) => (
            <RoadRiskRow
              key={road.selectionKey}
              road={road}
              isSelected={road.selectionKey === selectedRoad?.selectionKey}
              onSelect={() => onSelectRoad(road)}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function RoadTimeSeriesTab({ series, total, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading time series from `/roads/analytics/timeseries`." />;
  }

  if (!series.length) {
    return (
      <EmptyAnalyticsState message="No road time series is available for the current selection." />
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
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Monthly road incident curve</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Monthly counts returned by `/roads/analytics/timeseries`.
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

function RoadHighwaysTab({
  highwayItems,
  otherCount,
  selectedRoad,
  onSelectRoad,
  isLoading,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading highway ranking from `/roads/analytics/highways`." />;
  }

  if (!highwayItems.length) {
    return (
      <EmptyAnalyticsState message="No highway ranking is available for the current road selection." />
    );
  }

  const topHighway = highwayItems[0] || null;

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Highways</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Highway ranking</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Ranked highway categories returned by `/roads/analytics/highways`.
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
                  <p className="mt-1 text-xs text-cyan-100/55">
                    {item.name || "Highway class aggregate"}
                  </p>
                </div>
                <span className="text-sm font-semibold text-cyan-50">
                  {formatCount(item.count)}
                </span>
              </div>
              <BarRow
                item={{ label: item.highway, count: item.count }}
                maxValue={highwayItems[0]?.count || 1}
              />
            </button>
          ))}

          {otherCount ? (
            <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/55 px-4 py-4">
              <BarRow
                item={{ label: "Other", count: otherCount }}
                maxValue={highwayItems[0]?.count || otherCount}
              />
            </div>
          ) : null}
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
          value={formatCount(highwayItems.length + (otherCount ? 1 : 0))}
          meta={otherCount ? "Includes grouped remainder" : "Direct ranked result"}
        />
        {otherCount ? (
          <MiniMetricCard
            label="Other count"
            value={formatCount(otherCount)}
            meta="Remaining highway groups outside the visible list"
          />
        ) : null}
      </section>
    </div>
  );
}

function RoadAnomalyTab({ anomalyData, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading anomaly watch from `/roads/analytics/anomaly`." />;
  }

  if (!anomalyData) {
    return (
      <EmptyAnalyticsState message="No road anomaly result is available for the current filter set." />
    );
  }

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-5">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Anomaly Watch</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Target month comparison</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          The selected target month is compared against the previous baseline window.
        </p>

        <div className="mt-6 grid gap-3 md:grid-cols-2">
          <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
            <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Target</p>
            <p className="mt-3 text-3xl font-semibold text-cyan-50">
              {formatRatio(anomalyData.ratio)}
            </p>
            <p className="mt-2 text-sm text-cyan-100/60">
              {anomalyData.flag ? "Flagged above baseline" : "Within expected range"}
            </p>
          </div>

          <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
            <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">
              Baseline Mean
            </p>
            <p className="mt-3 text-3xl font-semibold text-cyan-50">
              {formatCount(anomalyData.baselineMean)}
            </p>
            <p className="mt-2 text-sm text-cyan-100/60">
              Average monthly incidents across the lookback window
            </p>
          </div>
        </div>

        <div className="mt-4 rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">
                Target Month
              </p>
              <p className="mt-2 text-lg font-semibold text-cyan-50">
                {formatMonthLabel(anomalyData.target)}
              </p>
            </div>
            <div className="text-right">
              <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">
                Target Count
              </p>
              <p className="mt-2 text-lg font-semibold text-cyan-50">
                {formatCount(anomalyData.targetCount)}
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Target month"
          value={formatMonthLabel(anomalyData.target)}
          meta="Driven by `/roads/analytics/anomaly`"
        />
        <MiniMetricCard
          label="Ratio"
          value={formatRatio(anomalyData.ratio)}
          meta={anomalyData.flag ? "Flagged anomaly" : "No anomaly flag"}
        />
        <MiniMetricCard
          label="Target incidents"
          value={formatCount(anomalyData.targetCount)}
          meta={`Baseline mean ${formatCount(anomalyData.baselineMean)}`}
        />
      </section>
    </div>
  );
}

function RoadRiskRow({ road, isSelected, onSelect }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.2fr),minmax(0,1fr),minmax(0,0.9fr),minmax(0,0.85fr)] ${
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
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Exposure</p>
        <p className="mt-2 text-sm text-cyan-50">{formatCount(road.incidents)} incidents</p>
        <p className="mt-1 text-xs text-cyan-100/55">
          {formatMetricValue(road.incidentsPerKm, "incidents/km")}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Network</p>
        <p className="mt-2 text-sm text-cyan-50">{formatDistanceKm(road.lengthKm)}</p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {road.lsoaName || road.location || "No LSOA recorded"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Risk</p>
        <p className="mt-2 text-sm text-cyan-50">{road.riskBand || "Observed"}</p>
        <p className="mt-1 text-xs text-cyan-100/55">
          {formatMetricValue(road.score, "score")}
        </p>
      </div>
    </button>
  );
}

function SlidingRoadDrawer({ road, onClose }) {
  return (
    <div className="pointer-events-none absolute inset-0 z-30 overflow-hidden">
      <button
        type="button"
        aria-label="Close roads drawer"
        onClick={onClose}
        className={`absolute inset-0 bg-black/45 transition-opacity duration-300 ${
          road ? "pointer-events-auto opacity-100" : "opacity-0"
        }`}
      />

      <div
        className={`absolute inset-y-0 right-0 w-full border-l border-white/10 bg-[#030b0e] shadow-2xl transition-transform duration-300 sm:w-[46vw] sm:max-w-[46vw] ${
          road ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="h-full overflow-y-auto p-4">
          {road ? (
            <div className="space-y-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                    Road Details
                  </p>
                  <h2 className="mt-2 text-2xl font-semibold text-cyan-50">
                    {road.name || road.highway}
                  </h2>
                  <p className="mt-1 text-sm text-cyan-100/60">
                    Selected from `{road.sourceType === "highways" ? "/roads/analytics/highways" : "/roads/analytics/risk"}`
                  </p>
                </div>

                <button
                  type="button"
                  onClick={onClose}
                  className="flex h-9 w-9 items-center justify-center rounded-lg border border-cyan-100/10 text-cyan-100/60 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
                >
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-4 w-4"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <line x1="18" y1="6" x2="6" y2="18" />
                    <line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              </div>

              <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                  Segment Identity
                </p>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <RoadDetailField label="Road name" value={road.name || "Not supplied"} />
                  <RoadDetailField label="Highway" value={road.highway || "Unclassified"} />
                  <RoadDetailField label="Segment ID" value={road.roadId} subtle />
                  <RoadDetailField label="LSOA Name" value={road.lsoaName || "Not supplied"} />
                  <RoadDetailField label="Location" value={road.location || "Not supplied"} />
                  <RoadDetailField
                    label="Geometry"
                    value={road.geometry?.type || "Not supplied"}
                    subtle
                  />
                </div>
              </section>

              <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                  Risk Metrics
                </p>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <RoadDetailField
                    label="Incidents"
                    value={formatCount(road.incidents)}
                  />
                  <RoadDetailField
                    label="Incidents per km"
                    value={formatMetricValue(road.incidentsPerKm)}
                  />
                  <RoadDetailField label="Length" value={formatDistanceKm(road.lengthKm)} />
                  <RoadDetailField label="Risk band" value={road.riskBand || "Observed"} />
                  <RoadDetailField label="Score" value={formatMetricValue(road.score)} />
                  <RoadDetailField
                    label="Count"
                    value={formatCount(road.count || road.incidents)}
                  />
                </div>
              </section>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

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

function TimeSeriesChart({ series }) {
  const width = 720;
  const height = 260;
  const padding = 20;
  const maxCount = Math.max(...series.map((item) => item.count), 1);
  const points = series.map((item, index) => {
    const x =
      series.length === 1
        ? width / 2
        : padding + (index / (series.length - 1)) * (width - padding * 2);
    const y = height - padding - (item.count / maxCount) * (height - padding * 2);
    return { ...item, x, y };
  });
  const linePath = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const areaPath = `${linePath} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[260px] w-full">
        {[0, 1, 2, 3].map((step) => {
          const y = padding + (step / 3) * (height - padding * 2);

          return (
            <line
              key={step}
              x1={padding}
              x2={width - padding}
              y1={y}
              y2={y}
              stroke="rgba(178, 245, 234, 0.12)"
              strokeWidth="1"
            />
          );
        })}

        <path d={areaPath} fill="rgba(34, 211, 238, 0.14)" />
        <path d={linePath} fill="none" stroke="#22d3ee" strokeWidth="3" strokeLinecap="round" />

        {points.map((point) => (
          <circle key={point.month} cx={point.x} cy={point.y} r="4" fill="#39ef7d" />
        ))}
      </svg>

      <div className="mt-3 grid gap-2 text-xs text-cyan-100/60 md:grid-cols-4">
        {series.map((item) => (
          <div
            key={item.month}
            className="rounded-xl border border-cyan-100/10 bg-cyan-100/5 px-3 py-2"
          >
            <p>{formatMonthLabel(item.month)}</p>
            <p className="mt-1 font-semibold text-cyan-50">{formatCount(item.count)}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function BarRow({ item, maxValue }) {
  const widthPercent = maxValue > 0 ? (item.count / maxValue) * 100 : 0;

  return (
    <div>
      <div className="mb-2 flex items-center justify-between gap-3">
        <span className="text-sm text-cyan-100/75">{item.label}</span>
        <span className="text-sm font-semibold text-cyan-50">{formatCount(item.count)}</span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-cyan-100/10">
        <div
          className="h-full rounded-full bg-gradient-to-r from-cyan-400 to-emerald-400"
          style={{ width: `${widthPercent}%` }}
        />
      </div>
    </div>
  );
}

function MiniMetricCard({ label, value, meta = "Current filtered feed" }) {
  return (
    <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
      <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">{label}</p>
      <p className="mt-3 text-lg font-semibold text-cyan-50">{value}</p>
      <p className="mt-1 text-sm text-cyan-100/60">{meta}</p>
    </div>
  );
}

function EmptyAnalyticsState({ message }) {
  return (
    <div className="flex h-full min-h-[320px] items-center justify-center px-6 text-center">
      <div>
        <p className="text-lg font-semibold text-cyan-50">Nothing to plot yet</p>
        <p className="mt-2 text-sm text-cyan-100/60">{message}</p>
      </div>
    </div>
  );
}

function normalizeRoadTimeseries(payload) {
  const seriesSource = Array.isArray(payload?.series) ? payload.series : resolveItems(payload);
  const series = seriesSource
    .map((item) => ({
      month: getRoadProperty(item, "month", "target", "label"),
      count: getRoadNumber(item, "count", "incidents", "incident_count", "total"),
    }))
    .filter((item) => item.month);

  const total = getRoadNumber(payload, "total", "total_incidents", "count") || sumCounts(series);

  return { series, total };
}

function normalizeRoadHighwayData(payload) {
  const items = resolveItems(payload).map((item, index) => normalizeRoadHighwayItem(item, index));

  return {
    items,
    otherCount: getRoadNumber(payload, "other_count", "otherCount"),
  };
}

function normalizeRoadHighwayItem(item, index) {
  const name =
    getRoadProperty(item, "name", "road_name", "roadName", "label", "title") || null;
  const highway =
    getRoadProperty(item, "highway", "highway_type", "road_class", "classification", "name") ||
    `Highway ${index + 1}`;

  return {
    ...toRoadRecord(item),
    sourceType: "highways",
    name,
    highway,
    count: getRoadNumber(item, "count", "incidents", "incident_count", "total"),
    selectionKey: createRoadSelectionKey("highway", item, index),
  };
}

function normalizeRoadRiskItems(payload) {
  return resolveItems(payload).map((item, index) => {
    const roadRecord = toRoadRecord(item);

    return {
      ...roadRecord,
      sourceType: "risk",
      selectionKey: createRoadSelectionKey("risk", item, index),
      count: roadRecord.incidents,
    };
  });
}

function normalizeRoadAnomaly(payload) {
  return {
    target: getRoadProperty(payload, "target", "month"),
    targetCount: getRoadNumber(payload, "target_count", "targetCount", "count"),
    baselineMean: getRoadNumber(payload, "baseline_mean", "baselineMean", "mean"),
    ratio: getRoadNumber(payload, "ratio"),
    flag: Boolean(getRoadProperty(payload, "flag")) || getRoadNumber(payload, "ratio") >= 1.5,
  };
}

function toRoadRecord(item) {
  const properties = item?.properties || item || {};
  const lengthKm = getRoadLengthKm(properties);
  const incidentsPerKm =
    getRoadNumber(
      properties,
      "incidents_per_km",
      "incidentsPerKm",
      "risk",
      "risk_rate",
      "incident_rate",
    ) || null;

  return {
    ...properties,
    geometry: item?.geometry || properties?.geometry || null,
    roadId: getRoadProperty(properties, "road_id", "roadId", "segment_id", "segmentId", "id"),
    name: getRoadProperty(properties, "name", "road_name", "roadName", "title", "label"),
    highway:
      getRoadProperty(
        properties,
        "highway",
        "highway_type",
        "road_class",
        "classification",
        "ref",
      ) || "Unclassified",
    lsoaName: getRoadProperty(properties, "lsoa_name", "lsoaName"),
    location: getRoadProperty(properties, "location", "location_text", "display_location"),
    incidents: getRoadNumber(
      properties,
      "incidents",
      "incident_count",
      "incidents_count",
      "crime_count",
      "count",
      "total",
    ),
    lengthKm,
    incidentsPerKm,
    riskBand:
      getRoadProperty(properties, "risk_band", "riskBand", "band") ||
      deriveRiskBand(incidentsPerKm),
    score: getRoadNumber(properties, "score", "risk_score", "riskScore", "index"),
  };
}

function resolveItems(payload) {
  const candidates = [
    payload,
    payload?.items,
    payload?.results,
    payload?.rows,
    payload?.series,
    payload?.features,
    payload?.data,
    payload?.data?.items,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }

  return [];
}

function getRoadProperty(source, ...keys) {
  const record = source?.properties || source || {};

  for (const key of keys) {
    const value = record?.[key];

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return null;
}

function getRoadNumber(source, ...keys) {
  const value = getRoadProperty(source, ...keys);
  const numericValue = Number(value);

  return Number.isFinite(numericValue) ? numericValue : 0;
}

function getRoadLengthKm(source) {
  const lengthKm = getRoadNumber(source, "length_km", "lengthKm", "network_length_km");

  if (lengthKm > 0) {
    return lengthKm;
  }

  const lengthM = getRoadNumber(source, "length_m", "lengthM");

  if (lengthM > 0) {
    return lengthM / 1000;
  }

  return 0;
}

function createRoadSelectionKey(prefix, item, index) {
  return [
    prefix,
    getRoadProperty(item, "road_id", "roadId", "segment_id", "segmentId", "id", "highway", "name"),
    getRoadProperty(item, "name", "road_name", "roadName", "label", "title"),
    index,
  ]
    .filter(Boolean)
    .join("-");
}

function isHighRiskRoad(road) {
  const band = String(road?.riskBand || "").toLowerCase();

  if (band.includes("high") || band.includes("elevated")) {
    return true;
  }

  return Number(road?.incidentsPerKm) >= 5 || Number(road?.score) >= 5;
}

function deriveRiskBand(incidentsPerKm) {
  const value = Number(incidentsPerKm);

  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }

  if (value >= 8) {
    return "High";
  }

  if (value >= 4) {
    return "Elevated";
  }

  return "Observed";
}

function getSummaryNumber(summaryData, ...keys) {
  const records = [summaryData, summaryData?.counts, summaryData?.summary];

  for (const record of records) {
    if (!record) {
      continue;
    }

    const value = getRoadNumber(record, ...keys);

    if (value > 0) {
      return value;
    }
  }

  return 0;
}

function sumRoadLengthsKm(items) {
  return items.reduce((total, item) => total + (Number(item?.lengthKm) || 0), 0);
}

function sumCounts(items) {
  return items.reduce((total, item) => total + (Number(item?.count) || 0), 0);
}

function createDefaultFiltersFromMeta(months) {
  if (!months?.min || !months?.max) {
    return { ...DEFAULT_ROAD_FILTERS };
  }

  const maxIndex = monthToIndex(months.max);
  const minIndex = monthToIndex(months.min);
  const fromIndex = Math.max(minIndex, maxIndex - 2);

  return {
    ...DEFAULT_ROAD_FILTERS,
    monthFrom: indexToMonth(fromIndex),
    monthTo: months.max,
  };
}

function areRoadFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.lsoaName === right?.lsoaName
  );
}

function monthToIndex(month) {
  const [year, value] = month.split("-").map(Number);
  return year * 12 + (value - 1);
}

function indexToMonth(index) {
  const year = Math.floor(index / 12);
  const month = (index % 12) + 1;
  return `${year}-${String(month).padStart(2, "0")}`;
}

function formatMonthLabel(month) {
  if (!month) {
    return "Unknown month";
  }

  const [year, value] = month.slice(0, 7).split("-").map(Number);

  if (!year || !value) {
    return month;
  }

  return new Intl.DateTimeFormat("en-GB", {
    month: "short",
    year: "numeric",
  }).format(new Date(Date.UTC(year, value - 1, 1)));
}

function formatCount(value) {
  return new Intl.NumberFormat("en-GB").format(Number(value) || 0);
}

function formatDistanceKm(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "No data";
  }

  return `${numericValue.toFixed(numericValue >= 10 ? 0 : 1)} km`;
}

function formatMetricValue(value, suffix = "") {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "No data";
  }

  return `${numericValue.toFixed(numericValue >= 10 ? 0 : 2)}${suffix ? ` ${suffix}` : ""}`;
}

function formatRatio(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "No data";
  }

  return `${numericValue.toFixed(2)}x`;
}

export default RoadsPage;
