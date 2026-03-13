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
import { WEST_YORKSHIRE_BBOX } from "../utils/crimeUtils";

const DEFAULT_ROAD_FILTERS = {
  monthFrom: toMonthValue(new Date(new Date().getFullYear(), new Date().getMonth() - 2, 1)),
  monthTo: toMonthValue(new Date()),
  crimeType: "",
  outcomeCategory: "",
  highway: "",
  bbox: { ...WEST_YORKSHIRE_BBOX },
};

const FILTER_REQUEST_DEBOUNCE_MS = 450;
const ROAD_RISK_FETCH_LIMIT = 100;
const ROAD_RISK_PAGE_SIZE = 25;
const ROAD_CHART_LIMIT = 8;
const ROAD_WORKSPACE_TABS = [
  { id: "risk", label: "Risk Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "highways", label: "Highways" },
  { id: "crime-types", label: "Crime Types" },
  { id: "outcomes", label: "Outcomes" },
];
const RISK_SORT_OPTIONS = [
  { value: "risk_score", label: "Risk Score" },
  { value: "incident_count", label: "Incident Count" },
  { value: "incidents_per_km", label: "Incidents / km" },
];

function RoadsPage({ docsUrl }) {
  const [activeTab, setActiveTab] = useState("risk");
  const [riskSort, setRiskSort] = useState("risk_score");
  const [riskPage, setRiskPage] = useState(1);
  const [roadFilters, setRoadFilters] = useState(DEFAULT_ROAD_FILTERS);
  const [appliedRoadFilters, setAppliedRoadFilters] = useState(DEFAULT_ROAD_FILTERS);
  const [analyticsMeta, setAnalyticsMeta] = useState(null);
  const [overviewData, setOverviewData] = useState(null);
  const [chartsData, setChartsData] = useState(createEmptyRoadChartsData());
  const [riskRows, setRiskRows] = useState([]);
  const [riskMeta, setRiskMeta] = useState(null);
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
        setRoadFilters(createDefaultFiltersFromMeta(payload?.months));
        setAppliedRoadFilters(createDefaultFiltersFromMeta(payload?.months));
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
        areRoadFiltersEqual(current, roadFilters) ? current : cloneRoadFilters(roadFilters),
      );
    }, FILTER_REQUEST_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [loadingMeta, roadFilters]);

  useEffect(() => {
    setRiskPage(1);
  }, [
    appliedRoadFilters.bbox?.maxLat,
    appliedRoadFilters.bbox?.maxLon,
    appliedRoadFilters.bbox?.minLat,
    appliedRoadFilters.bbox?.minLon,
    appliedRoadFilters.crimeType,
    appliedRoadFilters.highway,
    appliedRoadFilters.monthFrom,
    appliedRoadFilters.monthTo,
    appliedRoadFilters.outcomeCategory,
    riskSort,
  ]);

  const monthOptions = useMemo(() => {
    const rangedOptions = createMonthOptionsFromRange(
      analyticsMeta?.months?.min,
      analyticsMeta?.months?.max,
    );

    return rangedOptions.length ? rangedOptions : createMonthOptions(48);
  }, [analyticsMeta?.months?.max, analyticsMeta?.months?.min]);

  const crimeTypeOptions = useMemo(() => {
    const items = analyticsMeta?.crime_types;

    if (!Array.isArray(items) || !items.length) {
      return CRIME_TYPE_OPTIONS;
    }

    return items.map((item) => ({ value: item, label: item }));
  }, [analyticsMeta?.crime_types]);

  const outcomeOptions = useMemo(() => {
    const items = analyticsMeta?.outcomes;

    if (!Array.isArray(items) || !items.length) {
      return OUTCOME_CATEGORY_OPTIONS;
    }

    return items.map((item) => ({ value: item, label: item }));
  }, [analyticsMeta?.outcomes]);

  const highwayOptions = useMemo(() => {
    const items = Array.isArray(analyticsMeta?.highways) ? analyticsMeta.highways : [];
    return items.map((item) => ({ value: item, label: item }));
  }, [analyticsMeta?.highways]);

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
      bbox: appliedRoadFilters.bbox || WEST_YORKSHIRE_BBOX,
      crimeTypes: appliedRoadFilters.crimeType ? [appliedRoadFilters.crimeType] : undefined,
      lastOutcomeCategories: appliedRoadFilters.outcomeCategory
        ? [appliedRoadFilters.outcomeCategory]
        : undefined,
      highways: appliedRoadFilters.highway ? [appliedRoadFilters.highway] : undefined,
    }),
    [
      appliedRoadFilters.bbox,
      appliedRoadFilters.crimeType,
      appliedRoadFilters.highway,
      appliedRoadFilters.outcomeCategory,
      effectiveDateRange.from,
      effectiveDateRange.to,
    ],
  );

  const panelFilters = useMemo(
    () => ({
      monthFrom: roadFilters.monthFrom,
      monthTo: roadFilters.monthTo,
      crimeType: roadFilters.crimeType,
      outcomeCategory: roadFilters.outcomeCategory,
      lsoaName: roadFilters.highway,
    }),
    [roadFilters.crimeType, roadFilters.highway, roadFilters.monthFrom, roadFilters.monthTo, roadFilters.outcomeCategory],
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
        const [overviewResult, chartsResult, riskResult] = await Promise.allSettled([
          roadsService.getRoadAnalyticsOverview(sharedRoadQuery, {
            signal: controller.signal,
          }),
          roadsService.getRoadAnalyticsCharts(
            {
              ...sharedRoadQuery,
              timeseriesGroupBy: "overall",
              groupLimit: 5,
              limit: ROAD_CHART_LIMIT,
            },
            {
              signal: controller.signal,
            },
          ),
          roadsService.getRoadAnalyticsRisk(
            {
              ...sharedRoadQuery,
              sort: riskSort,
              limit: ROAD_RISK_FETCH_LIMIT,
            },
            {
              signal: controller.signal,
            },
          ),
        ]);

        if (controller.signal.aborted) {
          return;
        }

        let nextRiskRows = [];
        let nextHighwayItems = [];
        const analyticsErrors = [];

        if (overviewResult.status === "fulfilled") {
          setOverviewData(normalizeRoadOverview(overviewResult.value));
        } else {
          setOverviewData(null);
          analyticsErrors.push(overviewResult.reason?.message || "Road overview unavailable");
        }

        if (chartsResult.status === "fulfilled") {
          const normalizedCharts = normalizeRoadCharts(chartsResult.value);
          nextHighwayItems = normalizedCharts.byHighway;
          setChartsData(normalizedCharts);
        } else {
          setChartsData(createEmptyRoadChartsData());
          analyticsErrors.push(chartsResult.reason?.message || "Road charts unavailable");
        }

        if (riskResult.status === "fulfilled") {
          const normalizedRiskData = normalizeRoadRiskResponse(riskResult.value);
          nextRiskRows = normalizedRiskData.items;
          setRiskRows(normalizedRiskData.items);
          setRiskMeta(normalizedRiskData.meta);
        } else {
          setRiskRows([]);
          setRiskMeta(null);
          setRiskErrorMessage(riskResult.reason?.message || "Road risk feed unavailable");
        }

        setSelectedRoad((current) => {
          if (!current?.selectionKey) {
            return null;
          }

          const matchedRoad =
            [...nextRiskRows, ...nextHighwayItems].find(
              (item) => item.selectionKey === current.selectionKey,
            ) || null;

          return matchedRoad ? mergeRoadSelection(current, matchedRoad) : null;
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
  }, [loadingMeta, riskSort, sharedRoadQuery]);

  const summaryCards = useMemo(() => {
    const totalIncidents = overviewData?.totalIncidents || chartsData.timeseries.total || 0;
    const roadsWithIncidents =
      overviewData?.roadsWithIncidents || analyticsMeta?.counts?.roads_with_incidents || 0;
    const roadCoveragePct = overviewData?.roadCoveragePct || 0;
    const averageIncidentsPerKm = overviewData?.averageIncidentsPerKm || 0;
    const topRoad = overviewData?.topRoad;
    const topCrimeType = overviewData?.topCrimeType;
    const topOutcome = overviewData?.topOutcome;

    return [
      {
        label: "Total Incidents",
        value: formatCount(totalIncidents),
        meta: `${formatMonthLabel(effectiveDateRange.from)} to ${formatMonthLabel(effectiveDateRange.to)}`,
        accent: "text-cyan-50",
      },
      {
        label: "Roads With Incidents",
        value: formatCount(roadsWithIncidents),
        meta: `${formatCount(overviewData?.totalSegments || analyticsMeta?.counts?.road_segments_total || 0)} segments in scope`,
        accent: "text-[#39ef7d]",
      },
      {
        label: "Road Coverage",
        value: formatPercent(roadCoveragePct),
        meta: `${formatCount(overviewData?.roadsWithoutIncidents || 0)} roads without linked incidents`,
        accent: "text-[#60a5fa]",
      },
      {
        label: "Avg Incidents / km",
        value: formatMetricValue(averageIncidentsPerKm, "incidents/km"),
        meta:
          overviewData?.currentVsPreviousPct !== null && overviewData?.currentVsPreviousPct !== undefined
            ? `${formatSignedPercent(overviewData.currentVsPreviousPct)} vs previous period`
            : "Current filtered selection",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Top Road",
        value: topRoad?.name || topRoad?.highway || "No data",
        meta: topRoad
          ? `${formatCount(topRoad.incidents)} incidents · ${formatBandLabel(topRoad.riskBand)}`
          : "No ranked road available",
        accent: "text-[#ffb072]",
      },
      {
        label: "Dominant Crime",
        value: topCrimeType?.crimeType || "No data",
        meta: topOutcome?.outcome
          ? `${topOutcome.outcome} · ${formatCount(topOutcome.count)}`
          : "No dominant outcome",
        accent: "text-[#22c55e]",
      },
    ];
  }, [analyticsMeta?.counts?.road_segments_total, analyticsMeta?.counts?.roads_with_incidents, chartsData.timeseries.total, effectiveDateRange.from, effectiveDateRange.to, overviewData]);

  const insightMessages = useMemo(() => {
    const merged = [...(overviewData?.insights || []), ...(chartsData.insights || [])].filter(Boolean);
    return [...new Set(merged)].slice(0, 4);
  }, [chartsData.insights, overviewData?.insights]);

  const isApplyingFilters = useMemo(
    () => !areRoadFiltersEqual(appliedRoadFilters, roadFilters),
    [appliedRoadFilters, roadFilters],
  );

  const roadStatusLabel = loadingMeta
    ? "Loading road metadata..."
    : isApplyingFilters
      ? "Applying filters..."
      : loadingRiskFeed
        ? "Loading dangerous roads..."
        : riskErrorMessage
          ? "Road risk feed unavailable"
          : `Showing ${formatCount(riskRows.length)} dangerous roads`;

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Roads Page"
        subtitle="Road overview, charts, and risk endpoints drive this intelligence desk."
      />

      <div className="min-h-0 flex-1 overflow-hidden p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[320px,minmax(0,1.7fr)]">
          <div className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <FilterComponent
              filters={panelFilters}
              monthOptions={monthOptions}
              crimeTypeOptions={crimeTypeOptions}
              outcomeOptions={outcomeOptions}
              lsoaOptions={highwayOptions}
              visibleCrimeCount={riskRows.length}
              mode={isApplyingFilters ? "pending" : loadingRiskFeed ? "loading" : "risk desk"}
              layout="panel"
              title="Road Filters"
              visibleLabel="Ranked roads"
              categorySectionTitle="Shared Filters"
              crimeTypeLabel="Crime Type"
              outcomeLabel="Outcome Category"
              lsoaLabel="Highway"
              lsoaPlaceholder="Filter by road type"
              lsoaEmptyMessage="No highway values are available from `/roads/analytics/meta`."
              onChange={(key, value) => {
                setRoadFilters((current) => ({
                  ...current,
                  [key === "lsoaName" ? "highway" : key]: value,
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

            {insightMessages.length ? <RoadInsightStrip insights={insightMessages} /> : null}

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
                    Dangerous roads and charts
                  </h2>
                  <p className="mt-1 text-sm text-cyan-100/60">{roadStatusLabel}</p>
                </div>

                <div className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.25em] text-cyan-100/55">
                  {isApplyingFilters ? "Debounced filters" : "Overview + charts + risk"}
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
                    riskMeta={riskMeta}
                    selectedRoad={selectedRoad}
                    page={riskPage}
                    pageSize={ROAD_RISK_PAGE_SIZE}
                    sort={riskSort}
                    onChangePage={setRiskPage}
                    onChangeSort={setRiskSort}
                    onSelectRoad={setSelectedRoad}
                    isLoading={loadingRiskFeed}
                  />
                ) : null}

                {activeTab === "timeseries" ? (
                  <RoadTimeSeriesTab
                    timeseries={chartsData.timeseries}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "highways" ? (
                  <RoadHighwaysTab
                    highwayItems={chartsData.byHighway}
                    selectedRoad={selectedRoad}
                    onSelectRoad={setSelectedRoad}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "crime-types" ? (
                  <RoadCrimeTypesTab items={chartsData.byCrimeType} isLoading={loadingAnalytics} />
                ) : null}

                {activeTab === "outcomes" ? (
                  <RoadOutcomesTab
                    outcomeItems={chartsData.byOutcome}
                    bandBreakdown={chartsData.bandBreakdown}
                    isLoading={loadingAnalytics}
                  />
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
            Meta: `/roads/analytics/meta` | Overview: `/roads/analytics/overview` | Charts:
            {" "}
            `/roads/analytics/charts` | Risk: `/roads/analytics/risk`
          </span>
        </div>

        <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
          <span>{roadStatusLabel}</span>
          <strong className="text-[#39ef7d]">Road Analytics</strong>
          <span className="text-cyan-100/60">Risk + charts</span>
        </div>
      </div>

      <SlidingRoadDrawer road={selectedRoad} onClose={() => setSelectedRoad(null)} />
    </div>
  );
}

function RoadRiskTab({
  riskRows,
  riskMeta,
  selectedRoad,
  page,
  pageSize,
  sort,
  onChangePage,
  onChangeSort,
  onSelectRoad,
  isLoading,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading dangerous roads from `/roads/analytics/risk`." />;
  }

  if (!riskRows.length) {
    return (
      <EmptyAnalyticsState message="No road segments match the current filter set. Adjust the filters to repopulate the risk feed." />
    );
  }

  const totalPages = Math.max(1, Math.ceil(riskRows.length / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const pageStartIndex = (currentPage - 1) * pageSize;
  const pagedRows = riskRows.slice(pageStartIndex, pageStartIndex + pageSize);
  const visibleStart = pageStartIndex + 1;
  const visibleEnd = pageStartIndex + pagedRows.length;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/5 px-4 py-3 text-sm text-cyan-100/70">
        <div>
          Ranked shortlist returned by `/roads/analytics/risk` using the selected sort.
        </div>

        <div className="flex flex-wrap gap-2">
          {RISK_SORT_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => onChangeSort(option.value)}
              className={`rounded-full px-3 py-1.5 text-xs uppercase tracking-[0.18em] transition-colors ${
                sort === option.value
                  ? "bg-cyan-100/10 text-cyan-50"
                  : "bg-transparent text-cyan-100/55 hover:bg-cyan-100/5 hover:text-cyan-50"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className="border-b border-white/5 px-4 py-3 text-xs uppercase tracking-[0.18em] text-cyan-100/45">
        Showing {formatCount(visibleStart)}-{formatCount(visibleEnd)} of {formatCount(riskRows.length)} loaded rows · fetch limit {formatCount(riskMeta?.limit || riskRows.length)} · sorted by {sort.replaceAll("_", " ")}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {pagedRows.map((road) => (
            <RoadRiskRow
              key={road.selectionKey}
              road={road}
              isSelected={road.selectionKey === selectedRoad?.selectionKey}
              onSelect={() => onSelectRoad(road)}
            />
          ))}
        </div>
      </div>

      {totalPages > 1 ? (
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/5 px-4 py-3">
          <p className="text-xs uppercase tracking-[0.18em] text-cyan-100/45">
            Page {formatCount(currentPage)} of {formatCount(totalPages)}
          </p>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => onChangePage(currentPage - 1)}
              disabled={currentPage === 1}
              className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
            >
              Previous
            </button>

            {Array.from({ length: totalPages }, (_, index) => index + 1).map((pageNumber) => (
              <button
                key={pageNumber}
                type="button"
                onClick={() => onChangePage(pageNumber)}
                className={`rounded-full px-3 py-1.5 text-xs uppercase tracking-[0.18em] transition-colors ${
                  currentPage === pageNumber
                    ? "bg-cyan-100/10 text-cyan-50"
                    : "bg-transparent text-cyan-100/55 hover:bg-cyan-100/5 hover:text-cyan-50"
                }`}
              >
                {pageNumber}
              </button>
            ))}

            <button
              type="button"
              onClick={() => onChangePage(currentPage + 1)}
              disabled={currentPage === totalPages}
              className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
            >
              Next
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

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

function RoadInsightStrip({ insights }) {
  return (
    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
      {insights.map((insight) => (
        <div
          key={insight}
          className="rounded-[18px] border border-cyan-100/10 bg-cyan-100/5 px-4 py-3 text-sm text-cyan-100/75"
        >
          {insight}
        </div>
      ))}
    </div>
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
            <div className="grid gap-4 grid-cols-1 grid-rows-2">
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
                      Selected from `{road.sourceType === "highway" ? "/roads/analytics/charts" : "/roads/analytics/risk"}`
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

                <div className="flex w-full justify-between gap-6 p-2">
                  <section className="w-full rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Segment Identity
                    </p>
                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      <RoadDetailField label="Road name" value={road.name || "Not supplied"} />
                      <RoadDetailField label="Highway" value={road.highway || "Unclassified"} />
                      <RoadDetailField label="Segment ID" value={road.roadId || "Not supplied"} subtle />
                      <RoadDetailField
                        label="Length"
                        value={formatDistanceKm(road.lengthKm)}
                      />
                      <RoadDetailField
                        label="Source type"
                        value={road.sourceType === "highway" ? "Charts highway group" : "Risk feed row"}
                      />
                      <RoadDetailField
                        label="Message"
                        value={road.message || "No narrative supplied"}
                        subtle
                      />
                    </div>
                  </section>

                  <section className="w-full rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Risk Metrics
                    </p>
                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      <RoadDetailField label="Incidents" value={formatCount(road.incidents)} />
                      <RoadDetailField
                        label="Incidents per km"
                        value={formatMetricValue(road.incidentsPerKm)}
                      />
                      <RoadDetailField label="Risk band" value={formatBandLabel(road.riskBand)} />
                      <RoadDetailField label="Score" value={formatMetricValue(road.score)} />
                      <RoadDetailField
                        label="Share of incidents"
                        value={formatPercent(road.shareOfIncidents)}
                      />
                      <RoadDetailField
                        label="Previous change"
                        value={formatSignedPercent(road.previousPeriodChangePct)}
                      />
                    </div>
                  </section>
                </div>
              </div>

              <section className="flex min-h-[320px] flex-col rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                      Selection Signals
                    </p>
                    <p className="mt-1 text-sm text-cyan-100/60">
                      This drawer now stays inside the analytics contract and does not fetch extra road geometry.
                    </p>
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Dominant Crime</p>
                    <p className="mt-3 text-base font-semibold text-cyan-50">
                      {road.dominantCrimeType || "No dominant crime"}
                    </p>
                  </div>

                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Dominant Outcome</p>
                    <p className="mt-3 text-base font-semibold text-cyan-50">
                      {road.dominantOutcome || "No dominant outcome"}
                    </p>
                  </div>

                  <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/60 p-4 md:col-span-2">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Narrative</p>
                    <p className="mt-3 text-sm leading-6 text-cyan-100/70">
                      {road.message || "No narrative message is available for this row."}
                    </p>
                  </div>
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
  const [hoveredIndex, setHoveredIndex] = useState(null);
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
  const hovered = hoveredIndex !== null ? points[hoveredIndex] : null;

  return (
    <div className="relative">
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

        {points.map((point, index) => (
          <g
            key={`${point.month}-${index}`}
            onMouseEnter={() => setHoveredIndex(index)}
            onMouseLeave={() => setHoveredIndex(null)}
            className="cursor-pointer"
          >
            <circle cx={point.x} cy={point.y} r="14" fill="transparent" />
            <circle
              cx={point.x}
              cy={point.y}
              r={hoveredIndex === index ? 6 : 4}
              fill="#39ef7d"
              className="transition-all duration-150"
            />
          </g>
        ))}
      </svg>

      {hovered && (
        <div
          className="pointer-events-none absolute z-20 -translate-x-1/2 -translate-y-full rounded-xl border border-cyan-100/15 bg-[#030b0e]/95 px-3 py-2 text-xs shadow-lg backdrop-blur-sm"
          style={{
            left: `${(hovered.x / width) * 100}%`,
            top: `${(hovered.y / height) * 100}%`,
          }}
        >
          <p className="font-semibold text-cyan-50">{formatMonthLabel(hovered.month)}</p>
          <p className="mt-0.5 text-cyan-100/60">{formatCount(hovered.count)} incidents</p>
        </div>
      )}

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

function BandBreakdownRow({ item, maxValue }) {
  const widthPercent = maxValue > 0 ? (item.count / maxValue) * 100 : 0;

  return (
    <div>
      <div className="mb-2 flex items-center justify-between gap-3">
        <span className="text-sm text-cyan-100/75">{item.label}</span>
        <span className="text-sm font-semibold text-cyan-50">{formatCount(item.count)}</span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-cyan-100/10">
        <div
          className={`h-full rounded-full ${item.fillClass}`}
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

function normalizeRoadOverview(payload) {
  return {
    totalSegments: getRoadNumber(payload, "total_segments"),
    totalLengthKm: getRoadLengthKm(payload),
    roadsWithIncidents: getRoadNumber(payload, "roads_with_incidents"),
    roadsWithoutIncidents: getRoadNumber(payload, "roads_without_incidents"),
    roadCoveragePct: getRoadNumber(payload, "road_coverage_pct"),
    uniqueHighwayTypes: getRoadNumber(payload, "unique_highway_types"),
    totalIncidents: getRoadNumber(payload, "total_incidents"),
    averageIncidentsPerKm: getRoadNumber(payload, "avg_incidents_per_km"),
    currentVsPreviousPct: getRoadNumber(payload, "current_vs_previous_pct"),
    topRoad: payload?.top_road ? toRoadRecord(payload.top_road) : null,
    topHighway: payload?.top_highway
      ? normalizeRoadChartHighwayItem(payload.top_highway, 0)
      : null,
    topCrimeType: payload?.top_crime_type
      ? {
          crimeType: getRoadProperty(payload.top_crime_type, "crime_type"),
          count: getRoadNumber(payload.top_crime_type, "count"),
        }
      : null,
    topOutcome: payload?.top_outcome
      ? {
          outcome: getRoadProperty(payload.top_outcome, "outcome"),
          count: getRoadNumber(payload.top_outcome, "count"),
        }
      : null,
    bandBreakdown: normalizeBandBreakdown(payload?.band_breakdown),
    insights: normalizeInsights(payload?.insights),
  };
}

function normalizeRoadCharts(payload) {
  const timeseries = payload?.timeseries || {};

  return {
    timeseries: {
      groupBy: getRoadProperty(timeseries, "groupBy", "group_by") || "overall",
      series: Array.isArray(timeseries?.series)
        ? timeseries.series.map((seriesItem, index) => ({
            key: getRoadProperty(seriesItem, "key") || `series-${index + 1}`,
            total: getRoadNumber(seriesItem, "total"),
            points: Array.isArray(seriesItem?.points)
              ? seriesItem.points
                  .map((point) => ({
                    month: getRoadProperty(point, "month"),
                    count: getRoadNumber(point, "count"),
                  }))
                  .filter((point) => point.month)
              : [],
          }))
        : [],
      total: getRoadNumber(timeseries, "total"),
      peak: timeseries?.peak
        ? {
            month: getRoadProperty(timeseries.peak, "month"),
            count: getRoadNumber(timeseries.peak, "count"),
          }
        : null,
      currentVsPreviousPct: getRoadNumber(timeseries, "current_vs_previous_pct"),
    },
    byHighway: resolveItems(payload?.by_highway).map((item, index) =>
      normalizeRoadChartHighwayItem(item, index),
    ),
    byCrimeType: resolveItems(payload?.by_crime_type).map((item, index) =>
      normalizeBreakdownItem(item, "crime_type", index),
    ),
    byOutcome: resolveItems(payload?.by_outcome).map((item, index) =>
      normalizeBreakdownItem(item, "outcome", index),
    ),
    bandBreakdown: normalizeBandBreakdown(payload?.band_breakdown),
    insights: normalizeInsights(payload?.insights),
  };
}

function normalizeRoadRiskResponse(payload) {
  const items = resolveItems(payload?.items).map((item, index) => normalizeRoadRiskItem(item, index));

  return {
    items,
    meta: {
      returned: getRoadNumber(payload?.meta, "returned") || items.length,
      limit: getRoadNumber(payload?.meta, "limit") || ROAD_RISK_FETCH_LIMIT,
      sort: getRoadProperty(payload?.meta, "sort") || "risk_score",
    },
  };
}

function normalizeRoadRiskItem(item, index) {
  const roadRecord = toRoadRecord(item);

  return {
    ...roadRecord,
    sourceType: "risk",
    selectionKey: createRoadSelectionKey("risk", item, index),
    message: getRoadProperty(item, "message"),
    shareOfIncidents: getRoadNumber(item, "share_of_incidents", "share"),
    previousPeriodChangePct: getRoadNumber(item, "previous_period_change_pct"),
    dominantCrimeType: getRoadProperty(item, "dominant_crime_type", "dominantCrimeType"),
    dominantOutcome: getRoadProperty(item, "dominant_outcome", "dominantOutcome"),
  };
}

function normalizeRoadChartHighwayItem(item, index) {
  return {
    ...toRoadRecord(item),
    sourceType: "highway",
    selectionKey: createRoadSelectionKey("highway", item, index),
    count: getRoadNumber(item, "count", "incident_count"),
    segmentCount: getRoadNumber(item, "segment_count"),
    share: getRoadNumber(item, "share", "share_of_incidents"),
    message: getRoadProperty(item, "message"),
  };
}

function normalizeBreakdownItem(item, key, index) {
  return {
    key: `${key}-${getRoadProperty(item, key) || index}`,
    label: getRoadProperty(item, key) || `Item ${index + 1}`,
    count: getRoadNumber(item, "count"),
    share: getRoadNumber(item, "share"),
  };
}

function normalizeBandBreakdown(breakdown) {
  return {
    red: getRoadNumber(breakdown, "red"),
    orange: getRoadNumber(breakdown, "orange"),
    green: getRoadNumber(breakdown, "green"),
  };
}

function normalizeBandRows(bandBreakdown) {
  return [
    { label: "Red", count: getRoadNumber(bandBreakdown, "red"), fillClass: "bg-[#ef4444]" },
    { label: "Orange", count: getRoadNumber(bandBreakdown, "orange"), fillClass: "bg-[#f97316]" },
    { label: "Green", count: getRoadNumber(bandBreakdown, "green"), fillClass: "bg-[#22c55e]" },
  ];
}

function normalizeInsights(items) {
  return Array.isArray(items) ? items.filter(Boolean) : [];
}

function hasBandBreakdown(bandBreakdown) {
  return Object.values(bandBreakdown || {}).some((value) => Number(value) > 0);
}

function resolveItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }

  return [];
}

function toRoadRecord(item) {
  const record = item?.properties || item || {};

  return {
    ...record,
    roadId: getRoadProperty(record, "segment_id", "segmentId", "road_id", "roadId", "id"),
    name: getRoadProperty(record, "name", "road_name", "roadName", "label", "title"),
    highway:
      getRoadProperty(record, "highway", "highway_type", "road_class", "classification") ||
      "Unclassified",
    incidents: getRoadNumber(record, "incident_count", "count", "incidents"),
    incidentsPerKm: getRoadNumber(record, "incidents_per_km", "incidentsPerKm"),
    lengthKm: getRoadLengthKm(record),
    score: getRoadNumber(record, "risk_score", "riskScore", "score"),
    riskBand:
      getRoadProperty(record, "band", "risk_band", "riskBand") ||
      deriveRiskBand(getRoadNumber(record, "incidents_per_km", "incidentsPerKm")),
    dominantCrimeType: getRoadProperty(record, "dominant_crime_type", "dominantCrimeType"),
    dominantOutcome: getRoadProperty(record, "dominant_outcome", "dominantOutcome"),
    message: getRoadProperty(record, "message"),
    shareOfIncidents: getRoadNumber(record, "share_of_incidents", "share"),
    previousPeriodChangePct: getRoadNumber(record, "previous_period_change_pct"),
    count: getRoadNumber(record, "count", "incident_count", "incidents"),
    segmentCount: getRoadNumber(record, "segment_count"),
  };
}

function mergeRoadSelection(current, next) {
  return {
    ...current,
    ...next,
    selectionKey: next?.selectionKey || current?.selectionKey,
    sourceType: next?.sourceType || current?.sourceType,
  };
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
  const lengthKm = getRoadNumber(source, "length_km", "lengthKm", "total_length_km");

  if (lengthKm > 0) {
    return lengthKm;
  }

  const lengthM = getRoadNumber(source, "length_m", "lengthM", "total_length_m");

  if (lengthM > 0) {
    return lengthM / 1000;
  }

  return 0;
}

function createRoadSelectionKey(prefix, item, index) {
  return [
    prefix,
    getRoadProperty(item, "segment_id", "segmentId", "road_id", "roadId", "id", "highway"),
    getRoadProperty(item, "name", "road_name", "roadName", "label"),
    index,
  ]
    .filter(Boolean)
    .join("-");
}

function cloneRoadFilters(filters) {
  return {
    ...filters,
    bbox: filters?.bbox ? { ...filters.bbox } : null,
  };
}

function createDefaultFiltersFromMeta(months) {
  if (!months?.min || !months?.max) {
    return {
      ...DEFAULT_ROAD_FILTERS,
      bbox: { ...WEST_YORKSHIRE_BBOX },
    };
  }

  const maxIndex = monthToIndex(months.max);
  const minIndex = monthToIndex(months.min);
  const fromIndex = Math.max(minIndex, maxIndex - 2);

  return {
    ...DEFAULT_ROAD_FILTERS,
    monthFrom: indexToMonth(fromIndex),
    monthTo: months.max,
    bbox: { ...WEST_YORKSHIRE_BBOX },
  };
}

function areRoadFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.highway === right?.highway &&
    areBboxesEqual(left?.bbox, right?.bbox)
  );
}

function areBboxesEqual(left, right) {
  if (!left && !right) {
    return true;
  }

  if (!left || !right) {
    return false;
  }

  return (
    left.minLon === right.minLon &&
    left.minLat === right.minLat &&
    left.maxLon === right.maxLon &&
    left.maxLat === right.maxLat
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

function deriveRiskBand(incidentsPerKm) {
  const value = Number(incidentsPerKm);

  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }

  if (value >= 8) {
    return "red";
  }

  if (value >= 4) {
    return "orange";
  }

  return "green";
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

function formatPercent(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return "0.0%";
  }

  return `${numericValue.toFixed(1)}%`;
}

function formatSignedPercent(value) {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue)) {
    return "No data";
  }

  const prefix = numericValue > 0 ? "+" : "";
  return `${prefix}${numericValue.toFixed(1)}%`;
}

function formatBandLabel(value) {
  const label = String(value || "").trim().toLowerCase();

  if (!label) {
    return "No band";
  }

  return label.charAt(0).toUpperCase() + label.slice(1);
}

function createEmptyRoadChartsData() {
  return {
    timeseries: {
      groupBy: "overall",
      series: [],
      total: 0,
      peak: null,
      currentVsPreviousPct: null,
    },
    byHighway: [],
    byCrimeType: [],
    byOutcome: [],
    bandBreakdown: { red: 0, orange: 0, green: 0 },
    insights: [],
  };
}

export default RoadsPage;
