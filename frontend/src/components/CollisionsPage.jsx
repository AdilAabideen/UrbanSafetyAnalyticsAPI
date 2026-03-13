import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import FilterComponent from "./FilterComponent";
import TopBar from "./TopBar";
import { createMonthOptions } from "../constants/crimeFilterOptions";
import { config } from "../config/env";
import { collisionsService } from "../services";
import { DEFAULT_CRIME_FILTERS, WEST_YORKSHIRE_BBOX, toSearchOptions } from "../utils/crimeUtils";

const COLLISIONS_PAGE_LIMIT = 250;
const BREAKDOWN_LIMIT = 10;
const FILTER_REQUEST_DEBOUNCE_MS = 450;
const WORKSPACE_TABS = [
  { id: "feed", label: "Incident Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "types", label: "Collision Types" },
  { id: "severity", label: "Severity" },
];

const DRAWER_MAP_SOURCE_ID = "collision-drawer-source";
const DRAWER_MAP_CIRCLE_LAYER_ID = "collision-drawer-circle";
const DRAWER_MAP_HALO_LAYER_ID = "collision-drawer-circle-halo";
const DRAWER_MAP_STYLE = {
  version: 8,
  glyphs: "mapbox://fonts/mapbox/{fontstack}/{range}.pbf",
  sources: {
    darkBase: {
      type: "raster",
      tiles: ["https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "By Adil Aabideen",
    },
  },
  layers: [
    {
      id: "dark-base-layer",
      type: "raster",
      source: "darkBase",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

function CollisionsPage({ docsUrl }) {
  const [activeTab, setActiveTab] = useState("feed");
  const [collisionFilters, setCollisionFilters] = useState({ ...DEFAULT_CRIME_FILTERS });
  const [appliedCollisionFilters, setAppliedCollisionFilters] = useState({ ...DEFAULT_CRIME_FILTERS });
  const [incidentPageIndex, setIncidentPageIndex] = useState(0);
  const [incidentCursorStack, setIncidentCursorStack] = useState([null]);
  const [collisionRows, setCollisionRows] = useState([]);
  const [incidentsMeta, setIncidentsMeta] = useState(null);
  const [summaryData, setSummaryData] = useState(null);
  const [timeseriesData, setTimeseriesData] = useState({ series: [], total: 0 });
  const [selectedCollision, setSelectedCollision] = useState(null);
  const [loadingIncidents, setLoadingIncidents] = useState(true);
  const [loadingAnalytics, setLoadingAnalytics] = useState(true);
  const [collisionErrorMessage, setCollisionErrorMessage] = useState("");
  const [analyticsErrorMessage, setAnalyticsErrorMessage] = useState("");

  useEffect(() => {
    const timerId = window.setTimeout(() => {
      setAppliedCollisionFilters((current) =>
        areCollisionFiltersEqual(current, collisionFilters)
          ? current
          : { ...collisionFilters },
      );
    }, FILTER_REQUEST_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [collisionFilters]);

  const monthOptions = useMemo(() => createMonthOptions(48), []);

  const collisionTypeOptions = useMemo(
    () =>
      toSearchOptions(
        [
          ...collisionRows.map((item) => item.collisionType),
          ...resolveItems(
            getCollisionProperty(summaryData, "top_collision_types", "top_incident_types", "top_crime_types"),
          ).map((item) =>
            getCollisionProperty(item, "collision_type", "incident_type", "crime_type", "label"),
          ),
        ],
        collisionFilters.crimeType,
      ),
    [collisionFilters.crimeType, collisionRows, summaryData],
  );

  const severityOptions = useMemo(
    () =>
      toSearchOptions(
        [
          ...collisionRows.map((item) => item.severityLabel),
          ...resolveItems(
            getCollisionProperty(summaryData, "top_severities", "top_outcomes", "top_outcome_categories"),
          ).map((item) =>
            getCollisionProperty(item, "severity", "outcome", "label", "last_outcome_category"),
          ),
        ],
        collisionFilters.outcomeCategory,
      ),
    [collisionFilters.outcomeCategory, collisionRows, summaryData],
  );

  const lsoaOptions = useMemo(
    () => toSearchOptions(collisionRows.map((item) => item.lsoaName), collisionFilters.lsoaName),
    [collisionFilters.lsoaName, collisionRows],
  );

  const effectiveDateRange = useMemo(
    () => ({
      from: appliedCollisionFilters.monthFrom || DEFAULT_CRIME_FILTERS.monthFrom,
      to: appliedCollisionFilters.monthTo || DEFAULT_CRIME_FILTERS.monthTo,
    }),
    [appliedCollisionFilters.monthFrom, appliedCollisionFilters.monthTo],
  );

  const sharedCollisionQuery = useMemo(
    () => ({
      from: effectiveDateRange.from,
      to: effectiveDateRange.to,
      bbox: WEST_YORKSHIRE_BBOX,
      collisionTypes: appliedCollisionFilters.crimeType
        ? [appliedCollisionFilters.crimeType]
        : undefined,
      severityValues: appliedCollisionFilters.outcomeCategory
        ? [appliedCollisionFilters.outcomeCategory]
        : undefined,
      lsoaNames: appliedCollisionFilters.lsoaName
        ? [appliedCollisionFilters.lsoaName]
        : undefined,
    }),
    [
      appliedCollisionFilters.crimeType,
      appliedCollisionFilters.lsoaName,
      appliedCollisionFilters.outcomeCategory,
      effectiveDateRange.from,
      effectiveDateRange.to,
    ],
  );

  useEffect(() => {
    setIncidentPageIndex(0);
    setIncidentCursorStack([null]);
    setSelectedCollision(null);
  }, [sharedCollisionQuery]);

  useEffect(() => {
    const controller = new AbortController();
    const currentCursor = incidentCursorStack[incidentPageIndex] || undefined;

    const loadIncidents = async () => {
      setLoadingIncidents(true);
      setCollisionErrorMessage("");

      try {
        const incidentsResult = await collisionsService.getCollisionIncidents(
          {
            ...sharedCollisionQuery,
            limit: COLLISIONS_PAGE_LIMIT,
            cursor: currentCursor,
          },
          {
            signal: controller.signal,
          },
        );

        if (controller.signal.aborted) {
          return;
        }

        const normalizedCollisions = resolveItems(incidentsResult?.items).map((item) =>
          normalizeCollisionRecord(item),
        );

        setCollisionRows(normalizedCollisions);
        setIncidentsMeta(incidentsResult?.meta || null);
        setSelectedCollision((current) => {
          if (!current?.recordId) {
            return null;
          }

          return (
            normalizedCollisions.find((collision) => collision.recordId === current.recordId) ||
            null
          );
        });
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setCollisionRows([]);
        setIncidentsMeta(null);
        setSelectedCollision(null);
        setCollisionErrorMessage(error?.message || "Failed to fetch collision incidents");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingIncidents(false);
        }
      }
    };

    void loadIncidents();

    return () => {
      controller.abort();
    };
  }, [incidentCursorStack, incidentPageIndex, sharedCollisionQuery]);

  useEffect(() => {
    const controller = new AbortController();

    const loadAnalytics = async () => {
      setLoadingAnalytics(true);
      setAnalyticsErrorMessage("");

      try {
        const [summaryResult, timeseriesResult] = await Promise.allSettled([
          collisionsService.getCollisionAnalyticsSummary(sharedCollisionQuery, {
            signal: controller.signal,
          }),
          collisionsService.getCollisionAnalyticsTimeseries(sharedCollisionQuery, {
            signal: controller.signal,
          }),
        ]);

        if (controller.signal.aborted) {
          return;
        }

        const analyticsErrors = [];

        if (summaryResult.status === "fulfilled") {
          setSummaryData(summaryResult.value);
        } else {
          setSummaryData(null);
          analyticsErrors.push(summaryResult.reason?.message || "Summary unavailable");
        }

        if (timeseriesResult.status === "fulfilled") {
          setTimeseriesData({
            series: Array.isArray(timeseriesResult.value?.series)
              ? timeseriesResult.value.series
              : [],
            total: Number(timeseriesResult.value?.total) || 0,
          });
        } else {
          setTimeseriesData({ series: [], total: 0 });
          analyticsErrors.push(timeseriesResult.reason?.message || "Time series unavailable");
        }

        setAnalyticsErrorMessage(analyticsErrors.join(" | "));
      } finally {
        if (!controller.signal.aborted) {
          setLoadingAnalytics(false);
        }
      }
    };

    void loadAnalytics();

    return () => {
      controller.abort();
    };
  }, [sharedCollisionQuery]);

  const summaryCards = useMemo(() => {
    const totalCollisions = getCollisionNumber(
      summaryData,
      "total_collisions",
      "total_incidents",
      "total_crimes",
    );
    const uniqueLsoas = getCollisionNumber(summaryData, "unique_lsoas");
    const uniqueCollisionTypes = getCollisionNumber(
      summaryData,
      "unique_collision_types",
      "unique_incident_types",
      "unique_crime_types",
    );
    const collisionsWithSeverity = getCollisionNumber(
      summaryData,
      "collisions_with_severity",
      "collisions_with_outcomes",
      "incidents_with_outcomes",
      "crimes_with_outcomes",
    );
    const topCollisionType =
      getCollisionProperty(summaryData, "top_collision_type", "top_incident_type", "top_crime_type")
      || null;
    const severityCoverage =
      totalCollisions > 0 ? Math.round((collisionsWithSeverity / totalCollisions) * 100) : 0;

    return [
      {
        label: "Returned Collisions",
        value: formatCount(collisionRows.length),
        meta: incidentsMeta?.nextCursor
          ? `Page ${incidentPageIndex + 1} with more collisions available`
          : `Page ${incidentPageIndex + 1} collision feed`,
        accent: "text-[#39ef7d]",
      },
      {
        label: "Total Collisions",
        value: formatCount(totalCollisions),
        meta: `${formatMonthLabel(effectiveDateRange.from)} to ${formatMonthLabel(effectiveDateRange.to)}`,
        accent: "text-cyan-50",
      },
      {
        label: "Unique LSOAs",
        value: formatCount(uniqueLsoas),
        meta: "Spatial coverage in filter set",
        accent: "text-[#60a5fa]",
      },
      {
        label: "Collision Categories",
        value: formatCount(uniqueCollisionTypes),
        meta: "Distinct types returned",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Top Collision Type",
        value:
          getCollisionProperty(topCollisionType, "collision_type", "incident_type", "crime_type")
          || "No data",
        meta: topCollisionType
          ? `${formatCount(getCollisionNumber(topCollisionType, "count"))} records`
          : "No collisions",
        accent: "text-[#ffb072]",
      },
      {
        label: "Records With Severity",
        value: formatCount(collisionsWithSeverity),
        meta: `${severityCoverage}% with severity data`,
        accent: "text-[#22c55e]",
      },
    ];
  }, [collisionRows.length, effectiveDateRange.from, effectiveDateRange.to, incidentPageIndex, incidentsMeta?.nextCursor, summaryData]);

  const typeBreakdownItems = useMemo(
    () =>
      normalizeBreakdownItems(
        getCollisionProperty(summaryData, "top_collision_types", "top_incident_types", "top_crime_types"),
        "collision_type",
        "incident_type",
        "crime_type",
      ),
    [summaryData],
  );

  const severityBreakdownItems = useMemo(
    () =>
      normalizeBreakdownItems(
        getCollisionProperty(summaryData, "top_severities", "top_outcomes", "top_outcome_categories"),
        "severity",
        "outcome",
        "last_outcome_category",
      ),
    [summaryData],
  );

  const isApplyingFilters = useMemo(
    () => !areCollisionFiltersEqual(collisionFilters, appliedCollisionFilters),
    [appliedCollisionFilters, collisionFilters],
  );

  const collisionStatusLabel = isApplyingFilters
    ? "Applying filters..."
    : loadingIncidents
      ? "Loading collisions..."
      : collisionErrorMessage
        ? "Collision feed unavailable"
        : `Showing ${formatCount(collisionRows.length)} collisions`;

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Collisions Page"
        subtitle="Collision incidents, KPI cards, and chart tabs mirror the crime intelligence desk."
      />

      <div className="min-h-0 flex-1 overflow-hidden p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[320px,minmax(0,1.7fr)]">
          <div className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <FilterComponent
              filters={collisionFilters}
              monthOptions={monthOptions}
              crimeTypeOptions={collisionTypeOptions}
              outcomeOptions={severityOptions}
              lsoaOptions={lsoaOptions}
              visibleCrimeCount={collisionRows.length}
              mode={isApplyingFilters ? "pending" : loadingIncidents ? "loading" : "collisions"}
              layout="panel"
              title="Collision Filters"
              visibleLabel="Visible collisions"
              categorySectionTitle="Shared Filters"
              crimeTypeLabel="Collision Type"
              outcomeLabel="Severity / Outcome"
              onChange={(key, value) => {
                setCollisionFilters((current) => ({
                  ...current,
                  [key]: value,
                }));
              }}
              onClear={() => {
                setCollisionFilters({ ...DEFAULT_CRIME_FILTERS });
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

            {collisionErrorMessage ? (
              <div className="rounded-[20px] border border-red-300/30 bg-[#480000b8] px-4 py-3 text-sm text-red-100">
                {collisionErrorMessage}
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
                    Collision feed and analytics
                  </h2>
                  <p className="mt-1 text-sm text-cyan-100/60">{collisionStatusLabel}</p>
                </div>

                <div className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.25em] text-cyan-100/55">
                  {isApplyingFilters ? "Debounced filters" : "Incidents + summary + timeseries"}
                </div>
              </div>

              <div className="flex flex-wrap gap-2 border-b border-white/5 px-4 py-3">
                {WORKSPACE_TABS.map((tab) => (
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
                {activeTab === "feed" ? (
                  <CollisionFeedTab
                    collisionRows={collisionRows}
                    hasNextPage={Boolean(incidentsMeta?.nextCursor)}
                    hasPreviousPage={incidentPageIndex > 0}
                    pageNumber={incidentPageIndex + 1}
                    selectedCollision={selectedCollision}
                    onNextPage={() => {
                      if (!incidentsMeta?.nextCursor) {
                        return;
                      }

                      setIncidentCursorStack((current) =>
                        current[incidentPageIndex + 1]
                          ? current
                          : [...current, incidentsMeta.nextCursor],
                      );
                      setIncidentPageIndex((current) => current + 1);
                    }}
                    onPreviousPage={() => {
                      setIncidentPageIndex((current) => Math.max(0, current - 1));
                    }}
                    onSelectCollision={setSelectedCollision}
                    isLoading={loadingIncidents}
                  />
                ) : null}

                {activeTab === "timeseries" ? (
                  <CollisionTimeSeriesTab
                    series={timeseriesData.series}
                    total={timeseriesData.total}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "types" ? (
                  <CollisionBreakdownTab
                    title="Collision type breakdown"
                    subtitle="Ranked distribution from the summary endpoint."
                    items={typeBreakdownItems}
                    isLoading={loadingAnalytics}
                    emptyMessage="No collision type analytics are available for the current selection."
                  />
                ) : null}

                {activeTab === "severity" ? (
                  <CollisionBreakdownTab
                    title="Severity breakdown"
                    subtitle="Ranked severity distribution from the summary endpoint."
                    items={severityBreakdownItems}
                    isLoading={loadingAnalytics}
                    emptyMessage="No severity analytics are available for the current selection."
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
            Incidents: {"/collisions/incidents"} | Summary: {"/collisions/analytics/summary"} |
            {" "}
            Timeseries: {"/collisions/analytics/timeseries"}
          </span>
        </div>

        <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
          <span>{collisionStatusLabel}</span>
          <strong className="text-[#39ef7d]">Collisions API</strong>
          <span className="text-cyan-100/60">Drawer + charts</span>
        </div>
      </div>

      <SlidingCollisionDrawer
        collision={selectedCollision}
        onClose={() => setSelectedCollision(null)}
      />
    </div>
  );
}

function CollisionFeedTab({
  collisionRows,
  hasNextPage,
  hasPreviousPage,
  pageNumber,
  selectedCollision,
  onNextPage,
  onPreviousPage,
  onSelectCollision,
  isLoading,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading collisions from `/collisions/incidents`." />;
  }

  if (!collisionRows.length) {
    return (
      <EmptyAnalyticsState message="No collisions match this filter set. Adjust the filters to repopulate the incident feed." />
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/5 px-4 py-3 text-sm text-cyan-100/70">
        <div>Paginated collision rows returned by `/collisions/incidents`.</div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onPreviousPage}
            disabled={!hasPreviousPage}
            className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
          >
            Previous
          </button>

          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-100/65">
            Page {formatCount(pageNumber)}
          </span>

          <button
            type="button"
            onClick={onNextPage}
            disabled={!hasNextPage}
            className="rounded-full border border-white/10 px-3 py-1.5 text-xs uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
          >
            Next
          </button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {collisionRows.map((collision) => (
            <CollisionFeedRow
              key={`${collision.recordId ?? collision.collisionId ?? collision.location}-${collision.month}`}
              collision={collision}
              isSelected={collision.recordId === selectedCollision?.recordId}
              onSelect={() => {
                onSelectCollision(collision);
              }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function CollisionTimeSeriesTab({ series, total, isLoading }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading time series from `/collisions/analytics/timeseries`." />;
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
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Monthly collision curve</h3>
        <p className="mt-1 text-sm text-cyan-100/60">
          Monthly counts returned by `/collisions/analytics/timeseries`.
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
          meta={peakMonth ? `${formatCount(peakMonth.count)} records` : ""}
        />
        <MiniMetricCard
          label="Quietest month"
          value={quietestMonth ? formatMonthLabel(quietestMonth.month) : "No data"}
          meta={quietestMonth ? `${formatCount(quietestMonth.count)} records` : ""}
        />
      </section>
    </div>
  );
}

function CollisionBreakdownTab({ title, subtitle, items, isLoading, emptyMessage }) {
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
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top category"
          value={topItems[0]?.label || "No data"}
          meta={topItems[0] ? `${formatCount(topItems[0].count)} records` : ""}
        />
        <MiniMetricCard
          label="Visible categories"
          value={formatCount(items.length)}
          meta="Direct ranked result"
        />
      </section>
    </div>
  );
}

function CollisionFeedRow({ collision, isSelected, onSelect }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.25fr),minmax(0,1.05fr),minmax(0,0.85fr),minmax(0,0.95fr)] ${
        isSelected ? "bg-cyan-100/10" : "bg-transparent hover:bg-white/[0.03]"
      }`}
    >
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Collision type</p>
        <p className="mt-2 truncate text-sm font-semibold text-cyan-50">
          {collision.collisionType || "Unknown"}
        </p>
        <p className="mt-1 text-xs text-cyan-100/55">
          Record {collision.recordId || "—"}
          {collision.collisionId ? ` / Collision ${collision.collisionId}` : ""}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Location</p>
        <p className="mt-2 truncate text-sm text-cyan-50">
          {collision.location || "Location unavailable"}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {collision.lsoaName || "No LSOA recorded"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Reported</p>
        <p className="mt-2 text-sm text-cyan-50">{formatMonthLabel(collision.month)}</p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {collision.reportedBy || "Unknown source"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Severity</p>
        <p className="mt-2 text-sm text-cyan-50">
          {collision.severityLabel || "Pending or not recorded"}
        </p>
      </div>
    </button>
  );
}

function SlidingCollisionDrawer({ collision, onClose }) {
  return (
    <div className="pointer-events-none absolute inset-0 z-30 overflow-hidden">
      <button
        type="button"
        aria-label="Close collision drawer"
        onClick={onClose}
        className={`absolute inset-0 bg-black/45 transition-opacity duration-300 ${
          collision ? "pointer-events-auto opacity-100" : "opacity-0"
        }`}
      />

      <div
        className={`absolute inset-y-0 right-0 w-full border-l border-white/10 bg-[#030b0e] shadow-2xl transition-transform duration-300 sm:w-[60vw] sm:max-w-[60vw] ${
          collision ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="h-full overflow-y-auto p-4">
          {collision ? (
            <div className="grid h-full gap-4 lg:grid-cols-[minmax(260px,0.82fr),minmax(0,1.18fr)]">
              <div className="flex min-h-0 flex-col gap-3 overflow-y-auto">
                <CollisionInfoPanel collision={collision} onClose={onClose} />
              </div>

              <CollisionLocationMap collision={collision} />
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function CollisionInfoPanel({ collision, onClose }) {
  return (
    <div className="pointer-events-auto flex h-full w-full flex-col gap-3 overflow-y-auto rounded-[20px] border border-cyan-200/10 bg-[#071316]/70 p-3 shadow-none">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold tracking-wide text-cyan-50">
          Collision Information
        </h2>
        <button
          type="button"
          onClick={onClose}
          className="flex h-7 w-7 items-center justify-center rounded-md text-cyan-100/50 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
        >
          <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        </button>
      </div>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-base font-medium uppercase tracking-wider text-cyan-100/50">
          Type of Collision
        </h3>
        <div className="flex items-center justify-between text-sm text-cyan-50">
          <span className="text-lg font-semibold">{collision.collisionType ?? "—"}</span>
        </div>
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-base font-medium uppercase tracking-wider text-cyan-100/50">
          Collision Report
        </h3>
        <CollisionInfoField label="Record ID" value={collision.recordId} subtle />
        <CollisionInfoField label="Collision ID" value={collision.collisionId} subtle />
        <CollisionInfoField label="Month" value={collision.month} />
        <CollisionInfoField label="Reported By" value={collision.reportedBy} />
        <CollisionInfoField label="Falls Within" value={collision.fallsWithin} />
        <CollisionInfoField label="Location" value={collision.location} />
        <CollisionInfoField label="LSOA Code" value={collision.lsoaCode} />
        <CollisionInfoField label="LSOA Name" value={collision.lsoaName} />
        <CollisionInfoField label="Severity" value={collision.severityLabel} />
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-base font-medium uppercase tracking-wider text-cyan-100/50">
          Further Information
        </h3>
        <CollisionInfoField label="Context" value={collision.context} />
      </section>
    </div>
  );
}

function CollisionInfoField({ label, value, subtle = false }) {
  const valueClassName = subtle
    ? "text-sm font-medium break-all"
    : "text-base font-semibold";

  return (
    <div className="flex flex-col items-start text-sm text-cyan-50">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}:
      </span>
      <span className={valueClassName}>{value ?? "—"}</span>
    </div>
  );
}

function CollisionLocationMap({ collision }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const collisionFeature = useMemo(() => toCollisionGeoJsonFeature(collision), [collision]);
  const mapErrorMessage = !config.mapboxAccessToken
    ? "Set VITE_MAPBOX_ACCESS_TOKEN to render the collision location map."
    : !mapboxgl.supported()
      ? "Mapbox GL JS is not supported in this browser."
      : mapRuntimeErrorMessage;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return undefined;
    }

    if (!config.mapboxAccessToken || !mapboxgl.supported()) {
      return undefined;
    }

    mapboxgl.accessToken = config.mapboxAccessToken;

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: DRAWER_MAP_STYLE,
      center: getCollisionFeatureCenter(collisionFeature),
      zoom: 14,
      attributionControl: false,
      interactive: false,
    });

    mapRef.current = map;

    map.on("load", () => {
      if (collisionFeature) {
        upsertCollisionMapFeature(map, collisionFeature);
        map.jumpTo({
          center: getCollisionFeatureCenter(collisionFeature),
          zoom: 15,
        });
      }
    });

    map.on("error", (event) => {
      const message = event?.error?.message;

      if (message) {
        setMapRuntimeErrorMessage(message);
      }
    });

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [collisionFeature]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map || !collisionFeature) {
      return;
    }

    const updateFeature = () => {
      upsertCollisionMapFeature(map, collisionFeature);
      map.easeTo({
        center: getCollisionFeatureCenter(collisionFeature),
        zoom: 15,
        duration: 300,
      });
    };

    if (map.isStyleLoaded()) {
      updateFeature();
      return;
    }

    map.once("load", updateFeature);
  }, [collisionFeature]);

  return (
    <div className="flex min-h-[320px] flex-col rounded-[20px] border border-white/10 bg-[#071316]/70 p-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
            Collision Location
          </p>
          <p className="mt-1 text-sm text-cyan-100/70">Rendered from row GeoJSON / coordinates</p>
        </div>
        {collisionFeature ? (
          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-cyan-100/60">
            {collisionFeature.geometry.type}
          </span>
        ) : null}
      </div>

      {mapErrorMessage ? (
        <div className="mb-3 rounded-xl border border-red-300/30 bg-[#480000b8] px-3 py-3 text-sm text-red-100">
          {mapErrorMessage}
        </div>
      ) : null}

      {!collisionFeature ? (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[16px] border border-cyan-100/10 bg-cyan-100/5 px-4 text-center text-sm text-cyan-100/70">
          No geometry is available for this collision.
        </div>
      ) : (
        <div ref={mapContainerRef} className="min-h-[260px] flex-1 overflow-hidden rounded-[16px]" />
      )}
    </div>
  );
}

function toCollisionGeoJsonFeature(collision) {
  if (!collision) {
    return null;
  }

  if (collision.geometry) {
    return {
      type: "Feature",
      geometry: collision.geometry,
      properties: {
        id: collision.recordId,
        collision_id: collision.collisionId,
        collision_type: collision.collisionType,
      },
    };
  }

  if (Number.isFinite(collision.lon) && Number.isFinite(collision.lat)) {
    return {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [collision.lon, collision.lat],
      },
      properties: {
        id: collision.recordId,
        collision_id: collision.collisionId,
        collision_type: collision.collisionType,
      },
    };
  }

  return null;
}

function getCollisionFeatureCenter(feature) {
  const coordinates = feature?.geometry?.type === "Point" ? feature.geometry.coordinates : null;

  return Array.isArray(coordinates)
    ? coordinates
    : [
        WEST_YORKSHIRE_BBOX.minLon +
          ((WEST_YORKSHIRE_BBOX.maxLon - WEST_YORKSHIRE_BBOX.minLon) / 2),
        WEST_YORKSHIRE_BBOX.minLat +
          ((WEST_YORKSHIRE_BBOX.maxLat - WEST_YORKSHIRE_BBOX.minLat) / 2),
      ];
}

function upsertCollisionMapFeature(map, feature) {
  const sourceData = {
    type: "FeatureCollection",
    features: [feature],
  };

  const existingSource = map.getSource(DRAWER_MAP_SOURCE_ID);

  if (existingSource) {
    existingSource.setData(sourceData);
    return;
  }

  map.addSource(DRAWER_MAP_SOURCE_ID, {
    type: "geojson",
    data: sourceData,
  });

  map.addLayer({
    id: DRAWER_MAP_HALO_LAYER_ID,
    type: "circle",
    source: DRAWER_MAP_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-radius": 16,
      "circle-color": "rgba(34, 211, 238, 0.18)",
      "circle-stroke-width": 0,
    },
  });

  map.addLayer({
    id: DRAWER_MAP_CIRCLE_LAYER_ID,
    type: "circle",
    source: DRAWER_MAP_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-radius": 7,
      "circle-color": "#22d3ee",
      "circle-stroke-color": "#071316",
      "circle-stroke-width": 2,
    },
  });
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
          <p className="mt-0.5 text-cyan-100/60">{formatCount(hovered.count)} records</p>
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

function areCollisionFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.lsoaName === right?.lsoaName
  );
}

function resolveItems(items) {
  return Array.isArray(items) ? items : [];
}

function getCollisionProperty(source, ...keys) {
  const record = source?.properties || source || {};

  for (const key of keys) {
    const value = record?.[key];

    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return null;
}

function getCollisionNumber(source, ...keys) {
  const value = getCollisionProperty(source, ...keys);
  const numericValue = Number(value);

  return Number.isFinite(numericValue) ? numericValue : 0;
}

function normalizeCollisionRecord(feature) {
  const properties = feature?.properties || feature || {};
  const coordinates =
    feature?.geometry?.type === "Point"
      ? feature.geometry.coordinates
      : Number.isFinite(Number(properties?.lon)) && Number.isFinite(Number(properties?.lat))
        ? [Number(properties.lon), Number(properties.lat)]
        : null;

  return {
    ...properties,
    geometry: feature?.geometry || null,
    lon: coordinates ? Number(coordinates[0]) : getCollisionNumber(properties, "lon", "longitude"),
    lat: coordinates ? Number(coordinates[1]) : getCollisionNumber(properties, "lat", "latitude"),
    recordId: getCollisionProperty(properties, "recordId", "record_id", "id", "ID"),
    collisionId: getCollisionProperty(
      properties,
      "collisionId",
      "collision_id",
      "incident_id",
      "reference",
      "collision_reference",
    ),
    month: getCollisionProperty(properties, "month", "Month"),
    collisionType: getCollisionProperty(
      properties,
      "collisionType",
      "collision_type",
      "incident_type",
      "accident_type",
      "crime_type",
    ),
    severityLabel: getCollisionProperty(
      properties,
      "severity",
      "severity_label",
      "accident_severity",
      "casualty_severity",
      "outcomeCategory",
      "last_outcome_category",
    ),
    reportedBy: getCollisionProperty(
      properties,
      "reportedBy",
      "reported_by",
      "police_force",
      "source",
    ),
    fallsWithin: getCollisionProperty(
      properties,
      "fallsWithin",
      "falls_within",
      "area_name",
      "borough",
    ),
    location: getCollisionProperty(
      properties,
      "location",
      "location_text",
      "road_name",
      "street_name",
      "display_location",
    ) || (coordinates ? `${coordinates[1]}, ${coordinates[0]}` : null),
    lsoaCode: getCollisionProperty(properties, "lsoaCode", "lsoa_code"),
    lsoaName: getCollisionProperty(properties, "lsoaName", "lsoa_name"),
    context: getCollisionProperty(properties, "context", "Context"),
  };
}

function normalizeBreakdownItems(items, ...labelKeys) {
  return resolveItems(items)
    .map((item) => ({
      label: getCollisionProperty(item, ...labelKeys) || "Unknown",
      count: getCollisionNumber(item, "count"),
    }))
    .filter((item) => item.label);
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

export default CollisionsPage;
