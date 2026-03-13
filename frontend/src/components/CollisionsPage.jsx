import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import FilterComponent from "./FilterComponent";
import TopBar from "./TopBar";
import { createMonthOptionsFromRange } from "../constants/crimeFilterOptions";
import { config } from "../config/env";
import { collisionsService } from "../services";
import { WEST_YORKSHIRE_BBOX, toSearchOptions } from "../utils/crimeUtils";

const COLLISION_PAGE_LIMIT = 250;
const COLLISION_BREAKDOWN_LIMIT = 1000;
const BREAKDOWN_LIMIT = 10;
const FILTER_REQUEST_DEBOUNCE_MS = 450;
const COLLISION_MONTHS = {
  min: "2025-01",
  max: "2025-06",
};
const DEFAULT_COLLISION_FILTERS = {
  monthFrom: COLLISION_MONTHS.min,
  monthTo: COLLISION_MONTHS.max,
  collisionSeverity: "",
  roadType: "",
  weatherCondition: "",
  lightCondition: "",
  roadSurfaceCondition: "",
  lsoaCode: "",
};
const WORKSPACE_TABS = [
  { id: "feed", label: "Incident Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "casualties", label: "Casualty Severity" },
  { id: "severity", label: "Collision Severity" },
  { id: "road-type", label: "Road Type" },
  { id: "weather", label: "Weather" },
  { id: "light", label: "Light" },
  { id: "surface", label: "Road Surface" },
  { id: "speed", label: "Speed Limit" },
  { id: "hour", label: "By Hour" },
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
  const [collisionFilters, setCollisionFilters] = useState({ ...DEFAULT_COLLISION_FILTERS });
  const [appliedCollisionFilters, setAppliedCollisionFilters] = useState({
    ...DEFAULT_COLLISION_FILTERS,
  });
  const [incidentPageIndex, setIncidentPageIndex] = useState(0);
  const [incidentCursorStack, setIncidentCursorStack] = useState([null]);
  const [collisionRows, setCollisionRows] = useState([]);
  const [breakdownRows, setBreakdownRows] = useState([]);
  const [catalogRows, setCatalogRows] = useState([]);
  const [incidentsMeta, setIncidentsMeta] = useState(null);
  const [breakdownMeta, setBreakdownMeta] = useState(null);
  const [summaryData, setSummaryData] = useState(null);
  const [timeseriesData, setTimeseriesData] = useState({ series: [], total: 0 });
  const [selectedCollision, setSelectedCollision] = useState(null);
  const [loadingIncidents, setLoadingIncidents] = useState(true);
  const [loadingAnalytics, setLoadingAnalytics] = useState(true);
  const [loadingBreakdowns, setLoadingBreakdowns] = useState(true);
  const [collisionErrorMessage, setCollisionErrorMessage] = useState("");
  const [analyticsErrorMessage, setAnalyticsErrorMessage] = useState("");
  const [breakdownErrorMessage, setBreakdownErrorMessage] = useState("");

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

  const monthOptions = useMemo(
    () => createMonthOptionsFromRange(COLLISION_MONTHS.min, COLLISION_MONTHS.max),
    [],
  );

  const effectiveDateRange = useMemo(
    () => ({
      from: appliedCollisionFilters.monthFrom || COLLISION_MONTHS.min,
      to: appliedCollisionFilters.monthTo || COLLISION_MONTHS.max,
    }),
    [appliedCollisionFilters.monthFrom, appliedCollisionFilters.monthTo],
  );

  const sharedCollisionQuery = useMemo(
    () => ({
      from: effectiveDateRange.from,
      to: effectiveDateRange.to,
      bbox: WEST_YORKSHIRE_BBOX,
      collisionSeverities: appliedCollisionFilters.collisionSeverity
        ? [appliedCollisionFilters.collisionSeverity]
        : undefined,
      roadTypes: appliedCollisionFilters.roadType
        ? [appliedCollisionFilters.roadType]
        : undefined,
      weatherConditions: appliedCollisionFilters.weatherCondition
        ? [appliedCollisionFilters.weatherCondition]
        : undefined,
      lightConditions: appliedCollisionFilters.lightCondition
        ? [appliedCollisionFilters.lightCondition]
        : undefined,
      roadSurfaceConditions: appliedCollisionFilters.roadSurfaceCondition
        ? [appliedCollisionFilters.roadSurfaceCondition]
        : undefined,
      lsoaCodes: appliedCollisionFilters.lsoaCode
        ? [appliedCollisionFilters.lsoaCode]
        : undefined,
    }),
    [
      appliedCollisionFilters.collisionSeverity,
      appliedCollisionFilters.lightCondition,
      appliedCollisionFilters.lsoaCode,
      appliedCollisionFilters.roadSurfaceCondition,
      appliedCollisionFilters.roadType,
      appliedCollisionFilters.weatherCondition,
      effectiveDateRange.from,
      effectiveDateRange.to,
    ],
  );

  const catalogQuery = useMemo(
    () => ({
      from: effectiveDateRange.from,
      to: effectiveDateRange.to,
      bbox: WEST_YORKSHIRE_BBOX,
    }),
    [effectiveDateRange.from, effectiveDateRange.to],
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
            limit: COLLISION_PAGE_LIMIT,
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

  useEffect(() => {
    const controller = new AbortController();

    const loadBreakdowns = async () => {
      setLoadingBreakdowns(true);
      setBreakdownErrorMessage("");

      try {
        const [breakdownResult, catalogResult] = await Promise.allSettled([
          collisionsService.getCollisionIncidents(
            {
              ...sharedCollisionQuery,
              limit: COLLISION_BREAKDOWN_LIMIT,
            },
            {
              signal: controller.signal,
            },
          ),
          collisionsService.getCollisionIncidents(
            {
              ...catalogQuery,
              limit: COLLISION_BREAKDOWN_LIMIT,
            },
            {
              signal: controller.signal,
            },
          ),
        ]);

        if (controller.signal.aborted) {
          return;
        }

        const breakdownErrors = [];

        if (breakdownResult.status === "fulfilled") {
          setBreakdownRows(
            resolveItems(breakdownResult.value?.items).map((item) =>
              normalizeCollisionRecord(item),
            ),
          );
          setBreakdownMeta(breakdownResult.value?.meta || null);
        } else {
          setBreakdownRows([]);
          setBreakdownMeta(null);
          breakdownErrors.push(
            breakdownResult.reason?.message || "Grouped incident breakdowns unavailable",
          );
        }

        if (catalogResult.status === "fulfilled") {
          setCatalogRows(
            resolveItems(catalogResult.value?.items).map((item) => normalizeCollisionRecord(item)),
          );
        } else {
          setCatalogRows([]);
          breakdownErrors.push(
            catalogResult.reason?.message || "Filter catalogue unavailable",
          );
        }

        setBreakdownErrorMessage(breakdownErrors.join(" | "));
      } finally {
        if (!controller.signal.aborted) {
          setLoadingBreakdowns(false);
        }
      }
    };

    void loadBreakdowns();

    return () => {
      controller.abort();
    };
  }, [catalogQuery, sharedCollisionQuery]);

  const collisionSeverityOptions = useMemo(
    () =>
      toSearchOptions(
        catalogRows.map((item) => item.severityLabel),
        collisionFilters.collisionSeverity,
      ),
    [catalogRows, collisionFilters.collisionSeverity],
  );

  const roadTypeOptions = useMemo(
    () => toSearchOptions(catalogRows.map((item) => item.roadType), collisionFilters.roadType),
    [catalogRows, collisionFilters.roadType],
  );

  const weatherConditionOptions = useMemo(
    () =>
      toSearchOptions(
        catalogRows.map((item) => item.weatherCondition),
        collisionFilters.weatherCondition,
      ),
    [catalogRows, collisionFilters.weatherCondition],
  );

  const lightConditionOptions = useMemo(
    () =>
      toSearchOptions(
        catalogRows.map((item) => item.lightCondition),
        collisionFilters.lightCondition,
      ),
    [catalogRows, collisionFilters.lightCondition],
  );

  const roadSurfaceOptions = useMemo(
    () =>
      toSearchOptions(
        catalogRows.map((item) => item.roadSurfaceCondition),
        collisionFilters.roadSurfaceCondition,
      ),
    [catalogRows, collisionFilters.roadSurfaceCondition],
  );

  const lsoaCodeOptions = useMemo(
    () => buildLsoaCodeOptions(catalogRows, collisionFilters.lsoaCode),
    [catalogRows, collisionFilters.lsoaCode],
  );

  const customFilterFields = useMemo(
    () => [
      {
        key: "collisionSeverity",
        label: "Collision Severity",
        options: collisionSeverityOptions,
        placeholder: "All severities",
      },
      {
        key: "roadType",
        label: "Road Type",
        options: roadTypeOptions,
        placeholder: "All road types",
      },
      {
        key: "weatherCondition",
        label: "Weather Condition",
        options: weatherConditionOptions,
        placeholder: "All weather conditions",
      },
      {
        key: "lightCondition",
        label: "Light Condition",
        options: lightConditionOptions,
        placeholder: "All light conditions",
      },
      {
        key: "roadSurfaceCondition",
        label: "Road Surface",
        options: roadSurfaceOptions,
        placeholder: "All road surfaces",
      },
      {
        key: "lsoaCode",
        label: "LSOA Code",
        options: lsoaCodeOptions,
        placeholder: "Search LSOA code",
      },
    ],
    [
      collisionSeverityOptions,
      lightConditionOptions,
      lsoaCodeOptions,
      roadSurfaceOptions,
      roadTypeOptions,
      weatherConditionOptions,
    ],
  );

  const summaryCards = useMemo(() => {
    const totalCollisions = getCollisionNumber(summaryData, "total_collisions");
    const totalCasualties = getCollisionNumber(summaryData, "total_casualties");
    const avgCasualtiesPerCollision = getCollisionNumber(
      summaryData,
      "avg_casualties_per_collision",
    );
    const fatalCasualties = getCollisionNumber(summaryData, "fatal_casualties");
    const topSeverity = resolveSummaryLeader(
      summaryData,
      ["top_collision_severity", "top_severity"],
      ["collision_severity", "severity", "label"],
    );
    const topRoadType = resolveSummaryLeader(
      summaryData,
      ["top_road_type"],
      ["road_type", "roadType", "label"],
    );

    return [
      {
        label: "Total Collisions",
        value: formatCount(totalCollisions),
        meta: `${formatMonthLabel(effectiveDateRange.from)} to ${formatMonthLabel(effectiveDateRange.to)}`,
        accent: "text-cyan-50",
      },
      {
        label: "Total Casualties",
        value: formatCount(totalCasualties),
        meta: "Direct from summary analytics",
        accent: "text-[#39ef7d]",
      },
      {
        label: "Avg Casualties / Collision",
        value: formatDecimal(avgCasualtiesPerCollision),
        meta: "Current filtered selection",
        accent: "text-[#60a5fa]",
      },
      {
        label: "Fatal Casualties",
        value: formatCount(fatalCasualties),
        meta: "Most severe casualty total",
        accent: "text-[#ff6b6b]",
      },
      {
        label: "Top Severity",
        value: topSeverity?.label || "No data",
        meta: topSeverity ? `${formatCount(topSeverity.count)} collisions` : "No collisions",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Top Road Type",
        value: topRoadType?.label || "No data",
        meta: topRoadType ? `${formatCount(topRoadType.count)} collisions` : "No collisions",
        accent: "text-[#ffb072]",
      },
    ];
  }, [effectiveDateRange.from, effectiveDateRange.to, summaryData]);

  const casualtySeverityItems = useMemo(
    () =>
      summaryData
        ? [
            {
              label: "Fatal",
              count: getCollisionNumber(summaryData, "fatal_casualties"),
            },
            {
              label: "Serious",
              count: getCollisionNumber(summaryData, "serious_casualties"),
            },
            {
              label: "Slight",
              count: getCollisionNumber(summaryData, "slight_casualties"),
            },
          ]
        : [],
    [summaryData],
  );

  const severityBreakdownItems = useMemo(
    () => buildCollisionBreakdown(breakdownRows, (item) => item.severityLabel),
    [breakdownRows],
  );

  const roadTypeBreakdownItems = useMemo(
    () => buildCollisionBreakdown(breakdownRows, (item) => item.roadType),
    [breakdownRows],
  );

  const weatherBreakdownItems = useMemo(
    () => buildCollisionBreakdown(breakdownRows, (item) => item.weatherCondition),
    [breakdownRows],
  );

  const lightBreakdownItems = useMemo(
    () => buildCollisionBreakdown(breakdownRows, (item) => item.lightCondition),
    [breakdownRows],
  );

  const roadSurfaceBreakdownItems = useMemo(
    () => buildCollisionBreakdown(breakdownRows, (item) => item.roadSurfaceCondition),
    [breakdownRows],
  );

  const speedLimitBreakdownItems = useMemo(
    () =>
      buildCollisionBreakdown(
        breakdownRows,
        (item) => item.speedLimit,
        sortSpeedBreakdownItems,
      ),
    [breakdownRows],
  );

  const hourlyBreakdownItems = useMemo(
    () => buildHourlyBreakdown(breakdownRows),
    [breakdownRows],
  );

  const filteredCollisionCount = getCollisionNumber(summaryData, "total_collisions") || breakdownRows.length;
  const isApplyingFilters = useMemo(
    () => !areCollisionFiltersEqual(collisionFilters, appliedCollisionFilters),
    [appliedCollisionFilters, collisionFilters],
  );
  const collisionStatusLabel = isApplyingFilters
    ? "Applying filters..."
    : loadingIncidents || loadingAnalytics || loadingBreakdowns
      ? "Loading collisions..."
      : collisionErrorMessage || analyticsErrorMessage || breakdownErrorMessage
        ? "Collision workspace unavailable"
        : `Showing ${formatCount(filteredCollisionCount)} collisions`;

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
              customFields={customFilterFields}
              visibleCrimeCount={filteredCollisionCount}
              mode={isApplyingFilters ? "pending" : loadingIncidents ? "loading" : "collisions"}
              layout="panel"
              title="Collision Filters"
              visibleLabel="Filtered collisions"
              categorySectionTitle="Shared Filters"
              onChange={(key, value) => {
                setCollisionFilters((current) => ({
                  ...current,
                  [key]: value,
                }));
              }}
              onClear={() => {
                setCollisionFilters({ ...DEFAULT_COLLISION_FILTERS });
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

            {breakdownErrorMessage ? (
              <div className="rounded-[20px] border border-amber-300/25 bg-amber-300/5 px-4 py-3 text-sm text-amber-100/85">
                {breakdownErrorMessage}
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
                  {isApplyingFilters
                    ? "Debounced filters"
                    : "Summary + timeseries + grouped incidents"}
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

                {activeTab === "casualties" ? (
                  <CollisionBreakdownTab
                    title="Casualty severity totals"
                    subtitle="Fatal, serious, and slight casualty totals from `/collisions/analytics/summary`."
                    items={casualtySeverityItems}
                    isLoading={loadingAnalytics}
                    emptyMessage="No casualty severity totals are available for the current selection."
                    limit={casualtySeverityItems.length}
                  />
                ) : null}

                {activeTab === "severity" ? (
                  <CollisionBreakdownTab
                    title="Collision severity breakdown"
                    subtitle="Grouped from `/collisions/incidents` using collision severity."
                    items={severityBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No collision severity breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "road-type" ? (
                  <CollisionBreakdownTab
                    title="Road type breakdown"
                    subtitle="Grouped from `/collisions/incidents` using road type."
                    items={roadTypeBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No road type breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "weather" ? (
                  <CollisionBreakdownTab
                    title="Weather condition breakdown"
                    subtitle="Grouped from `/collisions/incidents` using weather conditions."
                    items={weatherBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No weather breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "light" ? (
                  <CollisionBreakdownTab
                    title="Light condition breakdown"
                    subtitle="Grouped from `/collisions/incidents` using light conditions."
                    items={lightBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No light condition breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "surface" ? (
                  <CollisionBreakdownTab
                    title="Road surface breakdown"
                    subtitle="Grouped from `/collisions/incidents` using road surface conditions."
                    items={roadSurfaceBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No road surface breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "speed" ? (
                  <CollisionBreakdownTab
                    title="Speed limit breakdown"
                    subtitle="Grouped from `/collisions/incidents` using speed limit values."
                    items={speedLimitBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No speed limit breakdown is available for the current selection."
                  />
                ) : null}

                {activeTab === "hour" ? (
                  <CollisionBreakdownTab
                    title="Collisions by hour of day"
                    subtitle="Grouped from `/collisions/incidents` using the reported collision time."
                    items={hourlyBreakdownItems}
                    isLoading={loadingBreakdowns}
                    emptyMessage="No hour-of-day breakdown is available for the current selection."
                    limit={hourlyBreakdownItems.length}
                  />
                ) : null}
              </div>
            </div>

            {breakdownMeta?.nextCursor ? (
              <div className="rounded-[20px] border border-cyan-200/10 bg-[#030b0e]/70 px-4 py-3 text-xs text-cyan-100/60">
                Breakdown charts are built from the first {formatCount(COLLISION_BREAKDOWN_LIMIT)} incidents returned by `/collisions/incidents`.
              </div>
            ) : null}
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
          <span className="text-cyan-100/60">Feed + cards + grouped charts</span>
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
              key={`${collision.recordId ?? collision.collisionId ?? collision.location}-${collision.date ?? collision.month}`}
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
          Monthly collision counts returned by `/collisions/analytics/timeseries`.
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
          meta={peakMonth ? `${formatCount(peakMonth.count)} collisions` : ""}
        />
        <MiniMetricCard
          label="Quietest month"
          value={quietestMonth ? formatMonthLabel(quietestMonth.month) : "No data"}
          meta={quietestMonth ? `${formatCount(quietestMonth.count)} collisions` : ""}
        />
      </section>
    </div>
  );
}

function CollisionBreakdownTab({
  title,
  subtitle,
  items,
  isLoading,
  emptyMessage,
  limit = BREAKDOWN_LIMIT,
}) {
  if (isLoading) {
    return <EmptyAnalyticsState message={`Loading ${title.toLowerCase()} analytics.`} />;
  }

  if (!items.length) {
    return <EmptyAnalyticsState message={emptyMessage} />;
  }

  const visibleItems = items.slice(0, limit);
  const topCategory =
    [...items].sort(
      (left, right) => right.count - left.count || left.label.localeCompare(right.label),
    )[0] || null;
  const totalCount = items.reduce((sum, item) => sum + item.count, 0);

  return (
    <div className="grid h-full gap-4 overflow-y-auto p-4 xl:grid-cols-[minmax(0,1.2fr),320px]">
      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Bar Chart</p>
        <h3 className="mt-2 text-lg font-semibold text-cyan-50">{title}</h3>
        <p className="mt-1 text-sm text-cyan-100/60">{subtitle}</p>

        <div className="mt-6 space-y-4">
          {visibleItems.map((item) => (
            <BarRow
              key={item.label}
              item={item}
              maxValue={topCategory?.count || 1}
            />
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <MiniMetricCard
          label="Top category"
          value={topCategory?.label || "No data"}
          meta={topCategory ? `${formatCount(topCategory.count)} collisions` : ""}
        />
        <MiniMetricCard
          label="Visible categories"
          value={formatCount(visibleItems.length)}
          meta={`${formatCount(totalCount)} total collisions plotted`}
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
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.05fr),minmax(0,1fr),minmax(0,1.1fr),minmax(0,0.85fr)] ${
        isSelected ? "bg-cyan-100/10" : "bg-transparent hover:bg-white/[0.03]"
      }`}
    >
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Severity</p>
        <p className="mt-2 truncate text-sm font-semibold text-cyan-50">
          {collision.severityLabel || "Unknown"}
        </p>
        <p className="mt-1 text-xs text-cyan-100/55">
          Record {collision.recordId || "—"} {collision.collisionId ? `· ${collision.collisionId}` : ""}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Road + speed</p>
        <p className="mt-2 truncate text-sm text-cyan-50">
          {collision.roadType || "Road type unavailable"}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {collision.speedLimit || "Speed not recorded"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Conditions</p>
        <p className="mt-2 truncate text-sm text-cyan-50">
          {collision.weatherCondition || "Weather unavailable"}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {collision.lightCondition || "Light unavailable"} · {collision.roadSurfaceCondition || "Surface unavailable"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Impact</p>
        <p className="mt-2 text-sm text-cyan-50">
          {formatCount(collision.casualties)} casualties · {formatCount(collision.vehicles)} vehicles
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {formatCollisionTimestamp(collision)} · {collision.lsoaCode || collision.lsoaName || "No LSOA"}
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
          Collision Snapshot
        </h3>
        <CollisionInfoField label="Severity" value={collision.severityLabel} />
        <CollisionInfoField label="Timestamp" value={formatCollisionTimestamp(collision)} />
        <CollisionInfoField label="Record ID" value={collision.recordId} subtle />
        <CollisionInfoField label="Collision ID" value={collision.collisionId} subtle />
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-base font-medium uppercase tracking-wider text-cyan-100/50">
          Road Conditions
        </h3>
        <CollisionInfoField label="Road Type" value={collision.roadType} />
        <CollisionInfoField label="Speed Limit" value={collision.speedLimit} />
        <CollisionInfoField label="Weather" value={collision.weatherCondition} />
        <CollisionInfoField label="Light" value={collision.lightCondition} />
        <CollisionInfoField label="Road Surface" value={collision.roadSurfaceCondition} />
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-base font-medium uppercase tracking-wider text-cyan-100/50">
          Impact + Location
        </h3>
        <CollisionInfoField label="Casualties" value={collision.casualties} />
        <CollisionInfoField label="Vehicles" value={collision.vehicles} />
        <CollisionInfoField label="Location" value={collision.location} />
        <CollisionInfoField label="LSOA Code" value={collision.lsoaCode} />
        <CollisionInfoField label="LSOA Name" value={collision.lsoaName} />
        <CollisionInfoField label="Falls Within" value={collision.fallsWithin} />
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
          <p className="mt-1 text-sm text-cyan-100/70">Rendered from incident geometry / coordinates</p>
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
        collision_severity: collision.severityLabel,
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
        collision_severity: collision.severityLabel,
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
          <p className="mt-0.5 text-cyan-100/60">{formatCount(hovered.count)} collisions</p>
        </div>
      )}

      <div className="mt-3 grid gap-2 text-xs text-cyan-100/60 md:grid-cols-3 xl:grid-cols-6">
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

function MiniMetricCard({ label, value, meta = "Current filtered view" }) {
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
    left?.collisionSeverity === right?.collisionSeverity &&
    left?.roadType === right?.roadType &&
    left?.weatherCondition === right?.weatherCondition &&
    left?.lightCondition === right?.lightCondition &&
    left?.roadSurfaceCondition === right?.roadSurfaceCondition &&
    left?.lsoaCode === right?.lsoaCode
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
  const dateValue = getCollisionProperty(
    properties,
    "date",
    "collision_date",
    "accident_date",
    "reported_date",
  );

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
      "collision_reference",
      "reference",
      "accident_index",
    ),
    month:
      getCollisionProperty(properties, "month", "Month")
      || (typeof dateValue === "string" ? dateValue.slice(0, 7) : null),
    date: dateValue,
    time: getCollisionProperty(
      properties,
      "time",
      "collision_time",
      "accident_time",
      "reported_time",
    ),
    severityLabel: getCollisionProperty(
      properties,
      "collision_severity",
      "collisionSeverity",
      "severity",
      "severity_label",
      "accident_severity",
    ),
    roadType: getCollisionProperty(properties, "road_type", "roadType"),
    weatherCondition: getCollisionProperty(
      properties,
      "weather_conditions",
      "weatherCondition",
      "weather_condition",
    ),
    lightCondition: getCollisionProperty(
      properties,
      "light_conditions",
      "lightCondition",
      "light_condition",
    ),
    roadSurfaceCondition: getCollisionProperty(
      properties,
      "road_surface_conditions",
      "roadSurfaceCondition",
      "road_surface_condition",
    ),
    speedLimit: getCollisionProperty(properties, "speed_limit", "speedLimit"),
    casualties: getCollisionNumber(
      properties,
      "number_of_casualties",
      "casualties",
      "casualty_count",
    ),
    vehicles: getCollisionNumber(
      properties,
      "number_of_vehicles",
      "vehicles",
      "vehicle_count",
    ),
    reportedBy: getCollisionProperty(properties, "reportedBy", "reported_by", "police_force", "source"),
    fallsWithin: getCollisionProperty(
      properties,
      "fallsWithin",
      "falls_within",
      "local_authority_name",
      "local_authority",
    ),
    location:
      getCollisionProperty(
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

function resolveSummaryLeader(source, fieldKeys, labelKeys) {
  const leader = getCollisionProperty(source, ...fieldKeys);

  if (!leader) {
    return null;
  }

  if (typeof leader === "string") {
    return { label: leader, count: 0 };
  }

  return {
    label: getCollisionProperty(leader, ...labelKeys) || "Unknown",
    count: getCollisionNumber(leader, "count"),
  };
}

function buildCollisionBreakdown(rows, getLabel, customSort) {
  const counts = new Map();

  rows.forEach((row) => {
    const label = getLabel(row);

    if (!label) {
      return;
    }

    counts.set(label, (counts.get(label) || 0) + 1);
  });

  const items = [...counts.entries()].map(([label, count]) => ({ label, count }));

  if (customSort) {
    return customSort(items);
  }

  return items.sort(
    (left, right) => right.count - left.count || left.label.localeCompare(right.label),
  );
}

function buildHourlyBreakdown(rows) {
  if (!rows.length) {
    return [];
  }

  const hourlyCounts = new Map();

  for (let hour = 0; hour < 24; hour += 1) {
    hourlyCounts.set(String(hour).padStart(2, "0"), 0);
  }

  rows.forEach((row) => {
    const hour = parseCollisionHour(row.time);

    if (hour === null) {
      return;
    }

    const hourKey = String(hour).padStart(2, "0");
    hourlyCounts.set(hourKey, (hourlyCounts.get(hourKey) || 0) + 1);
  });

  return [...hourlyCounts.entries()].map(([hour, count]) => ({
    label: `${hour}:00`,
    count,
  }));
}

function parseCollisionHour(timeValue) {
  if (!timeValue || typeof timeValue !== "string") {
    return null;
  }

  const matched = timeValue.match(/^(\d{1,2})/);

  if (!matched) {
    return null;
  }

  const hour = Number(matched[1]);

  return Number.isInteger(hour) && hour >= 0 && hour <= 23 ? hour : null;
}

function sortSpeedBreakdownItems(items) {
  return [...items].sort((left, right) => {
    const leftSpeed = parseLeadingNumber(left.label);
    const rightSpeed = parseLeadingNumber(right.label);

    if (leftSpeed !== null && rightSpeed !== null && leftSpeed !== rightSpeed) {
      return leftSpeed - rightSpeed;
    }

    return right.count - left.count || left.label.localeCompare(right.label);
  });
}

function parseLeadingNumber(value) {
  const matched = typeof value === "string" ? value.match(/^(\d+)/) : null;
  return matched ? Number(matched[1]) : null;
}

function buildLsoaCodeOptions(rows, selectedValue = "") {
  const labels = new Map();

  rows.forEach((row) => {
    if (!row.lsoaCode) {
      return;
    }

    if (!labels.has(row.lsoaCode)) {
      labels.set(
        row.lsoaCode,
        row.lsoaName ? `${row.lsoaCode} · ${row.lsoaName}` : row.lsoaCode,
      );
    }
  });

  const options = [...labels.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([value, label]) => ({ value, label }));

  if (selectedValue && !options.some((option) => option.value === selectedValue)) {
    options.unshift({ value: selectedValue, label: selectedValue });
  }

  return options;
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

function formatDateLabel(dateValue) {
  if (!dateValue) {
    return "Unknown date";
  }

  const normalizedDate = dateValue.length === 10 ? `${dateValue}T00:00:00Z` : dateValue;
  const parsedDate = new Date(normalizedDate);

  if (Number.isNaN(parsedDate.getTime())) {
    return dateValue;
  }

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(parsedDate);
}

function formatCollisionTimestamp(collision) {
  const dateLabel = collision.date ? formatDateLabel(collision.date) : formatMonthLabel(collision.month);
  return collision.time ? `${dateLabel} · ${collision.time}` : dateLabel;
}

function formatCount(value) {
  return new Intl.NumberFormat("en-GB").format(Number(value) || 0);
}

function formatDecimal(value) {
  return new Intl.NumberFormat("en-GB", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(Number(value) || 0);
}

export default CollisionsPage;
