import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import FilterComponent from "./FilterComponent";
import InfoComponents from "./InfoComponents";
import TopBar from "./TopBar";
import {
  CRIME_TYPE_OPTIONS,
  OUTCOME_CATEGORY_OPTIONS,
  createMonthOptions,
  createMonthOptionsFromRange,
} from "../constants/crimeFilterOptions";
import { config } from "../config/env";
import { crimeService } from "../services";
import {
  DEFAULT_CRIME_FILTERS,
  WEST_YORKSHIRE_BBOX,
  normalizeCrimeFeature,
  toSearchOptions,
} from "../utils/crimeUtils";

const CRIME_PAGE_LIMIT = 250;
const CRIME_PAGE_REQUEST_PAGES = 4;
const BREAKDOWN_LIMIT = 10;
const FILTER_REQUEST_DEBOUNCE_MS = 450;
const WORKSPACE_TABS = [
  { id: "feed", label: "Incident Feed" },
  { id: "timeseries", label: "Time Series" },
  { id: "types", label: "Crime Types" },
  { id: "outcomes", label: "Outcomes" },
];

function CrimePage({ docsUrl }) {
  const [activeTab, setActiveTab] = useState("feed");
  const [crimeFilters, setCrimeFilters] = useState(DEFAULT_CRIME_FILTERS);
  const [appliedCrimeFilters, setAppliedCrimeFilters] = useState(DEFAULT_CRIME_FILTERS);
  const [analyticsMeta, setAnalyticsMeta] = useState(null);
  const [crimeRows, setCrimeRows] = useState([]);
  const [incidentsMeta, setIncidentsMeta] = useState(null);
  const [summaryData, setSummaryData] = useState(null);
  const [timeseriesData, setTimeseriesData] = useState({ series: [], total: 0 });
  const [typeBreakdownData, setTypeBreakdownData] = useState({ items: [], otherCount: 0 });
  const [outcomeBreakdownData, setOutcomeBreakdownData] = useState({ items: [], otherCount: 0 });
  const [lsoaOptions, setLsoaOptions] = useState([]);
  const [selectedCrime, setSelectedCrime] = useState(null);
  const [loadingMeta, setLoadingMeta] = useState(true);
  const [loadingIncidents, setLoadingIncidents] = useState(true);
  const [loadingAnalytics, setLoadingAnalytics] = useState(true);
  const [loadingCrimeDetail, setLoadingCrimeDetail] = useState(false);
  const [crimeErrorMessage, setCrimeErrorMessage] = useState("");
  const [analyticsErrorMessage, setAnalyticsErrorMessage] = useState("");
  const [detailErrorMessage, setDetailErrorMessage] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    const loadMeta = async () => {
      setLoadingMeta(true);

      try {
        const payload = await crimeService.getAnalyticsMeta({ signal: controller.signal });

        if (controller.signal.aborted) {
          return;
        }

        setAnalyticsMeta(payload);
        setCrimeFilters((current) => {
          const defaultFilters = createDefaultFiltersFromMeta(payload?.months);
          setAppliedCrimeFilters(defaultFilters);
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
      setAppliedCrimeFilters((current) =>
        areCrimeFiltersEqual(current, crimeFilters) ? current : crimeFilters,
      );
    }, FILTER_REQUEST_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [crimeFilters, loadingMeta]);

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

  const effectiveDateRange = useMemo(
    () => ({
      from:
        appliedCrimeFilters.monthFrom ||
        analyticsMeta?.months?.min ||
        DEFAULT_CRIME_FILTERS.monthFrom,
      to:
        appliedCrimeFilters.monthTo ||
        analyticsMeta?.months?.max ||
        DEFAULT_CRIME_FILTERS.monthTo,
    }),
    [
      analyticsMeta?.months?.max,
      analyticsMeta?.months?.min,
      appliedCrimeFilters.monthFrom,
      appliedCrimeFilters.monthTo,
    ],
  );

  const sharedCrimeQuery = useMemo(
    () => ({
      from: effectiveDateRange.from,
      to: effectiveDateRange.to,
      bbox: WEST_YORKSHIRE_BBOX,
      crimeTypes: appliedCrimeFilters.crimeType ? [appliedCrimeFilters.crimeType] : undefined,
      lastOutcomeCategories: appliedCrimeFilters.outcomeCategory
        ? [appliedCrimeFilters.outcomeCategory]
        : undefined,
      lsoaNames: appliedCrimeFilters.lsoaName ? [appliedCrimeFilters.lsoaName] : undefined,
    }),
    [
      appliedCrimeFilters.crimeType,
      appliedCrimeFilters.lsoaName,
      appliedCrimeFilters.outcomeCategory,
      effectiveDateRange.from,
      effectiveDateRange.to,
    ],
  );

  useEffect(() => {
    if (loadingMeta) {
      return undefined;
    }

    const controller = new AbortController();

    const loadCrimeWorkspace = async () => {
      setLoadingIncidents(true);
      setLoadingAnalytics(true);
      setCrimeErrorMessage("");
      setAnalyticsErrorMessage("");

      try {
        const incidentsPromise = loadAllIncidentPages(sharedCrimeQuery, controller.signal);
        const [incidentsResult, summaryResult, timeseriesResult, typesResult, outcomesResult] =
          await Promise.allSettled([
            incidentsPromise,
            crimeService.getCrimeAnalyticsSummary(sharedCrimeQuery, {
              signal: controller.signal,
            }),
            crimeService.getCrimeAnalyticsTimeseries(sharedCrimeQuery, {
              signal: controller.signal,
            }),
            crimeService.getCrimeAnalyticsTypes(
              { ...sharedCrimeQuery, limit: BREAKDOWN_LIMIT },
              {
                signal: controller.signal,
              },
            ),
            crimeService.getCrimeAnalyticsOutcomes(
              { ...sharedCrimeQuery, limit: BREAKDOWN_LIMIT },
              {
                signal: controller.signal,
              },
            ),
          ]);

        if (controller.signal.aborted) {
          return;
        }

        if (incidentsResult.status === "fulfilled") {
          const normalizedCrimes = incidentsResult.value.items.map((item) => normalizeCrimeFeature(item));

          setCrimeRows(normalizedCrimes);
          setIncidentsMeta(incidentsResult.value.meta);
          setLsoaOptions(
            toSearchOptions(
              normalizedCrimes.map((crime) => crime.lsoaName),
              appliedCrimeFilters.lsoaName,
            ),
          );
          setSelectedCrime((current) => {
            if (!current?.recordId) {
              return null;
            }

            const matchedCrime =
              normalizedCrimes.find((crime) => crime.recordId === current.recordId) || null;

            if (!matchedCrime) {
              return null;
            }

            return (
              {
                ...matchedCrime,
                geometry: current.geometry || matchedCrime.geometry || null,
                lon: current.lon ?? matchedCrime.lon ?? null,
                lat: current.lat ?? matchedCrime.lat ?? null,
                context: current.context ?? matchedCrime.context ?? null,
              }
            );
          });
        } else {
          setCrimeRows([]);
          setIncidentsMeta(null);
          setLsoaOptions([]);
          setSelectedCrime(null);
          setCrimeErrorMessage(
            incidentsResult.reason?.message || "Failed to fetch crime incidents",
          );
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

        if (typesResult.status === "fulfilled") {
          setTypeBreakdownData({
            items: normalizeBreakdownItems(typesResult.value?.items, "crime_type"),
            otherCount: Number(typesResult.value?.other_count) || 0,
          });
        } else {
          setTypeBreakdownData({ items: [], otherCount: 0 });
          analyticsErrors.push(typesResult.reason?.message || "Crime type breakdown unavailable");
        }

        if (outcomesResult.status === "fulfilled") {
          setOutcomeBreakdownData({
            items: normalizeBreakdownItems(outcomesResult.value?.items, "outcome"),
            otherCount: Number(outcomesResult.value?.other_count) || 0,
          });
        } else {
          setOutcomeBreakdownData({ items: [], otherCount: 0 });
          analyticsErrors.push(
            outcomesResult.reason?.message || "Outcome breakdown unavailable",
          );
        }

        setAnalyticsErrorMessage(analyticsErrors.join(" | "));
      } finally {
        if (!controller.signal.aborted) {
          setLoadingIncidents(false);
          setLoadingAnalytics(false);
        }
      }
    };

    void loadCrimeWorkspace();

    return () => {
      controller.abort();
    };
  }, [appliedCrimeFilters.lsoaName, loadingMeta, sharedCrimeQuery]);

  useEffect(() => {
    const selectedCrimeId = selectedCrime?.recordId;
    const hasGeometry = Boolean(selectedCrime?.geometry);

    if (!selectedCrimeId) {
      setLoadingCrimeDetail(false);
      setDetailErrorMessage("");
      return undefined;
    }

    if (hasGeometry) {
      setLoadingCrimeDetail(false);
      setDetailErrorMessage("");
      return undefined;
    }

    const controller = new AbortController();

    const loadCrimeDetail = async () => {
      setLoadingCrimeDetail(true);
      setDetailErrorMessage("");

      try {
        const detailFeature = await crimeService.getCrimeById(selectedCrimeId, {
          signal: controller.signal,
        });

        if (controller.signal.aborted) {
          return;
        }

        const normalizedDetail = normalizeCrimeFeature(detailFeature);

        setSelectedCrime((current) => {
          if (!current || current.recordId !== selectedCrimeId) {
            return current;
          }

          return {
            ...current,
            ...normalizedDetail,
          };
        });
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setDetailErrorMessage(error?.message || "Failed to fetch crime detail GeoJSON");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingCrimeDetail(false);
        }
      }
    };

    void loadCrimeDetail();

    return () => {
      controller.abort();
    };
  }, [selectedCrime?.geometry, selectedCrime?.recordId]);

  const summaryCards = useMemo(() => {
    const topCrimeType = summaryData?.top_crime_type;
    const outcomeCoverage =
      summaryData?.total_crimes > 0
        ? Math.round((summaryData.crimes_with_outcomes / summaryData.total_crimes) * 100)
        : 0;

    return [
      {
        label: "Returned Incidents",
        value: formatCount(crimeRows.length),
        meta: incidentsMeta?.truncated ? "Paginated incident list" : "Current incident feed",
        accent: "text-[#39ef7d]",
      },
      {
        label: "Total Crimes",
        value: formatCount(summaryData?.total_crimes || 0),
        meta: `${formatMonthLabel(effectiveDateRange.from)} to ${formatMonthLabel(effectiveDateRange.to)}`,
        accent: "text-cyan-50",
      },
      {
        label: "Unique LSOAs",
        value: formatCount(summaryData?.unique_lsoas || 0),
        meta: "Spatial coverage in filter set",
        accent: "text-[#60a5fa]",
      },
      {
        label: "Crime Categories",
        value: formatCount(summaryData?.unique_crime_types || 0),
        meta: "Distinct crime types returned",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Top Crime Type",
        value: topCrimeType?.crime_type || "No data",
        meta: topCrimeType ? `${formatCount(topCrimeType.count)} incidents` : "No incidents",
        accent: "text-[#ffb072]",
      },
      {
        label: "Cases With Outcomes",
        value: formatCount(summaryData?.crimes_with_outcomes || 0),
        meta: `${outcomeCoverage}% with outcome data`,
        accent: "text-[#22c55e]",
      },
    ];
  }, [crimeRows.length, effectiveDateRange.from, effectiveDateRange.to, incidentsMeta?.truncated, summaryData]);

  const isApplyingFilters = useMemo(
    () => !areCrimeFiltersEqual(crimeFilters, appliedCrimeFilters),
    [appliedCrimeFilters, crimeFilters],
  );

  const crimeStatusLabel = loadingMeta
    ? "Loading filter metadata..."
    : isApplyingFilters
      ? "Applying filters..."
      : loadingIncidents
        ? "Loading incidents..."
        : crimeErrorMessage
          ? "Incident feed unavailable"
          : `Showing ${formatCount(crimeRows.length)} incidents`;

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Crime Page"
        subtitle="Canonical analytics endpoints drive the incident feed, KPI cards, and chart tabs."
      />

      <div className="min-h-0 flex-1 overflow-hidden p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[320px,minmax(0,1.7fr)]">
          <div className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <FilterComponent
              filters={crimeFilters}
              monthOptions={monthOptions}
              crimeTypeOptions={crimeTypeOptions}
              outcomeOptions={OUTCOME_CATEGORY_OPTIONS}
              lsoaOptions={lsoaOptions}
              visibleCrimeCount={crimeRows.length}
              mode={isApplyingFilters ? "pending" : loadingIncidents ? "loading" : "incidents"}
              layout="panel"
              onChange={(key, value) => {
                setCrimeFilters((current) => ({
                  ...current,
                  [key]: value,
                }));
              }}
              onClear={() => {
                setCrimeFilters(createDefaultFiltersFromMeta(analyticsMeta?.months));
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

            {crimeErrorMessage ? (
              <div className="rounded-[20px] border border-red-300/30 bg-[#480000b8] px-4 py-3 text-sm text-red-100">
                {crimeErrorMessage}
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
                    Incident feed and analytics
                  </h2>
                  <p className="mt-1 text-sm text-cyan-100/60">{crimeStatusLabel}</p>
                </div>

                <div className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.25em] text-cyan-100/55">
                  {isApplyingFilters ? "Debounced filters" : "No map endpoint"}
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
                  <IncidentFeedTab
                    crimeRows={crimeRows}
                    selectedCrime={selectedCrime}
                    onSelectCrime={setSelectedCrime}
                    isLoading={loadingIncidents}
                    isTruncated={Boolean(incidentsMeta?.nextCursor || incidentsMeta?.truncated)}
                  />
                ) : null}

                {activeTab === "timeseries" ? (
                  <TimeSeriesTab
                    series={timeseriesData.series}
                    total={timeseriesData.total}
                    isLoading={loadingAnalytics}
                  />
                ) : null}

                {activeTab === "types" ? (
                  <BreakdownTab
                    title="Crime type breakdown"
                    subtitle="Ranked distribution from `/crimes/analytics/types`."
                    items={typeBreakdownData.items}
                    otherCount={typeBreakdownData.otherCount}
                    isLoading={loadingAnalytics}
                    emptyMessage="No crime type analytics are available for the current selection."
                  />
                ) : null}

                {activeTab === "outcomes" ? (
                  <BreakdownTab
                    title="Outcome breakdown"
                    subtitle="Ranked distribution from `/crimes/analytics/outcomes`."
                    items={outcomeBreakdownData.items}
                    otherCount={outcomeBreakdownData.otherCount}
                    isLoading={loadingAnalytics}
                    emptyMessage="No outcome analytics are available for the current selection."
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
            Incidents: `/crimes/incidents` | Summary: `/crimes/analytics/summary`
          </span>
        </div>

        <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
          <span>{crimeStatusLabel}</span>
          <strong className="text-[#39ef7d]">Analytics API</strong>
          <span className="text-cyan-100/60">Drawer + charts</span>
        </div>
      </div>

      <SlidingCrimeDrawer
        crime={selectedCrime}
        isLoadingDetail={loadingCrimeDetail}
        detailErrorMessage={detailErrorMessage}
        onClose={() => setSelectedCrime(null)}
      />
    </div>
  );
}

async function loadAllIncidentPages(sharedCrimeQuery, signal) {
  const firstPage = await crimeService.getCrimeIncidents(
    {
      ...sharedCrimeQuery,
      limit: CRIME_PAGE_LIMIT,
    },
    { signal },
  );

  const items = Array.isArray(firstPage?.items) ? [...firstPage.items] : [];
  let nextCursor = firstPage?.meta?.nextCursor || null;
  let pageCount = 1;

  while (nextCursor && pageCount < CRIME_PAGE_REQUEST_PAGES && !signal.aborted) {
    const nextPage = await crimeService.getCrimeIncidents(
      {
        ...sharedCrimeQuery,
        limit: CRIME_PAGE_LIMIT,
        cursor: nextCursor,
      },
      { signal },
    );

    items.push(...(Array.isArray(nextPage?.items) ? nextPage.items : []));
    nextCursor = nextPage?.meta?.nextCursor || null;
    pageCount += 1;
  }

  return {
    items,
    meta: {
      ...(firstPage?.meta || {}),
      nextCursor,
      truncated: Boolean(firstPage?.meta?.truncated || nextCursor),
    },
  };
}

function IncidentFeedTab({ crimeRows, selectedCrime, onSelectCrime, isLoading, isTruncated }) {
  if (isLoading) {
    return <EmptyAnalyticsState message="Loading incidents from `/crimes/incidents`." />;
  }

  if (!crimeRows.length) {
    return (
      <EmptyAnalyticsState message="No crimes match this filter set. Adjust the filters to repopulate the incident feed." />
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      {isTruncated ? (
        <div className="border-b border-white/5 px-4 py-3 text-sm text-amber-100/85">
          More incident pages are available for this filter set.
        </div>
      ) : null}

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="divide-y divide-white/5">
          {crimeRows.map((crime) => (
            <CrimeFeedRow
              key={`${crime.recordId ?? crime.crimeId ?? crime.location}-${crime.month}`}
              crime={crime}
              isSelected={crime.recordId === selectedCrime?.recordId}
              onSelect={() => {
                onSelectCrime(crime);
              }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function TimeSeriesTab({ series, total, isLoading }) {
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

function BreakdownTab({ title, subtitle, items, otherCount, isLoading, emptyMessage }) {
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

function CrimeFeedRow({ crime, isSelected, onSelect }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`grid w-full gap-4 px-4 py-4 text-left transition-colors lg:grid-cols-[minmax(0,1.25fr),minmax(0,1.05fr),minmax(0,0.85fr),minmax(0,0.95fr)] ${
        isSelected ? "bg-cyan-100/10" : "bg-transparent hover:bg-white/[0.03]"
      }`}
    >
      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Crime type</p>
        <p className="mt-2 truncate text-sm font-semibold text-cyan-50">
          {crime.crimeType || "Unknown"}
        </p>
        <p className="mt-1 text-xs text-cyan-100/55">
          Record {crime.recordId || "—"}
          {crime.crimeId ? ` / Crime ${crime.crimeId}` : ""}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Location</p>
        <p className="mt-2 truncate text-sm text-cyan-50">
          {crime.location || "Location unavailable"}
        </p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {crime.lsoaName || "No LSOA recorded"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Reported</p>
        <p className="mt-2 text-sm text-cyan-50">{formatMonthLabel(crime.month)}</p>
        <p className="mt-1 truncate text-xs text-cyan-100/55">
          {crime.reportedBy || "Unknown source"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-[11px] uppercase tracking-[0.25em] text-cyan-100/45">Outcome</p>
        <p className="mt-2 text-sm text-cyan-50">
          {crime.outcomeCategory || "Pending or not recorded"}
        </p>
      </div>
    </button>
  );
}

function SlidingCrimeDrawer({ crime, isLoadingDetail, detailErrorMessage, onClose }) {
  return (
    <div className="pointer-events-none absolute inset-0 z-30 overflow-hidden">
      <button
        type="button"
        aria-label="Close crime drawer"
        onClick={onClose}
        className={`absolute inset-0 bg-black/45 transition-opacity duration-300 ${
          crime ? "pointer-events-auto opacity-100" : "opacity-0"
        }`}
      />

      <div
        className={`absolute inset-y-0 right-0 w-full border-l border-white/10 bg-[#030b0e] shadow-2xl transition-transform duration-300 sm:w-[60vw] sm:max-w-[60vw] ${
          crime ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="h-full overflow-y-auto p-4">
          {crime ? (
            <div className="grid h-full gap-4 lg:grid-cols-[minmax(260px,0.82fr),minmax(0,1.18fr)]">
              <div className="flex min-h-0 flex-col gap-3 overflow-y-auto">
                {detailErrorMessage ? (
                  <div className="rounded-xl border border-red-300/30 bg-[#480000b8] px-3 py-3 text-sm text-red-100">
                    {detailErrorMessage}
                  </div>
                ) : null}
                {isLoadingDetail ? (
                  <div className="rounded-xl border border-cyan-100/10 bg-cyan-100/5 px-3 py-3 text-sm text-cyan-100/80">
                    Loading crime GeoJSON...
                  </div>
                ) : null}

                <InfoComponents
                  compact
                  className="h-full max-w-none rounded-[20px] bg-[#071316]/70 shadow-none"
                  showActionButton={false}
                  recordId={crime.recordId}
                  crimeId={crime.crimeId}
                  crimeType={crime.crimeType}
                  month={crime.month}
                  reportedBy={crime.reportedBy}
                  fallsWithin={crime.fallsWithin}
                  location={crime.location}
                  lsoaCode={crime.lsoaCode}
                  lsoaName={crime.lsoaName}
                  outcomeCategory={crime.outcomeCategory}
                  context={crime.context}
                  onClose={onClose}
                />
              </div>

              <CrimeLocationMap crime={crime} isLoadingDetail={isLoadingDetail} />
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

const DRAWER_MAP_SOURCE_ID = "crime-drawer-source";
const DRAWER_MAP_CIRCLE_LAYER_ID = "crime-drawer-circle";
const DRAWER_MAP_HALO_LAYER_ID = "crime-drawer-circle-halo";
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

function CrimeLocationMap({ crime, isLoadingDetail }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const crimeFeature = useMemo(() => toCrimeGeoJsonFeature(crime), [crime]);
  const mapErrorMessage = !config.mapboxAccessToken
    ? "Set VITE_MAPBOX_ACCESS_TOKEN to render the crime location map."
    : !mapboxgl.supported()
      ? "Mapbox GL JS is not supported in this browser."
      : mapRuntimeErrorMessage;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return undefined;
    }

    if (!config.mapboxAccessToken) {
      return undefined;
    }

    if (!mapboxgl.supported()) {
      return undefined;
    }

    mapboxgl.accessToken = config.mapboxAccessToken;

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: DRAWER_MAP_STYLE,
      center: getCrimeFeatureCenter(crimeFeature),
      zoom: 14,
      attributionControl: false,
      interactive: false,
    });

    mapRef.current = map;

    map.on("load", () => {
      if (crimeFeature) {
        upsertCrimeMapFeature(map, crimeFeature);
        map.jumpTo({
          center: getCrimeFeatureCenter(crimeFeature),
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
  }, [crimeFeature]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map || !crimeFeature) {
      return;
    }

    const updateFeature = () => {
      upsertCrimeMapFeature(map, crimeFeature);
      map.easeTo({
        center: getCrimeFeatureCenter(crimeFeature),
        zoom: 15,
        duration: 300,
      });
    };

    if (map.isStyleLoaded()) {
      updateFeature();
      return;
    }

    map.once("load", updateFeature);
  }, [crimeFeature]);

  return (
    <div className="flex min-h-[320px] flex-col rounded-[20px] border border-white/10 bg-[#071316]/70 p-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
            Crime Location
          </p>
          <p className="mt-1 text-sm text-cyan-100/70">Rendered from GeoJSON</p>
        </div>
        {crimeFeature ? (
          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-cyan-100/60">
            {crimeFeature.geometry.type}
          </span>
        ) : null}
      </div>

      {mapErrorMessage ? (
        <div className="mb-3 rounded-xl border border-red-300/30 bg-[#480000b8] px-3 py-3 text-sm text-red-100">
          {mapErrorMessage}
        </div>
      ) : null}

      {!crimeFeature && isLoadingDetail ? (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[16px] border border-cyan-100/10 bg-cyan-100/5 text-sm text-cyan-100/70">
          Loading GeoJSON for the selected crime...
        </div>
      ) : !crimeFeature ? (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[16px] border border-cyan-100/10 bg-cyan-100/5 px-4 text-center text-sm text-cyan-100/70">
          No geometry is available for this crime.
        </div>
      ) : (
        <div ref={mapContainerRef} className="min-h-[260px] flex-1 overflow-hidden rounded-[16px]" />
      )}
    </div>
  );
}

function toCrimeGeoJsonFeature(crime) {
  if (!crime) {
    return null;
  }

  if (crime.geometry) {
    return {
      type: "Feature",
      geometry: crime.geometry,
      properties: {
        id: crime.recordId,
        crime_id: crime.crimeId,
        crime_type: crime.crimeType,
      },
    };
  }

  if (Number.isFinite(crime.lon) && Number.isFinite(crime.lat)) {
    return {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [crime.lon, crime.lat],
      },
      properties: {
        id: crime.recordId,
        crime_id: crime.crimeId,
        crime_type: crime.crimeType,
      },
    };
  }

  return null;
}

function getCrimeFeatureCenter(feature) {
  const coordinates = feature?.geometry?.type === "Point" ? feature.geometry.coordinates : null;
  return Array.isArray(coordinates) ? coordinates : [WEST_YORKSHIRE_BBOX.minLon + ((WEST_YORKSHIRE_BBOX.maxLon - WEST_YORKSHIRE_BBOX.minLon) / 2), WEST_YORKSHIRE_BBOX.minLat + ((WEST_YORKSHIRE_BBOX.maxLat - WEST_YORKSHIRE_BBOX.minLat) / 2)];
}

function upsertCrimeMapFeature(map, feature) {
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

function createDefaultFiltersFromMeta(months) {
  if (!months?.min || !months?.max) {
    return { ...DEFAULT_CRIME_FILTERS };
  }

  const maxIndex = monthToIndex(months.max);
  const minIndex = monthToIndex(months.min);
  const fromIndex = Math.max(minIndex, maxIndex - 2);

  return {
    ...DEFAULT_CRIME_FILTERS,
    monthFrom: indexToMonth(fromIndex),
    monthTo: months.max,
  };
}

function areCrimeFiltersEqual(left, right) {
  return (
    left?.monthFrom === right?.monthFrom &&
    left?.monthTo === right?.monthTo &&
    left?.crimeType === right?.crimeType &&
    left?.outcomeCategory === right?.outcomeCategory &&
    left?.lsoaName === right?.lsoaName
  );
}

function normalizeBreakdownItems(items, labelKey) {
  if (!Array.isArray(items)) {
    return [];
  }

  return items.map((item) => ({
    label: item?.[labelKey] || "Unknown",
    count: Number(item?.count) || 0,
  }));
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

export default CrimePage;
