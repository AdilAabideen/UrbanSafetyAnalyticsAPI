import { useEffect, useMemo, useState } from "react";
import TopBar from "./TopBar";
import { watchlistService } from "../services";

const METRICS_TABS = [
  { id: "previous-metrics", label: "Previous Metrics" },
  { id: "new-metrics", label: "New Metrics" },
];
const PREVIOUS_METRICS_SUBTABS = [
  { id: "forecast", label: "Forecast" },
  { id: "risk", label: "Risk" },
  { id: "hotspots", label: "Hotspots" },
];
const NEW_METRICS_SUBTABS = [
  { id: "run-forecast", label: "Run Forecast" },
  { id: "run-risk", label: "Run Risk" },
  { id: "run-hotspots", label: "Run Hotspots" },
];

function ViewWatchlistPage({
  docsUrl,
  accessToken,
  selectedWatchlistId,
  onSelectWatchlist,
  onCreateNew,
}) {
  const [watchlists, setWatchlists] = useState([]);
  const [selectedWatchlist, setSelectedWatchlist] = useState(null);
  const [loadingList, setLoadingList] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [listErrorMessage, setListErrorMessage] = useState("");
  const [detailErrorMessage, setDetailErrorMessage] = useState("");
  const [metricsDrawerOpen, setMetricsDrawerOpen] = useState(false);
  const [activeMetricsTab, setActiveMetricsTab] = useState(METRICS_TABS[0].id);

  useEffect(() => {
    if (!accessToken) {
      setWatchlists([]);
      setSelectedWatchlist(null);
      setListErrorMessage("");
      setDetailErrorMessage("");
      return undefined;
    }

    const controller = new AbortController();

    const loadWatchlists = async () => {
      setLoadingList(true);
      setListErrorMessage("");

      try {
        const items = await watchlistService.getWatchlists(accessToken, {
          signal: controller.signal,
        });

        if (controller.signal.aborted) {
          return;
        }

        setWatchlists(items);

        if (!selectedWatchlistId && items.length) {
          onSelectWatchlist?.(items[0].id);
        }

        if (selectedWatchlistId) {
          const matchedWatchlist = items.find((item) => item.id === selectedWatchlistId) || null;

          if (matchedWatchlist) {
            setSelectedWatchlist((current) =>
              current?.id === matchedWatchlist.id
                ? {
                    ...matchedWatchlist,
                    preference: current.preference || matchedWatchlist.preference,
                  }
                : matchedWatchlist,
            );
          }
        }
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setWatchlists([]);
        setListErrorMessage(error?.message || "Failed to load watchlists.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingList(false);
        }
      }
    };

    void loadWatchlists();

    return () => {
      controller.abort();
    };
  }, [accessToken, onSelectWatchlist, selectedWatchlistId]);

  useEffect(() => {
    if (!accessToken || !selectedWatchlistId) {
      if (!watchlists.length) {
        setSelectedWatchlist(null);
      }

      setDetailErrorMessage("");
      setLoadingDetail(false);
      return undefined;
    }

    const controller = new AbortController();

    const loadWatchlist = async () => {
      setLoadingDetail(true);
      setDetailErrorMessage("");

      try {
        const watchlist = await watchlistService.getWatchlistById(selectedWatchlistId, accessToken, {
          signal: controller.signal,
        });

        if (controller.signal.aborted) {
          return;
        }

        setSelectedWatchlist(watchlist);
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setDetailErrorMessage(error?.message || "Failed to load the selected watchlist.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingDetail(false);
        }
      }
    };

    void loadWatchlist();

    return () => {
      controller.abort();
    };
  }, [accessToken, selectedWatchlistId, watchlists.length]);

  const summaryCards = useMemo(() => {
    const preference = selectedWatchlist?.preference;

    return [
      {
        label: "Watchlist ID",
        value: selectedWatchlist?.id ? `#${selectedWatchlist.id}` : "Unavailable",
        meta: selectedWatchlist?.userId ? `User ${selectedWatchlist.userId}` : "Stored watchlist record",
        accent: "text-cyan-50",
      },
      {
        label: "Window Months",
        value: preference?.windowMonths ? String(preference.windowMonths) : "No preference",
        meta: "Rolling analysis window",
        accent: "text-[#60a5fa]",
      },
      {
        label: "Travel Mode",
        value: formatTravelMode(preference?.travelMode),
        meta: "Movement profile preference",
        accent: "text-[#39ef7d]",
      },
      {
        label: "Crime Types",
        value: formatCount(preference?.crimeTypes?.length || 0),
        meta: preference?.crimeTypes?.length ? "Selected categories" : "No crime filters stored",
        accent: "text-[#f59e0b]",
      },
    ];
  }, [selectedWatchlist]);

  if (!accessToken) {
    return (
      <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar
          docsUrl={docsUrl}
          title="View Watchlists"
          subtitle="Sign in to load and inspect saved watchlists."
        />
        <div className="grid min-h-0 flex-1 place-items-center p-6">
          <div className="w-full max-w-[520px] rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-8 text-center shadow-2xl">
            <p className="text-[11px] uppercase tracking-[0.32em] text-cyan-100/40">Authentication Required</p>
            <h2 className="mt-4 text-2xl font-semibold text-cyan-50">No active session</h2>
            <p className="mt-3 text-sm leading-6 text-cyan-100/60">
              Log in first so the viewer can call the authenticated `/watchlists` endpoints.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="View Watchlists"
        subtitle="Review saved watchlists, inspect their stored preference payloads, and reopen any record."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[360px,minmax(0,1fr)]">
          <aside className="flex min-h-0 flex-col overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/90 shadow-2xl">
            <div className="flex items-center justify-between gap-3 border-b border-white/5 px-5 py-4">
              <div>
                <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Watchlists</p>
                <h2 className="mt-2 text-xl font-semibold text-cyan-50">
                  {loadingList ? "Loading..." : `${formatCount(watchlists.length)} Loaded`}
                </h2>
              </div>

              <button
                type="button"
                onClick={onCreateNew}
                className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-3 py-1.5 text-xs font-medium uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-50/20"
              >
                Create New
              </button>
            </div>

            {listErrorMessage ? (
              <div className="mx-4 mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                {listErrorMessage}
              </div>
            ) : null}

            <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
              {watchlists.length ? (
                <div className="flex flex-col gap-3">
                  {watchlists.map((watchlist) => {
                    const isActive = watchlist.id === selectedWatchlistId;

                    return (
                      <button
                        key={watchlist.id}
                        type="button"
                        onClick={() => onSelectWatchlist?.(watchlist.id)}
                        className={`rounded-[20px] border px-4 py-4 text-left transition-colors ${
                          isActive
                            ? "border-cyan-300/30 bg-cyan-50/10"
                            : "border-white/5 bg-[#071316]/70 hover:border-cyan-100/20 hover:bg-cyan-100/5"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">
                              Watchlist #{watchlist.id}
                            </p>
                            <h3 className="mt-2 text-base font-semibold text-cyan-50">
                              {watchlist.name}
                            </h3>
                          </div>
                          <span className="rounded-full border border-cyan-100/10 bg-[#030b0e]/70 px-3 py-1 text-[11px] uppercase tracking-[0.18em] text-cyan-100/55">
                            {formatTravelMode(watchlist.preference?.travelMode)}
                          </span>
                        </div>

                        <div className="mt-4 grid grid-cols-2 gap-2 text-xs text-cyan-100/55">
                          <span>{watchlist.preference?.windowMonths || 0} months</span>
                          <span>{watchlist.preference?.crimeTypes?.length || 0} crime types</span>
                        </div>

                        <p className="mt-4 text-xs uppercase tracking-[0.18em] text-cyan-100/35">
                          {formatTimestamp(watchlist.createdAt)}
                        </p>
                      </button>
                    );
                  })}
                </div>
              ) : (
                <div className="grid h-full min-h-[280px] place-items-center rounded-[22px] border border-dashed border-cyan-100/10 bg-[#071316]/50 px-6 text-center">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/35">Empty State</p>
                    <h3 className="mt-3 text-xl font-semibold text-cyan-50">No watchlists yet</h3>
                    <p className="mt-3 text-sm leading-6 text-cyan-100/60">
                      Create a watchlist to populate this desk. Newly created watchlists will land here automatically.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </aside>

          <section className="flex min-h-0 flex-col overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/90 shadow-2xl">
            <div className="border-b border-white/5 px-5 py-5">
              <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Selected Watchlist</p>
              <h2 className="mt-2 text-2xl font-semibold text-cyan-50">
                {selectedWatchlist?.name || (loadingDetail ? "Loading detail..." : "No watchlist selected")}
              </h2>
              <p className="mt-2 text-sm text-cyan-100/60">
                {selectedWatchlist
                  ? `Created ${formatTimestamp(selectedWatchlist.createdAt)}`
                  : "Choose a watchlist from the list to inspect its stored preference and bounding box."}
              </p>
            </div>

            {detailErrorMessage ? (
              <div className="mx-5 mt-5 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                {detailErrorMessage}
              </div>
            ) : null}

            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5">
              {selectedWatchlist ? (
                <div className="space-y-5">
                  <div className="rounded-[24px] border border-white/5 bg-[#071316]/75 p-5">
                    <div className="flex flex-wrap items-start justify-between gap-4">
                      <div>
                        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Live Record</p>
                        <h3 className="mt-3 text-3xl font-semibold text-cyan-50">
                          {selectedWatchlist.name}
                        </h3>
                        <p className="mt-3 max-w-2xl text-sm leading-6 text-cyan-100/60">
                          This watchlist stores a bbox plus a preference object for time window, crime categories, and travel mode. The detail panel is reading the single-watchlist endpoint.
                        </p>
                      </div>

                      <button
                        type="button"
                        onClick={() => setMetricsDrawerOpen(true)}
                        className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-4 py-2 text-xs font-medium uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-50/20"
                      >
                        Metrics
                      </button>
                    </div>
                  </div>

                  <div className="grid gap-3 lg:grid-cols-4">
                    {summaryCards.map((card) => (
                      <article
                        key={card.label}
                        className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4"
                      >
                        <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">
                          {card.label}
                        </p>
                        <p className={`mt-3 text-xl font-semibold ${card.accent}`}>{card.value}</p>
                        <p className="mt-2 text-sm text-cyan-100/55">{card.meta}</p>
                      </article>
                    ))}
                  </div>

                  <div className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr),minmax(320px,0.8fr)]">
                    <section className="rounded-[24px] border border-white/5 bg-[#071316]/70 p-5">
                      <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Bounding Box</p>
                      <h3 className="mt-3 text-xl font-semibold text-cyan-50">Stored coverage envelope</h3>
                      <div className="mt-5 grid gap-3 md:grid-cols-2">
                        <CoordinateCard label="Min Longitude" value={selectedWatchlist.minLon} />
                        <CoordinateCard label="Min Latitude" value={selectedWatchlist.minLat} />
                        <CoordinateCard label="Max Longitude" value={selectedWatchlist.maxLon} />
                        <CoordinateCard label="Max Latitude" value={selectedWatchlist.maxLat} />
                      </div>
                    </section>

                    <section className="rounded-[24px] border border-white/5 bg-[#071316]/70 p-5">
                      <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Preference Payload</p>
                      <h3 className="mt-3 text-xl font-semibold text-cyan-50">Saved analytics settings</h3>
                      <div className="mt-5 space-y-4">
                        <PreferenceRow
                          label="Travel Mode"
                          value={formatTravelMode(selectedWatchlist.preference?.travelMode)}
                        />
                        <PreferenceRow
                          label="Window"
                          value={
                            selectedWatchlist.preference?.windowMonths
                              ? `${selectedWatchlist.preference.windowMonths} months`
                              : "No preference"
                          }
                        />
                        <div>
                          <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">Crime Types</p>
                          {selectedWatchlist.preference?.crimeTypes?.length ? (
                            <div className="mt-3 flex flex-wrap gap-2">
                              {selectedWatchlist.preference.crimeTypes.map((crimeType) => (
                                <span
                                  key={crimeType}
                                  className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-3 py-1.5 text-xs font-medium text-cyan-50"
                                >
                                  {formatCrimeTypeLabel(crimeType)}
                                </span>
                              ))}
                            </div>
                          ) : (
                            <p className="mt-3 text-sm text-cyan-100/55">No crime types stored on this record.</p>
                          )}
                        </div>
                      </div>
                    </section>
                  </div>
                </div>
              ) : (
                <div className="grid h-full min-h-[420px] place-items-center rounded-[24px] border border-dashed border-cyan-100/10 bg-[#071316]/50 px-6 text-center">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/35">Ready</p>
                    <h3 className="mt-3 text-2xl font-semibold text-cyan-50">
                      {loadingDetail ? "Loading watchlist..." : "Select a watchlist"}
                    </h3>
                    <p className="mt-3 max-w-lg text-sm leading-6 text-cyan-100/60">
                      Pick a watchlist from the left rail to inspect the saved bbox and preference object returned by `/watchlists`.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </section>
        </div>
      </div>

      <MetricsDrawer
        activeTab={activeMetricsTab}
        isOpen={metricsDrawerOpen}
        onClose={() => setMetricsDrawerOpen(false)}
        onTabChange={setActiveMetricsTab}
        accessToken={accessToken}
        selectedWatchlist={selectedWatchlist}
        onWatchlistUpdated={setSelectedWatchlist}
        watchlistName={selectedWatchlist?.name || "Watchlist"}
      />
    </div>
  );
}

function MetricsDrawer({
  activeTab,
  isOpen,
  onClose,
  onTabChange,
  accessToken,
  selectedWatchlist,
  onWatchlistUpdated,
  watchlistName,
}) {
  const [activePreviousSubtab, setActivePreviousSubtab] = useState(PREVIOUS_METRICS_SUBTABS[0].id);
  const [activeNewSubtab, setActiveNewSubtab] = useState(NEW_METRICS_SUBTABS[0].id);
  const [historyItems, setHistoryItems] = useState([]);
  const [historyErrorMessage, setHistoryErrorMessage] = useState("");
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [expandedHistoryId, setExpandedHistoryId] = useState(null);
  const [runForms, setRunForms] = useState(createRunForms(selectedWatchlist?.preference));
  const [runFeedback, setRunFeedback] = useState({});
  const [runningMetricId, setRunningMetricId] = useState("");

  useEffect(() => {
    setRunForms(createRunForms(selectedWatchlist?.preference));
    setRunFeedback({});
  }, [selectedWatchlist?.id, selectedWatchlist?.preference]);

  useEffect(() => {
    if (!isOpen || activeTab !== "previous-metrics" || !selectedWatchlist?.id || !accessToken) {
      return undefined;
    }

    const controller = new AbortController();

    const loadHistory = async () => {
      setLoadingHistory(true);
      setHistoryErrorMessage("");

      try {
        const items = await getHistoryForSubtab(
          activePreviousSubtab,
          selectedWatchlist.id,
          accessToken,
          { signal: controller.signal },
        );

        if (controller.signal.aborted) {
          return;
        }

        setHistoryItems(items);
        setExpandedHistoryId((current) =>
          items.some((item) => item.id === current) ? current : null,
        );
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setHistoryItems([]);
        setExpandedHistoryId(null);
        setHistoryErrorMessage(error?.message || "Failed to load previous metrics.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingHistory(false);
        }
      }
    };

    void loadHistory();

    return () => {
      controller.abort();
    };
  }, [accessToken, activePreviousSubtab, activeTab, isOpen, selectedWatchlist?.id]);

  const handleRunMetric = async () => {
    if (!selectedWatchlist?.id || !accessToken) {
      return;
    }

    const draftPreference = buildPreferenceFromRunForms(runForms, activeNewSubtab);
    setRunningMetricId(activeNewSubtab);
    setRunFeedback((current) => ({
      ...current,
      [activeNewSubtab]: {
        tone: "",
        message: "",
        result: null,
      },
    }));

    try {
      const updatedWatchlist = await watchlistService.updateWatchlist(
        selectedWatchlist.id,
        {
          preference: draftPreference,
        },
        accessToken,
      );

      onWatchlistUpdated?.(updatedWatchlist);

      const runResult = await runMetricForSubtab(
        activeNewSubtab,
        selectedWatchlist.id,
        accessToken,
      );

      setRunFeedback((current) => ({
        ...current,
        [activeNewSubtab]: {
          tone: "success",
          message: `Stored run #${runResult.watchlistRunId} completed successfully.`,
          result: runResult,
        },
      }));
    } catch (error) {
      setRunFeedback((current) => ({
        ...current,
        [activeNewSubtab]: {
          tone: "error",
          message: error?.message || "Failed to run watchlist analytics.",
          result: null,
        },
      }));
    } finally {
      setRunningMetricId("");
    }
  };

  const activeRunState = runFeedback[activeNewSubtab] || {};

  return (
    <>
      <div
        aria-hidden={!isOpen}
        className={`absolute inset-0 z-40 bg-[#02080c]/55 transition-opacity duration-300 ${
          isOpen ? "pointer-events-auto opacity-100" : "pointer-events-none opacity-0"
        }`}
        onClick={onClose}
      />

      <aside
        aria-hidden={!isOpen}
        className={`absolute right-0 top-0 z-50 flex h-full w-[80vw] max-w-none flex-col border-l border-white/8 bg-[#020b10] shadow-2xl transition-transform duration-300 ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between gap-4 border-b border-white/5 px-6 py-5">
          <div>
            <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Watchlist Metrics</p>
            <h2 className="mt-2 text-2xl font-semibold text-cyan-50">{watchlistName}</h2>
          </div>

          <button
            type="button"
            onClick={onClose}
            className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-4 py-2 text-xs font-medium uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10"
          >
            Close
          </button>
        </div>

        <div className="flex items-center gap-2 border-b border-white/5 px-6 py-4">
          {METRICS_TABS.map((tab) => {
            const isActive = tab.id === activeTab;

            return (
              <button
                key={tab.id}
                type="button"
                onClick={() => onTabChange(tab.id)}
                className={`rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-cyan-50 text-[#021116]"
                    : "border border-cyan-100/10 bg-cyan-100/5 text-cyan-100/70 hover:bg-cyan-100/10 hover:text-cyan-50"
                }`}
              >
                {tab.label}
              </button>
            );
          })}
        </div>

        <div className="min-h-0 flex-1 overflow-hidden p-6">
          <div className="h-full overflow-y-auto rounded-[24px] border border-white/5 bg-[#071316]/70 p-5">
            {activeTab === "previous-metrics" ? (
              <div className="space-y-4">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Metrics Workspace</p>
                  <h3 className="mt-2 text-xl font-semibold text-cyan-50">Previous Metrics</h3>
                </div>

                <div className="flex items-center gap-2">
                  {PREVIOUS_METRICS_SUBTABS.map((tab) => {
                    const isActive = tab.id === activePreviousSubtab;

                    return (
                      <button
                        key={tab.id}
                        type="button"
                        onClick={() => setActivePreviousSubtab(tab.id)}
                        className={`rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                          isActive
                            ? "bg-cyan-50 text-[#021116]"
                            : "border border-cyan-100/10 bg-cyan-100/5 text-cyan-100/70 hover:bg-cyan-100/10 hover:text-cyan-50"
                        }`}
                      >
                        {tab.label}
                      </button>
                    );
                  })}
                </div>

                {historyErrorMessage ? (
                  <div className="rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                    {historyErrorMessage}
                  </div>
                ) : null}

                {loadingHistory ? (
                  <EmptyPanel label="Loading previous metrics..." />
                ) : historyItems.length ? (
                  <div className="space-y-3">
                    {historyItems.map((item) => {
                      const isExpanded = expandedHistoryId === item.id;

                      return (
                        <div
                          key={`${item.reportType}-${item.id}`}
                          className="overflow-hidden rounded-[20px] border border-white/5 bg-[#030b0e]/70"
                        >
                          <button
                            type="button"
                            onClick={() => setExpandedHistoryId((current) => (current === item.id ? null : item.id))}
                            className="flex w-full items-center justify-between gap-4 px-4 py-4 text-left transition-colors hover:bg-cyan-100/5"
                          >
                            <div>
                              <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">
                                Run #{item.id}
                              </p>
                              <h4 className="mt-2 text-base font-semibold text-cyan-50">
                                {getHistoryHeadline(item)}
                              </h4>
                              <p className="mt-2 text-sm text-cyan-100/55">
                                {getHistorySummary(item)}
                              </p>
                            </div>
                            <span className="text-xs uppercase tracking-[0.18em] text-cyan-100/45">
                              {formatTimestamp(item.createdAt)}
                            </span>
                          </button>

                          {isExpanded ? (
                            <div className="border-t border-white/5 px-4 py-4">
                              <MetricResultPanel
                                mode="stored"
                                subtab={activePreviousSubtab}
                                result={item}
                              />
                            </div>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <EmptyPanel label="No previous metrics stored for this watchlist and category yet." />
                )}
              </div>
            ) : activeTab === "new-metrics" ? (
              <div className="space-y-5">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Metrics Workspace</p>
                  <h3 className="mt-2 text-xl font-semibold text-cyan-50">New Metrics</h3>
                </div>

                <div className="flex items-center gap-2">
                  {NEW_METRICS_SUBTABS.map((tab) => {
                    const isActive = tab.id === activeNewSubtab;

                    return (
                      <button
                        key={tab.id}
                        type="button"
                        onClick={() => setActiveNewSubtab(tab.id)}
                        className={`rounded-full px-4 py-2 text-sm font-medium transition-colors ${
                          isActive
                            ? "bg-cyan-50 text-[#021116]"
                            : "border border-cyan-100/10 bg-cyan-100/5 text-cyan-100/70 hover:bg-cyan-100/10 hover:text-cyan-50"
                        }`}
                      >
                        {tab.label}
                      </button>
                    );
                  })}
                </div>

                <RunMetricForm
                  activeSubtab={activeNewSubtab}
                  formState={runForms}
                  onChange={setRunForms}
                />

                {activeRunState.message ? (
                  <div
                    className={`rounded-[16px] border px-4 py-3 text-sm ${
                      activeRunState.tone === "error"
                        ? "border-red-300/30 bg-[#4a0f0fd0] text-red-100"
                        : "border-emerald-300/20 bg-emerald-400/10 text-emerald-100"
                    }`}
                  >
                    {activeRunState.message}
                  </div>
                ) : null}

                <button
                  type="button"
                  onClick={handleRunMetric}
                  disabled={runningMetricId === activeNewSubtab}
                  className="rounded-full bg-cyan-50 px-5 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
                >
                  {runningMetricId === activeNewSubtab ? "Running..." : "Run Metric"}
                </button>

                {activeRunState.result ? (
                  <MetricResultPanel
                    mode="run"
                    subtab={activeNewSubtab}
                    result={activeRunState.result}
                  />
                ) : (
                  <EmptyPanel label="Run a metric to see the stored wrapper and result here." />
                )}
              </div>
            ) : (
              <EmptyPanel label="Metrics workspace unavailable." />
            )}
          </div>
        </div>
      </aside>
    </>
  );
}

function RunMetricForm({ activeSubtab, formState, onChange }) {
  const draft = formState[activeSubtab];

  return (
    <div className="space-y-4 rounded-[20px] border border-white/5 bg-[#030b0e]/70 p-4">
      <div>
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Run Configuration</p>
        <h3 className="mt-2 text-xl font-semibold text-cyan-50">{getRunFormTitle(activeSubtab)}</h3>
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <MetricField
          label="Window Months"
          type="number"
          value={draft.windowMonths}
          onChange={(value) => updateRunForm(onChange, activeSubtab, "windowMonths", value)}
        />
        {(activeSubtab === "run-forecast" || activeSubtab === "run-risk") ? (
          <MetricField
            label="Travel Mode"
            type="select"
            value={draft.travelMode}
            options={[
              { value: "walk", label: "Walk" },
              { value: "drive", label: "Drive" },
            ]}
            onChange={(value) => {
              updateRunForm(onChange, activeSubtab, "travelMode", value);
              if (value === "walk") {
                updateRunForm(onChange, activeSubtab, "includeCollisions", false);
              }
            }}
          />
        ) : null}
        <MetricField
          label="Crime Types"
          value={draft.crimeTypes}
          placeholder="Comma separated, leave blank for all"
          onChange={(value) => updateRunForm(onChange, activeSubtab, "crimeTypes", value)}
        />
        {activeSubtab === "run-forecast" ? (
          <MetricField
            label="Baseline Months"
            type="number"
            value={draft.baselineMonths}
            onChange={(value) => updateRunForm(onChange, activeSubtab, "baselineMonths", value)}
          />
        ) : null}
        {activeSubtab === "run-hotspots" ? (
          <MetricField
            label="Hotspot K"
            type="number"
            value={draft.hotspotK}
            onChange={(value) => updateRunForm(onChange, activeSubtab, "hotspotK", value)}
          />
        ) : null}
        {(activeSubtab === "run-forecast" || activeSubtab === "run-risk") ? (
          <>
            <MetricField
              label="Weight Crime"
              type="number"
              value={draft.weightCrime}
              onChange={(value) => updateRunForm(onChange, activeSubtab, "weightCrime", value)}
            />
            <MetricField
              label="Weight Collision"
              type="number"
              value={draft.weightCollision}
              onChange={(value) => updateRunForm(onChange, activeSubtab, "weightCollision", value)}
            />
          </>
        ) : null}
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        {(activeSubtab === "run-forecast" || activeSubtab === "run-risk") ? (
          <MetricToggle
            checked={draft.includeCollisions}
            disabled={draft.travelMode !== "drive"}
            label="Include Collisions"
            onChange={(value) => updateRunForm(onChange, activeSubtab, "includeCollisions", value)}
          />
        ) : null}
        {activeSubtab === "run-forecast" ? (
          <MetricToggle
            checked={draft.includeForecast}
            label="Enable Forecast"
            onChange={(value) => updateRunForm(onChange, activeSubtab, "includeForecast", value)}
          />
        ) : null}
        {activeSubtab === "run-hotspots" ? (
          <MetricToggle
            checked={draft.includeHotspotStability}
            label="Enable Hotspots"
            onChange={(value) => updateRunForm(onChange, activeSubtab, "includeHotspotStability", value)}
          />
        ) : null}
      </div>
    </div>
  );
}

function EmptyPanel({ label }) {
  return (
    <div className="grid min-h-[220px] place-items-center rounded-[20px] border border-dashed border-cyan-100/10 bg-[#030b0e]/50 px-6 text-center">
      <p className="text-sm text-cyan-100/55">{label}</p>
    </div>
  );
}

function MetricResultPanel({ mode, result, subtab }) {
  const crimeRows = getResultsByCrimeType(result).map(([crimeType, payload]) => ({
    crimeType,
    payload,
  }));

  return (
    <div className="space-y-4 rounded-[20px] border border-white/5 bg-[#071316]/75 p-4">
      <div>
        <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">
          {mode === "run" ? "Latest Run" : "Stored Result"}
        </p>
        <h4 className="mt-2 text-lg font-semibold text-cyan-50">
          {getMetricPanelTitle(subtab)}
        </h4>
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <StatPill label="Run ID" value={result.watchlistRunId || result.id || "—"} />
        <StatPill label="Stored At" value={formatTimestamp(result.storedAt || result.createdAt)} />
        <StatPill label="Type" value={result.reportType || getReportTypeLabel(subtab)} />
      </div>

      {result.request ? (
        <section className="rounded-[18px] border border-white/5 bg-[#030b0e]/75 p-4">
          <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">Request</p>
          <div className="mt-3">
            <JsonObjectViewer data={result.request} />
          </div>
        </section>
      ) : null}

      {crimeRows.length ? (
        <div className="space-y-3">
          {crimeRows.map(({ crimeType, payload }) => (
            <div key={crimeType} className="rounded-[18px] border border-white/5 bg-[#030b0e]/75 p-4">
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{crimeType}</p>
              <div className="mt-3">
                <JsonObjectViewer data={payload} />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <EmptyPanel label="No result payload returned for this metric." />
      )}
    </div>
  );
}

function JsonObjectViewer({ data, level = 0 }) {
  const entries = Object.entries(data || {});

  if (!entries.length) {
    return <p className="text-sm text-cyan-100/50">No data available.</p>;
  }

  return (
    <div className={`space-y-3 ${level > 0 ? "rounded-[16px] border border-white/5 bg-[#071316]/55 p-3" : ""}`}>
      {entries.map(([key, value]) => {
        const label = toReadableLabel(key);

        if (value && typeof value === "object" && !Array.isArray(value)) {
          return (
            <div key={`${level}-${key}`}>
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{label}</p>
              <div className="mt-2">
                <JsonObjectViewer data={value} level={level + 1} />
              </div>
            </div>
          );
        }

        if (Array.isArray(value)) {
          return (
            <div key={`${level}-${key}`}>
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{label}</p>
              <div className="mt-2 space-y-2">
                {value.length ? (
                  value.map((item, index) =>
                    item && typeof item === "object" ? (
                      <JsonObjectViewer key={`${key}-${index}`} data={item} level={level + 1} />
                    ) : (
                      <div
                        key={`${key}-${index}`}
                        className="rounded-[14px] border border-white/5 bg-[#071316]/55 px-3 py-2 text-sm text-cyan-50"
                      >
                        {formatViewerValue(item)}
                      </div>
                    ),
                  )
                ) : (
                  <p className="text-sm text-cyan-100/50">No values.</p>
                )}
              </div>
            </div>
          );
        }

        return (
          <div
            key={`${level}-${key}`}
            className="grid gap-2 rounded-[14px] border border-white/5 bg-[#071316]/55 px-3 py-3 md:grid-cols-[180px,minmax(0,1fr)]"
          >
            <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{label}</p>
            <p className="text-sm font-medium text-cyan-50 break-words">{formatViewerValue(value)}</p>
          </div>
        );
      })}
    </div>
  );
}

function MetricField({ label, onChange, options = [], type = "text", value, ...props }) {
  if (type === "select") {
    return (
      <label className="flex flex-col gap-2">
        <span className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">{label}</span>
        <select
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="rounded-[14px] border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm text-cyan-50 outline-none transition-colors focus:border-cyan-400/40"
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
    );
  }

  return (
    <label className="flex flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">{label}</span>
      <input
        {...props}
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-[14px] border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
      />
    </label>
  );
}

function MetricToggle({ checked, disabled, label, onChange }) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={`rounded-[16px] border px-4 py-4 text-left transition-colors ${
        checked
          ? "border-cyan-300/35 bg-cyan-50/10 text-cyan-50"
          : "border-cyan-100/10 bg-[#030b0e]/80 text-cyan-100/60 hover:bg-cyan-100/5 hover:text-cyan-50"
      } disabled:cursor-not-allowed disabled:opacity-45`}
    >
      <p className="text-[11px] uppercase tracking-[0.24em]">{label}</p>
      <p className="mt-2 text-sm font-medium">{checked ? "Enabled" : "Disabled"}</p>
    </button>
  );
}

function StatPill({ label, value }) {
  return (
    <div className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4">
      <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{toReadableLabel(label)}</p>
      <p className="mt-2 text-base font-semibold text-cyan-50">{String(value ?? "—")}</p>
    </div>
  );
}

function createRunForms(preference) {
  const base = {
    windowMonths: String(preference?.windowMonths || 6),
    crimeTypes: Array.isArray(preference?.crimeTypes) ? preference.crimeTypes.join(", ") : "",
    travelMode: normalizeTravelMode(preference?.travelMode),
    includeCollisions: Boolean(preference?.includeCollisions),
    baselineMonths: String(preference?.baselineMonths || 6),
    hotspotK: String(preference?.hotspotK || 20),
    includeHotspotStability: Boolean(preference?.includeHotspotStability),
    includeForecast: Boolean(preference?.includeForecast),
    weightCrime: String(preference?.weightCrime || 1),
    weightCollision: String(preference?.weightCollision || 0.8),
  };

  return {
    "run-forecast": { ...base },
    "run-risk": { ...base },
    "run-hotspots": { ...base },
  };
}

function normalizeTravelMode(value) {
  const normalized = String(value || "").toLowerCase();
  return normalized === "drive" ? "drive" : "walk";
}

function updateRunForm(setter, subtab, key, value) {
  setter((current) => ({
    ...current,
    [subtab]: {
      ...current[subtab],
      [key]: value,
    },
  }));
}

function buildPreferenceFromRunForms(runForms, activeSubtab) {
  const source = runForms[activeSubtab];
  const travelMode = normalizeTravelMode(source.travelMode);
  const includeCollisions = travelMode === "drive" ? Boolean(source.includeCollisions) : false;

  return {
    window_months: Number(source.windowMonths) || 6,
    crime_types: parseCrimeTypesInput(source.crimeTypes),
    travel_mode: travelMode,
    include_collisions: includeCollisions,
    baseline_months: Number(source.baselineMonths) || 6,
    hotspot_k: Number(source.hotspotK) || 20,
    include_hotspot_stability:
      activeSubtab === "run-hotspots" ? Boolean(source.includeHotspotStability) : false,
    include_forecast: activeSubtab === "run-forecast" ? Boolean(source.includeForecast) : false,
    weight_crime: Number(source.weightCrime) || 1,
    weight_collision: Number(source.weightCollision) || 0.8,
  };
}

function parseCrimeTypesInput(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

async function getHistoryForSubtab(subtab, watchlistId, accessToken, requestOptions = {}) {
  if (subtab === "forecast") {
    return watchlistService.getRiskForecastResults(watchlistId, accessToken, { limit: 20 }, requestOptions);
  }

  if (subtab === "hotspots") {
    return watchlistService.getHotspotStabilityResults(watchlistId, accessToken, { limit: 20 }, requestOptions);
  }

  return watchlistService.getRiskScoreResults(watchlistId, accessToken, { limit: 20 }, requestOptions);
}

async function runMetricForSubtab(subtab, watchlistId, accessToken) {
  if (subtab === "run-forecast") {
    return watchlistService.runRiskForecast(watchlistId, accessToken);
  }

  if (subtab === "run-hotspots") {
    return watchlistService.runHotspotStability(watchlistId, accessToken);
  }

  return watchlistService.runRiskScore(watchlistId, accessToken);
}

function getHistoryHeadline(item) {
  const request = item.request || {};

  if (item.reportType === "risk_forecast") {
    return request.target ? `Target ${request.target}` : "Forecast run";
  }

  if (item.reportType === "hotspot_stability") {
    return [request.from, request.to].filter(Boolean).join(" to ") || "Hotspot run";
  }

  return [request.from, request.to].filter(Boolean).join(" to ") || "Risk score run";
}

function getHistorySummary(item) {
  const rows = getResultsByCrimeType(item);

  if (!rows.length) {
    return "No persisted result summary available.";
  }

  const [crimeType, payload] = rows[0];

  if (item.reportType === "risk_forecast") {
    return `${crimeType}: expected ${payload?.forecast?.expected_count ?? "—"}, ${payload?.forecast?.predicted_band || "unknown"} band`;
  }

  if (item.reportType === "hotspot_stability") {
    return `${crimeType}: avg jaccard ${payload?.summary?.average_jaccard ?? "—"}, persistent ${payload?.summary?.persistent_hotspot_count ?? "—"}`;
  }

  return `${crimeType}: score ${payload?.risk_score ?? payload?.score ?? "—"}, ${payload?.band || "unknown"} band`;
}

function getResultsByCrimeType(resultWrapper) {
  return Object.entries(resultWrapper?.result?.results_by_crime_type || {});
}

function getRunFormTitle(activeSubtab) {
  if (activeSubtab === "run-forecast") {
    return "Run Forecast";
  }

  if (activeSubtab === "run-hotspots") {
    return "Run Hotspots";
  }

  return "Run Risk";
}

function getMetricPanelTitle(subtab) {
  if (subtab === "forecast" || subtab === "run-forecast") {
    return "Forecast Result";
  }

  if (subtab === "hotspots" || subtab === "run-hotspots") {
    return "Hotspot Result";
  }

  return "Risk Result";
}

function getReportTypeLabel(subtab) {
  if (subtab === "forecast" || subtab === "run-forecast") {
    return "risk_forecast";
  }

  if (subtab === "hotspots" || subtab === "run-hotspots") {
    return "hotspot_stability";
  }

  return "risk_score";
}

function toReadableLabel(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatViewerValue(value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }

  if (typeof value === "boolean") {
    return value ? "True" : "False";
  }

  return String(value);
}

function CoordinateCard({ label, value }) {
  return (
    <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/80 p-4">
      <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">{label}</p>
      <p className="mt-3 text-lg font-semibold text-cyan-50">{formatCoordinate(value)}</p>
    </div>
  );
}

function PreferenceRow({ label, value }) {
  return (
    <div className="rounded-[18px] border border-white/5 bg-[#030b0e]/80 p-4">
      <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/40">{label}</p>
      <p className="mt-3 text-base font-semibold text-cyan-50">{value}</p>
    </div>
  );
}

function formatCount(value) {
  return Number(value || 0).toLocaleString("en-GB");
}

function formatCoordinate(value) {
  return Number(value || 0).toFixed(6);
}

function formatTimestamp(value) {
  if (!value) {
    return "Unknown timestamp";
  }

  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatTravelMode(value) {
  if (!value) {
    return "No mode";
  }

  return value
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(" ");
}

function formatCrimeTypeLabel(value) {
  if (!value) {
    return "Unknown";
  }

  return value
    .split(/[_-]+/)
    .join(" ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

export default ViewWatchlistPage;
