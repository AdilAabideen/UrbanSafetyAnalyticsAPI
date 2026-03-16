import { useEffect, useMemo, useState } from "react";
import TopBar from "./TopBar";
import WatchlistPolygonMap from "./watchlist/WatchlistPolygonMap";
import WatchlistField from "./watchlist/WatchlistField";
import WatchlistCrimeTypeMultiSelect from "./watchlist/WatchlistCrimeTypeMultiSelect";
import WatchlistModeSelect from "./watchlist/WatchlistModeSelect";
import { watchlistService } from "../services";
import {
  WATCHLIST_CRIME_TYPE_OPTIONS,
  WATCHLIST_MODE_OPTIONS,
  apiDateToMonthValue,
  monthValueToApiDate,
} from "../utils/watchlistUtils";

const PAGE_TABS = [
  { id: "main", label: "Main" },
  { id: "map", label: "Map" },
  { id: "risk-scoring", label: "Risk Scoring" },
  { id: "forecast", label: "Forecast" },
  { id: "overview", label: "Overview and Edit" },
];

function createEditForm(watchlist) {
  return {
    name: watchlist?.name || "",
    minLon: toInputValue(watchlist?.minLon),
    minLat: toInputValue(watchlist?.minLat),
    maxLon: toInputValue(watchlist?.maxLon),
    maxLat: toInputValue(watchlist?.maxLat),
    startMonth: apiDateToMonthValue(watchlist?.preference?.startMonth),
    endMonth: apiDateToMonthValue(watchlist?.preference?.endMonth),
    crimeTypes: Array.isArray(watchlist?.preference?.crimeTypes)
      ? watchlist.preference.crimeTypes.join(", ")
      : "",
    mode: watchlist?.preference?.travelMode || "",
  };
}

function createForecastForm(watchlist) {
  const normalizedMode = String(watchlist?.preference?.travelMode || "").toLowerCase();

  return {
    startMonth: apiDateToMonthValue(watchlist?.preference?.startMonth),
    mode: normalizedMode === "drive" || normalizedMode === "driving" ? "drive" : "walk",
    crimeTypes: Array.isArray(watchlist?.preference?.crimeTypes)
      ? watchlist.preference.crimeTypes
      : [],
  };
}

function toInputValue(value) {
  const number = Number(value);
  return Number.isFinite(number) ? String(number) : "";
}

function parseOptionalNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function parseCrimeTypes(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function ViewWatchlistPage({
  docsUrl,
  accessToken,
  selectedWatchlistId,
  onSelectWatchlist,
  onCreateNew,
}) {
  const [activeTab, setActiveTab] = useState("main");
  const [watchlists, setWatchlists] = useState([]);
  const [selectedWatchlist, setSelectedWatchlist] = useState(null);
  const [editForm, setEditForm] = useState(createEditForm(null));
  const [forecastForm, setForecastForm] = useState(createForecastForm(null));
  const [loadingList, setLoadingList] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [savingWatchlist, setSavingWatchlist] = useState(false);
  const [deletingWatchlist, setDeletingWatchlist] = useState(false);
  const [listErrorMessage, setListErrorMessage] = useState("");
  const [detailErrorMessage, setDetailErrorMessage] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [refreshToken, setRefreshToken] = useState(0);

  const [computingRiskScore, setComputingRiskScore] = useState(false);
  const [loadingRiskRuns, setLoadingRiskRuns] = useState(false);
  const [riskRuns, setRiskRuns] = useState([]);
  const [riskRunsErrorMessage, setRiskRunsErrorMessage] = useState("");
  const [riskActionMessage, setRiskActionMessage] = useState("");
  const [latestRiskResult, setLatestRiskResult] = useState(null);
  const [riskRefreshToken, setRiskRefreshToken] = useState(0);

  const [runningForecast, setRunningForecast] = useState(false);
  const [forecastActionMessage, setForecastActionMessage] = useState("");
  const [forecastResult, setForecastResult] = useState(null);
  const [loadingBasicMetrics, setLoadingBasicMetrics] = useState(false);
  const [basicMetricsResult, setBasicMetricsResult] = useState(null);
  const [basicMetricsErrorMessage, setBasicMetricsErrorMessage] = useState("");
  const [basicMetricsRefreshToken, setBasicMetricsRefreshToken] = useState(0);
  const [loadingMapEvents, setLoadingMapEvents] = useState(false);
  const [mapEventsResult, setMapEventsResult] = useState(null);
  const [mapEventsErrorMessage, setMapEventsErrorMessage] = useState("");
  const [mapEventsRefreshToken, setMapEventsRefreshToken] = useState(0);

  useEffect(() => {
    if (!accessToken) {
      setWatchlists([]);
      setSelectedWatchlist(null);
      setEditForm(createEditForm(null));
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

        if (!items.length) {
          onSelectWatchlist?.(null);
          setSelectedWatchlist(null);
          return;
        }

        const selectedExists = items.some((item) => item.id === selectedWatchlistId);
        if (!selectedWatchlistId || !selectedExists) {
          onSelectWatchlist?.(items[0].id);
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
  }, [accessToken, onSelectWatchlist, refreshToken, selectedWatchlistId]);

  useEffect(() => {
    if (!accessToken || !selectedWatchlistId) {
      setSelectedWatchlist(null);
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

        setSelectedWatchlist(null);
        setDetailErrorMessage(error?.message || "Failed to load watchlist detail.");
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
  }, [accessToken, selectedWatchlistId, refreshToken]);

  useEffect(() => {
    setEditForm(createEditForm(selectedWatchlist));
    setForecastForm(createForecastForm(selectedWatchlist));
    setActionMessage("");
    setRiskRuns([]);
    setLatestRiskResult(null);
    setRiskActionMessage("");
    setRiskRunsErrorMessage("");
    setRiskRefreshToken(0);
    setForecastActionMessage("");
    setForecastResult(null);
    setLoadingBasicMetrics(false);
    setBasicMetricsResult(null);
    setBasicMetricsErrorMessage("");
    setBasicMetricsRefreshToken(0);
    setLoadingMapEvents(false);
    setMapEventsResult(null);
    setMapEventsErrorMessage("");
    setMapEventsRefreshToken(0);
  }, [selectedWatchlist?.id]);

  useEffect(() => {
    if (activeTab !== "main" || !accessToken || !selectedWatchlist?.id) {
      return undefined;
    }

    const controller = new AbortController();

    const loadBasicMetrics = async () => {
      setLoadingBasicMetrics(true);
      setBasicMetricsErrorMessage("");

      try {
        const response = await watchlistService.getWatchlistBasicMetrics(
          selectedWatchlist.id,
          accessToken,
          { signal: controller.signal },
        );

        if (controller.signal.aborted) {
          return;
        }

        setBasicMetricsResult(response);
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setBasicMetricsResult(null);
        setBasicMetricsErrorMessage(error?.message || "Failed to load basic metrics.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingBasicMetrics(false);
        }
      }
    };

    void loadBasicMetrics();

    return () => {
      controller.abort();
    };
  }, [accessToken, activeTab, basicMetricsRefreshToken, selectedWatchlist?.id]);

  useEffect(() => {
    if (activeTab !== "map" || !accessToken || !selectedWatchlist?.id) {
      return undefined;
    }

    const controller = new AbortController();

    const loadMapEvents = async () => {
      setLoadingMapEvents(true);
      setMapEventsErrorMessage("");

      try {
        const response = await watchlistService.getWatchlistMapEvents(
          selectedWatchlist.id,
          accessToken,
          { signal: controller.signal },
        );

        if (controller.signal.aborted) {
          return;
        }

        setMapEventsResult(response);
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setMapEventsResult(null);
        setMapEventsErrorMessage(error?.message || "Failed to load map event overlays.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingMapEvents(false);
        }
      }
    };

    void loadMapEvents();

    return () => {
      controller.abort();
    };
  }, [accessToken, activeTab, mapEventsRefreshToken, selectedWatchlist?.id]);

  useEffect(() => {
    if (activeTab !== "risk-scoring" || !accessToken || !selectedWatchlist?.id) {
      return undefined;
    }

    const controller = new AbortController();

    const loadRiskRuns = async () => {
      setLoadingRiskRuns(true);
      setRiskRunsErrorMessage("");

      try {
        const runs = await watchlistService.getWatchlistRiskScoreRuns(
          selectedWatchlist.id,
          accessToken,
          { limit: 50 },
          { signal: controller.signal },
        );

        if (controller.signal.aborted) {
          return;
        }

        setRiskRuns(runs);
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setRiskRuns([]);
        setRiskRunsErrorMessage(error?.message || "Failed to load risk-score runs.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingRiskRuns(false);
        }
      }
    };

    void loadRiskRuns();

    return () => {
      controller.abort();
    };
  }, [accessToken, activeTab, riskRefreshToken, selectedWatchlist?.id]);

  const isValidMonthRange =
    Boolean(editForm.startMonth) &&
    Boolean(editForm.endMonth) &&
    editForm.startMonth <= editForm.endMonth;
  const bbox = {
    minLon: parseOptionalNumber(editForm.minLon),
    minLat: parseOptionalNumber(editForm.minLat),
    maxLon: parseOptionalNumber(editForm.maxLon),
    maxLat: parseOptionalNumber(editForm.maxLat),
  };
  const hasValidBbox =
    [bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat].every((value) => Number.isFinite(value)) &&
    bbox.minLon < bbox.maxLon &&
    bbox.minLat < bbox.maxLat;
  const selectedWatchlistBbox = {
    minLon: parseOptionalNumber(selectedWatchlist?.minLon),
    minLat: parseOptionalNumber(selectedWatchlist?.minLat),
    maxLon: parseOptionalNumber(selectedWatchlist?.maxLon),
    maxLat: parseOptionalNumber(selectedWatchlist?.maxLat),
  };
  const hasSelectedWatchlistBbox =
    [
      selectedWatchlistBbox.minLon,
      selectedWatchlistBbox.minLat,
      selectedWatchlistBbox.maxLon,
      selectedWatchlistBbox.maxLat,
    ].every((value) => Number.isFinite(value)) &&
    selectedWatchlistBbox.minLon < selectedWatchlistBbox.maxLon &&
    selectedWatchlistBbox.minLat < selectedWatchlistBbox.maxLat;
  const selectedWatchlistPolygonPoints = hasSelectedWatchlistBbox
    ? [
        [selectedWatchlistBbox.minLon, selectedWatchlistBbox.minLat],
        [selectedWatchlistBbox.maxLon, selectedWatchlistBbox.minLat],
        [selectedWatchlistBbox.maxLon, selectedWatchlistBbox.maxLat],
        [selectedWatchlistBbox.minLon, selectedWatchlistBbox.maxLat],
      ]
    : [];

  const canSave =
    Boolean(selectedWatchlist?.id) &&
    String(editForm.name).trim().length > 0 &&
    String(editForm.mode).trim().length > 0 &&
    isValidMonthRange &&
    hasValidBbox &&
    !savingWatchlist;

  const summaryCards = useMemo(
    () => [
      {
        label: "Watchlists Loaded",
        value: String(watchlists.length),
        meta: loadingList ? "Loading list..." : "From GET /watchlists",
      },
      {
        label: "Selected ID",
        value: selectedWatchlist?.id ? `#${selectedWatchlist.id}` : "None",
        meta: selectedWatchlist?.name || "No selected watchlist",
      },
      {
        label: "Travel Mode",
        value: selectedWatchlist?.preference?.travelMode || "Not set",
        meta: "Stored preference",
      },
      {
        label: "Crime Types",
        value: String(selectedWatchlist?.preference?.crimeTypes?.length || 0),
        meta: "Stored preference",
      },
    ],
    [loadingList, selectedWatchlist, watchlists.length],
  );

  const handleFieldChange = (key, value) => {
    setActionMessage("");
    setEditForm((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const handleSave = async () => {
    if (!canSave || !selectedWatchlist?.id || !accessToken) {
      return;
    }

    setSavingWatchlist(true);
    setActionMessage("");

    const payload = {
      name: editForm.name.trim(),
      min_lon: bbox.minLon,
      min_lat: bbox.minLat,
      max_lon: bbox.maxLon,
      max_lat: bbox.maxLat,
      preference: {
        start_month: monthValueToApiDate(editForm.startMonth),
        end_month: monthValueToApiDate(editForm.endMonth),
        crime_types: parseCrimeTypes(editForm.crimeTypes),
        travel_mode: editForm.mode.toLowerCase(),
      },
    };

    try {
      const updatedWatchlist = await watchlistService.updateWatchlist(
        selectedWatchlist.id,
        payload,
        accessToken,
      );

      setSelectedWatchlist(updatedWatchlist);
      setWatchlists((current) =>
        current.map((watchlist) =>
          watchlist.id === updatedWatchlist.id ? updatedWatchlist : watchlist,
        ),
      );
      setActionMessage("Watchlist updated successfully.");
    } catch (error) {
      setActionMessage(error?.message || "Failed to update watchlist.");
    } finally {
      setSavingWatchlist(false);
    }
  };

  const handleDelete = async () => {
    if (!selectedWatchlist?.id || !accessToken || deletingWatchlist) {
      return;
    }

    const confirmed = window.confirm(
      `Delete watchlist "${selectedWatchlist.name}" (ID ${selectedWatchlist.id})?`,
    );

    if (!confirmed) {
      return;
    }

    setDeletingWatchlist(true);
    setActionMessage("");

    try {
      await watchlistService.deleteWatchlist(selectedWatchlist.id, accessToken);
      setActionMessage("Watchlist deleted successfully.");
      setRefreshToken((current) => current + 1);
    } catch (error) {
      setActionMessage(error?.message || "Failed to delete watchlist.");
    } finally {
      setDeletingWatchlist(false);
    }
  };

  const handleRunRiskScore = async () => {
    if (!selectedWatchlist?.id || !accessToken || computingRiskScore) {
      return;
    }

    setComputingRiskScore(true);
    setRiskActionMessage("");

    try {
      const response = await watchlistService.computeWatchlistRiskScore(
        selectedWatchlist.id,
        accessToken,
      );

      setLatestRiskResult(response);
      setRiskActionMessage("Risk score computed successfully.");
      setRiskRefreshToken((current) => current + 1);
    } catch (error) {
      setRiskActionMessage(error?.message || "Failed to compute risk score.");
    } finally {
      setComputingRiskScore(false);
    }
  };

  const handleRefreshRiskRuns = () => {
    setRiskRefreshToken((current) => current + 1);
  };

  const handleRefreshBasicMetrics = () => {
    setBasicMetricsRefreshToken((current) => current + 1);
  };

  const handleRefreshMapEvents = () => {
    setMapEventsRefreshToken((current) => current + 1);
  };

  const canRunForecast =
    Boolean(selectedWatchlist?.id) &&
    Boolean(forecastForm.startMonth) &&
    String(forecastForm.mode).trim().length > 0 &&
    !runningForecast;

  const handleForecastFieldChange = (key, value) => {
    setForecastActionMessage("");
    setForecastForm((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const handleRunForecast = async () => {
    if (!canRunForecast || !selectedWatchlist?.id || !accessToken) {
      return;
    }

    setRunningForecast(true);
    setForecastActionMessage("");

    const payload = {
      start_month: monthValueToApiDate(forecastForm.startMonth),
      mode: String(forecastForm.mode || "").toLowerCase(),
      crime_types: Array.isArray(forecastForm.crimeTypes)
        ? forecastForm.crimeTypes.filter(Boolean)
        : [],
    };

    try {
      const response = await watchlistService.forecastWatchlistNextMonth(
        selectedWatchlist.id,
        payload,
        accessToken,
      );

      setForecastResult(response);
      setForecastActionMessage("Forecast computed successfully.");
    } catch (error) {
      setForecastActionMessage(error?.message || "Failed to compute forecast.");
    } finally {
      setRunningForecast(false);
    }
  };

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
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="View Watchlists"
        subtitle="Review watchlists, edit preferences, and run risk scoring analytics."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[300px,minmax(0,1fr)]">
          <aside className="flex min-h-0 flex-col overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/90 shadow-2xl">
            <div className="flex items-center justify-between gap-3 border-b border-white/5 px-5 py-4">
              <div>
                <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Watchlists</p>
                <h2 className="mt-2 text-xl font-semibold text-cyan-50">
                  {loadingList ? "Loading..." : `${watchlists.length} Loaded`}
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
                        <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">
                          Watchlist #{watchlist.id}
                        </p>
                        <h3 className="mt-2 text-base font-semibold text-cyan-50">{watchlist.name}</h3>
                        <p className="mt-2 text-xs text-cyan-100/55">{formatTimestamp(watchlist.createdAt)}</p>
                      </button>
                    );
                  })}
                </div>
              ) : (
                <div className="grid h-full min-h-[280px] place-items-center rounded-[22px] border border-dashed border-cyan-100/10 bg-[#071316]/50 px-6 text-center">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/35">Empty State</p>
                    <h3 className="mt-3 text-xl font-semibold text-cyan-50">No watchlists yet</h3>
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

              <div className="mt-4 flex flex-wrap gap-2">
                {PAGE_TABS.map((tab) => {
                  const isActive = activeTab === tab.id;

                  return (
                    <button
                      key={tab.id}
                      type="button"
                      onClick={() => setActiveTab(tab.id)}
                      className={`rounded-full px-3 py-1.5 text-xs font-medium uppercase tracking-[0.18em] transition-colors ${
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
            </div>

            {detailErrorMessage ? (
              <div className="mx-5 mt-5 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                {detailErrorMessage}
              </div>
            ) : null}

            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5">
              {selectedWatchlist ? (
                activeTab === "main" ? (
                  <MainTab
                    loadingBasicMetrics={loadingBasicMetrics}
                    basicMetricsResult={basicMetricsResult}
                    basicMetricsErrorMessage={basicMetricsErrorMessage}
                    selectedWatchlist={selectedWatchlist}
                    onRefreshBasicMetrics={handleRefreshBasicMetrics}
                  />
                ) : activeTab === "map" ? (
                  <MapTab
                    hasSelectedWatchlistBbox={hasSelectedWatchlistBbox}
                    loadingMapEvents={loadingMapEvents}
                    mapEventsErrorMessage={mapEventsErrorMessage}
                    mapEventsResult={mapEventsResult}
                    selectedWatchlist={selectedWatchlist}
                    selectedWatchlistBbox={selectedWatchlistBbox}
                    selectedWatchlistPolygonPoints={selectedWatchlistPolygonPoints}
                    onRefreshMapEvents={handleRefreshMapEvents}
                  />
                ) : activeTab === "overview" ? (
                  <div className="space-y-5">
                    <div className="grid gap-3 lg:grid-cols-4">
                      {summaryCards.map((card) => (
                        <article
                          key={card.label}
                          className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4"
                        >
                          <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">{card.label}</p>
                          <p className="mt-3 text-xl font-semibold text-cyan-50">{card.value}</p>
                          <p className="mt-2 text-sm text-cyan-100/55">{card.meta}</p>
                        </article>
                      ))}
                    </div>

                    <div className="grid gap-3 md:grid-cols-2">
                      <WatchlistField
                        label="NAME"
                        value={editForm.name}
                        onChange={(value) => handleFieldChange("name", value)}
                      />
                      <WatchlistModeSelect
                        label="MODE"
                        value={editForm.mode}
                        options={WATCHLIST_MODE_OPTIONS}
                        onChange={(value) => handleFieldChange("mode", value)}
                      />
                      <WatchlistField
                        label="START MONTH"
                        type="month"
                        value={editForm.startMonth}
                        onChange={(value) => handleFieldChange("startMonth", value)}
                      />
                      <WatchlistField
                        label="END MONTH"
                        type="month"
                        value={editForm.endMonth}
                        onChange={(value) => handleFieldChange("endMonth", value)}
                      />
                      <WatchlistField
                        label="MIN LONGITUDE"
                        value={editForm.minLon}
                        inputMode="decimal"
                        onChange={(value) => handleFieldChange("minLon", value)}
                      />
                      <WatchlistField
                        label="MIN LATITUDE"
                        value={editForm.minLat}
                        inputMode="decimal"
                        onChange={(value) => handleFieldChange("minLat", value)}
                      />
                      <WatchlistField
                        label="MAX LONGITUDE"
                        value={editForm.maxLon}
                        inputMode="decimal"
                        onChange={(value) => handleFieldChange("maxLon", value)}
                      />
                      <WatchlistField
                        label="MAX LATITUDE"
                        value={editForm.maxLat}
                        inputMode="decimal"
                        onChange={(value) => handleFieldChange("maxLat", value)}
                      />
                    </div>

                    <label className="flex flex-col gap-2">
                      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">CRIME TYPES (comma-separated)</span>
                      <textarea
                        value={editForm.crimeTypes}
                        onChange={(event) => handleFieldChange("crimeTypes", event.target.value)}
                        rows={3}
                        className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
                      />
                    </label>

                    {!isValidMonthRange ? (
                      <div className="rounded-[16px] border border-amber-300/30 bg-amber-950/60 px-4 py-3 text-sm text-amber-100">
                        Start month must be before or equal to end month.
                      </div>
                    ) : null}

                    {actionMessage ? (
                      <div className="rounded-[16px] border border-cyan-100/20 bg-cyan-950/40 px-4 py-3 text-sm text-cyan-100">
                        {actionMessage}
                      </div>
                    ) : null}

                    <div className="flex flex-wrap gap-3">
                      <button
                        type="button"
                        onClick={handleSave}
                        disabled={!canSave}
                        className="rounded-[14px] bg-cyan-50 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
                      >
                        {savingWatchlist ? "Saving..." : "Save Watchlist"}
                      </button>

                      <button
                        type="button"
                        onClick={handleDelete}
                        disabled={deletingWatchlist || !selectedWatchlist?.id}
                        className="rounded-[14px] border border-red-300/30 bg-red-950/40 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-red-100 transition-colors hover:bg-red-900/50 disabled:cursor-not-allowed disabled:opacity-45"
                      >
                        {deletingWatchlist ? "Deleting..." : "Delete Watchlist"}
                      </button>
                    </div>
                  </div>
                ) : activeTab === "risk-scoring" ? (
                  <RiskScoringTab
                    computingRiskScore={computingRiskScore}
                    latestRiskResult={latestRiskResult}
                    loadingRiskRuns={loadingRiskRuns}
                    riskActionMessage={riskActionMessage}
                    riskRuns={riskRuns}
                    riskRunsErrorMessage={riskRunsErrorMessage}
                    selectedWatchlist={selectedWatchlist}
                    onRefreshRuns={handleRefreshRiskRuns}
                    onRunRiskScore={handleRunRiskScore}
                  />
                ) : (
                  <ForecastTab
                    canRunForecast={canRunForecast}
                    forecastActionMessage={forecastActionMessage}
                    forecastForm={forecastForm}
                    forecastResult={forecastResult}
                    runningForecast={runningForecast}
                    onFieldChange={handleForecastFieldChange}
                    onRunForecast={handleRunForecast}
                  />
                )
              ) : (
                <div className="grid h-full min-h-[420px] place-items-center rounded-[24px] border border-dashed border-cyan-100/10 bg-[#071316]/50 px-6 text-center">
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/35">Ready</p>
                    <h3 className="mt-3 text-2xl font-semibold text-cyan-50">
                      {loadingDetail ? "Loading watchlist..." : "Select a watchlist"}
                    </h3>
                  </div>
                </div>
              )}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

function MapTab({
  hasSelectedWatchlistBbox,
  loadingMapEvents,
  mapEventsErrorMessage,
  mapEventsResult,
  onRefreshMapEvents,
  selectedWatchlist,
  selectedWatchlistBbox,
  selectedWatchlistPolygonPoints,
}) {
  const crimeFeatures = Array.isArray(mapEventsResult?.crimes?.features)
    ? mapEventsResult.crimes.features
    : [];
  const collisionFeatures = Array.isArray(mapEventsResult?.collisions?.features)
    ? mapEventsResult.collisions.features
    : [];
  const userReportedFeatures = Array.isArray(mapEventsResult?.user_reported_events?.features)
    ? mapEventsResult.user_reported_events.features
    : [];

  return (
    <div className="space-y-5">
      <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Map Overlay</p>
            <h3 className="mt-2 text-xl font-semibold text-cyan-50">Watchlist Aerial View</h3>
            <p className="mt-1 text-sm text-cyan-100/60">
              Event points and bbox overlay for watchlist #{selectedWatchlist?.id}
            </p>
          </div>
          <button
            type="button"
            onClick={onRefreshMapEvents}
            className="rounded-[14px] border border-cyan-100/10 bg-cyan-100/5 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-cyan-50 transition-colors hover:bg-cyan-100/10"
          >
            {loadingMapEvents ? "Loading..." : "Refresh"}
          </button>
        </div>

        <div className="mt-4 grid gap-2 sm:grid-cols-3">
          <div className="rounded-[12px] border border-white/5 bg-[#030b0e]/70 px-3 py-2 text-xs text-cyan-100/75">
            <span className="mr-2 inline-block h-2.5 w-2.5 rounded-full bg-red-500" />
            Crimes: <span className="font-semibold text-cyan-50">{crimeFeatures.length}</span>
          </div>
          <div className="rounded-[12px] border border-white/5 bg-[#030b0e]/70 px-3 py-2 text-xs text-cyan-100/75">
            <span className="mr-2 inline-block h-2.5 w-2.5 rounded-full bg-blue-500" />
            Collisions: <span className="font-semibold text-cyan-50">{collisionFeatures.length}</span>
          </div>
          <div className="rounded-[12px] border border-white/5 bg-[#030b0e]/70 px-3 py-2 text-xs text-cyan-100/75">
            <span className="mr-2 inline-block h-2.5 w-2.5 rounded-full bg-green-500" />
            User Reported: <span className="font-semibold text-cyan-50">{userReportedFeatures.length}</span>
          </div>
        </div>

        {mapEventsErrorMessage ? (
          <div className="mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
            {mapEventsErrorMessage}
          </div>
        ) : null}
      </div>

      {hasSelectedWatchlistBbox ? (
        <div className="h-[540px] overflow-hidden rounded-[20px] border border-white/5 bg-[#020a0f]">
          <WatchlistPolygonMap
            collisionsFeatureCollection={mapEventsResult?.collisions}
            crimesFeatureCollection={mapEventsResult?.crimes}
            polygonPoints={selectedWatchlistPolygonPoints}
            polygonClosed
            readOnly
            userReportedEventsFeatureCollection={mapEventsResult?.user_reported_events}
          />
        </div>
      ) : (
        <div className="rounded-[16px] border border-amber-300/30 bg-amber-950/60 px-4 py-4 text-sm text-amber-100">
          This watchlist does not have a valid bbox yet. A map polygon needs: minLon &lt; maxLon and minLat
          &lt; maxLat.
          <div className="mt-2 text-xs text-amber-100/80">
            Current values: minLon={toDisplayValue(selectedWatchlistBbox.minLon)}, minLat=
            {toDisplayValue(selectedWatchlistBbox.minLat)}, maxLon={toDisplayValue(selectedWatchlistBbox.maxLon)},
            maxLat={toDisplayValue(selectedWatchlistBbox.maxLat)}
          </div>
        </div>
      )}
    </div>
  );
}

function MainTab({
  loadingBasicMetrics,
  basicMetricsResult,
  basicMetricsErrorMessage,
  selectedWatchlist,
  onRefreshBasicMetrics,
}) {
  const dangerousRoads = Array.isArray(basicMetricsResult?.most_dangerous_roads)
    ? basicMetricsResult.most_dangerous_roads
    : [];
  const crimeCategoryBreakdown = Array.isArray(basicMetricsResult?.crime_category_breakdown)
    ? basicMetricsResult.crime_category_breakdown
    : [];
  const maxCrimeCategoryCount = Math.max(
    1,
    ...crimeCategoryBreakdown.map((item) => Number(item?.count) || 0),
  );
  const startMonth = formatMonthLabel(selectedWatchlist?.preference?.startMonth);
  const endMonth = formatMonthLabel(selectedWatchlist?.preference?.endMonth);

  return (
    <div className="space-y-5">
      <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Main Analytics</p>
            <h3 className="mt-2 text-xl font-semibold text-cyan-50">Watchlist Basic Metrics</h3>
            <p className="mt-1 text-sm text-cyan-100/60">
              Window: {startMonth} to {endMonth}
            </p>
          </div>

          <button
            type="button"
            onClick={onRefreshBasicMetrics}
            className="rounded-[14px] border border-cyan-100/10 bg-cyan-100/5 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-cyan-50 transition-colors hover:bg-cyan-100/10"
          >
            {loadingBasicMetrics ? "Loading..." : "Refresh"}
          </button>
        </div>

        {basicMetricsErrorMessage ? (
          <div className="mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
            {basicMetricsErrorMessage}
          </div>
        ) : null}
      </div>

      {basicMetricsResult ? (
        <>
          <div className="grid gap-3 md:grid-cols-3">
            <MetricCard
              label="Number of Crimes"
              value={toDisplayValue(basicMetricsResult.number_of_crimes)}
            />
            <MetricCard
              label="Number of Collisions"
              value={toDisplayValue(basicMetricsResult.number_of_collisions)}
            />
            <MetricCard
              label="User Reported Events"
              value={toDisplayValue(basicMetricsResult.number_of_user_reported_events)}
            />
          </div>

          <div className="grid gap-3 lg:grid-cols-2">
            <article className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4">
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">Most Dangerous Roads</p>
              {dangerousRoads.length ? (
                <div className="mt-3 space-y-3">
                  {dangerousRoads.map((road, index) => (
                    <div
                      key={`${road.segment_id}-${index}`}
                      className="rounded-[12px] border border-white/5 bg-[#071316]/70 p-3"
                    >
                      <p className="text-sm font-semibold text-cyan-50">
                        {road.road_name || `Segment ${toDisplayValue(road.segment_id)}`}
                      </p>
                      <p className="mt-1 text-xs text-cyan-100/60">
                        Danger Score: {formatMetricNumber(road.danger_score, 2)}
                      </p>
                      <p className="mt-1 text-xs text-cyan-100/60">
                        Crimes {toDisplayValue(road.crime_count)} · Collisions {toDisplayValue(road.collision_count)} · User Reports{" "}
                        {toDisplayValue(road.user_reported_event_count)}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="mt-3 text-sm text-cyan-100/60">No dangerous roads returned for this window.</p>
              )}
            </article>

            <article className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4">
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">Crime Category Breakdown</p>
              {crimeCategoryBreakdown.length ? (
                <div className="mt-3 space-y-3">
                  {crimeCategoryBreakdown.map((item, index) => {
                    const count = Number(item?.count) || 0;
                    const widthPct = Math.max(3, (count / maxCrimeCategoryCount) * 100);
                    return (
                      <div key={`${item?.crime_type || "crime"}-${index}`} className="space-y-1">
                        <div className="flex items-center justify-between gap-3 text-xs">
                          <span className="text-cyan-100/80">{item?.crime_type || "Unknown"}</span>
                          <span className="font-semibold text-cyan-50">{toDisplayValue(item?.count)}</span>
                        </div>
                        <div className="h-2 rounded-full bg-cyan-100/10">
                          <div
                            className="h-2 rounded-full bg-cyan-300/60"
                            style={{ width: `${Math.min(100, widthPct)}%` }}
                          />
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <p className="mt-3 text-sm text-cyan-100/60">No crime category breakdown returned for this window.</p>
              )}
            </article>
          </div>
        </>
      ) : (
        <div className="rounded-[16px] border border-dashed border-cyan-100/10 bg-[#030b0e]/65 px-4 py-5 text-sm text-cyan-100/60">
          {loadingBasicMetrics
            ? "Loading basic metrics..."
            : "No basic metrics available yet. Click Refresh to load analytics for this watchlist."}
        </div>
      )}
    </div>
  );
}

function RiskScoringTab({
  computingRiskScore,
  latestRiskResult,
  loadingRiskRuns,
  riskActionMessage,
  riskRuns,
  riskRunsErrorMessage,
  selectedWatchlist,
  onRefreshRuns,
  onRunRiskScore,
}) {
  const resolvedLatestResult = latestRiskResult || riskRuns?.[0]?.data || null;
  const riskResult =
    resolvedLatestResult?.risk_result ||
    resolvedLatestResult?.riskResult ||
    resolvedLatestResult?.risk ||
    {};
  const comparison =
    resolvedLatestResult?.comparison ||
    (isComparisonPayload(resolvedLatestResult) ? resolvedLatestResult : {});
  const components = riskResult?.components || {};
  const distribution = comparison?.distribution || {};
  const computedScore = extractRiskScoreValue(resolvedLatestResult);
  const riskBand = getRiskBandFromScore(computedScore);
  const riskMessage = getRiskBandMessage(riskBand);
  const cohortSize = Number(comparison?.cohort_size ?? comparison?.sample_size);
  const mostRecentRun = riskRuns?.[0]?.data || {};
  const startMonth = formatMonthLabel(
      resolvedLatestResult?.start_month ??
      resolvedLatestResult?.startMonth ??
      mostRecentRun?.start_month ??
      mostRecentRun?.startMonth ??
      selectedWatchlist?.preference?.startMonth,
  );
  const endMonth = formatMonthLabel(
      resolvedLatestResult?.end_month ??
      resolvedLatestResult?.endMonth ??
      mostRecentRun?.end_month ??
      mostRecentRun?.endMonth ??
      selectedWatchlist?.preference?.endMonth,
  );
  const noCrimes = extractIncidentCount(resolvedLatestResult, mostRecentRun, "crime");
  const noCollisions = extractIncidentCount(resolvedLatestResult, mostRecentRun, "collision");
  const noUserReportedEvents = extractIncidentCount(resolvedLatestResult, mostRecentRun, "user");

  return (
    <div className="space-y-5">
      <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Risk Scoring</p>
        <h3 className="mt-2 text-xl font-semibold text-cyan-50">Run a Risk Score too see how risky your watchlist area is</h3>


        <div className="mt-4 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={onRunRiskScore}
            disabled={computingRiskScore}
            className="rounded-[14px] bg-cyan-50 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
          >
            {computingRiskScore ? "Running..." : "Run Risk Score"}
          </button>

          <button
            type="button"
            onClick={onRefreshRuns}
            className="rounded-[14px] border border-cyan-100/10 bg-cyan-100/5 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-cyan-50 transition-colors hover:bg-cyan-100/10"
          >
            Refresh Runs
          </button>
        </div>

        {riskActionMessage ? (
          <div className="mt-4 rounded-[16px] border border-cyan-100/20 bg-cyan-950/40 px-4 py-3 text-sm text-cyan-100">
            {riskActionMessage}
          </div>
        ) : null}
      </div>

      {resolvedLatestResult ? (
        <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Latest Result</p>

          <div className="mt-4 rounded-[18px] border border-cyan-200/20 bg-gradient-to-r from-[#0c1f23] via-[#0b1a1e] to-[#1f1111] p-4">
            <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/50">Computed Score</p>
            <div className="mt-2 flex flex-wrap items-end justify-between gap-3">
              <p className={`text-4xl font-bold ${getRiskScoreTextClass(computedScore)}`}>{formatRiskScore(computedScore)}</p>
              <p className={`text-sm font-semibold uppercase tracking-[0.16em] ${getRiskScoreTextClass(computedScore)}`}>
                {riskBand}
              </p>
            </div>
            <p className="mt-3 text-sm text-cyan-100/80">{riskMessage}</p>
          </div>

          <div className="mt-4 grid gap-3 md:grid-cols-3">
            <MetricCard label="Crime Component" value={formatMetricNumber(components.crime_component, 4)} />
            <MetricCard label="Collision Density" value={formatMetricNumber(components.collision_density, 6)} />
            <MetricCard label="User Reported" value={formatMetricNumber(components.user_support, 6)} />
          </div>

          <div className="mt-4 rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4 text-sm leading-7 text-cyan-100/80">
            In your area from <span className="font-semibold text-cyan-50">{startMonth}</span> to{" "}
            <span className="font-semibold text-cyan-50">{endMonth}</span>, there were exactly{" "}
            <span className="font-semibold text-cyan-50">{toDisplayValue(noCrimes)}</span> number of crimes,{" "}
            <span className="font-semibold text-cyan-50">{toDisplayValue(noCollisions)}</span> number of collisions,
            and <span className="font-semibold text-cyan-50">{toDisplayValue(noUserReportedEvents)}</span> number of
            reported events by users. This deems it a risky place, as compared by{" "}
            <span className="font-semibold text-cyan-50">{toDisplayValue(cohortSize)}</span> different areas.
            Your Area Ranked <span className="font-semibold text-cyan-50">{toDisplayValue(comparison.rank)}</span> out of <span className="font-semibold text-cyan-50">{toDisplayValue(comparison.rank_out_of)}</span> areas.
          </div>

          <div className="mt-4 grid gap-3 lg:grid-cols-2">
            <article className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4">
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">Comparison</p>
              <DetailRow label="Cohort Type" value={toDisplayValue(comparison.cohort_type)} />
              <DetailRow label="Cohort Size" value={toDisplayValue(comparison.cohort_size)} />
              <DetailRow label="Percentile" value={formatMetricNumber(comparison.percentile, 2)} />
            </article>

            <article className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4">
              <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">Distribution</p>
              <DetailRow label="Minimum" value={toDisplayValue(distribution.min)} />
              <DetailRow label="Median" value={toDisplayValue(distribution.median)} />
              <DetailRow label="Maximum" value={toDisplayValue(distribution.max)} />
              <DetailRow label="Raw Score" value={formatMetricNumber(riskResult.raw_score, 3)} />
            </article>
          </div>
        </section>
      ) : null}

      <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <div className="flex items-center justify-between gap-3">
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Previous Runs</p>
          <span className="text-xs text-cyan-100/60">{loadingRiskRuns ? "Loading..." : `${riskRuns.length} runs`}</span>
        </div>

        {riskRunsErrorMessage ? (
          <div className="mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
            {riskRunsErrorMessage}
          </div>
        ) : null}

        {riskRuns.length ? (
          <div className="mt-4 space-y-3">
            {riskRuns.map((run, index) => (
              <article
                key={run.id || `${run.createdAt}-${index}`}
                className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-4"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-xs uppercase tracking-[0.18em] text-cyan-100/55">
                    Run {run.id ? `#${run.id}` : `#${index + 1}`}
                  </p>
                  <p className="text-xs text-cyan-100/55">{formatTimestamp(run.createdAt)}</p>
                </div>
                <p className="mt-2 text-sm text-cyan-100/75">
                  Score:{" "}
                  <strong className={getRiskScoreTextClass(run.score ?? extractRiskScoreValue(run.data))}>
                    {formatRiskScore(run.score ?? extractRiskScoreValue(run.data))}
                  </strong>
                </p>
              </article>
            ))}
          </div>
        ) : (
          <div className="mt-4 rounded-[16px] border border-dashed border-cyan-100/10 bg-[#030b0e]/65 px-4 py-5 text-sm text-cyan-100/55">
            No risk-score runs found for this watchlist yet.
          </div>
        )}
      </section>
    </div>
  );
}

function DetailRow({ label, value }) {
  return (
    <p className="mt-2 flex items-start justify-between gap-3 text-sm">
      <span className="text-cyan-100/55">{label}</span>
      <span className="text-right text-cyan-50">{toDisplayValue(value)}</span>
    </p>
  );
}

function ForecastTab({
  canRunForecast,
  forecastActionMessage,
  forecastForm,
  forecastResult,
  runningForecast,
  onFieldChange,
  onRunForecast,
}) {
  const forecastPayload = forecastResult?.forecast || forecastResult || {};
  const intervalCrimes = forecastPayload?.intervals?.crimes || {};
  const intervalCollisions = forecastPayload?.intervals?.collisions_count || {};
  const bandClass = getForecastBandTextClass(forecastPayload.band);
  const summaryCopy = getForecastSummaryCopy(forecastPayload.band);

  return (
    <div className="space-y-5">
      <div className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
        <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Forecast</p>
        <h3 className="mt-2 text-xl font-semibold text-cyan-50">Compute next-month forecast</h3>
        <p className="mt-2 text-sm text-cyan-100/60">
          Calls <code>POST /watchlists/{`{watchlist_id}`}/analytics/forecast</code> with
          `start_month`, `mode`, and `crime_types`.
        </p>

        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <WatchlistField
            label="START MONTH"
            type="month"
            value={forecastForm.startMonth}
            onChange={(value) => onFieldChange("startMonth", value)}
          />

          <div className="rounded-[12px] border border-cyan-200/10 bg-[#071316]/70 px-4 py-3">
            <p className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">MODE</p>
            <div className="mt-3 flex gap-2">
              {["walk", "drive"].map((mode) => {
                const active = forecastForm.mode === mode;
                return (
                  <button
                    key={mode}
                    type="button"
                    onClick={() => onFieldChange("mode", mode)}
                    className={`rounded-[12px] border px-3 py-2 text-xs font-medium uppercase tracking-[0.14em] transition-colors ${
                      active
                        ? "border-cyan-300/40 bg-cyan-50/12 text-cyan-50"
                        : "border-cyan-100/10 bg-[#030b0e]/80 text-cyan-100/70 hover:bg-cyan-100/10 hover:text-cyan-50"
                    }`}
                  >
                    {mode}
                  </button>
                );
              })}
            </div>
          </div>
        </div>

        <div className="mt-3">
          <WatchlistCrimeTypeMultiSelect
            label="CRIME TYPES"
            values={Array.isArray(forecastForm.crimeTypes) ? forecastForm.crimeTypes : []}
            options={WATCHLIST_CRIME_TYPE_OPTIONS}
            onChange={(values) => onFieldChange("crimeTypes", values)}
          />
        </div>

        <div className="mt-4 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={onRunForecast}
            disabled={!canRunForecast}
            className="rounded-[14px] bg-cyan-50 px-4 py-2 text-sm font-semibold uppercase tracking-[0.16em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
          >
            {runningForecast ? "Running..." : "Run Forecast"}
          </button>
        </div>

        {forecastActionMessage ? (
          <div className="mt-4 rounded-[16px] border border-cyan-100/20 bg-cyan-950/40 px-4 py-3 text-sm text-cyan-100">
            {forecastActionMessage}
          </div>
        ) : null}
      </div>

      {forecastResult ? (
        <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Forecast Result</p>

          <div className="mt-4 rounded-[18px] border border-cyan-200/20 bg-gradient-to-r from-[#0c1f23] via-[#0b1a1e] to-[#1f1111] p-5">
            <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/50">Computed Score</p>
            <p className={`mt-2 text-5xl font-bold ${bandClass}`}>
              {Number.isFinite(Number(forecastPayload.score)) ? `${Math.round(Number(forecastPayload.score))}%` : "—"}
            </p>
            <p className={`mt-3 text-base font-semibold ${bandClass}`}>{summaryCopy}</p>

            <p className="mt-4 text-lg text-cyan-50">
              Expected crimes <span className="font-bold">{toDisplayValue(forecastPayload.expected_crime_count)}</span>, expected
              collisions <span className="font-bold">{toDisplayValue(forecastPayload.expected_collision_count)}</span>.
            </p>
          </div>

          <div className="mt-4 grid gap-3 md:grid-cols-2">
            <MetricCard
              label="Crime Count Band"
              value={`${toDisplayValue(intervalCrimes.low)} - ${toDisplayValue(intervalCrimes.high)}`}
            />
            <MetricCard
              label="Collision Count Band"
              value={`${toDisplayValue(intervalCollisions.low)} - ${toDisplayValue(intervalCollisions.high)}`}
            />
          </div>
        </section>
      ) : null}
    </div>
  );
}

function MetricCard({ label, value, valueClass = "text-cyan-50" }) {
  return (
    <article className="rounded-[16px] border border-white/5 bg-[#030b0e]/75 p-3">
      <p className="text-[11px] uppercase tracking-[0.22em] text-cyan-100/40">{label}</p>
      <p className={`mt-2 text-sm font-semibold ${valueClass}`}>{value}</p>
    </article>
  );
}

function toDisplayValue(value) {
  if (value === undefined || value === null || value === "") {
    return "—";
  }

  return String(value);
}

function extractRiskScoreValue(payload) {
  const score = Number(
    payload?.risk_result?.risk_score ??
      payload?.riskResult?.riskScore ??
      payload?.risk_result?.score ??
      payload?.risk_score ??
      payload?.riskScore ??
      payload?.score ??
      payload?.subject_score ??
      payload?.subjectScore ??
      payload?.comparison?.subject_score ??
      payload?.comparison?.subjectScore ??
      payload?.risk?.risk_score ??
      payload?.risk?.riskScore ??
      payload?.risk?.score,
  );

  return Number.isFinite(score) ? score : null;
}

function isComparisonPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return false;
  }

  return (
    Object.prototype.hasOwnProperty.call(payload, "cohort_type") ||
    Object.prototype.hasOwnProperty.call(payload, "subject_score") ||
    Object.prototype.hasOwnProperty.call(payload, "distribution")
  );
}

function formatRiskScore(score) {
  return Number.isFinite(score) ? `${Math.round(score)}%` : "Unavailable";
}

function getRiskBandFromScore(score) {
  if (!Number.isFinite(score)) {
    return "Unknown";
  }

  if (score > 60) {
    return "Red";
  }

  if (score > 30) {
    return "Orange";
  }

  return "Green";
}

function getRiskBandMessage(riskBand) {
  if (riskBand === "Red") {
    return "Your area is deemed to be risky.";
  }

  if (riskBand === "Orange") {
    return "Your area is deemed to have elevated risk.";
  }

  if (riskBand === "Green") {
    return "Your area is deemed to be lower risk right now.";
  }

  return "Risk status is currently unavailable.";
}

function getRiskScoreTextClass(score) {
  if (!Number.isFinite(score)) {
    return "text-cyan-50";
  }

  if (score > 60) {
    return "text-red-300";
  }

  if (score > 30) {
    return "text-orange-300";
  }

  return "text-[#39ef7d]";
}

function formatMonthLabel(value) {
  if (!value) {
    return "N/A";
  }

  const stringValue = String(value);
  const date = new Date(stringValue);

  if (!Number.isNaN(date.getTime())) {
    return date.toLocaleDateString(undefined, { month: "short", year: "numeric" });
  }

  return stringValue;
}

function extractIncidentCount(latestRiskResult, mostRecentRun, kind) {
  const candidatesByKind = {
    crime: [
      latestRiskResult?.data_used?.official_crime_count,
      mostRecentRun?.data_used?.official_crime_count,
    ],
    collision: [
      latestRiskResult?.data_used?.collision_count,
      mostRecentRun?.data_used?.collision_count,
    ],
    user: [
      latestRiskResult?.data_used?.approved_user_report_count,
      mostRecentRun?.data_used?.approved_user_report_count,
    ],
  };

  const candidates = candidatesByKind[kind] || [];
  const firstNumber = candidates.find((value) => Number.isFinite(Number(value)));

  return Number.isFinite(Number(firstNumber)) ? Number(firstNumber) : "N/A";
}

function getForecastBandTextClass(band) {
  const normalizedBand = String(band || "").toLowerCase();

  if (normalizedBand === "red") {
    return "text-red-300";
  }

  if (normalizedBand === "orange" || normalizedBand === "amber") {
    return "text-orange-300";
  }

  return "text-[#39ef7d]";
}

function getForecastSummaryCopy(band) {
  const normalizedBand = String(band || "").toLowerCase();

  if (normalizedBand === "red") {
    return "This is deemed bad.";
  }

  if (normalizedBand === "orange" || normalizedBand === "amber") {
    return "This is deemed concerning.";
  }

  return "This is deemed relatively safer.";
}

function formatMetricNumber(value, digits = 2) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(digits) : "—";
}

function formatTimestamp(value) {
  if (!value) {
    return "No timestamp";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return date.toLocaleString();
}

export default ViewWatchlistPage;
