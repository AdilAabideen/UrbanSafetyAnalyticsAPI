import { useEffect, useMemo, useState } from "react";
import TopBar from "./TopBar";
import { watchlistService } from "../services";

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
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
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
                        onClick={onCreateNew}
                        className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-4 py-2 text-xs font-medium uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-50/20"
                      >
                        Create Another
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
    </div>
  );
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
