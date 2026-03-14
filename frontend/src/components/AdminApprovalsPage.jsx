import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import TopBar from "./TopBar";
import { config } from "../config/env";
import { reportedEventsService, tilesService } from "../services";
import {
  DEFAULT_MONTH_FROM,
  DEFAULT_MONTH_TO,
  WEST_YORKSHIRE_BBOX,
  WEST_YORKSHIRE_CENTER,
} from "../utils/crimeUtils";

const STATUS_OPTIONS = [
  { value: "", label: "All Statuses" },
  { value: "pending", label: "Pending" },
  { value: "approved", label: "Approved" },
  { value: "rejected", label: "Rejected" },
];
const KIND_OPTIONS = [
  { value: "", label: "All Types" },
  { value: "crime", label: "Crime" },
  { value: "collision", label: "Collision" },
];
const REPORTER_OPTIONS = [
  { value: "", label: "All Reporters" },
  { value: "anonymous", label: "Anonymous" },
  { value: "authenticated", label: "Authenticated" },
];
const LOCATION_SOURCE_ID = "admin-approvals-location";
const LOCATION_HALO_LAYER_ID = "admin-approvals-location-halo";
const LOCATION_POINT_LAYER_ID = "admin-approvals-location-point";
const ROADS_SOURCE_ID = "admin-approvals-roads";
const ROADS_LAYER_ID = "admin-approvals-roads-layer";

function AdminApprovalsPage({ docsUrl, accessToken, isAdmin }) {
  const [reports, setReports] = useState([]);
  const [selectedReport, setSelectedReport] = useState(null);
  const [statusFilter, setStatusFilter] = useState("pending");
  const [eventKindFilter, setEventKindFilter] = useState("");
  const [reporterTypeFilter, setReporterTypeFilter] = useState("");
  const [fromMonth, setFromMonth] = useState("");
  const [toMonth, setToMonth] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [cursorStack, setCursorStack] = useState([null]);
  const [meta, setMeta] = useState(null);
  const [loadingList, setLoadingList] = useState(false);
  const [listErrorMessage, setListErrorMessage] = useState("");
  const [moderationNotes, setModerationNotes] = useState("");
  const [moderationFeedback, setModerationFeedback] = useState(null);
  const [savingModeration, setSavingModeration] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    setPageIndex(0);
    setCursorStack([null]);
  }, [eventKindFilter, fromMonth, reporterTypeFilter, statusFilter, toMonth]);

  useEffect(() => {
    if (!accessToken || !isAdmin) {
      setReports([]);
      setSelectedReport(null);
      setMeta(null);
      setListErrorMessage("");
      return undefined;
    }

    const controller = new AbortController();
    const currentCursor = cursorStack[pageIndex] || undefined;

    const loadReports = async () => {
      setLoadingList(true);
      setListErrorMessage("");

      try {
        const response = await reportedEventsService.getAdminReportedEvents(
          {
            status: statusFilter || undefined,
            eventKind: eventKindFilter || undefined,
            reporterType: reporterTypeFilter || undefined,
            from: fromMonth || undefined,
            to: toMonth || undefined,
            limit: 50,
            cursor: currentCursor,
          },
          accessToken,
          {
            signal: controller.signal,
          },
        );

        if (controller.signal.aborted) {
          return;
        }

        setReports(response.items);
        setMeta(response.meta || null);

        setSelectedReport((current) => {
          if (!response.items.length) {
            return null;
          }

          const currentMatch = current
            ? response.items.find((report) => report.id === current.id) || null
            : null;

          return currentMatch || response.items[0];
        });
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setReports([]);
        setMeta(null);
        setSelectedReport(null);
        setListErrorMessage(error?.message || "Failed to load the admin moderation queue.");
      } finally {
        if (!controller.signal.aborted) {
          setLoadingList(false);
        }
      }
    };

    void loadReports();

    return () => {
      controller.abort();
    };
  }, [
    accessToken,
    cursorStack,
    eventKindFilter,
    fromMonth,
    isAdmin,
    pageIndex,
    refreshToken,
    reporterTypeFilter,
    statusFilter,
    toMonth,
  ]);

  useEffect(() => {
    setModerationNotes(selectedReport?.moderationNotes || "");
    setModerationFeedback(null);
  }, [selectedReport?.id, selectedReport?.moderationNotes]);

  const summaryCards = useMemo(() => {
    const pendingCount = reports.filter((report) => report.moderationStatus === "pending").length;
    const authenticatedCount = reports.filter((report) => report.reporterType === "authenticated").length;
    const anonymousCount = reports.filter((report) => report.reporterType === "anonymous").length;

    return [
      {
        label: "Queue Loaded",
        value: formatCount(reports.length),
        meta: meta?.nextCursor ? "More reports available" : "Current page loaded",
        accent: "text-cyan-50",
      },
      {
        label: "Pending Review",
        value: formatCount(pendingCount),
        meta: "Needs moderator action",
        accent: "text-[#f59e0b]",
      },
      {
        label: "Authenticated",
        value: formatCount(authenticatedCount),
        meta: "Signed-in submissions",
        accent: "text-[#39ef7d]",
      },
      {
        label: "Anonymous",
        value: formatCount(anonymousCount),
        meta: "Public submissions",
        accent: "text-[#60a5fa]",
      },
    ];
  }, [meta?.nextCursor, reports]);

  if (!accessToken) {
    return (
      <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar
          docsUrl={docsUrl}
          title="Admin Approvals"
          subtitle="Sign in with an admin account to moderate reported crime and collision submissions."
        />
        <div className="grid min-h-0 flex-1 place-items-center p-6">
          <div className="w-full max-w-[520px] rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-8 text-center shadow-2xl">
            <p className="text-[11px] uppercase tracking-[0.32em] text-cyan-100/40">Authentication Required</p>
            <h2 className="mt-4 text-2xl font-semibold text-cyan-50">No active session</h2>
            <p className="mt-3 text-sm leading-6 text-cyan-100/60">
              Log in first so the moderation desk can call the authenticated admin endpoints.
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (!isAdmin) {
    return (
      <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar
          docsUrl={docsUrl}
          title="Admin Approvals"
          subtitle="This moderation desk is only available to admin users."
        />
        <div className="grid min-h-0 flex-1 place-items-center p-6">
          <div className="w-full max-w-[520px] rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-8 text-center shadow-2xl">
            <p className="text-[11px] uppercase tracking-[0.32em] text-cyan-100/40">Admin Only</p>
            <h2 className="mt-4 text-2xl font-semibold text-cyan-50">Access restricted</h2>
            <p className="mt-3 text-sm leading-6 text-cyan-100/60">
              Your current account does not have admin permissions for `/admin/reported-events`.
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
        title="Admin Approvals"
        subtitle="Moderate reported crime and collision submissions, review reporter context, and approve or reject each event."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid h-full min-h-0 gap-4 xl:grid-cols-[400px,minmax(0,1fr)]">
          <aside className="flex min-h-0 flex-col overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/90 shadow-2xl">
            <div className="flex items-center justify-between gap-3 border-b border-white/5 px-5 py-4">
              <div>
                <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/40">Moderation Queue</p>
                <h2 className="mt-2 text-xl font-semibold text-cyan-50">
                  {loadingList ? "Loading..." : `${formatCount(reports.length)} Loaded`}
                </h2>
              </div>
              <span className="rounded-full border border-cyan-100/10 bg-cyan-50/10 px-3 py-1.5 text-[11px] uppercase tracking-[0.18em] text-cyan-50">
                Admin
              </span>
            </div>

            <div className="grid gap-3 border-b border-white/5 px-4 py-4 md:grid-cols-2">
              <FilterSelect
                label="Status"
                value={statusFilter}
                options={STATUS_OPTIONS}
                onChange={setStatusFilter}
              />
              <FilterSelect
                label="Type"
                value={eventKindFilter}
                options={KIND_OPTIONS}
                onChange={setEventKindFilter}
              />
              <FilterSelect
                label="Reporter"
                value={reporterTypeFilter}
                options={REPORTER_OPTIONS}
                onChange={setReporterTypeFilter}
              />
              <MonthField label="From" value={fromMonth} onChange={setFromMonth} />
              <MonthField label="To" value={toMonth} onChange={setToMonth} />
            </div>

            <div className="flex items-center justify-between border-b border-white/5 px-4 py-3 text-xs text-cyan-100/55">
              <button
                type="button"
                onClick={() => setPageIndex((current) => Math.max(0, current - 1))}
                disabled={pageIndex === 0}
                className="rounded-full border border-white/10 px-3 py-1.5 uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
              >
                Previous
              </button>
              <span>Page {formatCount(pageIndex + 1)}</span>
              <button
                type="button"
                onClick={() => {
                  if (!meta?.nextCursor) {
                    return;
                  }

                  setCursorStack((current) =>
                    current[pageIndex + 1] ? current : [...current, meta.nextCursor],
                  );
                  setPageIndex((current) => current + 1);
                }}
                disabled={!meta?.nextCursor}
                className="rounded-full border border-white/10 px-3 py-1.5 uppercase tracking-[0.18em] text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-35"
              >
                Next
              </button>
            </div>

            {listErrorMessage ? (
              <div className="mx-4 mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                {listErrorMessage}
              </div>
            ) : null}

            <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
              {reports.length ? (
                <div className="flex flex-col gap-3">
                  {reports.map((report) => {
                    const isActive = report.id === selectedReport?.id;

                    return (
                      <button
                        key={report.id}
                        type="button"
                        onClick={() => setSelectedReport(report)}
                        className={`rounded-[20px] border px-4 py-4 text-left transition-colors ${
                          isActive
                            ? "border-cyan-300/30 bg-cyan-50/10"
                            : "border-white/5 bg-[#071316]/70 hover:border-cyan-100/20 hover:bg-cyan-100/5"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">
                              Report #{report.id}
                            </p>
                            <h3 className="mt-2 text-base font-semibold text-cyan-50">
                              {formatEventKind(report.eventKind)} · {formatModerationStatus(report.moderationStatus)}
                            </h3>
                          </div>
                          <span className="rounded-full border border-cyan-100/10 bg-[#030b0e]/70 px-3 py-1 text-[11px] uppercase tracking-[0.18em] text-cyan-100/55">
                            {report.reporterType}
                          </span>
                        </div>

                        <p className="mt-4 text-sm text-cyan-100/60">
                          {formatReportSubtitle(report)}
                        </p>

                        <div className="mt-4 flex items-center justify-between gap-3 text-xs uppercase tracking-[0.18em] text-cyan-100/35">
                          <span>{formatTimestamp(report.createdAt)}</span>
                          <span>{report.reporterEmail || "No email"}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              ) : (
                <div className="grid min-h-full place-items-center rounded-[20px] border border-white/5 bg-[#071316]/60 p-6 text-center">
                  <div>
                    <p className="text-lg font-semibold text-cyan-50">No reports in this queue</p>
                    <p className="mt-2 text-sm text-cyan-100/60">
                      Adjust the moderation filters to inspect a different part of the review queue.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </aside>

          <section className="flex min-h-0 flex-col gap-4 overflow-hidden">
            <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
              {summaryCards.map((card) => (
                <article
                  key={card.label}
                  className="rounded-[20px] border border-white/5 bg-[#030b0e]/90 p-4 shadow-2xl"
                >
                  <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                    {card.label}
                  </p>
                  <p className={`mt-3 text-lg font-semibold ${card.accent}`}>{card.value}</p>
                  <p className="mt-1 text-xs text-cyan-100/60">{card.meta}</p>
                </article>
              ))}
            </div>

            {selectedReport ? (
              <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,0.92fr),minmax(320px,1.08fr)]">
                <div className="min-h-0 overflow-y-auto rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-4 shadow-2xl">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
                        Selected Report
                      </p>
                      <h2 className="mt-2 text-2xl font-semibold text-cyan-50">
                        #{selectedReport.id} · {formatEventKind(selectedReport.eventKind)}
                      </h2>
                    </div>
                    <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.18em] text-cyan-100/60">
                      {formatModerationStatus(selectedReport.moderationStatus)}
                    </span>
                  </div>

                  <div className="mt-4 grid gap-4 lg:grid-cols-2">
                    <DetailCard
                      title="Reporter"
                      rows={[
                        ["Reporter Type", selectedReport.reporterType || "—"],
                        ["Reporter Email", selectedReport.reporterEmail || "Not available"],
                        ["User ID", selectedReport.userId ?? "Anonymous"],
                        ["Created", formatTimestamp(selectedReport.createdAt)],
                      ]}
                    />
                    <DetailCard
                      title="Moderation"
                      rows={[
                        ["Status", formatModerationStatus(selectedReport.moderationStatus)],
                        ["Approved Flag", selectedReport.adminApproved ? "Yes" : "No"],
                        ["Moderated By", selectedReport.moderatedBy ?? "Pending"],
                        ["Moderated At", selectedReport.moderatedAt ? formatTimestamp(selectedReport.moderatedAt) : "Pending"],
                      ]}
                    />
                    <DetailCard
                      title="Submission"
                      rows={[
                        ["Event Date", selectedReport.eventDate || "—"],
                        ["Event Time", selectedReport.eventTime || "—"],
                        ["Month", selectedReport.month || "—"],
                        ["Segment ID", selectedReport.segmentId ?? "Not snapped"],
                        ["Snap Distance", selectedReport.snapDistanceM !== null ? `${selectedReport.snapDistanceM}m` : "Not snapped"],
                      ]}
                    />
                    <DetailCard
                      title={selectedReport.eventKind === "crime" ? "Crime Details" : "Collision Details"}
                      rows={selectedReport.eventKind === "crime"
                        ? [["Crime Type", selectedReport.details?.crimeType || "—"]]
                        : [
                            ["Weather", selectedReport.details?.weatherCondition || "—"],
                            ["Light", selectedReport.details?.lightCondition || "—"],
                            ["Vehicles", selectedReport.details?.numberOfVehicles || "—"],
                          ]}
                    />
                    <DetailCard
                      title="Location"
                      rows={[
                        ["Longitude", selectedReport.longitude],
                        ["Latitude", selectedReport.latitude],
                      ]}
                    />
                  </div>

                  <div className="mt-4 rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">
                      Description
                    </p>
                    <p className="mt-3 text-sm leading-6 text-cyan-100/75">
                      {selectedReport.description || "No description was supplied for this report."}
                    </p>
                  </div>

                  <div className="mt-4 rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">
                          Moderation Action
                        </p>
                        <h3 className="mt-2 text-lg font-semibold text-cyan-50">Approve or reject this report</h3>
                      </div>
                    </div>

                    <label className="mt-4 flex flex-col gap-2">
                      <span className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">
                        Moderation Notes
                      </span>
                      <textarea
                        value={moderationNotes}
                        onChange={(event) => setModerationNotes(event.target.value)}
                        rows={4}
                        placeholder="Optional moderation notes for the reporting team."
                        className="rounded-[16px] border border-cyan-200/10 bg-[#021116] px-4 py-3 text-sm text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
                      />
                    </label>

                    {moderationFeedback ? (
                      <div
                        className={`mt-4 rounded-[16px] border px-4 py-3 text-sm ${
                          moderationFeedback.tone === "error"
                            ? "border-red-300/30 bg-[#4a0f0fd0] text-red-100"
                            : "border-emerald-300/20 bg-emerald-400/10 text-emerald-100"
                        }`}
                      >
                        {moderationFeedback.text}
                      </div>
                    ) : null}

                    <div className="mt-4 flex flex-wrap gap-3">
                      <button
                        type="button"
                        disabled={savingModeration}
                        onClick={() =>
                          handleModerationAction({
                            accessToken,
                            moderationNotes,
                            report: selectedReport,
                            setModerationFeedback,
                            setRefreshToken,
                            setReports,
                            setSavingModeration,
                            setSelectedReport,
                            status: "approved",
                          })
                        }
                        className="rounded-full border border-emerald-300/20 bg-emerald-400/10 px-4 py-2 text-sm font-medium uppercase tracking-[0.18em] text-emerald-100 transition-colors hover:bg-emerald-400/20 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {savingModeration ? "Saving..." : "Approve Report"}
                      </button>
                      <button
                        type="button"
                        disabled={savingModeration}
                        onClick={() =>
                          handleModerationAction({
                            accessToken,
                            moderationNotes,
                            report: selectedReport,
                            setModerationFeedback,
                            setRefreshToken,
                            setReports,
                            setSavingModeration,
                            setSelectedReport,
                            status: "rejected",
                          })
                        }
                        className="rounded-full border border-red-300/20 bg-red-400/10 px-4 py-2 text-sm font-medium uppercase tracking-[0.18em] text-red-100 transition-colors hover:bg-red-400/20 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {savingModeration ? "Saving..." : "Reject Report"}
                      </button>
                    </div>
                  </div>
                </div>

                <ReportedEventMap report={selectedReport} />
              </div>
            ) : (
              <div className="grid min-h-0 flex-1 place-items-center rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-6 shadow-2xl">
                <div className="text-center">
                  <p className="text-lg font-semibold text-cyan-50">No report selected</p>
                  <p className="mt-2 text-sm text-cyan-100/60">
                    Pick a report from the moderation queue to inspect the evidence and approve or reject it.
                  </p>
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

async function handleModerationAction({
  accessToken,
  moderationNotes,
  report,
  setModerationFeedback,
  setRefreshToken,
  setReports,
  setSavingModeration,
  setSelectedReport,
  status,
}) {
  if (!report?.id) {
    return;
  }

  setSavingModeration(true);
  setModerationFeedback(null);

  try {
    const updatedReport = await reportedEventsService.moderateReportedEvent(
      report.id,
      {
        moderation_status: status,
        moderation_notes: moderationNotes.trim() || null,
      },
      accessToken,
    );

    setSelectedReport(updatedReport);
    setReports((current) =>
      current.map((item) => (item.id === updatedReport.id ? updatedReport : item)),
    );
    setModerationFeedback({
      tone: "success",
      text: `Report #${updatedReport.id} marked as ${formatModerationStatus(status).toLowerCase()}.`,
    });
    setRefreshToken((current) => current + 1);
  } catch (error) {
    setModerationFeedback({
      tone: "error",
      text: error?.message || "Failed to update the report moderation status.",
    });
  } finally {
    setSavingModeration(false);
  }
}

function ReportedEventMap({ report }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const pointFeature = useMemo(() => buildPointFeature(report), [report]);
  const roadsTileUrl = useMemo(
    () =>
      tilesService.getRoadVectorTilesUrl({
        includeRisk: false,
        startMonth: DEFAULT_MONTH_FROM,
        endMonth: DEFAULT_MONTH_TO,
      }),
    [],
  );
  const mapIsSupported = useMemo(() => mapboxgl.supported(), []);
  const resolvedMapErrorMessage = mapIsSupported
    ? mapRuntimeErrorMessage
    : "Mapbox GL JS is not supported in this browser.";

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current || !mapIsSupported) {
      return undefined;
    }

    if (config.mapboxAccessToken) {
      mapboxgl.accessToken = config.mapboxAccessToken;
    }

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      center: WEST_YORKSHIRE_CENTER,
      zoom: 13,
      attributionControl: false,
      style: {
        version: 8,
        sources: {
          darkBase: {
            type: "raster",
            tiles: ["https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "By Adil Aabideen",
          },
          [ROADS_SOURCE_ID]: {
            type: "vector",
            tiles: [roadsTileUrl],
            minzoom: 0,
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
          {
            id: ROADS_LAYER_ID,
            type: "line",
            source: ROADS_SOURCE_ID,
            "source-layer": "roads",
            paint: {
              "line-color": "#00d26a",
              "line-width": [
                "interpolate",
                ["linear"],
                ["zoom"],
                8,
                0.35,
                12,
                0.7,
                16,
                1.4,
              ],
              "line-opacity": 0.75,
            },
          },
        ],
      },
    });

    mapRef.current = map;

    map.on("load", () => {
      map.addSource(LOCATION_SOURCE_ID, {
        type: "geojson",
        data: pointFeature || emptyFeatureCollection(),
      });

      map.addLayer({
        id: LOCATION_HALO_LAYER_ID,
        type: "circle",
        source: LOCATION_SOURCE_ID,
        paint: {
          "circle-radius": 18,
          "circle-color": "rgba(34, 211, 238, 0.18)",
          "circle-stroke-width": 0,
        },
      });

      map.addLayer({
        id: LOCATION_POINT_LAYER_ID,
        type: "circle",
        source: LOCATION_SOURCE_ID,
        paint: {
          "circle-radius": 7,
          "circle-color": report?.eventKind === "crime" ? "#39ef7d" : "#60a5fa",
          "circle-stroke-color": "#021116",
          "circle-stroke-width": 2,
        },
      });

      map.fitBounds(
        [
          [WEST_YORKSHIRE_BBOX.minLon, WEST_YORKSHIRE_BBOX.minLat],
          [WEST_YORKSHIRE_BBOX.maxLon, WEST_YORKSHIRE_BBOX.maxLat],
        ],
        { padding: 40, duration: 0 },
      );
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
  }, [mapIsSupported, pointFeature, report?.eventKind, roadsTileUrl]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map || !map.isStyleLoaded()) {
      return;
    }

    const source = map.getSource(LOCATION_SOURCE_ID);

    if (source) {
      source.setData(pointFeature || emptyFeatureCollection());
    }

    if (pointFeature) {
      map.easeTo({
        center: pointFeature.features[0].geometry.coordinates,
        zoom: 15,
        duration: 300,
      });
    }
  }, [pointFeature]);

  return (
    <div className="flex min-h-0 flex-col overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/90 p-4 shadow-2xl">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">Report Location</p>
          <h2 className="mt-2 text-xl font-semibold text-cyan-50">Map Preview</h2>
        </div>
        {report ? (
          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-xs uppercase tracking-[0.18em] text-cyan-100/60">
            {formatEventKind(report.eventKind)}
          </span>
        ) : null}
      </div>

      {resolvedMapErrorMessage ? (
        <div className="mt-4 rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
          {resolvedMapErrorMessage}
        </div>
      ) : null}

      <div className="mt-4 rounded-[16px] border border-white/5 bg-[#071316]/70 px-4 py-3 text-sm text-cyan-100/65">
        {report
          ? `${report.latitude.toFixed(5)}, ${report.longitude.toFixed(5)}`
          : "No coordinates available"}
      </div>

      <div ref={mapContainerRef} className="mt-4 min-h-[320px] flex-1 overflow-hidden rounded-[18px]" />
    </div>
  );
}

function DetailCard({ title, rows }) {
  return (
    <section className="rounded-[20px] border border-white/5 bg-[#071316]/70 p-4">
      <p className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">{title}</p>
      <div className="mt-4 space-y-3">
        {rows.map(([label, value]) => (
          <div key={label}>
            <p className="text-xs uppercase tracking-[0.18em] text-cyan-100/40">{label}</p>
            <p className="mt-1 text-sm font-medium text-cyan-50">{value}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

function FilterSelect({ label, value, options, onChange }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">{label}</span>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-[14px] border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm text-cyan-50 outline-none transition-colors focus:border-cyan-400/40"
      >
        {options.map((option) => (
          <option key={option.value || "all"} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function MonthField({ label, value, onChange }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.24em] text-cyan-100/45">{label}</span>
      <input
        type="month"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-[14px] border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm text-cyan-50 outline-none transition-colors focus:border-cyan-400/40"
      />
    </label>
  );
}

function buildPointFeature(report) {
  if (!report) {
    return null;
  }

  return {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: {
          type: "Point",
          coordinates: [report.longitude, report.latitude],
        },
        properties: {},
      },
    ],
  };
}

function emptyFeatureCollection() {
  return { type: "FeatureCollection", features: [] };
}

function formatEventKind(value) {
  if (value === "crime") {
    return "Crime";
  }

  if (value === "collision") {
    return "Collision";
  }

  return value || "Unknown";
}

function formatModerationStatus(value) {
  if (!value) {
    return "Pending";
  }

  return value.charAt(0).toUpperCase() + value.slice(1);
}

function formatReportSubtitle(report) {
  if (report.eventKind === "crime") {
    return report.details?.crimeType || "Crime type not supplied";
  }

  return [
    report.details?.weatherCondition,
    report.details?.lightCondition,
    report.details?.numberOfVehicles
      ? `${report.details.numberOfVehicles} vehicles`
      : null,
  ]
    .filter(Boolean)
    .join(" · ");
}

function formatTimestamp(value) {
  if (!value) {
    return "Unknown timestamp";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatCount(value) {
  return new Intl.NumberFormat("en-GB").format(Number(value) || 0);
}

export default AdminApprovalsPage;
