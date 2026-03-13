import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import TopBar from "./TopBar";
import { CRIME_TYPE_OPTIONS } from "../constants/crimeFilterOptions";
import { config } from "../config/env";
import { reportedEventsService, roadsService } from "../services";
import {
  DEFAULT_MONTH_FROM,
  DEFAULT_MONTH_TO,
  WEST_YORKSHIRE_BBOX,
  WEST_YORKSHIRE_CENTER,
} from "../utils/crimeUtils";

const LOCATION_SOURCE_ID = "reported-event-location";
const LOCATION_HALO_LAYER_ID = "reported-event-location-halo";
const LOCATION_POINT_LAYER_ID = "reported-event-location-point";
const ROADS_SOURCE_ID = "reported-event-roads";
const ROADS_LAYER_ID = "reported-event-roads-layer";
const WEATHER_CONDITION_OPTIONS = [
  "Fine no high winds",
  "Raining no high winds",
  "Other",
];
const LIGHT_CONDITION_OPTIONS = [
  "Daylight",
  "Darkness - lights lit",
  "Darkness - no lighting",
  "Darkness - lighting unknown",
];

function ReportedEventPage({
  docsUrl,
  accessToken,
  eventKind,
  title,
  subtitle,
  onReportCreated,
}) {
  const [locationInputMode, setLocationInputMode] = useState("map");
  const [submittingReport, setSubmittingReport] = useState(false);
  const [reportErrorMessage, setReportErrorMessage] = useState("");
  const [submittedReport, setSubmittedReport] = useState(null);
  const [reportForm, setReportForm] = useState(() =>
    createDefaultReportForm(eventKind),
  );
  const isCrime = eventKind === "crime";
  const parsedCoordinates = useMemo(
    () => parseCoordinates(reportForm.longitude, reportForm.latitude),
    [reportForm.latitude, reportForm.longitude],
  );
  const hasSharedFields = Boolean(reportForm.eventDate) && Boolean(parsedCoordinates);
  const hasEventSpecificFields = isCrime
    ? Boolean(reportForm.crimeType.trim())
    : [
        reportForm.weatherCondition.trim(),
        reportForm.lightCondition.trim(),
        Number(reportForm.numberOfVehicles) > 0,
      ].every(Boolean);
  const canSubmit =
    hasSharedFields &&
    hasEventSpecificFields &&
    !submittingReport;

  useEffect(() => {
    setReportForm(createDefaultReportForm(eventKind));
    setSubmittedReport(null);
    setReportErrorMessage("");
    setLocationInputMode("map");
  }, [eventKind]);

  const handleFieldChange = (key, value) => {
    setReportErrorMessage("");
    setReportForm((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const handleCoordinateSelect = ({ longitude, latitude }) => {
    setReportErrorMessage("");
    setReportForm((current) => ({
      ...current,
      longitude: formatCoordinateInput(longitude),
      latitude: formatCoordinateInput(latitude),
    }));
  };

  const handleSubmit = async () => {
    if (!canSubmit || !parsedCoordinates) {
      return;
    }

    const payload = {
      event_kind: eventKind,
      event_date: reportForm.eventDate,
      event_time: reportForm.eventTime.trim() || null,
      longitude: parsedCoordinates.longitude,
      latitude: parsedCoordinates.latitude,
      description: reportForm.description.trim() || null,
      ...(isCrime
        ? {
            crime: {
              crime_type: reportForm.crimeType.trim(),
            },
          }
        : {
            collision: {
              weather_condition: reportForm.weatherCondition.trim(),
              light_condition: reportForm.lightCondition.trim(),
              number_of_vehicles: Number(reportForm.numberOfVehicles),
            },
          }),
    };

    setSubmittingReport(true);
    setReportErrorMessage("");

    try {
      const report = await reportedEventsService.createReportedEvent(
        payload,
        accessToken,
      );

      setSubmittedReport(report);
      setReportForm(createDefaultReportForm(eventKind, {
        longitude: formatCoordinateInput(parsedCoordinates.longitude),
        latitude: formatCoordinateInput(parsedCoordinates.latitude),
      }));
      setLocationInputMode("map");
      onReportCreated?.(report);
    } catch (error) {
      setReportErrorMessage(error?.message || "Failed to submit the report.");
    } finally {
      setSubmittingReport(false);
    }
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title={title}
        subtitle={subtitle}
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid h-full min-h-0 grid-cols-2 grid-rows-1 overflow-hidden rounded-[26px] border border-white/5">
          <section className="flex h-full min-h-0 flex-col border-r-2 border-r-white/5">
            <div className="border-b border-white/5 px-5 py-4">
              <p className="text-[11px] uppercase tracking-[0.35em] text-cyan-100/40">
                Public Submission
              </p>
              <h2 className="mt-2 text-xl font-semibold text-cyan-50">{title}</h2>
              <p className="mt-2 max-w-xl text-sm text-cyan-100/55">
                Submit a {eventKind} report with date, time, description, and a precise location. Coordinates can come from the map or manual text entry.
              </p>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-3 py-0">
              <div className="space-y-3 rounded-[24px] p-4">
                <ReportField
                  label="EVENT KIND"
                  value={isCrime ? "Crime" : "Collision"}
                  readOnly
                />
                <div className="grid gap-3 md:grid-cols-2">
                  <ReportField
                    label="EVENT DATE"
                    type="date"
                    value={reportForm.eventDate}
                    onChange={(value) => handleFieldChange("eventDate", value)}
                  />
                  <ReportField
                    label="EVENT TIME"
                    type="time"
                    value={reportForm.eventTime}
                    onChange={(value) => handleFieldChange("eventTime", value)}
                    placeholder="17:45"
                  />
                </div>

                <ReportTextArea
                  label="DESCRIPTION"
                  value={reportForm.description}
                  placeholder={
                    isCrime
                      ? "Describe what happened, where it happened, and any immediate concerns."
                      : "Describe the collision, junction reference, and any visible impact."
                  }
                  onChange={(value) => handleFieldChange("description", value)}
                />

                <div className="rounded-[18px] border border-cyan-200/10 bg-[#071316]/70 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <p className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
                        Location Coordinates
                      </p>
                      <p className="mt-1 text-sm text-cyan-100/55">
                        {locationInputMode === "map"
                          ? "Click the map on the right to populate longitude and latitude."
                          : "Type longitude and latitude directly, then the map preview will update."}
                      </p>
                    </div>

                    <div className="inline-flex rounded-full border border-cyan-100/10 bg-[#030b0e]/80 p-1">
                      <LocationModeButton
                        label="Map Input"
                        active={locationInputMode === "map"}
                        onClick={() => setLocationInputMode("map")}
                      />
                      <LocationModeButton
                        label="Text Input"
                        active={locationInputMode === "text"}
                        onClick={() => setLocationInputMode("text")}
                      />
                    </div>
                  </div>

                  <div className="mt-4 grid gap-3 md:grid-cols-2">
                    <ReportField
                      label="LONGITUDE"
                      value={reportForm.longitude}
                      placeholder="-1.549000"
                      inputMode="decimal"
                      readOnly={locationInputMode === "map"}
                      onChange={(value) => handleFieldChange("longitude", value)}
                    />
                    <ReportField
                      label="LATITUDE"
                      value={reportForm.latitude}
                      placeholder="53.801000"
                      inputMode="decimal"
                      readOnly={locationInputMode === "map"}
                      onChange={(value) => handleFieldChange("latitude", value)}
                    />
                  </div>
                </div>

                {isCrime ? (
                  <ReportDatalistField
                    label="CRIME TYPE"
                    value={reportForm.crimeType}
                    options={CRIME_TYPE_OPTIONS.map((option) => option.value)}
                    listId="reported-crime-type-options"
                    placeholder="Violence and sexual offences"
                    onChange={(value) => handleFieldChange("crimeType", value)}
                  />
                ) : (
                  <>
                    <ReportDatalistField
                      label="WEATHER CONDITION"
                      value={reportForm.weatherCondition}
                      options={WEATHER_CONDITION_OPTIONS}
                      listId="reported-weather-condition-options"
                      placeholder="Fine no high winds"
                      onChange={(value) => handleFieldChange("weatherCondition", value)}
                    />
                    <ReportDatalistField
                      label="LIGHT CONDITION"
                      value={reportForm.lightCondition}
                      options={LIGHT_CONDITION_OPTIONS}
                      listId="reported-light-condition-options"
                      placeholder="Daylight"
                      onChange={(value) => handleFieldChange("lightCondition", value)}
                    />
                    <ReportField
                      label="NUMBER OF VEHICLES"
                      type="number"
                      min="1"
                      step="1"
                      value={reportForm.numberOfVehicles}
                      placeholder="2"
                      onChange={(value) => handleFieldChange("numberOfVehicles", value)}
                    />
                  </>
                )}

                {reportErrorMessage ? (
                  <div className="rounded-[16px] border border-red-300/30 bg-[#4a0f0fd0] px-4 py-3 text-sm text-red-100">
                    {reportErrorMessage}
                  </div>
                ) : null}

                {submittedReport ? (
                  <ReportedEventSuccessCard report={submittedReport} />
                ) : null}
              </div>
            </div>

            <div className="border-t border-white/5 px-5 py-4">
              <p className="text-sm text-cyan-100/55">
                {accessToken
                  ? "This report will be submitted as an authenticated user."
                  : "No token detected. This report will be submitted anonymously."}
              </p>

              <button
                type="button"
                onClick={handleSubmit}
                disabled={!canSubmit}
                className="mt-4 w-full rounded-[16px] bg-cyan-50 px-4 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-[#021116] transition-colors hover:bg-white disabled:cursor-not-allowed disabled:bg-cyan-100/20 disabled:text-cyan-100/40"
              >
                {submittingReport
                  ? `Submitting ${isCrime ? "Crime" : "Collision"}...`
                  : `Submit ${isCrime ? "Crime" : "Collision"} Report`}
              </button>
            </div>
          </section>

          <ReportedEventLocationPanel
            eventKind={eventKind}
            locationInputMode={locationInputMode}
            coordinates={parsedCoordinates}
            onCoordinateSelect={handleCoordinateSelect}
          />
        </div>
      </div>
    </div>
  );
}

function ReportedEventLocationPanel({
  eventKind,
  locationInputMode,
  coordinates,
  onCoordinateSelect,
}) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const locationModeRef = useRef(locationInputMode);
  const onCoordinateSelectRef = useRef(onCoordinateSelect);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const pointFeature = useMemo(
    () => buildPointFeature(coordinates),
    [coordinates],
  );
  const roadsTileUrl = useMemo(
    () =>
      roadsService.getVectorTilesUrl({
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
    locationModeRef.current = locationInputMode;
    onCoordinateSelectRef.current = onCoordinateSelect;
  }, [locationInputMode, onCoordinateSelect]);

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
      minZoom: 7,
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
    map.addControl(new mapboxgl.NavigationControl({ visualizePitch: false }), "bottom-right");

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
          "circle-color": eventKind === "crime" ? "#39ef7d" : "#60a5fa",
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

    map.on("click", (event) => {
      if (locationModeRef.current !== "map") {
        return;
      }

      onCoordinateSelectRef.current({
        longitude: event.lngLat.lng,
        latitude: event.lngLat.lat,
      });
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
  }, [eventKind, mapIsSupported, pointFeature, roadsTileUrl]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map) {
      return;
    }

    map.getCanvas().style.cursor = locationInputMode === "map" ? "crosshair" : "";
  }, [locationInputMode]);

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
        center: pointFeature.geometry.coordinates,
        zoom: Math.max(map.getZoom(), 14),
        duration: 300,
      });
    }
  }, [pointFeature]);

  return (
    <section className="relative h-full min-h-0 overflow-hidden bg-[#020a0f]">
      <div className="absolute left-4 right-4 top-4 z-10 flex flex-wrap items-start justify-between gap-3">
        <div className="rounded-[18px] border border-white/10 bg-[#030b0e]/80 px-4 py-3 backdrop-blur-sm">
          <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">
            {locationInputMode === "map" ? "Map Input" : "Text Input"}
          </p>
          <p className="mt-2 text-sm text-cyan-50">
            {locationInputMode === "map"
              ? "Click anywhere on the map to set the report location."
              : "Enter longitude and latitude in the form. The map will preview the point."}
          </p>
        </div>

        <div className="rounded-[16px] border border-white/10 bg-[#030b0e]/80 px-4 py-3 backdrop-blur-sm">
          <p className="text-xs text-cyan-100/55">
            {pointFeature ? "Location selected" : "No location selected"}
          </p>
          <p className="mt-1 text-sm font-medium text-cyan-50">
            {coordinates
              ? `${coordinates.latitude.toFixed(5)}, ${coordinates.longitude.toFixed(5)}`
              : "Awaiting coordinates"}
          </p>
        </div>
      </div>

      {resolvedMapErrorMessage ? (
        <div className="absolute left-4 right-4 top-28 z-10 rounded-[18px] border border-red-300/30 bg-[#480000d0] px-4 py-3 text-sm text-red-100">
          {resolvedMapErrorMessage}
        </div>
      ) : null}

      {locationInputMode === "text" ? (
        <div className="pointer-events-none absolute bottom-4 left-4 z-10 rounded-[16px] border border-white/10 bg-[#030b0e]/80 px-4 py-3 backdrop-blur-sm">
          <p className="text-xs text-cyan-100/55">Manual coordinate mode</p>
          <p className="mt-1 text-sm font-medium text-cyan-50">
            Map clicks are disabled until you switch back to map input.
          </p>
        </div>
      ) : null}

      <div ref={mapContainerRef} className="h-full w-full" />
    </section>
  );
}

function ReportField({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  inputMode,
  min,
  step,
  readOnly = false,
}) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>
      <input
        type={type}
        value={value}
        inputMode={inputMode}
        min={min}
        step={step}
        placeholder={placeholder}
        readOnly={readOnly}
        onChange={(event) => onChange?.(event.target.value)}
        className={`rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 ${
          readOnly
            ? "cursor-default opacity-80"
            : "focus:border-cyan-400/40"
        }`}
      />
    </label>
  );
}

function ReportTextArea({ label, value, onChange, placeholder }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>
      <textarea
        value={value}
        rows={4}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-3 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
      />
    </label>
  );
}

function ReportDatalistField({
  label,
  value,
  onChange,
  placeholder,
  listId,
  options,
}) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}
      </span>
      <input
        list={listId}
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
      />
      <datalist id={listId}>
        {options.map((option) => (
          <option key={option} value={option} />
        ))}
      </datalist>
    </label>
  );
}

function LocationModeButton({ label, active, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-3 py-1.5 text-xs uppercase tracking-[0.18em] transition-colors ${
        active
          ? "bg-cyan-50 text-[#021116]"
          : "text-cyan-100/60 hover:bg-cyan-100/10 hover:text-cyan-50"
      }`}
    >
      {label}
    </button>
  );
}

function ReportedEventSuccessCard({ report }) {
  return (
    <div className="rounded-[18px] border border-cyan-400/20 bg-cyan-400/5 p-4">
      <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/45">
        Report Submitted
      </p>
      <h3 className="mt-2 text-lg font-semibold text-cyan-50">
        Report #{report.id || "Pending"}
      </h3>
      <div className="mt-4 grid gap-3 md:grid-cols-2">
        <SuccessMetric
          label="Moderation"
          value={report.moderationStatus || "pending"}
        />
        <SuccessMetric
          label="Reporter Type"
          value={report.reporterType || "anonymous"}
        />
        <SuccessMetric
          label="Snap Distance"
          value={
            report.snapDistanceM !== null && report.snapDistanceM !== undefined
              ? `${report.snapDistanceM}m`
              : "Not snapped yet"
          }
        />
        <SuccessMetric
          label="Created"
          value={formatCreatedAt(report.createdAt)}
        />
      </div>
    </div>
  );
}

function SuccessMetric({ label, value }) {
  return (
    <div className="rounded-[14px] border border-cyan-100/10 bg-[#030b0e]/65 px-3 py-3">
      <p className="text-[11px] uppercase tracking-[0.2em] text-cyan-100/45">{label}</p>
      <p className="mt-2 text-sm font-semibold text-cyan-50">{value}</p>
    </div>
  );
}

function createDefaultReportForm(eventKind, overrides = {}) {
  const now = new Date();
  const defaultEventDate = new Date(
    now.getTime() - now.getTimezoneOffset() * 60_000,
  )
    .toISOString()
    .slice(0, 10);

  return {
    eventDate: defaultEventDate,
    eventTime: "",
    longitude: "",
    latitude: "",
    description: "",
    crimeType: "",
    weatherCondition: "",
    lightCondition: "",
    numberOfVehicles: eventKind === "collision" ? "2" : "",
    ...overrides,
  };
}

function parseCoordinates(longitudeValue, latitudeValue) {
  const longitude = Number(longitudeValue);
  const latitude = Number(latitudeValue);

  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    return null;
  }

  return { longitude, latitude };
}

function formatCoordinateInput(value) {
  return Number(value).toFixed(6);
}

function buildPointFeature(coordinates) {
  if (!coordinates) {
    return null;
  }

  return {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: {
          type: "Point",
          coordinates: [coordinates.longitude, coordinates.latitude],
        },
        properties: {},
      },
    ],
  };
}

function emptyFeatureCollection() {
  return { type: "FeatureCollection", features: [] };
}

function formatCreatedAt(value) {
  if (!value) {
    return "Just now";
  }

  const createdAt = new Date(value);

  if (Number.isNaN(createdAt.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(createdAt);
}

export default ReportedEventPage;
