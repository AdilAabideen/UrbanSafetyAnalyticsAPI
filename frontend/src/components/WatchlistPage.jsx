import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import TopBar from "./TopBar";
import { config } from "../config/env";
import { roadsService } from "../services";
import {
  DEFAULT_MONTH_FROM,
  DEFAULT_MONTH_TO,
  WEST_YORKSHIRE_BBOX,
  WEST_YORKSHIRE_CENTER,
} from "../utils/crimeUtils";

const ROADS_SOURCE_ID = "watchlist-roads";
const ROADS_LAYER_ID = "watchlist-roads-layer";
const DRAW_SOURCE_ID = "watchlist-draw";
const DRAW_FILL_LAYER_ID = "watchlist-draw-fill";
const DRAW_LINE_LAYER_ID = "watchlist-draw-line";
const DRAW_POINT_LAYER_ID = "watchlist-draw-point";
const EMPTY_FEATURE_COLLECTION = { type: "FeatureCollection", features: [] };

function WatchlistPage({ docsUrl }) {
  const [watchlistForm, setWatchlistForm] = useState(createDefaultWatchlistForm);
  const [polygonPoints, setPolygonPoints] = useState([]);
  const [polygonClosed, setPolygonClosed] = useState(false);
  const parsedBbox = useMemo(() => parseBboxFromForm(watchlistForm), [watchlistForm]);
  const isFormComplete = useMemo(
    () =>
      [
        watchlistForm.id,
        watchlistForm.userId,
        watchlistForm.name,
        watchlistForm.createdAt,
        watchlistForm.minLon,
        watchlistForm.minLat,
        watchlistForm.maxLon,
        watchlistForm.maxLat,
      ].every((value) => String(value).trim().length > 0),
    [watchlistForm],
  );
  const canProceed = isFormComplete && Boolean(parsedBbox) && polygonClosed;

  const handleFieldChange = (key, value) => {
    setWatchlistForm((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const clearPolygon = () => {
    setPolygonPoints([]);
    setPolygonClosed(false);
    setWatchlistForm((current) => ({
      ...current,
      minLon: "",
      minLat: "",
      maxLon: "",
      maxLat: "",
    }));
  };

  const handlePolygonStart = () => {
    clearPolygon();
  };

  const handlePolygonDraft = (points) => {
    setPolygonPoints(points);
    setPolygonClosed(false);
  };

  const handlePolygonComplete = (points, bbox) => {
    setPolygonPoints(points);
    setPolygonClosed(true);
    setWatchlistForm((current) => ({
      ...current,
      minLon: formatCoordinateInput(bbox.minLon),
      minLat: formatCoordinateInput(bbox.minLat),
      maxLon: formatCoordinateInput(bbox.maxLon),
      maxLat: formatCoordinateInput(bbox.maxLat),
    }));
  };

  const handleNext = () => {
    if (!canProceed || !parsedBbox) {
      return;
    }

    console.log("watchlistDraft", {
      id: watchlistForm.id,
      user_id: watchlistForm.userId,
      name: watchlistForm.name,
      min_lon: parsedBbox.minLon,
      min_lat: parsedBbox.minLat,
      max_lon: parsedBbox.maxLon,
      max_lat: parsedBbox.maxLat,
      created_at: watchlistForm.createdAt,
      polygon: toClosedPolygonCoordinates(polygonPoints),
    });
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Watchlists"
        subtitle="Create a watchlist by entering the fields on the left and drawing a polygon on the map."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="grid grid-cols-2 grid-rows-1 h-full min-h-0 overflow-hidden rounded-[26px] border border-white/5  ">
          <section className="flex h-full min-h-0 flex-col border-r-2 border-r-white/5 ">
            <div className="border-b border-white/5 px-5 py-4">
              <p className="text-[11px] uppercase tracking-[0.35em] text-cyan-100/40">
                Watchlist Setup
              </p>
              <h2 className="mt-2 text-xl font-semibold text-cyan-50">Input Fields</h2>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-0 ">
              <div className="space-y-3 rounded-[24px]  p-4">
                <WatchlistField
                  label="ID"
                  value={watchlistForm.id}
                  placeholder="1001"
                  onChange={(value) => handleFieldChange("id", value)}
                />
                <WatchlistField
                  label="USER_ID"
                  value={watchlistForm.userId}
                  placeholder="42"
                  onChange={(value) => handleFieldChange("userId", value)}
                />
                <WatchlistField
                  label="NAME"
                  value={watchlistForm.name}
                  placeholder="City Centre Risk Watch"
                  onChange={(value) => handleFieldChange("name", value)}
                />
                <WatchlistField
                  label="CREATED_AT"
                  type="datetime-local"
                  value={watchlistForm.createdAt}
                  onChange={(value) => handleFieldChange("createdAt", value)}
                />
                <WatchlistField
                  label="MIN_LON"
                  value={watchlistForm.minLon}
                  placeholder="-1.620000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("minLon", value)}
                />
                <WatchlistField
                  label="MIN_LAT"
                  value={watchlistForm.minLat}
                  placeholder="53.780000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("minLat", value)}
                />
                <WatchlistField
                  label="MAX_LON"
                  value={watchlistForm.maxLon}
                  placeholder="-1.500000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("maxLon", value)}
                />
                <WatchlistField
                  label="MAX_LAT"
                  value={watchlistForm.maxLat}
                  placeholder="53.840000"
                  inputMode="decimal"
                  onChange={(value) => handleFieldChange("maxLat", value)}
                />
              </div>
            </div>

            <div className="border-t border-white/5 px-5 py-4">
              <p className="text-sm text-cyan-100/55">
                {canProceed
                  ? "Polygon captured and all fields are populated."
                  : "Complete every field and finish the polygon on the map to continue."}
              </p>

              {canProceed ? (
                <button
                  type="button"
                  onClick={handleNext}
                  className="mt-4 w-full rounded-[16px] bg-cyan-50 px-4 py-3 text-sm font-semibold uppercase tracking-[0.18em] text-[#021116] transition-colors hover:bg-white"
                >
                  Next
                </button>
              ) : null}
            </div>
          </section>

          <WatchlistPolygonMap
            polygonPoints={polygonPoints}
            polygonClosed={polygonClosed}
            onStartPolygon={handlePolygonStart}
            onPolygonDraft={handlePolygonDraft}
            onPolygonComplete={handlePolygonComplete}
            onClearPolygon={clearPolygon}
          />
        </div>
      </div>
    </div>
  );
}

function WatchlistPolygonMap({
  polygonPoints,
  polygonClosed,
  onStartPolygon,
  onPolygonDraft,
  onPolygonComplete,
  onClearPolygon,
}) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const polygonPointsRef = useRef(polygonPoints);
  const polygonClosedRef = useRef(polygonClosed);
  const drawModeRef = useRef(false);
  const onStartPolygonRef = useRef(onStartPolygon);
  const onPolygonDraftRef = useRef(onPolygonDraft);
  const onPolygonCompleteRef = useRef(onPolygonComplete);
  const onClearPolygonRef = useRef(onClearPolygon);
  const [drawMode, setDrawMode] = useState(false);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const roadsTileUrl = useMemo(
    () => roadsService.getVectorTilesUrl({ startMonth: DEFAULT_MONTH_FROM, endMonth: DEFAULT_MONTH_TO }),
    [],
  );
  const mapIsSupported = useMemo(() => mapboxgl.supported(), []);
  const resolvedMapErrorMessage = mapIsSupported
    ? mapRuntimeErrorMessage
    : "Mapbox GL JS is not supported in this browser.";

  useEffect(() => {
    polygonPointsRef.current = polygonPoints;
    polygonClosedRef.current = polygonClosed;
    onStartPolygonRef.current = onStartPolygon;
    onPolygonDraftRef.current = onPolygonDraft;
    onPolygonCompleteRef.current = onPolygonComplete;
    onClearPolygonRef.current = onClearPolygon;
  }, [onClearPolygon, onPolygonComplete, onPolygonDraft, onStartPolygon, polygonClosed, polygonPoints]);

  useEffect(() => {
    drawModeRef.current = drawMode;
  }, [drawMode]);

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
              "line-color": [
                "match",
                ["downcase", ["coalesce", ["get", "band"], ""]],
                "green",
                "#00d26a",
                "orange",
                "#f97316",
                "red",
                "#ef4444",
                "#00d26a",
              ],
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
              "line-opacity": 0.78,
            },
          },
        ],
      },
    });

    mapRef.current = map;
    map.addControl(new mapboxgl.NavigationControl({ visualizePitch: false }), "bottom-right");

    map.on("load", () => {
      map.addSource(DRAW_SOURCE_ID, {
        type: "geojson",
        data: buildDrawFeatureCollection(polygonPointsRef.current, polygonClosedRef.current),
      });

      map.addLayer({
        id: DRAW_FILL_LAYER_ID,
        type: "fill",
        source: DRAW_SOURCE_ID,
        filter: ["==", ["geometry-type"], "Polygon"],
        paint: {
          "fill-color": "#22d3ee",
          "fill-opacity": 0.16,
        },
      });

      map.addLayer({
        id: DRAW_LINE_LAYER_ID,
        type: "line",
        source: DRAW_SOURCE_ID,
        filter: ["any", ["==", ["geometry-type"], "LineString"], ["==", ["geometry-type"], "Polygon"]],
        paint: {
          "line-color": "#f8fafc",
          "line-width": 2.2,
          "line-opacity": 0.9,
        },
      });

      map.addLayer({
        id: DRAW_POINT_LAYER_ID,
        type: "circle",
        source: DRAW_SOURCE_ID,
        filter: ["==", ["geometry-type"], "Point"],
        paint: {
          "circle-radius": 5,
          "circle-color": "#22c55e",
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
      if (!drawModeRef.current || polygonClosedRef.current) {
        return;
      }

      const nextPoints = [...polygonPointsRef.current, [event.lngLat.lng, event.lngLat.lat]];
      onPolygonDraftRef.current(nextPoints);
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
  }, [mapIsSupported, roadsTileUrl]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map) {
      return;
    }

    map.getCanvas().style.cursor = drawMode ? "crosshair" : "";
  }, [drawMode]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map || !map.isStyleLoaded()) {
      return;
    }

    const source = map.getSource(DRAW_SOURCE_ID);

    if (source) {
      source.setData(buildDrawFeatureCollection(polygonPoints, polygonClosed));
    }
  }, [polygonClosed, polygonPoints]);

  const handleStartPolygon = () => {
    drawModeRef.current = true;
    setDrawMode(true);
    onStartPolygonRef.current();
  };

  const handleCompletePolygon = () => {
    if (polygonPoints.length < 3) {
      return;
    }

    const bbox = toBboxFromPoints(polygonPoints);

    if (!bbox) {
      return;
    }

    drawModeRef.current = false;
    setDrawMode(false);
    onPolygonCompleteRef.current(polygonPoints, bbox);

    const map = mapRef.current;
    if (map) {
      map.fitBounds(
        [
          [bbox.minLon, bbox.minLat],
          [bbox.maxLon, bbox.maxLat],
        ],
        { padding: 80, duration: 400 },
      );
    }
  };

  const handleClearPolygon = () => {
    drawModeRef.current = false;
    setDrawMode(false);
    onClearPolygonRef.current();
  };

  return (
    <section className="relative h-full min-h-0 overflow-hidden bg-[#020a0f]">
      <div className="absolute left-4 right-4 top-4 z-10 flex flex-wrap items-start justify-between gap-3">
        <div className="rounded-[18px] border border-white/10 bg-[#030b0e]/80 px-4 py-3 backdrop-blur-sm">
          <p className="text-[11px] uppercase tracking-[0.28em] text-cyan-100/40">Polygon Draw</p>
          <p className="mt-2 text-sm text-cyan-50">
            Start a polygon, place vertices on the map, then complete it to fill the bbox fields.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={handleStartPolygon}
            className="rounded-full bg-cyan-50 px-4 py-2 text-sm font-semibold text-[#021116] transition-colors hover:bg-white"
          >
            {polygonPoints.length ? "Redraw Polygon" : "Start Polygon"}
          </button>
          <button
            type="button"
            onClick={handleCompletePolygon}
            disabled={polygonPoints.length < 3}
            className="rounded-full border border-cyan-100/10 bg-[#030b0e]/80 px-4 py-2 text-sm text-cyan-50 transition-colors hover:bg-cyan-100/10 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Complete Polygon
          </button>
          <button
            type="button"
            onClick={handleClearPolygon}
            className="rounded-full border border-cyan-100/10 bg-[#030b0e]/80 px-4 py-2 text-sm text-cyan-50 transition-colors hover:bg-cyan-100/10"
          >
            Clear
          </button>
        </div>
      </div>

      {resolvedMapErrorMessage ? (
        <div className="absolute left-4 right-4 top-28 z-10 rounded-[18px] border border-red-300/30 bg-[#480000d0] px-4 py-3 text-sm text-red-100">
          {resolvedMapErrorMessage}
        </div>
      ) : null}

      <div className="absolute bottom-4 left-4 z-10 rounded-[16px] border border-white/10 bg-[#030b0e]/80 px-4 py-3 backdrop-blur-sm">
        <p className="text-xs text-cyan-100/55">
          {drawMode
            ? "Drawing mode active"
            : polygonClosed
              ? "Polygon completed"
              : "Polygon not completed"}
        </p>
        <p className="mt-1 text-sm font-medium text-cyan-50">
          {polygonPoints.length} vertex{polygonPoints.length === 1 ? "" : "es"}
        </p>
      </div>

      <div ref={mapContainerRef} className="h-full w-full" />
    </section>
  );
}

function WatchlistField({ label, value, onChange, placeholder, type = "text", inputMode }) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">{label}</span>
      <input
        type={type}
        value={value}
        inputMode={inputMode}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-md border border-cyan-200/10 bg-[#071316]/70 px-3 py-2 text-sm font-medium text-cyan-50 outline-none transition-colors placeholder:text-cyan-100/30 focus:border-cyan-400/40"
      />
    </label>
  );
}

function createDefaultWatchlistForm() {
  return {
    id: "",
    userId: "",
    name: "",
    minLon: "",
    minLat: "",
    maxLon: "",
    maxLat: "",
    createdAt: toLocalDateTimeValue(new Date()),
  };
}

function parseBboxFromForm(form) {
  const minLon = parseCoordinate(form.minLon);
  const minLat = parseCoordinate(form.minLat);
  const maxLon = parseCoordinate(form.maxLon);
  const maxLat = parseCoordinate(form.maxLat);

  if (
    minLon === null ||
    minLat === null ||
    maxLon === null ||
    maxLat === null ||
    minLon >= maxLon ||
    minLat >= maxLat
  ) {
    return null;
  }

  return { minLon, minLat, maxLon, maxLat };
}

function parseCoordinate(value) {
  if (value === "" || value === null || value === undefined) {
    return null;
  }

  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

function buildDrawFeatureCollection(points, isClosed) {
  if (!points.length) {
    return EMPTY_FEATURE_COLLECTION;
  }

  const features = points.map((point, index) => ({
    type: "Feature",
    geometry: {
      type: "Point",
      coordinates: point,
    },
    properties: {
      vertex: index + 1,
    },
  }));

  if (points.length >= 2) {
    features.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: isClosed ? toClosedPolygonCoordinates(points) : points,
      },
      properties: {},
    });
  }

  if (isClosed && points.length >= 3) {
    features.push({
      type: "Feature",
      geometry: {
        type: "Polygon",
        coordinates: [toClosedPolygonCoordinates(points)],
      },
      properties: {},
    });
  }

  return {
    type: "FeatureCollection",
    features,
  };
}

function toClosedPolygonCoordinates(points) {
  if (!points.length) {
    return [];
  }

  const firstPoint = points[0];
  const lastPoint = points[points.length - 1];

  if (firstPoint[0] === lastPoint[0] && firstPoint[1] === lastPoint[1]) {
    return points;
  }

  return [...points, firstPoint];
}

function toBboxFromPoints(points) {
  if (!points.length) {
    return null;
  }

  const longitudes = points.map((point) => point[0]);
  const latitudes = points.map((point) => point[1]);

  const minLon = Math.min(...longitudes);
  const minLat = Math.min(...latitudes);
  const maxLon = Math.max(...longitudes);
  const maxLat = Math.max(...latitudes);

  if (!Number.isFinite(minLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLon) || !Number.isFinite(maxLat)) {
    return null;
  }

  return { minLon, minLat, maxLon, maxLat };
}

function formatCoordinateInput(value) {
  return Number(value).toFixed(6);
}

function toLocalDateTimeValue(date) {
  const offsetMs = date.getTimezoneOffset() * 60 * 1000;
  return new Date(date.getTime() - offsetMs).toISOString().slice(0, 16);
}

export default WatchlistPage;
