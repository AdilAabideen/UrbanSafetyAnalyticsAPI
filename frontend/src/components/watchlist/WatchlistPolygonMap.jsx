import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import { config } from "../../config/env";
import { tilesService } from "../../services";
import {
  DEFAULT_MONTH_FROM,
  DEFAULT_MONTH_TO,
  WEST_YORKSHIRE_BBOX,
  WEST_YORKSHIRE_CENTER,
} from "../../utils/crimeUtils";
import { buildDrawFeatureCollection, toBboxFromPoints } from "../../utils/watchlistUtils";

const ROADS_SOURCE_ID = "watchlist-roads";
const ROADS_LAYER_ID = "watchlist-roads-layer";
const DRAW_SOURCE_ID = "watchlist-draw";
const DRAW_FILL_LAYER_ID = "watchlist-draw-fill";
const DRAW_LINE_LAYER_ID = "watchlist-draw-line";
const DRAW_POINT_LAYER_ID = "watchlist-draw-point";

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
    () => tilesService.getRoadVectorTilesUrl({ startMonth: DEFAULT_MONTH_FROM, endMonth: DEFAULT_MONTH_TO }),
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

export default WatchlistPolygonMap;
