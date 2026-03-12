import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import Sidebar from "./components/Sidebar";
import TopBar from "./components/TopBar";
import { config } from "./config/env";
import { crimeService, roadsService } from "./services";

const WEST_YORKSHIRE_CENTER = [-1.5491, 53.8008];
const MAPBOX_STYLE = "mapbox://styles/mapbox/dark-v11";
const ROADS_SOURCE_ID = "roads";
const ROADS_LAYER_ID = "roads-layer";
const CRIME_SOURCE_ID = "crimes";
const CRIME_FILL_LAYER_ID = "crime-fill-layer";
const CRIME_LINE_LAYER_ID = "crime-line-layer";
const CRIME_CLUSTER_CIRCLE_LAYER_ID = "crime-cluster-circle-layer";
const CRIME_POINT_CIRCLE_LAYER_ID = "crime-point-circle-layer";
const CRIME_LABEL_LAYER_ID = "crime-label-layer";
const EMPTY_FEATURE_COLLECTION = { type: "FeatureCollection", features: [] };
const CLUSTER_COUNT_EXPRESSION = [
  "coalesce",
  ["to-number", ["get", "count"]],
  ["to-number", ["get", "point_count"]],
  ["to-number", ["get", "cluster_count"]],
  0,
];
const HAS_CLUSTER_COUNT_EXPRESSION = [">", CLUSTER_COUNT_EXPRESSION, 0];

function getViewportQuery(map) {
  const bounds = map.getBounds();

  return {
    minLon: bounds.getWest(),
    minLat: bounds.getSouth(),
    maxLon: bounds.getEast(),
    maxLat: bounds.getNorth(),
    zoom: map.getZoom(),
  };
}

function ensureRoadsLayer(map) {
  if (!map.getSource(ROADS_SOURCE_ID)) {
    map.addSource(ROADS_SOURCE_ID, {
      type: "vector",
      tiles: [roadsService.getVectorTilesUrl()],
      minzoom: 0,
    });
  }

  if (!map.getLayer(ROADS_LAYER_ID)) {
    map.addLayer({
      id: ROADS_LAYER_ID,
      type: "line",
      source: ROADS_SOURCE_ID,
      "source-layer": "roads",
      paint: {
        "line-color": "#39ef7d",
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          8,
          0.4,
          12,
          0.75,
          16,
          1.5,
        ],
        "line-opacity": 0.75,
      },
    });
  }
}

function ensureCrimeLayers(map, data) {
  const existingSource = map.getSource(CRIME_SOURCE_ID);

  if (existingSource) {
    existingSource.setData(data);
    return;
  }

  map.addSource(CRIME_SOURCE_ID, {
    type: "geojson",
    data,
  });

  map.addLayer({
    id: CRIME_FILL_LAYER_ID,
    type: "fill",
    source: CRIME_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Polygon"],
    paint: {
      "fill-color": "#ff7a45",
      "fill-opacity": 0.14,
      "fill-outline-color": "#ffb072",
    },
  });

  map.addLayer({
    id: CRIME_LINE_LAYER_ID,
    type: "line",
    source: CRIME_SOURCE_ID,
    filter: ["==", ["geometry-type"], "LineString"],
    paint: {
      "line-color": "#ffb072",
      "line-width": [
        "interpolate",
        ["linear"],
        ["zoom"],
        8,
        1.2,
        12,
        2,
        16,
        4,
      ],
      "line-opacity": 0.85,
    },
  });

  map.addLayer({
    id: CRIME_CLUSTER_CIRCLE_LAYER_ID,
    type: "circle",
    source: CRIME_SOURCE_ID,
    filter: ["all", ["==", ["geometry-type"], "Point"], HAS_CLUSTER_COUNT_EXPRESSION],
    paint: {
      "circle-color": "#38bdf8",
      "circle-radius": [
        "interpolate",
        ["linear"],
        CLUSTER_COUNT_EXPRESSION,
        1,
        12,
        10,
        18,
        50,
        24,
        200,
        34,
      ],
      "circle-opacity": 0.9,
      "circle-stroke-color": "#fff4e8",
      "circle-stroke-width": 1.2,
    },
  });

  map.addLayer({
    id: CRIME_POINT_CIRCLE_LAYER_ID,
    type: "circle",
    source: CRIME_SOURCE_ID,
    filter: ["all", ["==", ["geometry-type"], "Point"], ["!", HAS_CLUSTER_COUNT_EXPRESSION]],
    paint: {
      "circle-color": "#ff7a45",
      "circle-radius": [
        "interpolate",
        ["linear"],
        ["zoom"],
        7,
        4,
        11,
        6,
        15,
        8,
      ],
      "circle-opacity": 0.9,
      "circle-stroke-color": "#fff4e8",
      "circle-stroke-width": 1.2,
    },
  });

  map.addLayer({
    id: CRIME_LABEL_LAYER_ID,
    type: "symbol",
    source: CRIME_SOURCE_ID,
    filter: HAS_CLUSTER_COUNT_EXPRESSION,
    layout: {
      "text-field": [
        "coalesce",
        ["to-string", ["get", "count"]],
        ["to-string", ["get", "point_count"]],
        ["to-string", ["get", "cluster_count"]],
        "",
      ],
      "text-size": 11,
      "text-font": ["Open Sans Bold", "Arial Unicode MS Bold"],
    },
    paint: {
      "text-color": "#042433",
    },
  });
}

function App() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const crimeRequestRef = useRef({ controller: null, requestId: 0 });
  const [roadErrorMessage, setRoadErrorMessage] = useState("");
  const [crimeErrorMessage, setCrimeErrorMessage] = useState("");
  const [loadingTiles, setLoadingTiles] = useState(true);
  const [loadingCrimes, setLoadingCrimes] = useState(true);
  const [crimeMode, setCrimeMode] = useState("points");
  const [crimeFeatureCount, setCrimeFeatureCount] = useState(0);
  const [crimeHasMore, setCrimeHasMore] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);
  const errorMessage = [roadErrorMessage, crimeErrorMessage].filter(Boolean).join(" | ");

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    if (!config.mapboxAccessToken) {
      setRoadErrorMessage("Set VITE_MAPBOX_ACCESS_TOKEN to render the Mapbox dark map.");
      setLoadingTiles(false);
      setLoadingCrimes(false);
      return;
    }

    if (!mapboxgl.supported()) {
      setRoadErrorMessage("Mapbox GL JS is not supported in this browser.");
      setLoadingTiles(false);
      setLoadingCrimes(false);
      return;
    }

    mapboxgl.accessToken = config.mapboxAccessToken;

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      center: WEST_YORKSHIRE_CENTER,
      zoom: 12,
      minZoom: 0,
      style: MAPBOX_STYLE,
    });

    const clearCrimeRequest = () => {
      if (crimeRequestRef.current.controller) {
        crimeRequestRef.current.controller.abort();
      }
    };

    const loadCrimesForViewport = async () => {
      if (!mapRef.current) {
        return;
      }

      clearCrimeRequest();

      const controller = new AbortController();
      const requestId = crimeRequestRef.current.requestId + 1;
      crimeRequestRef.current = { controller, requestId };

      setLoadingCrimes(true);
      setCrimeErrorMessage("");

      try {
        const result = await crimeService.getCrimesForViewport(getViewportQuery(map), {
          signal: controller.signal,
        });

        if (
          controller.signal.aborted ||
          !mapRef.current ||
          crimeRequestRef.current.requestId !== requestId
        ) {
          return;
        }

        ensureCrimeLayers(map, result.data);
        setCrimeMode(result.mode);
        setCrimeFeatureCount(result.featureCount);
        setCrimeHasMore(Boolean(result.nextCursor));
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        if (mapRef.current) {
          ensureCrimeLayers(map, EMPTY_FEATURE_COLLECTION);
        }

        setCrimeMode("unavailable");
        setCrimeFeatureCount(0);
        setCrimeHasMore(false);
        setCrimeErrorMessage(error?.message || "Failed to load crimes for the current viewport");
      } finally {
        if (!controller.signal.aborted && crimeRequestRef.current.requestId === requestId) {
          setLoadingCrimes(false);
        }
      }
    };

    mapRef.current = map;

    map.on("load", () => {
      setMapReady(true);
      setLoadingTiles(true);
      setLoadingCrimes(true);
      setRoadErrorMessage("");
      setCrimeErrorMessage("");

      ensureRoadsLayer(map);
      ensureCrimeLayers(map, EMPTY_FEATURE_COLLECTION);
      void loadCrimesForViewport();
    });

    map.on("idle", () => {
      setLoadingTiles(false);
    });

    map.on("movestart", () => {
      setLoadingTiles(true);
      setLoadingCrimes(true);
    });

    map.on("moveend", () => {
      void loadCrimesForViewport();
    });

    map.on("error", (event) => {
      const message = event?.error?.message;

      if (!message) {
        return;
      }

      if (event?.sourceId === CRIME_SOURCE_ID) {
        setCrimeErrorMessage(message);
        return;
      }

      setRoadErrorMessage(message);

      if (event?.sourceId === ROADS_SOURCE_ID) {
        setLoadingTiles(false);
      }
    });

    return () => {
      clearCrimeRequest();
      map.remove();
      mapRef.current = null;
    };
  }, []);

  const mapStatus = !mapReady
    ? errorMessage
      ? "Map unavailable"
      : "Starting map..."
    : loadingTiles || loadingCrimes
      ? "Loading map data..."
      : errorMessage
        ? "Map loaded with errors"
        : "Roads and crimes loaded";

  const crimeStatusLabel =
    crimeMode === "unavailable"
      ? "crime API unavailable"
      : `${crimeMode} ${crimeFeatureCount}${crimeHasMore ? "+" : ""}`;

  return (
    <div className="flex h-screen w-full">
      <Sidebar />

      <main className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar docsUrl={docsUrl} />

        <div className="min-h-0 flex-1">
          <div ref={mapContainerRef} className="h-full w-full" />
        </div>

        <div className="flex shrink-0 items-center justify-between gap-3 border-t border-cyan-200/10 bg-[#030b0e] px-3 py-2">
          <div className="flex flex-1 flex-col gap-1">
            {errorMessage ? (
              <span className="rounded-md border border-red-300/50 bg-[#480000b8] px-2 py-1 text-xs text-red-100">
                {errorMessage}
              </span>
            ) : null}
            <span className="text-[11px] text-cyan-100/60">API: {config.apiBaseUrl}</span>
          </div>

          <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
            <span>{mapStatus}</span>
            <strong className="text-[#39ef7d]">Mapbox GL JS</strong>
            <span className="text-cyan-100/60">Road tiles + {crimeStatusLabel}</span>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
