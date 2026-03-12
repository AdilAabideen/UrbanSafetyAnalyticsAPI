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
const CRIME_CIRCLE_LAYER_ID = "crime-circle-layer";

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
          0.8,
          12,
          1.5,
          16,
          3,
        ],
        "line-opacity": 0.95,
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
    id: CRIME_CIRCLE_LAYER_ID,
    type: "circle",
    source: CRIME_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-color": [
        "match",
        ["get", "severity"],
        "high",
        "#ff5e5e",
        "medium",
        "#ffb257",
        "low",
        "#ffd166",
        "#ff835c",
      ],
      "circle-radius": [
        "interpolate",
        ["linear"],
        ["zoom"],
        7,
        4,
        11,
        6,
        15,
        9,
      ],
      "circle-opacity": 0.9,
      "circle-stroke-color": "#fff4e8",
      "circle-stroke-width": 1.2,
    },
  });
}

function App() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [loadingTiles, setLoadingTiles] = useState(true);
  const [loadingCrimes, setLoadingCrimes] = useState(true);
  const [crimeSourceLabel, setCrimeSourceLabel] = useState("GeoJSON");
  const [mapReady, setMapReady] = useState(false);

  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    if (!config.mapboxAccessToken) {
      setErrorMessage("Set VITE_MAPBOX_ACCESS_TOKEN to render the Mapbox dark map.");
      setLoadingTiles(false);
      setLoadingCrimes(false);
      return;
    }

    if (!mapboxgl.supported()) {
      setErrorMessage("Mapbox GL JS is not supported in this browser.");
      setLoadingTiles(false);
      setLoadingCrimes(false);
      return;
    }

    mapboxgl.accessToken = config.mapboxAccessToken;

    const abortController = new AbortController();
    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      center: WEST_YORKSHIRE_CENTER,
      zoom: 12,
      minZoom: 0,
      style: MAPBOX_STYLE,
    });

    const loadCrimeOverlay = async () => {
      try {
        const { data, sourceLabel } = await crimeService.getCrimes({ signal: abortController.signal });

        if (!mapRef.current || abortController.signal.aborted) {
          return;
        }

        ensureCrimeLayers(map, data);
        setCrimeSourceLabel(sourceLabel);
        setErrorMessage("");
      } catch (error) {
        if (error?.name === "AbortError") {
          return;
        }

        setCrimeSourceLabel("unavailable");
        setErrorMessage(error?.message || "Failed to load crime GeoJSON");
      } finally {
        if (!abortController.signal.aborted) {
          setLoadingCrimes(false);
        }
      }
    };

    mapRef.current = map;

    map.on("load", () => {
      setMapReady(true);
      setLoadingTiles(true);
      setLoadingCrimes(true);
      setErrorMessage("");

      ensureRoadsLayer(map);
      void loadCrimeOverlay();
    });

    map.on("idle", () => {
      setLoadingTiles(false);
    });

    map.on("movestart", () => {
      setLoadingTiles(true);
    });

    map.on("error", (event) => {
      const message = event?.error?.message;

      if (!message) {
        return;
      }

      setErrorMessage(message);

      if (event?.sourceId === ROADS_SOURCE_ID) {
        setLoadingTiles(false);
      }
    });

    return () => {
      abortController.abort();
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
            <span className="text-cyan-100/60">Road tiles + {crimeSourceLabel}</span>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
