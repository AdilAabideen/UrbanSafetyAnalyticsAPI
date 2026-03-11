import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GeoJSON, MapContainer, TileLayer, useMapEvents } from "react-leaflet";
import Sidebar from "./components/Sidebar";
import TopBar from "./components/TopBar";
import { config } from "./config/env";
import { roadsService } from "./services";

const WEST_YORKSHIRE_CENTER = [53.8008, -1.5491];

function ViewportEvents({ onViewportChange }) {
  const map = useMapEvents({
    moveend: () => onViewportChange(map),
    zoomend: () => onViewportChange(map),
  });

  useEffect(() => {
    onViewportChange(map);
  }, [map, onViewportChange]);

  return null;
}

const EMPTY_FEATURE_COLLECTION = {
  type: "FeatureCollection",
  features: [],
};

function App() {
  const [roadsGeoJson, setRoadsGeoJson] = useState(EMPTY_FEATURE_COLLECTION);
  const [loadingRoads, setLoadingRoads] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const lastRequestRef = useRef(0);
  const abortControllerRef = useRef(null);

  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);

  const fetchRoadsForViewport = useCallback(async (map) => {
    const bounds = map.getBounds();
    const minLon = bounds.getWest();
    const minLat = bounds.getSouth();
    const maxLon = bounds.getEast();
    const maxLat = bounds.getNorth();

    if (minLon >= maxLon || minLat >= maxLat) {
      return;
    }

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    const controller = new AbortController();
    abortControllerRef.current = controller;
    const requestId = lastRequestRef.current + 1;
    lastRequestRef.current = requestId;

    setLoadingRoads(true);
    setErrorMessage("");

    try {
      const result = await roadsService.getRoadsInBoundingBox(
        { minLon, minLat, maxLon, maxLat, limit: 1200 },
        { signal: controller.signal },
      );

      if (lastRequestRef.current !== requestId) {
        return;
      }

      setRoadsGeoJson(result);
    } catch (error) {
      if (error?.name === "AbortError") {
        return;
      }

      if (lastRequestRef.current !== requestId) {
        return;
      }

      setErrorMessage(error?.message || "Unable to load roads");
      setRoadsGeoJson(EMPTY_FEATURE_COLLECTION);
    } finally {
      if (lastRequestRef.current === requestId) {
        setLoadingRoads(false);
      }
    }
  }, []);

  return (
    <div className="flex h-screen w-full">
      <Sidebar />

      <main className="flex flex-1 flex-col min-h-0 bg-[#071316]">
        <TopBar docsUrl={docsUrl} />

        <div className="flex flex-1 flex-col min-h-0">
          <MapContainer
            center={WEST_YORKSHIRE_CENTER}
            zoom={12}
            minZoom={6}
            className="h-full w-full"
            zoomControl={false}
          >
            <TileLayer
              attribution='&copy; OpenStreetMap contributors &copy; CARTO'
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            <GeoJSON
              key={roadsGeoJson.features.length}
              data={roadsGeoJson}
              style={{
                color: "#2fe66f",
                weight: 3,
                opacity: 0.95,
              }}
            />
            <ViewportEvents onViewportChange={fetchRoadsForViewport} />
          </MapContainer>
        </div>

        <div className="flex shrink-0 items-center justify-between gap-3 border-t border-cyan-200/10 bg-[#030b0e] px-3 py-2">
          <div className="flex-1">
            {errorMessage ? (
              <span className="rounded-md border border-red-300/50 bg-[#480000b8] px-2 py-1 text-xs text-red-100">
                {errorMessage}
              </span>
            ) : null}
          </div>

          <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
            <span>{loadingRoads ? "Loading roads..." : "Roads loaded"}</span>
            <strong className="text-[#39ef7d]">{roadsGeoJson.features.length} segments</strong>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
