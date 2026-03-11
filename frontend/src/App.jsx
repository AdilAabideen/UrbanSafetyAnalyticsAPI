import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GeoJSON, MapContainer, TileLayer, useMapEvents } from "react-leaflet";
import "./App.css";
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
    <div className="page-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <img
            className="brand-logo"
            src="https://westyorkshire.firearmslicensing.uk/logo"
            alt="West Yorkshire Police logo"
          />
        </div>
        <button className="nav-item is-active" type="button">
          Data Map
        </button>
      </aside>

      <main className="content-shell">
        <header className="topbar">
          <div>
            <h1>Data Map</h1>
            <p>View all roads in regions in West Yorkshire.</p>
          </div>
          <a className="docs-link" href={docsUrl} target="_blank" rel="noreferrer">
            Documentation
          </a>
        </header>

        <section className="map-panel">
          <MapContainer
            center={WEST_YORKSHIRE_CENTER}
            zoom={12}
            minZoom={6}
            className="map-canvas"
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

          <div className="map-status">
            <span>{loadingRoads ? "Loading roads..." : "Roads loaded"}</span>
            <strong>{roadsGeoJson.features.length} segments</strong>
          </div>

          {errorMessage ? <div className="map-error">{errorMessage}</div> : null}
        </section>
      </main>
    </div>
  );
}

export default App;

