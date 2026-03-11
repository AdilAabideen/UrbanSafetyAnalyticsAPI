import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import Sidebar from "./components/Sidebar";
import TopBar from "./components/TopBar";
import { config } from "./config/env";

const WEST_YORKSHIRE_CENTER = [-1.5491, 53.8008];

function App() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [loadingTiles, setLoadingTiles] = useState(true);
  const [mapReady, setMapReady] = useState(false);

  const docsUrl = useMemo(() => `${config.apiBaseUrl}/docs`, []);

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    const roadsTilesUrl = `${config.apiBaseUrl}/tiles/roads/{z}/{x}/{y}.mvt`;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      center: WEST_YORKSHIRE_CENTER,
      zoom: 12,
      minZoom: 0,
      style: {
        version: 8,
        sources: {
          darkBase: {
            type: "raster",
            tiles: ["https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "By Adil Aabideen",
          },
          roads: {
            type: "vector",
            tiles: [roadsTilesUrl],
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
            id: "roads-layer",
            type: "line",
            source: "roads",
            "source-layer": "roads",
            paint: {
              "line-color": "#1a8f47",
              "line-width": [
                "interpolate",
                ["linear"],
                ["zoom"],
                8,
                0.4,
                12,
                0.8,
                16,
                1.5,
              ],
              "line-opacity": 0.8,
            },
          },
        ],
      },
    });

    mapRef.current = map;

    map.on("load", () => {
      setMapReady(true);
      setLoadingTiles(true);
      setErrorMessage("");
    });

    map.on("idle", () => {
      setLoadingTiles(false);
    });

    map.on("movestart", () => {
      setLoadingTiles(true);
    });

    map.on("error", (event) => {
      const msg = event?.error?.message || "Failed to load map tiles";
      setErrorMessage(msg);
      setLoadingTiles(false);
    });

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  return (
    <div className="flex h-screen w-full">
      <Sidebar />

      <main className="flex min-h-0 flex-1 flex-col bg-[#071316]">
        <TopBar docsUrl={docsUrl} />

        <div className="min-h-0 flex-1">
          <div ref={mapContainerRef} className="h-full w-full" />
        </div>

        <div className="flex shrink-0 items-center justify-between gap-3 border-t border-white/5 bg-[#030b0e] px-3 py-2">
          <div className="flex flex-1 flex-col gap-1">
            {errorMessage ? (
              <span className="rounded-md border border-red-300/50 bg-[#480000b8] px-2 py-1 text-xs text-red-100">
                {errorMessage}
              </span>
            ) : null}
            <span className="text-[11px] text-cyan-100/60">API: {config.apiBaseUrl}</span>
          </div>

          <div className="flex shrink-0 items-center gap-2 text-xs text-[#d2faf0]">
            <span>
              {!mapReady ? "Starting map..." : loadingTiles ? "Loading roads..." : "Roads loaded"}
            </span>
            <strong className="text-[#39ef7d]">Vector tiles</strong>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;

