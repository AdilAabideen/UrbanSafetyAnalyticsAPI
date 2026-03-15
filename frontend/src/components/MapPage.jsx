import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import TopBar from "./TopBar";
import { config } from "../config/env";
import { createMonthOptions } from "../constants/crimeFilterOptions";
import { tilesService } from "../services";
import {
  DEFAULT_MONTH_FROM,
  DEFAULT_MONTH_TO,
  WEST_YORKSHIRE_BBOX,
  WEST_YORKSHIRE_CENTER,
} from "../utils/crimeUtils";

const ROADS_SOURCE_ID = "map-roads";
const ROADS_LAYER_ID = "map-roads-layer";
const FILTER_DEBOUNCE_MS = 350;

function logMapPageProperties(map) {
  const bounds = map.getBounds();
  const center = map.getCenter();
  const roadFeatures = map.querySourceFeatures(ROADS_SOURCE_ID, {
    sourceLayer: "roads",
  });

  console.log("MapPage properties", {
    center: { lng: center.lng, lat: center.lat },
    zoom: map.getZoom(),
    bearing: map.getBearing(),
    pitch: map.getPitch(),
    bounds: {
      west: bounds.getWest(),
      south: bounds.getSouth(),
      east: bounds.getEast(),
      north: bounds.getNorth(),
    },
    visibleRoadFeatureCount: roadFeatures.length,
    sampleRoadFeatureProperties: roadFeatures.slice(0, 10).map((feature) => feature.properties || {}),
  });
}

function MapPage({ docsUrl }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const hasLoggedPropertiesRef = useRef(false);
  const [loadingMap, setLoadingMap] = useState(true);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const monthOptions = useMemo(() => createMonthOptions(48).reverse(), []);
  const defaultStartIndex = useMemo(
    () => Math.max(0, monthOptions.findIndex((option) => option.value === DEFAULT_MONTH_FROM)),
    [monthOptions],
  );
  const defaultEndIndex = useMemo(
    () => {
      const index = monthOptions.findIndex((option) => option.value === DEFAULT_MONTH_TO);
      return index >= 0 ? index : monthOptions.length - 1;
    },
    [monthOptions],
  );
  const [crimeEnabled, setCrimeEnabled] = useState(true);
  const [collisionsEnabled, setCollisionsEnabled] = useState(true);
  const [userReportedEventsEnabled, setUserReportedEventsEnabled] = useState(true);
  const [startMonthIndex, setStartMonthIndex] = useState(Math.min(defaultStartIndex, defaultEndIndex));
  const [endMonthIndex, setEndMonthIndex] = useState(Math.max(defaultStartIndex, defaultEndIndex));
  const selectedStartMonth = monthOptions[startMonthIndex]?.value || DEFAULT_MONTH_FROM;
  const selectedEndMonth = monthOptions[endMonthIndex]?.value || DEFAULT_MONTH_TO;
  const selectedStartLabel = monthOptions[startMonthIndex]?.label || selectedStartMonth;
  const selectedEndLabel = monthOptions[endMonthIndex]?.label || selectedEndMonth;
  const [debouncedFilters, setDebouncedFilters] = useState(() => ({
    crime: true,
    collisions: true,
    userReportedEvents: true,
    startMonth: DEFAULT_MONTH_FROM,
    endMonth: DEFAULT_MONTH_TO,
  }));

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setDebouncedFilters({
        crime: crimeEnabled,
        collisions: collisionsEnabled,
        userReportedEvents: userReportedEventsEnabled,
        startMonth: selectedStartMonth,
        endMonth: selectedEndMonth,
      });
    }, FILTER_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [
    collisionsEnabled,
    crimeEnabled,
    selectedEndMonth,
    selectedStartMonth,
    userReportedEventsEnabled,
  ]);

  const roadsTileUrl = useMemo(
    () =>
      tilesService.getRoadVectorTilesUrl({
        includeRisk: true,
        crime: debouncedFilters.crime,
        collisions: debouncedFilters.collisions,
        userReportedEvents: debouncedFilters.userReportedEvents,
        startMonth: debouncedFilters.startMonth,
        endMonth: debouncedFilters.endMonth,
      }),
    [debouncedFilters],
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
      zoom: 12,
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
                "#22c55e",
                "orange",
                "#f97316",
                "red",
                "#ef4444",
                "#22c55e",
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
      map.fitBounds(
        [
          [WEST_YORKSHIRE_BBOX.minLon, WEST_YORKSHIRE_BBOX.minLat],
          [WEST_YORKSHIRE_BBOX.maxLon, WEST_YORKSHIRE_BBOX.maxLat],
        ],
        { padding: 36, duration: 0 },
      );

      setLoadingMap(false);
    });

    map.on("idle", () => {
      if (hasLoggedPropertiesRef.current || !map.isSourceLoaded(ROADS_SOURCE_ID)) {
        return;
      }

      hasLoggedPropertiesRef.current = true;
      logMapPageProperties(map);
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
  }, [mapIsSupported]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }

    const roadsSource = map.getSource(ROADS_SOURCE_ID);
    if (roadsSource?.setTiles) {
      roadsSource.setTiles([roadsTileUrl]);
      hasLoggedPropertiesRef.current = false;
    }
  }, [roadsTileUrl]);

  const handleStartMonthChange = (event) => {
    const nextIndex = Number(event.target.value);
    setStartMonthIndex(Math.min(nextIndex, endMonthIndex));
  };

  const handleEndMonthChange = (event) => {
    const nextIndex = Number(event.target.value);
    setEndMonthIndex(Math.max(nextIndex, startMonthIndex));
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar
        docsUrl={docsUrl}
        title="Data Map"
        subtitle="Road basemap view for West Yorkshire. Crime, collision, and roads analytics pages have been removed."
      />

      <div className="min-h-0 flex-1 p-4">
        <div className="relative h-full overflow-hidden rounded-[26px] border border-white/5 bg-[#030b0e]/80">
          <div
            ref={mapContainerRef}
            className="h-full w-full"
          />

          <div className="absolute left-4 top-4 z-10 w-[min(560px,calc(100%-2rem))] rounded-xl border border-cyan-100/15 bg-[#071316]/92 p-3 text-xs text-cyan-100 shadow-2xl">
            <div className="flex flex-wrap gap-3">
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={crimeEnabled}
                  onChange={(event) => setCrimeEnabled(event.target.checked)}
                />
                Crime
              </label>
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={collisionsEnabled}
                  onChange={(event) => setCollisionsEnabled(event.target.checked)}
                />
                Collisions
              </label>
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={userReportedEventsEnabled}
                  onChange={(event) => setUserReportedEventsEnabled(event.target.checked)}
                />
                User Reported Events
              </label>
            </div>

            <div className="mt-3">
              <div className="mb-1 flex items-center justify-between text-[11px] uppercase tracking-[0.16em] text-cyan-100/70">
                <span>Start Month</span>
                <span>{selectedStartLabel}</span>
              </div>
              <input
                className="w-full"
                type="range"
                min="0"
                max={Math.max(monthOptions.length - 1, 0)}
                value={startMonthIndex}
                onChange={handleStartMonthChange}
              />
            </div>

            <div className="mt-2">
              <div className="mb-1 flex items-center justify-between text-[11px] uppercase tracking-[0.16em] text-cyan-100/70">
                <span>End Month</span>
                <span>{selectedEndLabel}</span>
              </div>
              <input
                className="w-full"
                type="range"
                min="0"
                max={Math.max(monthOptions.length - 1, 0)}
                value={endMonthIndex}
                onChange={handleEndMonthChange}
              />
            </div>
          </div>

          {loadingMap ? (
            <div className="pointer-events-none absolute inset-x-4 top-4 rounded-xl border border-cyan-100/15 bg-[#071316]/90 px-4 py-2 text-xs text-cyan-100/75">
              Loading map...
            </div>
          ) : null}

          {resolvedMapErrorMessage ? (
            <div className="pointer-events-none absolute inset-x-4 bottom-4 rounded-xl border border-red-300/20 bg-red-950/80 px-4 py-3 text-xs text-red-100">
              {resolvedMapErrorMessage}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export default MapPage;
