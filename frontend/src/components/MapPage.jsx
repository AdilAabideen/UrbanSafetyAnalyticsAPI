import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import FilterComponent from "./FilterComponent";
import InfoComponents from "./InfoComponents";
import TopBar from "./TopBar";
import { config } from "../config/env";
import {
  CRIME_TYPE_OPTIONS,
  OUTCOME_CATEGORY_OPTIONS,
  createMonthOptions,
} from "../constants/crimeFilterOptions";
import { crimeService, roadsService } from "../services";
import {
  DEFAULT_CRIME_FILTERS,
  MAX_POINT_FILTER_PAGES,
  WEST_YORKSHIRE_CENTER,
  hasClusterCount,
  normalizeCrimeFeature,
  toSearchOptions,
} from "../utils/crimeUtils";

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

function ensureRoadsLayer(map, { startMonth, endMonth } = {}) {
  const tileUrl = roadsService.getVectorTilesUrl({ startMonth, endMonth });

  if (!map.getSource(ROADS_SOURCE_ID)) {
    map.addSource(ROADS_SOURCE_ID, {
      type: "vector",
      tiles: [tileUrl],
      minzoom: 0,
    });
  } else {
    map.getSource(ROADS_SOURCE_ID).setTiles([tileUrl]);
  }

  if (!map.getLayer(ROADS_LAYER_ID)) {
    map.addLayer({
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
          "#39ef7d",
        ],
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

function logRoadDebugInfo(map) {
  const roadFeatures = map
    .querySourceFeatures(ROADS_SOURCE_ID, { sourceLayer: "roads" })
    .slice(0, 10)
    .map((feature) => feature.properties || {});

  console.log("roadsService.getVectorTilesUrl()", roadsService.getVectorTilesUrl());
  console.log(`${ROADS_LAYER_ID} line-color`, map.getPaintProperty(ROADS_LAYER_ID, "line-color"));
  console.log(
    `${ROADS_SOURCE_ID} feature properties`,
    roadFeatures.length > 0 ? roadFeatures : "No road features loaded in the current viewport yet.",
  );
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
      "circle-color": [
        "step",
        CLUSTER_COUNT_EXPRESSION,
        "#22c55e",
        1000,
        "#f97316",
        4000,
        "#ef4444",
      ],
      "circle-radius": [
        "interpolate",
        ["linear"],
        CLUSTER_COUNT_EXPRESSION,
        1,
        10,
        10,
        15,
        50,
        20,
        200,
        28,
      ],
      "circle-opacity": 0.9,
      "circle-stroke-width": 0,
    },
  });

  map.addLayer({
    id: CRIME_POINT_CIRCLE_LAYER_ID,
    type: "circle",
    source: CRIME_SOURCE_ID,
    filter: ["all", ["==", ["geometry-type"], "Point"], ["!", HAS_CLUSTER_COUNT_EXPRESSION]],
    paint: {
      "circle-color": "#3b82f6",
      "circle-radius": [
        "interpolate",
        ["linear"],
        ["zoom"],
        7,
        3,
        11,
        4.5,
        15,
        6,
      ],
      "circle-opacity": 0.9,
      "circle-stroke-width": 0,
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

function MapPage({ docsUrl }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const crimeRequestRef = useRef({ controller: null, requestId: 0 });
  const crimeFiltersRef = useRef(DEFAULT_CRIME_FILTERS);
  const mapReadyRef = useRef(false);
  const reloadCrimesRef = useRef(() => Promise.resolve());
  const [roadErrorMessage, setRoadErrorMessage] = useState("");
  const [crimeErrorMessage, setCrimeErrorMessage] = useState("");
  const [loadingTiles, setLoadingTiles] = useState(true);
  const [loadingCrimes, setLoadingCrimes] = useState(true);
  const [crimeMode, setCrimeMode] = useState("points");
  const [crimeFeatureCount, setCrimeFeatureCount] = useState(0);
  const [crimeHasMore, setCrimeHasMore] = useState(false);
  const [mapReady, setMapReady] = useState(false);
  const [crimeFilters, setCrimeFilters] = useState(DEFAULT_CRIME_FILTERS);
  const [lsoaOptions, setLsoaOptions] = useState([]);
  const [selectedCrime, setSelectedCrime] = useState(null);
  const [infoPanelOpen, setInfoPanelOpen] = useState(false);

  const monthOptions = useMemo(() => createMonthOptions(48), []);
  const errorMessage = [roadErrorMessage, crimeErrorMessage].filter(Boolean).join(" | ");

  useEffect(() => {
    mapReadyRef.current = mapReady;
  }, [mapReady]);

  useEffect(() => {
    crimeFiltersRef.current = crimeFilters;

    if (mapReadyRef.current && reloadCrimesRef.current) {
      void reloadCrimesRef.current();
    }
  }, [crimeFilters]);

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

    const roadsTilesUrl = roadsService.getVectorTilesUrl({
      startMonth: crimeFiltersRef.current.monthFrom || undefined,
      endMonth: crimeFiltersRef.current.monthTo || undefined,
    });

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      center: WEST_YORKSHIRE_CENTER,
      zoom: 12,
      minZoom: 0,
      style: {
        version: 8,
        glyphs: "mapbox://fonts/mapbox/{fontstack}/{range}.pbf",
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
              "line-color": [
                "match",
                ["downcase", ["coalesce", ["get", "band"], ""]],
                "green",
                "#22c55e",
                "orange",
                "#f97316",
                "red",
                "#ef4444",
                "#39ef7d",
              ],
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
          },
        ],
      },
    });
    let hasLoggedRoadDebugInfo = false;

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

      const activeFilters = crimeFiltersRef.current;
      const forcePointsMode = Boolean(activeFilters.outcomeCategory || activeFilters.lsoaName);
      const controller = new AbortController();
      const requestId = crimeRequestRef.current.requestId + 1;
      crimeRequestRef.current = { controller, requestId };

      setLoadingCrimes(true);
      setCrimeErrorMessage("");

      ensureRoadsLayer(map, {
        startMonth: activeFilters.monthFrom || undefined,
        endMonth: activeFilters.monthTo || undefined,
      });

      try {
        const requestBase = {
          ...getViewportQuery(map),
          startMonth: activeFilters.monthFrom || undefined,
          endMonth: activeFilters.monthTo || undefined,
          crimeTypes: activeFilters.crimeType ? [activeFilters.crimeType] : undefined,
          lastOutcomeCategories: activeFilters.outcomeCategory
            ? [activeFilters.outcomeCategory]
            : undefined,
          lsoaNames: activeFilters.lsoaName ? [activeFilters.lsoaName] : undefined,
          mode: forcePointsMode ? "points" : "auto",
        };

        const result = await crimeService.getCrimesForViewport(requestBase, {
          signal: controller.signal,
        });

        if (
          controller.signal.aborted ||
          !mapRef.current ||
          crimeRequestRef.current.requestId !== requestId
        ) {
          return;
        }

        let nextCursor = result.nextCursor;
        let pageCount = 1;
        const combinedFeatures = [...result.data.features];

        while (
          forcePointsMode &&
          nextCursor &&
          pageCount < MAX_POINT_FILTER_PAGES &&
          !controller.signal.aborted
        ) {
          const nextPage = await crimeService.getCrimesForViewport(
            {
              ...requestBase,
              mode: "points",
              cursor: nextCursor,
            },
            { signal: controller.signal },
          );

          if (
            controller.signal.aborted ||
            !mapRef.current ||
            crimeRequestRef.current.requestId !== requestId
          ) {
            return;
          }

          combinedFeatures.push(...nextPage.data.features);
          nextCursor = nextPage.nextCursor;
          pageCount += 1;
        }

        const rawFeatureCollection = forcePointsMode
          ? { type: "FeatureCollection", features: combinedFeatures }
          : result.data;

        const normalizedCrimes = rawFeatureCollection.features
          .filter(
            (feature) =>
              feature?.geometry?.type === "Point" && !hasClusterCount(feature.properties),
          )
          .map((feature) => normalizeCrimeFeature(feature));

        setLsoaOptions(
          toSearchOptions(
            normalizedCrimes.map((crime) => crime.lsoaName),
            activeFilters.lsoaName,
          ),
        );

        ensureCrimeLayers(map, rawFeatureCollection);
        setCrimeMode(result.mode);
        setCrimeFeatureCount(rawFeatureCollection.features.length);
        setCrimeHasMore(Boolean(nextCursor));
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
        setLsoaOptions([]);
        setCrimeErrorMessage(error?.message || "Failed to load crimes for the current viewport");
      } finally {
        if (!controller.signal.aborted && crimeRequestRef.current.requestId === requestId) {
          setLoadingCrimes(false);
        }
      }
    };

    reloadCrimesRef.current = () => loadCrimesForViewport();
    mapRef.current = map;

    map.on("load", () => {
      setMapReady(true);
      setLoadingTiles(true);
      setLoadingCrimes(true);
      setRoadErrorMessage("");
      setCrimeErrorMessage("");

      ensureRoadsLayer(map, {
        startMonth: crimeFiltersRef.current.monthFrom || undefined,
        endMonth: crimeFiltersRef.current.monthTo || undefined,
      });
      ensureCrimeLayers(map, EMPTY_FEATURE_COLLECTION);
      void loadCrimesForViewport();
    });

    map.on("idle", () => {
      if (
        !hasLoggedRoadDebugInfo &&
        map.getLayer(ROADS_LAYER_ID) &&
        map.isSourceLoaded(ROADS_SOURCE_ID)
      ) {
        hasLoggedRoadDebugInfo = true;
        logRoadDebugInfo(map);
      }

      setLoadingTiles(false);
    });

    map.on("movestart", () => {
      setLoadingTiles(true);
      setLoadingCrimes(true);
    });

    map.on("moveend", () => {
      void loadCrimesForViewport();
    });

    map.on("click", CRIME_POINT_CIRCLE_LAYER_ID, (event) => {
      const feature = event.features?.[0];

      if (!feature) {
        return;
      }

      setSelectedCrime(normalizeCrimeFeature(feature));
      setInfoPanelOpen(true);
    });

    map.on("mouseenter", CRIME_POINT_CIRCLE_LAYER_ID, () => {
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("mouseleave", CRIME_POINT_CIRCLE_LAYER_ID, () => {
      map.getCanvas().style.cursor = "";
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
      reloadCrimesRef.current = () => Promise.resolve();
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
    <div className="flex min-h-0 flex-1 flex-col bg-[#071316]">
      <TopBar docsUrl={docsUrl} />

      <div className="relative min-h-0 flex-1">
        <div ref={mapContainerRef} className="h-full w-full" />

        <div className="pointer-events-none absolute bottom-4 left-4 z-10 flex items-end">
          <FilterComponent
            filters={crimeFilters}
            monthOptions={monthOptions}
            crimeTypeOptions={CRIME_TYPE_OPTIONS}
            outcomeOptions={OUTCOME_CATEGORY_OPTIONS}
            lsoaOptions={lsoaOptions}
            visibleCrimeCount={crimeFeatureCount}
            mode={crimeMode}
            onChange={(key, value) => {
              setCrimeFilters((current) => ({
                ...current,
                [key]: value,
              }));
            }}
            onClear={() => {
              setCrimeFilters({ ...DEFAULT_CRIME_FILTERS });
            }}
          />
        </div>

        {infoPanelOpen ? (
          <div className="pointer-events-none absolute inset-y-0 right-0 z-10 flex items-stretch p-4">
            <InfoComponents
              recordId={selectedCrime?.recordId}
              crimeId={selectedCrime?.crimeId}
              crimeType={selectedCrime?.crimeType}
              month={selectedCrime?.month}
              reportedBy={selectedCrime?.reportedBy}
              fallsWithin={selectedCrime?.fallsWithin}
              location={selectedCrime?.location}
              lsoaCode={selectedCrime?.lsoaCode}
              lsoaName={selectedCrime?.lsoaName}
              outcomeCategory={selectedCrime?.outcomeCategory}
              context={selectedCrime?.context}
              onClose={() => setInfoPanelOpen(false)}
            />
          </div>
        ) : null}
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
          <span>{mapStatus}</span>
          <strong className="text-[#39ef7d]">Mapbox GL JS</strong>
          <span className="text-cyan-100/60">Road tiles + {crimeStatusLabel}</span>
        </div>
      </div>
    </div>
  );
}

export default MapPage;
