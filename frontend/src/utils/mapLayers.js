import { tilesService } from "../services";

export const ROADS_SOURCE_ID = "roads";
export const ROADS_LAYER_ID = "roads-layer";
export const CRIME_SOURCE_ID = "crimes";
export const CRIME_FILL_LAYER_ID = "crime-fill-layer";
export const CRIME_LINE_LAYER_ID = "crime-line-layer";
export const CRIME_CLUSTER_CIRCLE_LAYER_ID = "crime-cluster-circle-layer";
export const CRIME_POINT_CIRCLE_LAYER_ID = "crime-point-circle-layer";
export const CRIME_LABEL_LAYER_ID = "crime-label-layer";
export const EMPTY_FEATURE_COLLECTION = { type: "FeatureCollection", features: [] };

export const CLUSTER_COUNT_EXPRESSION = [
  "coalesce",
  ["to-number", ["get", "count"]],
  ["to-number", ["get", "point_count"]],
  ["to-number", ["get", "cluster_count"]],
  0,
];

export const HAS_CLUSTER_COUNT_EXPRESSION = [">", CLUSTER_COUNT_EXPRESSION, 0];

export function getViewportQuery(map) {
  const bounds = map.getBounds();

  return {
    minLon: bounds.getWest(),
    minLat: bounds.getSouth(),
    maxLon: bounds.getEast(),
    maxLat: bounds.getNorth(),
    zoom: map.getZoom(),
  };
}

export function ensureRoadsLayer(map, { startMonth, endMonth } = {}) {
  const tileUrl = tilesService.getRoadVectorTilesUrl({ startMonth, endMonth });

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

export function logRoadDebugInfo(map) {
  const allFeatures = map.querySourceFeatures(ROADS_SOURCE_ID, { sourceLayer: "roads" });

  const byBand = { green: [], orange: [], red: [] };
  for (const f of allFeatures) {
    const band = f.properties?.band;
    if (band && byBand[band] && byBand[band].length < 5) {
      byBand[band].push(f.properties);
    }
  }

  console.log("tilesService.getRoadVectorTilesUrl()", tilesService.getRoadVectorTilesUrl());
  console.log(`${ROADS_LAYER_ID} line-color`, map.getPaintProperty(ROADS_LAYER_ID, "line-color"));
  console.log("green (5):", byBand.green);
  console.log("orange (5):", byBand.orange);
  console.log("red (5):", byBand.red);
  console.log(
    `Total features in viewport: ${allFeatures.length}`,
    `(green: ${byBand.green.length}, orange: ${byBand.orange.length}, red: ${byBand.red.length})`,
  );
}

export function ensureCrimeLayers(map, data) {
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
