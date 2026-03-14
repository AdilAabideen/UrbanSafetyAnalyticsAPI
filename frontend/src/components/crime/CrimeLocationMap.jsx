import { useEffect, useMemo, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import { config } from "../../config/env";
import { WEST_YORKSHIRE_BBOX } from "../../utils/crimeUtils";

const DRAWER_MAP_SOURCE_ID = "crime-drawer-source";
const DRAWER_MAP_CIRCLE_LAYER_ID = "crime-drawer-circle";
const DRAWER_MAP_HALO_LAYER_ID = "crime-drawer-circle-halo";
const DRAWER_MAP_STYLE = {
  version: 8,
  glyphs: "mapbox://fonts/mapbox/{fontstack}/{range}.pbf",
  sources: {
    darkBase: {
      type: "raster",
      tiles: ["https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "By Adil Aabideen",
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
  ],
};

function toCrimeGeoJsonFeature(crime) {
  if (!crime) {
    return null;
  }

  if (crime.geometry) {
    return {
      type: "Feature",
      geometry: crime.geometry,
      properties: {
        id: crime.recordId,
        crime_id: crime.crimeId,
        crime_type: crime.crimeType,
      },
    };
  }

  if (Number.isFinite(crime.lon) && Number.isFinite(crime.lat)) {
    return {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [crime.lon, crime.lat],
      },
      properties: {
        id: crime.recordId,
        crime_id: crime.crimeId,
        crime_type: crime.crimeType,
      },
    };
  }

  return null;
}

function getCrimeFeatureCenter(feature) {
  const coordinates = feature?.geometry?.type === "Point" ? feature.geometry.coordinates : null;
  return Array.isArray(coordinates) ? coordinates : [WEST_YORKSHIRE_BBOX.minLon + ((WEST_YORKSHIRE_BBOX.maxLon - WEST_YORKSHIRE_BBOX.minLon) / 2), WEST_YORKSHIRE_BBOX.minLat + ((WEST_YORKSHIRE_BBOX.maxLat - WEST_YORKSHIRE_BBOX.minLat) / 2)];
}

function upsertCrimeMapFeature(map, feature) {
  const sourceData = {
    type: "FeatureCollection",
    features: [feature],
  };

  const existingSource = map.getSource(DRAWER_MAP_SOURCE_ID);

  if (existingSource) {
    existingSource.setData(sourceData);
    return;
  }

  map.addSource(DRAWER_MAP_SOURCE_ID, {
    type: "geojson",
    data: sourceData,
  });

  map.addLayer({
    id: DRAWER_MAP_HALO_LAYER_ID,
    type: "circle",
    source: DRAWER_MAP_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-radius": 16,
      "circle-color": "rgba(34, 211, 238, 0.18)",
      "circle-stroke-width": 0,
    },
  });

  map.addLayer({
    id: DRAWER_MAP_CIRCLE_LAYER_ID,
    type: "circle",
    source: DRAWER_MAP_SOURCE_ID,
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-radius": 7,
      "circle-color": "#22d3ee",
      "circle-stroke-color": "#071316",
      "circle-stroke-width": 2,
    },
  });
}

function CrimeLocationMap({ crime, isLoadingDetail }) {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [mapRuntimeErrorMessage, setMapRuntimeErrorMessage] = useState("");
  const crimeFeature = useMemo(() => toCrimeGeoJsonFeature(crime), [crime]);
  const mapErrorMessage = !config.mapboxAccessToken
    ? "Set VITE_MAPBOX_ACCESS_TOKEN to render the crime location map."
    : !mapboxgl.supported()
      ? "Mapbox GL JS is not supported in this browser."
      : mapRuntimeErrorMessage;

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return undefined;
    }

    if (!config.mapboxAccessToken) {
      return undefined;
    }

    if (!mapboxgl.supported()) {
      return undefined;
    }

    mapboxgl.accessToken = config.mapboxAccessToken;

    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: DRAWER_MAP_STYLE,
      center: getCrimeFeatureCenter(crimeFeature),
      zoom: 14,
      attributionControl: false,
      interactive: false,
    });

    mapRef.current = map;

    map.on("load", () => {
      if (crimeFeature) {
        upsertCrimeMapFeature(map, crimeFeature);
        map.jumpTo({
          center: getCrimeFeatureCenter(crimeFeature),
          zoom: 15,
        });
      }
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
  }, [crimeFeature]);

  useEffect(() => {
    const map = mapRef.current;

    if (!map || !crimeFeature) {
      return;
    }

    const updateFeature = () => {
      upsertCrimeMapFeature(map, crimeFeature);
      map.easeTo({
        center: getCrimeFeatureCenter(crimeFeature),
        zoom: 15,
        duration: 300,
      });
    };

    if (map.isStyleLoaded()) {
      updateFeature();
      return;
    }

    map.once("load", updateFeature);
  }, [crimeFeature]);

  return (
    <div className="flex min-h-[320px] flex-col rounded-[20px] border border-white/10 bg-[#071316]/70 p-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.3em] text-cyan-100/45">
            Crime Location
          </p>
          <p className="mt-1 text-sm text-cyan-100/70">Rendered from GeoJSON</p>
        </div>
        {crimeFeature ? (
          <span className="rounded-full border border-cyan-100/10 bg-cyan-100/5 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-cyan-100/60">
            {crimeFeature.geometry.type}
          </span>
        ) : null}
      </div>

      {mapErrorMessage ? (
        <div className="mb-3 rounded-xl border border-red-300/30 bg-[#480000b8] px-3 py-3 text-sm text-red-100">
          {mapErrorMessage}
        </div>
      ) : null}

      {!crimeFeature && isLoadingDetail ? (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[16px] border border-cyan-100/10 bg-cyan-100/5 text-sm text-cyan-100/70">
          Loading GeoJSON for the selected crime...
        </div>
      ) : !crimeFeature ? (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[16px] border border-cyan-100/10 bg-cyan-100/5 px-4 text-center text-sm text-cyan-100/70">
          No geometry is available for this crime.
        </div>
      ) : (
        <div ref={mapContainerRef} className="min-h-[260px] flex-1 overflow-hidden rounded-[16px]" />
      )}
    </div>
  );
}

export default CrimeLocationMap;
