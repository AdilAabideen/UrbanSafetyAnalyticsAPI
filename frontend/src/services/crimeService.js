import { config } from "../config/env";
import { fetchJson, toFeatureCollection } from "./serviceUtils";

const SAMPLE_CRIME_GEOJSON_URL = "/temp.geojson";

async function fetchCrimeGeoJson(url, requestOptions = {}) {
  console.log(url)
  const data = await fetchJson(url, `Failed to fetch crime GeoJSON from ${url}`, requestOptions);
  return toFeatureCollection(data);
}

export const crimeService = {
  async getCrimes(requestOptions = {}) {
    if (config.crimeApiUrl) {
      try {
        return {
          data: await fetchCrimeGeoJson(config.crimeApiUrl, requestOptions),
          sourceLabel: "crime API",
        };
      } catch (error) {
        if (error?.name === "AbortError") {
          throw error;
        }
      }
    }

    const sampleUrl = config.crimeGeoJsonUrl || SAMPLE_CRIME_GEOJSON_URL;
    return {
      data: await fetchCrimeGeoJson(sampleUrl, requestOptions),
      sourceLabel: sampleUrl === SAMPLE_CRIME_GEOJSON_URL ? "sample GeoJSON" : sampleUrl,
    };
  },
};
