import { API_BASE_URL } from "../config/env";

export const tilesService = {
  getRoadVectorTilesUrl({
    includeRisk = true,
    crime = true,
    collisions = true,
    userReportedEvents = true,
    month,
    startMonth,
    endMonth,
    crimeTypes,
  } = {}) {
    const params = new URLSearchParams();

    params.set("includeRisk", includeRisk ? "true" : "false");
    params.set("crime", crime ? "true" : "false");
    params.set("collisions", collisions ? "true" : "false");
    params.set("userReportedEvents", userReportedEvents ? "true" : "false");

    if (month) {
      params.set("startMonth", month);
      params.set("endMonth", month);
    } else {
      if (startMonth) {
        params.set("startMonth", startMonth);
      }
  
      if (endMonth) {
        params.set("endMonth", endMonth);
      }
    }


    void crimeTypes;

    return `${API_BASE_URL}/tiles/roads/{z}/{x}/{y}.mvt?${params.toString()}`;
  },
};
