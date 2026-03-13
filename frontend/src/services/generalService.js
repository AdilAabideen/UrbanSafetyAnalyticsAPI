import { API_BASE_URL } from "../config/env";
import { fetchJson } from "./serviceUtils";

export const generalService = {
  async getRoot(requestOptions = {}) {
    return fetchJson(`${API_BASE_URL}/`, "Failed to reach API root", requestOptions);
  },

  async getHealth(requestOptions = {}) {
    return fetchJson(`${API_BASE_URL}/health`, "Health check failed", requestOptions);
  },

  async getLsoaCategories(requestOptions = {}) {
    return fetchJson(
      `${API_BASE_URL}/lsoa/categories`,
      "Failed to fetch LSOA categories",
      requestOptions,
    );
  },
};
