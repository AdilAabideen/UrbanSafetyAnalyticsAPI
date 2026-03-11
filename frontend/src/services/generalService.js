import { API_BASE_URL } from "../config/env";

async function parseJsonOrThrow(response, fallbackMessage) {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || fallbackMessage);
  }

  return response.json();
}

export const generalService = {
  async getRoot() {
    const response = await fetch(`${API_BASE_URL}/`);
    return parseJsonOrThrow(response, "Failed to reach API root");
  },

  async getHealth() {
    const response = await fetch(`${API_BASE_URL}/health`);
    return parseJsonOrThrow(response, "Health check failed");
  },
};

