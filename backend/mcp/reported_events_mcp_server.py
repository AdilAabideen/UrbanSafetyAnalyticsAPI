#!/usr/bin/env python3
"""
MCP server for Urban Safety Analytics reported-events API.

This server exposes one tool:
  - create_reported_event

It wraps the existing FastAPI endpoint:
  POST /reported-events

Auth behavior matches backend behavior:
  - with bearer token -> authenticated report
  - without bearer token -> anonymous report
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests
from mcp.server.fastmcp import FastMCP


DEFAULT_API_BASE_URL = os.getenv("MCP_API_BASE_URL", "http://localhost:8000").strip()
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("MCP_HTTP_TIMEOUT_SECONDS", "15"))


mcp = FastMCP("urban-safety-analytics")


def _resolved_base_url(api_base_url: Optional[str]) -> str:
    base = (api_base_url or DEFAULT_API_BASE_URL).strip()
    if not base:
        raise ValueError("api_base_url is empty")
    return base.rstrip("/")


def _safe_json(response: requests.Response) -> Dict[str, Any]:
    try:
        payload = response.json()
        return payload if isinstance(payload, dict) else {"raw": payload}
    except Exception:
        return {"raw_text": response.text}


@mcp.tool()
def create_reported_event(
    payload: Dict[str, Any],
    bearer_token: Optional[str] = None,
    api_base_url: Optional[str] = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """
    Create a user-reported crime/collision event through the existing backend API.

    Args:
      payload:
        JSON payload matching `ReportedEventCreateRequest`.
      bearer_token:
        Optional JWT bearer token. If omitted, event is created as anonymous.
      api_base_url:
        Optional override for backend base URL (default from MCP_API_BASE_URL or localhost).
      timeout_seconds:
        HTTP timeout in seconds.

    Returns:
      Structured result containing status and backend response payload.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object/dict")

    base_url = _resolved_base_url(api_base_url)
    url = f"{base_url}/reported-events"

    headers: Dict[str, str] = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    try:
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        return {
            "ok": False,
            "status_code": None,
            "error": "NETWORK_ERROR",
            "message": str(exc),
            "details": {"url": url},
        }

    body = _safe_json(response)
    if response.ok:
        return {
            "ok": True,
            "status_code": response.status_code,
            "data": body,
        }

    return {
        "ok": False,
        "status_code": response.status_code,
        "error": body.get("error"),
        "message": body.get("message"),
        "details": body.get("details"),
        "raw": body,
    }


if __name__ == "__main__":
    mcp.run()
