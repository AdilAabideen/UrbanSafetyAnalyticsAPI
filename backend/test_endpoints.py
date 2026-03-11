import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BBOX = {
    "minLon": -0.2,
    "minLat": 51.4,
    "maxLon": 0.2,
    "maxLat": 51.6,
    "limit": 1,
}


def fetch_json(base_url, path, params=None):
    url = f"{base_url.rstrip('/')}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        payload = body
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            pass
        return exc.code, payload
    except URLError as exc:
        return None, {"error": str(exc)}


def print_result(name, status, payload):
    print(f"{name}: {status}")
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    print()


def main():
    parser = argparse.ArgumentParser(description="Smoke test the Urban Risk Analytics API")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--min-lon", type=float, default=DEFAULT_BBOX["minLon"])
    parser.add_argument("--min-lat", type=float, default=DEFAULT_BBOX["minLat"])
    parser.add_argument("--max-lon", type=float, default=DEFAULT_BBOX["maxLon"])
    parser.add_argument("--max-lat", type=float, default=DEFAULT_BBOX["maxLat"])
    parser.add_argument("--limit", type=int, default=DEFAULT_BBOX["limit"])
    parser.add_argument("--road-id", type=int, default=None)
    args = parser.parse_args()

    bbox = {
        "minLon": args.min_lon,
        "minLat": args.min_lat,
        "maxLon": args.max_lon,
        "maxLat": args.max_lat,
        "limit": args.limit,
    }
    point = {
        "lon": (args.min_lon + args.max_lon) / 2,
        "lat": (args.min_lat + args.max_lat) / 2,
    }

    failures = []

    status, payload = fetch_json(args.base_url, "/")
    print_result("GET /", status, payload)
    if status != 200:
        failures.append("GET /")

    status, payload = fetch_json(args.base_url, "/health")
    print_result("GET /health", status, payload)
    if status != 200:
        failures.append("GET /health")

    status, roads_payload = fetch_json(args.base_url, "/roads", bbox)
    print_result("GET /roads", status, roads_payload)
    if status != 200:
        failures.append("GET /roads")

    status, nearest_payload = fetch_json(args.base_url, "/roads/nearest", point)
    print_result("GET /roads/nearest", status, nearest_payload)
    if status != 200:
        failures.append("GET /roads/nearest")

    stats_params = dict(bbox)
    stats_params.pop("limit", None)
    status, stats_payload = fetch_json(args.base_url, "/roads/stats", stats_params)
    print_result("GET /roads/stats", status, stats_payload)
    if status != 200:
        failures.append("GET /roads/stats")

    road_id = args.road_id
    if road_id is None and isinstance(roads_payload, dict):
        features = roads_payload.get("features") or []
        if features:
            road_id = features[0].get("properties", {}).get("id")
    if road_id is None and isinstance(nearest_payload, dict):
        road_id = nearest_payload.get("id")

    if road_id is None:
        print("GET /roads/{id}: skipped")
        print("Could not infer a road id. Pass --road-id to force this check.\n")
        failures.append("GET /roads/{id} skipped")
    else:
        status, road_payload = fetch_json(args.base_url, f"/roads/{road_id}")
        print_result(f"GET /roads/{road_id}", status, road_payload)
        if status != 200:
            failures.append(f"GET /roads/{road_id}")

    if failures:
        print("Failures:")
        for item in failures:
            print(f"- {item}")
        sys.exit(1)

    print("All endpoint checks passed.")


if __name__ == "__main__":
    main()
