# Roads Page Frontend Design Doc

Base URL:
- `http://127.0.0.1:8000`

This doc is for the frontend agent building the Roads page.

Final Roads page API shape:
- `GET /roads/analytics/meta`
- `GET /roads/analytics/overview`
- `GET /roads/analytics/charts`
- `GET /roads/analytics/risk`
- map rendering from `GET /tiles/roads/{z}/{x}/{y}.mvt`

Goal:
- render roads on the map using vector tiles
- let the user filter a road area by time, crime mix, outcome, and road type
- show quick facts for the current selection
- show a dangerous roads table
- show a small number of useful charts

Recommended Roads page layout:
- filter bar
- map
- quick facts cards + insight messages
- dangerous roads table
- charts section

---

## 1. Filter Model

These are the shared Roads analytics filters.

Time:
- `from=YYYY-MM`
- `to=YYYY-MM`

Optional bbox:
- `minLon`
- `minLat`
- `maxLon`
- `maxLat`

Optional categorical filters:
- `crimeType`
- `lastOutcomeCategory`
- `highway`

Notes:
- `crimeType`, `lastOutcomeCategory`, and `highway` can be repeated query params or comma-separated
- if using bbox, send all 4 bbox values together
- `from` and `to` are required for `overview`, `charts`, and `risk`

Recommended frontend filter state:
```ts
type RoadsFilters = {
  from: string
  to: string
  bbox?: {
    minLon: number
    minLat: number
    maxLon: number
    maxLat: number
  }
  crimeType?: string[]
  lastOutcomeCategory?: string[]
  highway?: string[]
}
```

---

## 2. Endpoint Roles

### `GET /roads/analytics/meta`
Use for:
- building the filter UI
- setting defaults
- showing broad dataset coverage

Good UI uses:
- populate highway dropdown
- populate crime type dropdown
- populate outcome dropdown
- set initial time range from `months.min` and `months.max`
- show a subtitle like `212,450 road segments`

Example response:
```json
{
  "months": {
    "min": "2023-02",
    "max": "2026-01"
  },
  "highways": ["motorway", "primary", "residential"],
  "crime_types": ["Shoplifting", "Vehicle crime"],
  "outcomes": ["Under investigation", "Unable to prosecute suspect"],
  "counts": {
    "road_segments_total": 212450,
    "named_roads_total": 76706,
    "total_length_m": 24448828.42,
    "roads_with_incidents": 33368,
    "incidents_total": 927447
  }
}
```

Frontend note:
- call this once on page load
- do not refetch on every filter change unless you want live dropdown narrowing

---

### `GET /roads/analytics/overview`
Use for:
- summary cards
- key facts
- small “what is happening here?” message block

This is the quick facts endpoint.

Good UI uses:
- `total_incidents`
- `roads_with_incidents`
- `road_coverage_pct`
- `avg_incidents_per_km`
- `top_road`
- `current_vs_previous_pct`
- `band_breakdown`
- `insights`

Example request:
```txt
/roads/analytics/overview?from=2023-02&to=2023-06&minLon=-1.6&minLat=53.78&maxLon=-1.52&maxLat=53.82&crimeType=Shoplifting&lastOutcomeCategory=Under%20investigation
```

Example response:
```json
{
  "filters": {
    "from": "2023-02",
    "to": "2023-06",
    "crimeType": ["Shoplifting"],
    "lastOutcomeCategory": ["Under investigation"],
    "highway": null,
    "bbox": {
      "minLon": -1.6,
      "minLat": 53.78,
      "maxLon": -1.52,
      "maxLat": 53.82
    }
  },
  "total_segments": 5500,
  "total_length_m": 123456.7,
  "roads_with_incidents": 420,
  "roads_without_incidents": 5080,
  "road_coverage_pct": 7.64,
  "unique_highway_types": 6,
  "total_incidents": 1200,
  "avg_incidents_per_km": 9.72,
  "top_road": {
    "segment_id": 7,
    "name": "Test Road",
    "highway": "primary",
    "length_m": 120.5,
    "incident_count": 80,
    "incidents_per_km": 66.39,
    "risk_score": 95.0,
    "band": "red"
  },
  "top_highway": {
    "highway": "residential",
    "segment_count": 3200,
    "length_m": 65432.1,
    "incident_count": 900,
    "incidents_per_km": 13.75
  },
  "top_crime_type": {
    "crime_type": "Shoplifting",
    "count": 350
  },
  "top_outcome": {
    "outcome": "Under investigation",
    "count": 260
  },
  "current_period": {
    "from": "2023-02",
    "to": "2023-06",
    "incident_count": 1200
  },
  "previous_period": {
    "from": "2022-09",
    "to": "2023-01",
    "incident_count": 1000
  },
  "current_vs_previous_pct": 20.0,
  "band_breakdown": {
    "red": 4,
    "orange": 10,
    "green": 406
  },
  "insights": [
    "Residential roads drive the largest incident volume in this selection.",
    "Shoplifting is the dominant road-linked crime type here.",
    "This period is 20.0% above the previous matched period."
  ]
}
```

Recommended quick facts card set:
- total incidents
- roads with incidents
- road coverage percent
- average incidents per km

Recommended highlight cards:
- top road
- top crime type
- top outcome

Recommended insight UI:
- render `insights` as short stacked callouts under the cards

---

### `GET /roads/analytics/risk`
Use for:
- dangerous roads table
- ranked shortlist
- “Top 10 risky roads” panel

This is the table endpoint.

Optional params:
- `limit`
- `sort=risk_score|incidents_per_km|incident_count`

Recommended default:
- `sort=risk_score`

Example request:
```txt
/roads/analytics/risk?from=2023-02&to=2023-06&minLon=-1.6&minLat=53.78&maxLon=-1.52&maxLat=53.82&sort=risk_score&limit=25
```

Example response:
```json
{
  "filters": {
    "from": "2023-02",
    "to": "2023-06",
    "crimeType": null,
    "lastOutcomeCategory": null,
    "highway": null,
    "bbox": {
      "minLon": -1.6,
      "minLat": 53.78,
      "maxLon": -1.52,
      "maxLat": 53.82
    }
  },
  "items": [
    {
      "segment_id": 7,
      "name": "Test Road",
      "highway": "primary",
      "length_m": 120.5,
      "incident_count": 80,
      "incidents_per_km": 66.39,
      "dominant_crime_type": "Shoplifting",
      "dominant_outcome": "Under investigation",
      "share_of_incidents": 6.67,
      "previous_period_change_pct": 60.0,
      "risk_score": 92.5,
      "band": "red",
      "message": "80 incidents, driven mainly by Shoplifting, 60.0% up vs previous period"
    }
  ],
  "meta": {
    "returned": 1,
    "limit": 25,
    "sort": "risk_score"
  }
}
```

Recommended table columns:
- road name
- highway
- incident count
- incidents per km
- dominant crime type
- dominant outcome
- previous period change
- band
- message

Recommended table interactions:
- sortable by `risk_score`, `incident_count`, or `incidents_per_km`
- clicking a row can pan the map near that road if you later add a road geometry lookup route again

Important interpretation:
- `risk_score` is the best default ranking
- it blends raw incident volume and normalized rate
- it is safer than sorting only by `incidents_per_km`

---

### `GET /roads/analytics/charts`
Use for:
- time series
- bar charts
- composition charts

This is the charts endpoint.

Optional params:
- `timeseriesGroupBy=overall|highway|crime_type|outcome`
- `groupLimit`
- `limit`

Recommended defaults:
- `timeseriesGroupBy=overall`
- `groupLimit=5`
- `limit=8`

Example request:
```txt
/roads/analytics/charts?from=2023-02&to=2023-06&timeseriesGroupBy=overall&limit=8
```

Example response:
```json
{
  "filters": {
    "from": "2023-02",
    "to": "2023-06",
    "crimeType": null,
    "lastOutcomeCategory": null,
    "highway": null,
    "bbox": null
  },
  "timeseries": {
    "groupBy": "overall",
    "series": [
      {
        "key": "overall",
        "points": [
          { "month": "2023-02", "count": 410 },
          { "month": "2023-03", "count": 430 }
        ]
      }
    ],
    "total": 840,
    "peak": {
      "month": "2023-03",
      "count": 430
    },
    "current_vs_previous_pct": 12.5
  },
  "by_highway": [
    {
      "highway": "residential",
      "segment_count": 3200,
      "length_m": 65432.1,
      "count": 900,
      "share": 75.0,
      "incidents_per_km": 13.75,
      "message": "residential roads are over-indexing for incidents relative to their length."
    }
  ],
  "by_crime_type": [
    {
      "crime_type": "Shoplifting",
      "count": 350,
      "share": 29.17
    }
  ],
  "by_outcome": [
    {
      "outcome": "Under investigation",
      "count": 260,
      "share": 21.67
    }
  ],
  "band_breakdown": {
    "red": 4,
    "orange": 10,
    "green": 406
  },
  "insights": [
    "Peak incident month in this selection is 2023-03.",
    "Residential roads dominate the current highway breakdown.",
    "Shoplifting is the largest road-linked crime type in this view."
  ]
}
```

Chart mapping:
- `timeseries.series`
  - `groupBy=overall` -> single line chart or area chart
  - `groupBy=highway|crime_type|outcome` -> multi-line chart or stacked bar chart
- `by_highway` -> horizontal bar chart
- `by_crime_type` -> horizontal or vertical bar chart
- `by_outcome` -> horizontal bar chart
- `band_breakdown` -> donut chart, stacked bar, or compact legend chart

Recommended chart section:
- one line chart at top
- two side-by-side bar charts below:
  - by highway
  - by crime type
- one smaller chart:
  - by outcome or band breakdown

---

## 3. Map Layer

### `GET /tiles/roads/{z}/{x}/{y}.mvt`
Alias:
- `/tiles/roads/{z}/{x}/{y}.pbf`

Use for:
- map rendering only

Optional query params:
- `includeRisk=true|false`
- `month=YYYY-MM`
- or `startMonth=YYYY-MM&endMonth=YYYY-MM`
- `crimeType`

Recommended frontend behavior:
- render roads from vector tiles
- keep analytics panels powered by the 4 roads analytics endpoints above
- if risk coloring is enabled on the map, pass the same time and crime filters that the page is using

Do not use analytics endpoints to draw the road geometry layer.

---

## 4. Suggested Frontend Fetch Flow

Initial page load:
1. call `/roads/analytics/meta`
2. set default filters
3. initialize the vector tile source for the map
4. call `/roads/analytics/overview`
5. call `/roads/analytics/charts`
6. call `/roads/analytics/risk`

On filter change:
- refetch `overview`
- refetch `charts`
- refetch `risk`
- refresh the map tile layer if the risk tile query params changed

On map move:
- update bbox in filter state
- debounce
- refetch `overview`
- refetch `charts`
- refetch `risk`

Recommended debounce:
- around `250ms` to `400ms` on map move end / filter typing

---

## 5. Minimal Roads Page Build

If you want the smallest useful Roads page:

Top:
- filter bar

Middle:
- map using `/tiles/roads/...`
- overview cards from `/roads/analytics/overview`

Below:
- dangerous roads table from `/roads/analytics/risk`
- line chart + two bar charts from `/roads/analytics/charts`

That gives:
- filtering
- quick facts
- table
- time series
- bar charts
- map

without needing extra roads endpoints.
