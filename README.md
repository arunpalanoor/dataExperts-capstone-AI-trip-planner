# AI Trip and Outdoor Activity Planner

Capstone project for the Data Experts bootcamp. Users save destinations and
preferences, then ask an agent to build a weather-aware itinerary — one that
reschedules outdoor activities around rain or bad air quality, builds a
packing list, and explains why it made each change.

Built on Databricks: Lakebase (managed Postgres) for storage, a Spark
pipeline for ingestion/enrichment, pgvector embeddings for semantic
retrieval, and a Databricks Agent Bricks agent wired to a custom MCP server
for both retrieval and write actions. Follows the connection/deploy patterns
from [`databricks-lakebase-app-day-3`](https://github.com/arunpalanoor/databricks-lakebase-app-day-3).

> Status: actively being built against a 2026-08-09 EOD deadline. This
> README tracks the plan and will get a cleanup pass before final
> submission.

## Capstone requirements → how this project meets them

| Requirement | How |
|---|---|
| Spark data pipeline | Ingests destination/attraction data (Wikimedia) and weather/air-quality data (Open-Meteo), enriches and writes it into Lakebase tables. See `pipeline/`. |
| Third-party API integration | Open-Meteo (geocoding, weather, air quality) + Wikimedia (descriptions, attractions). |
| Unstructured data processing | Destination descriptions, attraction info, activity requirements, and user notes are embedded (sentence-transformers) for semantic retrieval. |
| Databricks App with a frontend | A dashboard app (Flask) for viewing/managing trips and itineraries, deployed as its own Databricks App. |
| AI agent with tools (read + write) | Agent Bricks agent using a FastMCP server that exposes retrieval tools (search destinations/activities, check weather) and write tools (create/update itinerary items, packing list) against Lakebase. |

## Usage (end to end)

1. **Save a trip** — "I'm planning a trip to Yosemite, June 12–15, I like hiking and photography." App geocodes the destination, pulls its Wikimedia description + nearby attractions, stores a `trips` row and candidate `activities`.
2. **Generate itinerary** — "Build me the itinerary." Agent retrieves activities matching the user's interests via embedding similarity, cross-references the weather/AQI forecast for those dates, and lays out a day-by-day plan. Writes `itinerary_items`.
3. **Weather disrupts the plan** — When forecast changes make a scheduled outdoor activity a bad idea (rain, poor AQI), the agent reschedules it and explains why in plain language.
4. **Packing list** — "What should I pack?" Agent looks at the finalized itinerary plus weather/AQI/UV across those days and produces a packing list with reasons.
5. **Manual edits** — "Move the kayaking to the morning" / "Remove the museum stop." Agent performs the write directly against `itinerary_items`, no full regeneration.
6. **Ask about a place** — "What's worth seeing near the falls?" Agent semantically searches embedded Wikimedia attraction text and returns suggestions, which can be added to the itinerary.

### User profile

A lightweight `users` profile (interests, constraints/allergies, free-text
notes) is editable from a **settings section on the dashboard page** (no
modal/pop-up — same functionality, less UI work). Agent Bricks' system
prompt is fixed at agent-creation time and can't be templated per user, so
the profile reaches the agent via `get_user_profile` /
`update_user_profile` MCP tools, plus a static system-prompt instruction
telling the agent to always call `get_user_profile` first — the same
pattern Day 3 uses for `get_current_user`/`get_account_summary`.

## Third-party APIs

- [Open-Meteo Geocoding API](https://open-meteo.com/en/docs/geocoding-api) — destination name → coordinates.
- [Open-Meteo Weather API](https://open-meteo.com/en/docs) — hourly forecasts.
- [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api) — AQI, particulate matter, UV, pollen.
- [Wikimedia APIs](https://www.mediawiki.org/wiki/API:Main_page) — destination descriptions and nearby attractions.

Open-Meteo requires no API key for noncommercial use under its free limits.

## Architecture

```
Spark pipeline (pipeline/)
    Open-Meteo (geocoding/weather/air quality) --\
    Wikimedia (descriptions/attractions)         --> Lakebase tables + embeddings
                                                        |
                                                        v
Agent Bricks agent  --(MCP tool calls)-->  mcp_server/  --(reads/writes)-->  Lakebase (Postgres + pgvector)
                                                                                    ^
                                                                                    |
                                            dashboard/ (Flask app)  ----------------+
```

- `mcp_server/` and `dashboard/` are two separate Databricks Apps, same
  pattern as Day 3: one serves MCP tool calls to the agent, the other serves
  a human-facing UI. Both read/write the same Lakebase instance.
- `pipeline/` is a Spark job (run as a Databricks job/notebook) that pulls
  from the third-party APIs, transforms the data, computes embeddings for
  unstructured text, and writes to Lakebase.

## Lakebase tables

Schema: [`sql/schema.sql`](sql/schema.sql). Apply with `psql "$LAKEBASE_URL" -f sql/schema.sql`.

- `users` — profile + free-text preferences/notes.
- `trips` — one row per planned trip.
- `destinations` — trip stops, geocoded, with Wikimedia description.
- `activities` — candidate activities per destination (Wikimedia nearby attractions + user/agent-added), flagged `is_outdoor` / `air_quality_sensitive` for weather-aware scheduling.
- `itinerary_items` — the actual day-by-day plan, with `notes` capturing the agent's reasoning for placements/reschedules.
- `weather_snapshots` — hourly weather + air quality/UV/pollen per destination, from Open-Meteo.
- `packing_items` — generated packing list with per-item reasoning.
- `destination_embeddings` / `activity_embeddings` / `user_note_embeddings` — pgvector (384-dim, `all-MiniLM-L6-v2`) embeddings for semantic retrieval.

## Context engineering

Embed destination descriptions, attraction info, activity requirements, and
user notes (sentence-transformers, same model as Day 3:
`all-MiniLM-L6-v2`). Retrieve suitable activities by combining semantic
similarity (interests) with structured filters (current/forecast weather,
air quality).

## Agent capabilities

- Generate a day-by-day itinerary.
- Reschedule outdoor activities when rain or poor air quality is forecast.
- Build a packing list.
- Add, remove, or move itinerary items.
- Explain why it made each weather-based change.

## Repo layout (planned)

```
pipeline/        Spark ingestion + enrichment + embedding job(s)
mcp_server/      FastMCP server exposing itinerary tools (Databricks App)
dashboard/       Flask frontend (Databricks App)
sql/             Lakebase schema (sql/schema.sql)
```

## Setup

TODO — fill in once the pipeline/app/agent are running, following the
secrets + Git-folder deploy steps from Day 3's README.
