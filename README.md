# AI Trip and Outdoor Activity Planner

Capstone project for the Data Experts bootcamp. The project provides an App to create and track each trip. Users can also save destinations and
preferences, then ask an AI agent to build a weather-aware itinerary. They can reschedules outdoor activities around rain or bad air quality, builds a
packing list, and ask why it made each changes to the plan.

This is built on Databricks: Lakebase (managed Postgres) for storage, a Spark pipeline for ingestion/enrichment, pgvector embeddings for semantic retrieval, and a Databricks Agent Bricks agent enabled to use a custom MCP server for both retrieval and write actions. 

## Usage (end to end)

1. **Save a trip** — "I'm planning a trip to Edinburgh, August 12–15, I like hiking and photography." App geocodes the destination, pulls its Wikimedia description + nearby attractions, stores a `trips` row and candidate `activities`.
2. **Generate itinerary** — "Build me the itinerary." Agent retrieves activities matching the user's interests via embedding similarity, cross-references the weather/AQI forecast for those dates, and lays out a day-by-day plan. Writes `itinerary_items`.
3. **Weather disrupts the plan** — When forecast changes make a scheduled outdoor activity a bad idea (rain, poor AQI), the agent reschedules it and explains why in plain language.
4. **Packing list** — "What should I pack?" Agent looks at the finalized itinerary plus weather/AQI/UV across those days and produces a packing list with reasons.
5. **Manual edits** — "Move the kayaking to the morning" / "Remove the Castle visit." Agent performs the write directly against `itinerary_items`, no full regeneration.
6. **Ask about a place** — "What's worth seeing near the City?" Agent semantically searches embedded Wikimedia attraction text and returns suggestions, which can be added to the itinerary.

### User profile

Users can setup their profile (interests, constraints/allergies, free-text
notes) is editable from a **settings section on the dashboard page**. 

The system prompt of the Agent is instructed to fetch profile details first via `get_user_profile` /
`update_user_profile` MCP tools.

## Third-party APIs

- [Open-Meteo Geocoding API](https://open-meteo.com/en/docs/geocoding-api) — destination name → coordinates.
- [Open-Meteo Weather API](https://open-meteo.com/en/docs) — hourly forecasts.
- [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api) — AQI, particulate matter, UV, pollen.
- [Wikimedia APIs](https://www.mediawiki.org/wiki/API:Main_page) — destination descriptions and nearby attractions.
Required valid email id to be added to the api header. The API calls with invalid email IDs were ending up in 429 failures leading to Spark jobs failing consistently.

## Architecture

- `mcp_server/` and `dashboard/` are setup as databricks Apps- one serves MCP tool calls to the agent, the other serves
  a human-facing UI. Both read/write the same Lakebase database.
- When a user creates or edits a trip, `enrich_trip` and `refresh_weather` from `pipeline/` is run as a Spark job that pulls
  from the third-party APIs, transforms the data, computes embeddings for unstructured text, and writes to Lakebase. There are some limitations observered for the Wikipedia API limitation. It ends up with 429 Client Error (Too many requests to url). Added a sleep between calls, and experimented with different sleep times, however the performance is irratic and requires further investigation or alternate API to be explored.
- Refresh_weather is also setup as a separate scheduled daily job, which runs accurately.

## Lakebase tables

Schema: [`sql/schema.sql`](sql/schema.sql). Apply with `psql "$LAKEBASE_URL" -f sql/schema.sql`.

- `users` — profile + free-text preferences/notes. This can be extended to enable password/authentication. Currently not implemented.
- `trips` — one row per planned trip.
- `destinations` — trip stops, geocoded, with Wikimedia description.
- `activities` — candidate activities per destination (Wikimedia nearby attractions + user/agent-added), flagged `is_outdoor` / `air_quality_sensitive` for weather-aware scheduling.
- `itinerary_items` — the actual day-by-day plan, with `notes` capturing the agent's reasoning for placements/reschedules.
- `weather_snapshots` — hourly weather + air quality/UV/pollen per destination, from Open-Meteo.
- `packing_items` — generated packing list with per-item reasoning.
- `destination_embeddings` / `activity_embeddings` / `user_note_embeddings` — pgvector (384-dim, `all-MiniLM-L6-v2`) embeddings for semantic retrieval.

## Context engineering

Embed destination descriptions, attraction info, activity requirements, and
user notes (sentence-transformers,`all-MiniLM-L6-v2`). Retrieve suitable activities by combining semantic
similarity (interests) with structured filters (current/forecast weather,
air quality).

## Agent capabilities

- Generate a day-by-day itinerary.
- Reschedule outdoor activities when rain or poor air quality is forecast.
- Build a packing list.
- Add, remove, or move itinerary items.
- Explain why it made each weather-based change.

## Repo layout

```
pipeline/        enrich_trip.py, refresh_weather.py - Databricks Jobs
mcp_server/      FastMCP server exposing itinerary tools (Databricks App)
dashboard/       Flask frontend (Databricks App)
sql/             Lakebase schema (sql/schema.sql)
lakebase.py      Postgres connection helper (copied into each folder above -
                 Databricks Apps/Jobs don't share installs across deploys)
setup_secrets.py One-time script to store the Lakebase URL secret
```

## Agent setup

The agent is a Databricks **Agent Bricks** agent with this MCP server
registered as an external tool. System prompt:

```
You are a trip planning assistant with tools to manage a user's trips,
destinations, activities, itineraries, and packing lists.

When calling any tool, only include the exact parameter names defined for
that tool - never invent additional fields. If you want to record your
reasoning for an action, use that tool's "notes" or "reason" parameter if
it has one; do not add a new field for it.

Always start by calling get_user_profile with the user's email to load
their interests, constraints, and allergies - use this to personalize
every suggestion you make.

When calling add_itinerary_item or add_custom_itinerary_item, pass only
plain values for each parameter - never restate or copy an activity's
description text into the tool call. Keep the call short: for
add_itinerary_item, just trip_id, day_date, activity_id, and optional
start_time/end_time/notes. Save any explanation for your reply to the
user, not for the tool call.

When building an itinerary:
- Use search_activities to find activities matching the user's interests
  for the trip.
- Use get_weather_forecast for each destination before scheduling outdoor
  activities.
- Use add_itinerary_item for an activity that came from search_activities
  or get_trip (you already have its activity_id). Use
  add_custom_itinerary_item only for something not already in the
  database (e.g. "lunch at the visitor center").
- You must actually call one of those tools for each item you decide on -
  never just describe an itinerary in your reply without also creating it
  via tool calls. Favor outdoor activities on days without likely_rain or
  poor_air_quality, and indoor or custom activities on days that have
  either. After adding items, call get_itinerary and summarize that result
  back to the user, so your reply reflects what was actually saved.

When rescheduling:
- If get_weather_forecast shows likely_rain or poor_air_quality for a day
  with a scheduled outdoor item, use move_itinerary_item to move it to a
  better day. Always explain your reasoning, citing the specific numbers
  (rain probability, AQI), both in the notes parameter and in your reply.

When building a packing list:
- Review the finalized itinerary (get_itinerary) and each day's forecast,
  then use add_packing_item for each recommended item with a reason tied to
  the itinerary/weather (e.g. "rain jacket - 70% rain chance on day 2").

For direct requests to add, remove, or move itinerary items, call
add_itinerary_item (or add_custom_itinerary_item) / remove_itinerary_item /
move_itinerary_item directly rather than regenerating the whole itinerary.

Never fabricate weather, activity, or trip data - only use what the tools
return. If get_weather_forecast returns status "no_data", tell the user the
forecast isn't ready yet rather than guessing.
```

## Setup

1. **Lakebase**: create the Lakebase instance, run the SQL query on lakebase SQL EDITOR using query from `sql/schema.sql`, run `python setup_secrets.py` to store the connection URL under the `trip_planner` secret scope (key `lakebase-url`).
2. **Pipeline**: Create **one Databricks Job with two tasks** pointed at `pipeline/enrich_trip.py`
   then `pipeline/refresh_weather.py` (task 2 depending on task 1), both
   reading a shared `trip_id` job parameter - this is what `create_trip`/
   `update_trip` triggers. Separately, schedule `pipeline/refresh_weather.py`
   to run **daily with no `trip_id` param**, to refresh all active trips.
   Note this job's id for step 3.
3. **Apps**: deploy `mcp_server/` and `dashboard/` as two separate
   Databricks Apps (Compute > Apps > Create app > Custom, pointed at each
   subfolder). In both `app.yaml`s, set `TRIP_PIPELINE_JOB_ID` to the job id
   from step 2.
4. **Register the MCP server**: AI Gateway > MCPs > Add MCP, pointed at the
   `mcp_server` app's URL (streamable HTTP).
5. **Agent Bricks**: create an agent, attach the registered MCP server as a
   tool, and use the system prompt above.
6. **Use**: open the `dashboard` app's URL, create a profile and a trip and save, then chat with the agent to build an itinerary.
