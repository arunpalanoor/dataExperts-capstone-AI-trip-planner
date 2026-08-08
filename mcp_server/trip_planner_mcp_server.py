"""
Trip Planner MCP server.

Exposes read/write tools over MCP (Model Context Protocol) so a Databricks
Agent Bricks agent can manage user profiles, trips, destinations,
activities, itineraries, and packing lists against Lakebase.

This server never calls the third-party APIs (Open-Meteo, Wikimedia)
directly - that's pipeline/enrich_trip.py and pipeline/refresh_weather.py's
job. Creating or editing a trip here triggers that pipeline (see
_trigger_trip_pipeline below) so destination/weather data populates
asynchronously in the background.

Run locally:
    python trip_planner_mcp_server.py
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from fastmcp import FastMCP
from sentence_transformers import SentenceTransformer

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trip-planner-mcp-server")

EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# TRIP_PIPELINE_JOB_ID must point at a Databricks Job with two tasks sharing
# a `trip_id` job parameter: enrich_trip.py, then refresh_weather.py set to
# depend on it. Databricks' own task dependency handles the ordering
# (refresh_weather.py needs enrich_trip.py's geocoding to have already run),
# so this server only has to fire one job and can return immediately.
TRIP_PIPELINE_JOB_ID = os.environ.get("TRIP_PIPELINE_JOB_ID")

_w = WorkspaceClient()
_embedding_model = None

mcp = FastMCP("trip-planner")


def get_embedding_model() -> SentenceTransformer:
    """Lazy-load the embedding model (expensive, only needed on first use)."""
    global _embedding_model
    if _embedding_model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return _embedding_model


def _trigger_trip_pipeline(trip_id: int) -> str:
    """Fire-and-forget trigger for the trip enrichment pipeline job."""
    if not TRIP_PIPELINE_JOB_ID:
        logger.warning("TRIP_PIPELINE_JOB_ID not configured; skipping pipeline trigger.")
        return "not_triggered"
    try:
        _w.jobs.run_now(job_id=int(TRIP_PIPELINE_JOB_ID), job_parameters={"trip_id": str(trip_id)})
        return "triggered"
    except Exception:
        logger.exception(f"Failed to trigger trip pipeline for trip {trip_id}")
        return "trigger_failed"


# ---------------------------------------------------------------------------
# User profile
# ---------------------------------------------------------------------------

@mcp.tool
def get_user_profile(email: str) -> dict:
    """
    Get a user's profile: display name and free-text preferences/notes
    (interests, constraints, allergies). Call this first in any
    conversation so trip/itinerary suggestions can account for the user's
    stated preferences.

    Args:
        email: The user's email address.

    Returns:
        A dict with status and, on success, id/email/display_name/preferences.
    """
    rows = lakebase.run_query("SELECT * FROM users WHERE email = %s", (email,))
    if not rows:
        return {"status": "not_found", "message": f"No profile found for {email}."}
    return {"status": "success", **rows[0]}


@mcp.tool
def update_user_profile(email: str, display_name: str | None = None, preferences: str | None = None) -> dict:
    """
    Create or update a user's profile. Re-embeds the preferences text for
    semantic retrieval whenever it changes.

    Args:
        email: The user's email address.
        display_name: Optional display name.
        preferences: Free-text interests, constraints, allergies, etc.

    Returns:
        A dict with status and the updated profile.
    """
    existing = lakebase.run_query("SELECT id FROM users WHERE email = %s", (email,))
    if existing:
        user_id = existing[0]["id"]
        lakebase.run_write(
            """
            UPDATE users
            SET display_name = COALESCE(%s, display_name),
                preferences = COALESCE(%s, preferences),
                updated_at = now()
            WHERE id = %s
            """,
            (display_name, preferences, user_id),
        )
    else:
        inserted = lakebase.run_insert_returning(
            "INSERT INTO users (email, display_name, preferences) VALUES (%s, %s, %s) RETURNING id",
            (email, display_name, preferences),
        )
        user_id = inserted["id"]

    if preferences:
        embedding = get_embedding_model().encode(preferences).tolist()
        lakebase.run_write(
            """
            INSERT INTO user_note_embeddings (user_id, embedding, model_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, model_name)
            DO UPDATE SET embedding = EXCLUDED.embedding, created_at = now()
            """,
            (user_id, str(embedding), EMBEDDING_MODEL),
        )

    profile = lakebase.run_query("SELECT * FROM users WHERE id = %s", (user_id,))[0]
    return {"status": "success", **profile}


# ---------------------------------------------------------------------------
# Trips
# ---------------------------------------------------------------------------

@mcp.tool
def create_trip(email: str, name: str, start_date: str, end_date: str, destination_names: list[str]) -> dict:
    """
    Create a new trip for a user, with one or more destinations. Triggers
    the enrichment pipeline (geocoding, Wikipedia descriptions/attractions,
    embeddings, then weather/air-quality) in the background - the
    destinations won't have descriptions, activities, or weather yet when
    this returns; check back with get_trip shortly after.

    Args:
        email: The user's email address (profile is created if it doesn't exist).
        name: A name for the trip, e.g. "Yosemite in June".
        start_date: Trip start date, ISO format (YYYY-MM-DD).
        end_date: Trip end date, ISO format (YYYY-MM-DD).
        destination_names: One or more place names, e.g. ["Yosemite National Park"].

    Returns:
        A dict with status, trip_id, destination_ids, and pipeline_status.
    """
    existing_user = lakebase.run_query("SELECT id FROM users WHERE email = %s", (email,))
    user_id = existing_user[0]["id"] if existing_user else lakebase.run_insert_returning(
        "INSERT INTO users (email) VALUES (%s) RETURNING id", (email,)
    )["id"]

    trip = lakebase.run_insert_returning(
        """
        INSERT INTO trips (user_id, name, start_date, end_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (user_id, name, start_date, end_date),
    )
    trip_id = trip["id"]

    destination_ids = []
    for dest_name in destination_names:
        dest = lakebase.run_insert_returning(
            "INSERT INTO destinations (trip_id, name) VALUES (%s, %s) RETURNING id",
            (trip_id, dest_name),
        )
        destination_ids.append(dest["id"])

    pipeline_status = _trigger_trip_pipeline(trip_id)

    return {
        "status": "success",
        "trip_id": trip_id,
        "destination_ids": destination_ids,
        "pipeline_status": pipeline_status,
        "message": "Trip created. Destination details/weather are being fetched in the background.",
    }


@mcp.tool
def update_trip(
    trip_id: int,
    name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    add_destination_names: list[str] | None = None,
) -> dict:
    """
    Update a trip's details and/or add destinations to it. Re-triggers the
    enrichment pipeline for the whole trip - existing destinations are
    updated in place (not duplicated), and any newly added destinations get
    enriched for the first time.

    Args:
        trip_id: The trip to update.
        name: New trip name, if changing.
        start_date: New start date (YYYY-MM-DD), if changing.
        end_date: New end date (YYYY-MM-DD), if changing.
        status: One of 'planning', 'confirmed', 'completed', 'cancelled'.
        add_destination_names: Additional place names to add to this trip.

    Returns:
        A dict with status and pipeline_status.
    """
    lakebase.run_write(
        """
        UPDATE trips
        SET name = COALESCE(%s, name),
            start_date = COALESCE(%s, start_date),
            end_date = COALESCE(%s, end_date),
            status = COALESCE(%s, status),
            updated_at = now()
        WHERE id = %s
        """,
        (name, start_date, end_date, status, trip_id),
    )

    for dest_name in add_destination_names or []:
        lakebase.run_write(
            "INSERT INTO destinations (trip_id, name) VALUES (%s, %s)",
            (trip_id, dest_name),
        )

    pipeline_status = _trigger_trip_pipeline(trip_id)

    return {
        "status": "success",
        "trip_id": trip_id,
        "pipeline_status": pipeline_status,
        "message": "Trip updated. Enrichment pipeline re-triggered for this trip.",
    }


@mcp.tool
def get_trip(trip_id: int) -> dict:
    """
    Get a trip's full details: trip info, destinations (with descriptions
    once enriched), and how many candidate activities each destination has.

    Args:
        trip_id: The trip to look up.

    Returns:
        A dict with status, trip, and destinations (each with an activity_count).
    """
    trips = lakebase.run_query("SELECT * FROM trips WHERE id = %s", (trip_id,))
    if not trips:
        return {"status": "not_found", "message": f"No trip with id {trip_id}."}

    destinations = lakebase.run_query(
        """
        SELECT d.*, COUNT(a.id) AS activity_count
        FROM destinations d
        LEFT JOIN activities a ON a.destination_id = d.id
        WHERE d.trip_id = %s
        GROUP BY d.id
        ORDER BY d.id
        """,
        (trip_id,),
    )

    return {"status": "success", "trip": trips[0], "destinations": destinations}


@mcp.tool
def list_trips(email: str) -> dict:
    """
    List all trips for a user, most recently created first.

    Args:
        email: The user's email address.

    Returns:
        A dict with status and a list of trips.
    """
    trips = lakebase.run_query(
        """
        SELECT t.*
        FROM trips t
        JOIN users u ON u.id = t.user_id
        WHERE u.email = %s
        ORDER BY t.created_at DESC
        """,
        (email,),
    )
    return {"status": "success", "count": len(trips), "trips": trips}


# ---------------------------------------------------------------------------
# Activity search (semantic retrieval)
# ---------------------------------------------------------------------------

@mcp.tool
def search_activities(trip_id: int, query: str, limit: int = 10) -> dict:
    """
    Semantically search this trip's candidate activities (Wikipedia-sourced
    attractions near its destinations) by matching interests/keywords
    against embedded activity descriptions. Use this to find activities
    matching the user's stated interests before building or updating an
    itinerary.

    Args:
        trip_id: The trip whose destinations to search within.
        query: Natural language interests/keywords, e.g. "hiking and photography".
        limit: Maximum number of results (default 10).

    Returns:
        A dict with status and a list of matching activities, most similar first,
        each with a similarity score (0-1) and its is_outdoor/air_quality_sensitive flags.
    """
    if not query or not query.strip():
        return {"status": "error", "message": "query is required"}

    embedding = get_embedding_model().encode(query).tolist()
    embedding_str = str(embedding)

    results = lakebase.run_query(
        """
        SELECT
            a.id, a.destination_id, a.name, a.category, a.is_outdoor,
            a.air_quality_sensitive, a.description, a.wikimedia_url,
            1 - (e.embedding <=> %s::vector) AS similarity
        FROM activity_embeddings e
        JOIN activities a ON a.id = e.activity_id
        JOIN destinations d ON d.id = a.destination_id
        WHERE d.trip_id = %s
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (embedding_str, trip_id, embedding_str, limit),
    )

    return {"status": "success", "query": query, "count": len(results), "activities": results}


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

@mcp.tool
def get_weather_forecast(destination_id: int) -> dict:
    """
    Get the day-by-day weather/air-quality forecast summary for a
    destination, for use when building an itinerary or deciding whether to
    reschedule an outdoor activity. Each day includes derived flags
    (likely_rain, poor_air_quality, high_uv) plus the underlying numbers, so
    you can explain the specific reasoning behind any weather-based change.

    Args:
        destination_id: The destination to get the forecast for.

    Returns:
        A dict with status and daily_forecast (one entry per date), or
        status "no_data" if the weather pipeline hasn't run for this
        destination yet.
    """
    rows = lakebase.run_query(
        """
        SELECT
            (forecast_time AT TIME ZONE 'UTC')::date AS forecast_date,
            ROUND(AVG(temperature_c)::numeric, 1) AS avg_temperature_c,
            ROUND(MAX(precipitation_probability)::numeric, 0) AS max_precipitation_probability,
            ROUND(SUM(precipitation_mm)::numeric, 1) AS total_precipitation_mm,
            ROUND(MAX(wind_speed_kmh)::numeric, 1) AS max_wind_speed_kmh,
            ROUND(MAX(us_aqi)::numeric, 0) AS max_us_aqi,
            ROUND(MAX(uv_index)::numeric, 1) AS max_uv_index,
            ROUND(MAX(pollen_grass)::numeric, 1) AS max_pollen_grass,
            ROUND(MAX(pollen_tree)::numeric, 1) AS max_pollen_tree,
            ROUND(MAX(pollen_weed)::numeric, 1) AS max_pollen_weed
        FROM weather_snapshots
        WHERE destination_id = %s
        GROUP BY forecast_date
        ORDER BY forecast_date
        """,
        (destination_id,),
    )

    if not rows:
        return {
            "status": "no_data",
            "message": f"No weather data yet for destination {destination_id} - the weather pipeline may not have run for it yet.",
        }

    for day in rows:
        day["likely_rain"] = (day["max_precipitation_probability"] or 0) >= 50
        day["poor_air_quality"] = (day["max_us_aqi"] or 0) >= 101
        day["high_uv"] = (day["max_uv_index"] or 0) >= 8

    return {"status": "success", "destination_id": destination_id, "daily_forecast": rows}


# ---------------------------------------------------------------------------
# Itinerary
# ---------------------------------------------------------------------------

@mcp.tool
def get_itinerary(trip_id: int) -> dict:
    """
    Get a trip's full itinerary, ordered by day and position within each day.

    Args:
        trip_id: The trip to look up.

    Returns:
        A dict with status and a list of itinerary items, each including the
        linked activity's name/category/is_outdoor/description/destination_id.
    """
    items = lakebase.run_query(
        """
        SELECT
            i.id, i.trip_id, i.day_date, i.start_time, i.end_time, i.position,
            i.status, i.notes,
            a.id AS activity_id, a.name AS activity_name, a.category,
            a.is_outdoor, a.air_quality_sensitive, a.description, a.destination_id
        FROM itinerary_items i
        LEFT JOIN activities a ON a.id = i.activity_id
        WHERE i.trip_id = %s
        ORDER BY i.day_date, i.position
        """,
        (trip_id,),
    )
    return {"status": "success", "count": len(items), "itinerary": items}


@mcp.tool
def add_itinerary_item(
    trip_id: int,
    day_date: str,
    activity_id: int | None = None,
    destination_id: int | None = None,
    name: str | None = None,
    is_outdoor: bool = True,
    start_time: str | None = None,
    end_time: str | None = None,
    notes: str | None = None,
) -> dict:
    """
    Add an item to a trip's itinerary for a given day - either from an
    existing activity (e.g. a result from search_activities) or as a new
    custom activity (e.g. "lunch at the visitor center") tied to a
    destination. Appended to the end of that day's schedule; use
    move_itinerary_item afterward to reorder if needed.

    Args:
        trip_id: The trip this item belongs to.
        day_date: The day this item is scheduled for (YYYY-MM-DD).
        activity_id: An existing activity's id, if using one.
        destination_id: Required if activity_id is not given - which destination this custom item belongs to.
        name: Required if activity_id is not given - a name for the custom activity.
        is_outdoor: Whether this custom activity is outdoor (affects weather-based rescheduling later). Ignored if activity_id is given.
        start_time: Optional start time (HH:MM).
        end_time: Optional end time (HH:MM).
        notes: Optional notes, e.g. why this was scheduled here.

    Returns:
        A dict with status, the created itinerary_item_id, and activity_id used.
    """
    if activity_id is None:
        if destination_id is None or not name:
            return {"status": "error", "message": "Either activity_id, or both destination_id and name, are required."}
        inserted = lakebase.run_insert_returning(
            """
            INSERT INTO activities (destination_id, name, is_outdoor, source)
            VALUES (%s, %s, %s, 'agent_added')
            RETURNING id
            """,
            (destination_id, name, is_outdoor),
        )
        activity_id = inserted["id"]

    position_row = lakebase.run_query(
        "SELECT COALESCE(MAX(position), -1) AS max_position FROM itinerary_items WHERE trip_id = %s AND day_date = %s",
        (trip_id, day_date),
    )
    position = position_row[0]["max_position"] + 1

    item = lakebase.run_insert_returning(
        """
        INSERT INTO itinerary_items (trip_id, activity_id, day_date, start_time, end_time, position, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (trip_id, activity_id, day_date, start_time, end_time, position, notes),
    )

    return {"status": "success", "itinerary_item_id": item["id"], "activity_id": activity_id}


@mcp.tool
def move_itinerary_item(
    item_id: int,
    day_date: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    position: int | None = None,
    status: str | None = None,
    notes: str | None = None,
) -> dict:
    """
    Move, reschedule, or update an existing itinerary item. Always pass
    notes explaining the change when rescheduling for weather - the user
    needs to see why. Changing day_date or start_time auto-marks the item
    'rescheduled' unless you pass an explicit status.

    Args:
        item_id: The itinerary item to update.
        day_date: New day (YYYY-MM-DD), if moving to a different day.
        start_time: New start time (HH:MM), if changing.
        end_time: New end time (HH:MM), if changing.
        position: New position within the day, if reordering.
        status: One of 'scheduled', 'rescheduled', 'completed', 'cancelled'. Auto-set to 'rescheduled' if omitted and day_date/start_time changed.
        notes: Explanation for the change, e.g. "Moved from Tue to Thu - 80% rain chance Tue, clear Thu."

    Returns:
        A dict with status confirming the update.
    """
    if status is None and (day_date is not None or start_time is not None):
        status = "rescheduled"

    rows_affected = lakebase.run_write(
        """
        UPDATE itinerary_items
        SET day_date = COALESCE(%s, day_date),
            start_time = COALESCE(%s, start_time),
            end_time = COALESCE(%s, end_time),
            position = COALESCE(%s, position),
            status = COALESCE(%s, status),
            notes = COALESCE(%s, notes),
            updated_at = now()
        WHERE id = %s
        """,
        (day_date, start_time, end_time, position, status, notes, item_id),
    )

    if rows_affected == 0:
        return {"status": "not_found", "message": f"No itinerary item with id {item_id}."}
    return {"status": "success", "itinerary_item_id": item_id}


@mcp.tool
def remove_itinerary_item(item_id: int) -> dict:
    """
    Remove an item from a trip's itinerary entirely.

    Args:
        item_id: The itinerary item to remove.

    Returns:
        A dict confirming removal.
    """
    rows_affected = lakebase.run_write("DELETE FROM itinerary_items WHERE id = %s", (item_id,))
    if rows_affected == 0:
        return {"status": "not_found", "message": f"No itinerary item with id {item_id}."}
    return {"status": "success", "message": f"Removed itinerary item {item_id}."}


# ---------------------------------------------------------------------------
# Packing list
# ---------------------------------------------------------------------------

@mcp.tool
def get_packing_list(trip_id: int) -> dict:
    """
    Get a trip's packing list.

    Args:
        trip_id: The trip to look up.

    Returns:
        A dict with status and a list of packing items (name, reason, category, packed).
    """
    items = lakebase.run_query(
        "SELECT * FROM packing_items WHERE trip_id = %s ORDER BY category, item_name",
        (trip_id,),
    )
    return {"status": "success", "count": len(items), "packing_list": items}


@mcp.tool
def add_packing_item(trip_id: int, item_name: str, reason: str | None = None, category: str | None = None) -> dict:
    """
    Add an item to a trip's packing list. Base packing suggestions on the
    finalized itinerary and each day's weather forecast (get_itinerary +
    get_weather_forecast) - always include a reason so the user understands
    why it's recommended, e.g. "rain jacket - 70% rain chance on day 2".

    Args:
        trip_id: The trip this item belongs to.
        item_name: The item to pack, e.g. "rain jacket".
        reason: Why it's recommended, e.g. "showers forecast on day 2".
        category: e.g. clothing, gear, health, documents.

    Returns:
        A dict with status and the created packing_item_id.
    """
    item = lakebase.run_insert_returning(
        """
        INSERT INTO packing_items (trip_id, item_name, reason, category)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (trip_id, item_name, reason, category),
    )
    return {"status": "success", "packing_item_id": item["id"]}


@mcp.tool
def toggle_packing_item(item_id: int, packed: bool) -> dict:
    """
    Mark a packing list item as packed or not packed.

    Args:
        item_id: The packing item to update.
        packed: True if packed, False if not.

    Returns:
        A dict confirming the update.
    """
    rows_affected = lakebase.run_write(
        "UPDATE packing_items SET packed = %s WHERE id = %s", (packed, item_id)
    )
    if rows_affected == 0:
        return {"status": "not_found", "message": f"No packing item with id {item_id}."}
    return {"status": "success", "packing_item_id": item_id, "packed": packed}


@mcp.tool
def remove_packing_item(item_id: int) -> dict:
    """
    Remove an item from a trip's packing list.

    Args:
        item_id: The packing item to remove.

    Returns:
        A dict confirming removal.
    """
    rows_affected = lakebase.run_write("DELETE FROM packing_items WHERE id = %s", (item_id,))
    if rows_affected == 0:
        return {"status": "not_found", "message": f"No packing item with id {item_id}."}
    return {"status": "success", "message": f"Removed packing item {item_id}."}


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    mcp.run(transport="http", host="0.0.0.0", port=port)
