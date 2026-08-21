"""
Trip Planner dashboard: view trips, itineraries, weather, and packing
lists, and edit your profile (interests/notes used by the agent).

Talks to Lakebase directly (its own copy of lakebase.py), the same way the
MCP server does - this app and the MCP server are two independent
Databricks Apps that both read/write the same Lakebase instance, not one
calling the other over HTTP (same pattern as the Day 3 reference repo).

Creating a trip here triggers the same enrichment pipeline job the MCP
server's create_trip tool does.

Run locally:
    python app.py
"""

import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, redirect, render_template, request, url_for
from sentence_transformers import SentenceTransformer

import lakebase

app = Flask(__name__)

DEFAULT_USER_EMAIL = os.environ.get("DEFAULT_USER_EMAIL", "demo@example.com")
TRIP_PIPELINE_JOB_ID = os.environ.get("TRIP_PIPELINE_JOB_ID")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

_w = WorkspaceClient()
_embedding_model = None


def get_embedding_model() -> SentenceTransformer:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return _embedding_model


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page)."""
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


def _get_user(email: str) -> dict | None:
    rows = lakebase.run_query("SELECT * FROM users WHERE email = %s", (email,))
    return rows[0] if rows else None


def _get_or_create_user(email: str) -> dict:
    user = _get_user(email)
    if user:
        return user
    return lakebase.run_insert_returning("INSERT INTO users (email) VALUES (%s) RETURNING *", (email,))


def _trigger_trip_pipeline(trip_id: int) -> str:
    """Fire-and-forget trigger for the trip enrichment pipeline job (see
    mcp_server/trip_planner_mcp_server.py for the full explanation - this
    app needs its own copy since trips can also be created from here)."""
    if not TRIP_PIPELINE_JOB_ID:
        app.logger.warning("TRIP_PIPELINE_JOB_ID not configured; skipping pipeline trigger.")
        return "not_triggered"
    try:
        _w.jobs.run_now(job_id=int(TRIP_PIPELINE_JOB_ID), job_parameters={"trip_id": str(trip_id)})
        return "triggered"
    except Exception:
        app.logger.exception(f"Failed to trigger trip pipeline for trip {trip_id}")
        return "trigger_failed"


@app.route("/")
def index():
    email = request.args.get("email", DEFAULT_USER_EMAIL)
    user = _get_user(email)
    trips = []
    if user:
        trips = lakebase.run_query(
            "SELECT * FROM trips WHERE user_id = %s ORDER BY created_at DESC", (user["id"],)
        )
    return render_template("index.html", email=email, user=user, trips=trips, current_trip_id=None)


@app.route("/profile", methods=["POST"])
def update_profile():
    email = request.form.get("email", DEFAULT_USER_EMAIL)
    display_name = request.form.get("display_name", "")
    preferences = request.form.get("preferences", "")

    user = _get_or_create_user(email)
    lakebase.run_write(
        "UPDATE users SET display_name = %s, preferences = %s, updated_at = now() WHERE id = %s",
        (display_name, preferences, user["id"]),
    )

    if preferences:
        embedding = get_embedding_model().encode(preferences).tolist()
        lakebase.run_write(
            """
            INSERT INTO user_note_embeddings (user_id, embedding, model_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, model_name)
            DO UPDATE SET embedding = EXCLUDED.embedding, created_at = now()
            """,
            (user["id"], str(embedding), EMBEDDING_MODEL),
        )

    return redirect(url_for("index", email=email))


@app.route("/trips", methods=["POST"])
def create_trip():
    email = request.form.get("email", DEFAULT_USER_EMAIL)
    user = _get_or_create_user(email)

    name = request.form.get("name")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    destination_names = [d.strip() for d in request.form.get("destination_names", "").split(",") if d.strip()]

    trip = lakebase.run_insert_returning(
        "INSERT INTO trips (user_id, name, start_date, end_date) VALUES (%s, %s, %s, %s) RETURNING id",
        (user["id"], name, start_date, end_date),
    )
    trip_id = trip["id"]

    for dest_name in destination_names:
        lakebase.run_write("INSERT INTO destinations (trip_id, name) VALUES (%s, %s)", (trip_id, dest_name))

    _trigger_trip_pipeline(trip_id)

    return redirect(url_for("index", email=email))


@app.route("/trip/<int:trip_id>/edit", methods=["POST"])
def edit_trip(trip_id):
    name = request.form.get("name") or None
    start_date = request.form.get("start_date") or None
    end_date = request.form.get("end_date") or None
    status = request.form.get("status") or None
    add_destination_names = [
        d.strip() for d in request.form.get("add_destination_names", "").split(",") if d.strip()
    ]

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

    for dest_name in add_destination_names:
        lakebase.run_write("INSERT INTO destinations (trip_id, name) VALUES (%s, %s)", (trip_id, dest_name))

    _trigger_trip_pipeline(trip_id)

    return redirect(url_for("trip_detail", trip_id=trip_id))


@app.route("/trip/<int:trip_id>")
def trip_detail(trip_id):
    trip_rows = lakebase.run_query(
        """
        SELECT t.*, u.email AS user_email
        FROM trips t JOIN users u ON u.id = t.user_id
        WHERE t.id = %s
        """,
        (trip_id,),
    )
    if not trip_rows:
        return jsonify({"error": "Trip not found"}), 404
    trip = trip_rows[0]

    all_trips = lakebase.run_query(
        "SELECT * FROM trips WHERE user_id = %s ORDER BY created_at DESC", (trip["user_id"],)
    )

    destinations = lakebase.run_query(
        "SELECT * FROM destinations WHERE trip_id = %s ORDER BY id", (trip_id,)
    )

    itinerary = lakebase.run_query(
        """
        SELECT i.*, a.name AS activity_name, a.category, a.is_outdoor, a.description
        FROM itinerary_items i
        LEFT JOIN activities a ON a.id = i.activity_id
        WHERE i.trip_id = %s
        ORDER BY i.day_date, i.start_time NULLS LAST, i.position
        """,
        (trip_id,),
    )
    itinerary_by_day = {}
    for item in itinerary:
        itinerary_by_day.setdefault(item["day_date"], []).append(item)

    weather_by_destination = {}
    for dest in destinations:
        weather_by_destination[dest["id"]] = lakebase.run_query(
            """
            SELECT
                (forecast_time AT TIME ZONE 'UTC')::date AS forecast_date,
                ROUND(AVG(temperature_c)::numeric, 1) AS avg_temperature_c,
                ROUND(MAX(precipitation_probability)::numeric, 0) AS max_precipitation_probability,
                ROUND(MAX(us_aqi)::numeric, 0) AS max_us_aqi,
                ROUND(MAX(uv_index)::numeric, 1) AS max_uv_index
            FROM weather_snapshots
            WHERE destination_id = %s
            GROUP BY forecast_date
            ORDER BY forecast_date
            """,
            (dest["id"],),
        )

    packing_list = lakebase.run_query(
        "SELECT * FROM packing_items WHERE trip_id = %s ORDER BY category, item_name", (trip_id,)
    )

    return render_template(
        "trip.html",
        trip=trip,
        email=trip["user_email"],
        trips=all_trips,
        current_trip_id=trip_id,
        destinations=destinations,
        itinerary_by_day=itinerary_by_day,
        weather_by_destination=weather_by_destination,
        packing_list=packing_list,
    )


@app.route("/trip/<int:trip_id>/packing/<int:item_id>/toggle", methods=["POST"])
def toggle_packing(trip_id, item_id):
    packed = request.form.get("packed") == "true"
    lakebase.run_write("UPDATE packing_items SET packed = %s WHERE id = %s", (packed, item_id))
    return redirect(url_for("trip_detail", trip_id=trip_id))


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", 8001))
    app.run(debug=True, host=host, port=port)
