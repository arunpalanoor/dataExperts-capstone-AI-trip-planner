"""
Superseded by enrich_trip.ipynb, which is what the Databricks Job actually
runs (adds a %pip install cell and Wikipedia rate-limit handling that only
made sense to add directly in the notebook). Kept here for reference/diffing
since plain .py is easier to review than notebook JSON - keep both in sync
if you change the logic.

Databricks Job: enrich a trip's destinations after it is created or edited.

Widget parameter: trip_id

For each destination belonging to the trip:
  1. Geocode the name (Open-Meteo Geocoding) -> lat/lon/country/timezone/canonical_name.
  2. Fetch a Wikipedia summary for the destination -> description.
  3. Fetch nearby Wikipedia pages (geosearch) as candidate activities.
  4. Embed the destination description and each activity description.

Writes to destinations, activities, destination_embeddings, activity_embeddings.
Weather/air-quality data is handled separately by refresh_weather.py on a daily
schedule, to avoid re-fetching forecasts on every trip edit.

Destination counts per trip are small (a handful of rows), so this job reads
via plain psycopg2 (lakebase.run_query) rather than Spark - no real
parallelism benefit at this scale. The Spark data pipeline requirement is
met by refresh_weather.py instead, which processes many destinations' worth
of hourly forecast rows across all active trips at once.
"""

import requests
from sentence_transformers import SentenceTransformer

import lakebase

dbutils.widgets.text("trip_id", "")
TRIP_ID = int(dbutils.widgets.get("trip_id"))

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
model = SentenceTransformer(EMBEDDING_MODEL)


def geocode(name: str) -> dict | None:
    resp = requests.get(GEOCODING_URL, params={"name": name, "count": 1}, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("results") or []
    if not results:
        return None
    r = results[0]
    return {
        "canonical_name": r["name"],
        "latitude": r["latitude"],
        "longitude": r["longitude"],
        "country": r.get("country"),
        "timezone": r.get("timezone"),
    }


def wikipedia_summary(title: str) -> dict | None:
    resp = requests.get(
        WIKI_API_URL,
        params={
            "action": "query",
            "prop": "extracts",
            "exintro": True,
            "explaintext": True,
            "titles": title,
            "format": "json",
        },
        timeout=10,
    )
    resp.raise_for_status()
    pages = resp.json().get("query", {}).get("pages", {})
    page = next(iter(pages.values()), None)
    if not page or "missing" in page:
        return None
    return {
        "title": page.get("title"),
        "extract": page.get("extract") or "",
        "url": f"https://en.wikipedia.org/?curid={page.get('pageid')}",
    }


def wikipedia_nearby(lat: float, lon: float, radius_m: int = 10000, limit: int = 10) -> list[dict]:
    resp = requests.get(
        WIKI_API_URL,
        params={
            "action": "query",
            "list": "geosearch",
            "gscoord": f"{lat}|{lon}",
            "gsradius": radius_m,
            "gslimit": limit,
            "format": "json",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("query", {}).get("geosearch", [])


def upsert_destination_embedding(destination_id: int, text: str) -> None:
    if not text:
        return
    embedding = model.encode(text).tolist()
    lakebase.run_write(
        """
        INSERT INTO destination_embeddings (destination_id, embedding, model_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (destination_id, model_name)
        DO UPDATE SET embedding = EXCLUDED.embedding, created_at = now()
        """,
        (destination_id, str(embedding), EMBEDDING_MODEL),
    )


def upsert_activity_embedding(activity_id: int, text: str) -> None:
    if not text:
        return
    embedding = model.encode(text).tolist()
    lakebase.run_write(
        """
        INSERT INTO activity_embeddings (activity_id, embedding, model_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (activity_id, model_name)
        DO UPDATE SET embedding = EXCLUDED.embedding, created_at = now()
        """,
        (activity_id, str(embedding), EMBEDDING_MODEL),
    )


def enrich_destination(destination_id: int, name: str) -> None:
    geo = geocode(name)
    if geo is None:
        print(f"No geocoding match for destination {destination_id} ('{name}'), skipping.")
        return

    summary = wikipedia_summary(geo["canonical_name"]) or {}

    lakebase.run_write(
        """
        UPDATE destinations
        SET canonical_name = %s, latitude = %s, longitude = %s, country = %s,
            timezone = %s, description = %s, wikimedia_page_title = %s, wikimedia_url = %s
        WHERE id = %s
        """,
        (
            geo["canonical_name"], geo["latitude"], geo["longitude"], geo["country"],
            geo["timezone"], summary.get("extract"), summary.get("title"), summary.get("url"),
            destination_id,
        ),
    )
    upsert_destination_embedding(destination_id, summary.get("extract", ""))

    for page in wikipedia_nearby(geo["latitude"], geo["longitude"]):
        page_summary = wikipedia_summary(page["title"]) or {}
        existing = lakebase.run_query(
            "SELECT id FROM activities WHERE destination_id = %s AND wikimedia_page_title = %s",
            (destination_id, page["title"]),
        )
        if existing:
            activity_id = existing[0]["id"]
            lakebase.run_write(
                "UPDATE activities SET description = %s, latitude = %s, longitude = %s WHERE id = %s",
                (page_summary.get("extract"), page["lat"], page["lon"], activity_id),
            )
        else:
            inserted = lakebase.run_insert_returning(
                """
                INSERT INTO activities
                    (destination_id, name, description, source, latitude, longitude,
                     wikimedia_page_title, wikimedia_url)
                VALUES (%s, %s, %s, 'wikimedia', %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    destination_id, page["title"], page_summary.get("extract"),
                    page["lat"], page["lon"], page["title"], page_summary.get("url"),
                ),
            )
            activity_id = inserted["id"]
        upsert_activity_embedding(activity_id, page_summary.get("extract", ""))

    print(f"Enriched destination {destination_id} ('{geo['canonical_name']}').")


destinations = lakebase.run_query(
    "SELECT id, name FROM destinations WHERE trip_id = %s", (TRIP_ID,)
)

for row in destinations:
    enrich_destination(row["id"], row["name"])

print(f"Done enriching trip {TRIP_ID}.")
