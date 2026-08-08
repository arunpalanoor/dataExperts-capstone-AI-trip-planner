"""
Databricks Job: refresh hourly weather + air quality forecasts.

Widget parameter: trip_id (optional)
  - Empty (default): refresh every destination belonging to any active trip
    (trips that haven't ended yet). Run this on a daily Databricks Jobs
    schedule - configure the schedule in the Jobs UI, not in this script.
  - Set: refresh only that trip's destinations. The agent triggers this
    immediately after enrich_trip.py finishes for a newly created/edited
    trip, so it has weather right away instead of waiting for the next
    daily run. Must run AFTER enrich_trip.py for that trip - it depends on
    enrich_trip.py having already geocoded the destinations (lat/lon).

Decoupling the daily-all-trips refresh from the per-edit single-trip refresh
keeps Open-Meteo call volume down regardless of how often trips get edited,
while still giving new trips immediate data.

This is the job that satisfies the capstone's Spark data pipeline
requirement: it fans the per-destination Open-Meteo Weather + Air Quality
API calls out across the cluster with mapInPandas, and flattens each
destination's multi-day hourly forecast into one row per hour. The active
destination list itself comes from a plain psycopg2 query (lakebase.py) -
that list is small; the hourly forecast rows it expands into are not.

Note: pollen fields are only populated for European destinations (Open-Meteo
sources pollen from CAMS' European forecast) - null elsewhere, which the
weather_snapshots schema already allows.
"""

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

import lakebase

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

OUTPUT_SCHEMA = StructType([
    StructField("destination_id", IntegerType()),
    StructField("forecast_time", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("precipitation_probability", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("weathercode", IntegerType()),
    StructField("us_aqi", DoubleType()),
    StructField("pm2_5", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("uv_index", DoubleType()),
    StructField("pollen_grass", DoubleType()),
    StructField("pollen_tree", DoubleType()),
    StructField("pollen_weed", DoubleType()),
])
OUTPUT_COLUMNS = [f.name for f in OUTPUT_SCHEMA.fields]


def fetch_weather_for_destination(destination_id: int, lat: float, lon: float,
                                   start_date: str, end_date: str) -> list[dict]:
    weather_resp = requests.get(
        WEATHER_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation_probability,precipitation,wind_speed_10m,weather_code",
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "UTC",
        },
        timeout=15,
    )
    weather_resp.raise_for_status()
    weather = weather_resp.json().get("hourly", {})
    times = weather.get("time", [])

    aqi_resp = requests.get(
        AIR_QUALITY_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "us_aqi,pm2_5,pm10,uv_index,grass_pollen,birch_pollen,ragweed_pollen",
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "UTC",
        },
        timeout=15,
    )
    aqi_resp.raise_for_status()
    aqi = aqi_resp.json().get("hourly", {})

    def value_at(series: dict, key: str, i: int):
        values = series.get(key, [])
        return values[i] if i < len(values) else None

    rows = []
    for i, t in enumerate(times):
        rows.append({
            "destination_id": destination_id,
            "forecast_time": f"{t}+00:00",
            "temperature_c": value_at(weather, "temperature_2m", i),
            "precipitation_probability": value_at(weather, "precipitation_probability", i),
            "precipitation_mm": value_at(weather, "precipitation", i),
            "wind_speed_kmh": value_at(weather, "wind_speed_10m", i),
            "weathercode": value_at(weather, "weather_code", i),
            "us_aqi": value_at(aqi, "us_aqi", i),
            "pm2_5": value_at(aqi, "pm2_5", i),
            "pm10": value_at(aqi, "pm10", i),
            "uv_index": value_at(aqi, "uv_index", i),
            "pollen_grass": value_at(aqi, "grass_pollen", i),
            "pollen_tree": value_at(aqi, "birch_pollen", i),
            "pollen_weed": value_at(aqi, "ragweed_pollen", i),
        })
    return rows


def fetch_partition(pdf_iterator):
    for pdf in pdf_iterator:
        rows = []
        for _, row in pdf.iterrows():
            rows.extend(fetch_weather_for_destination(
                row["destination_id"], row["latitude"], row["longitude"],
                row["start_date"], row["end_date"],
            ))
        yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def upsert_weather_row(row: dict) -> None:
    lakebase.run_write(
        """
        INSERT INTO weather_snapshots (
            destination_id, forecast_time, temperature_c, precipitation_probability,
            precipitation_mm, wind_speed_kmh, weathercode, us_aqi, pm2_5, pm10,
            uv_index, pollen_grass, pollen_tree, pollen_weed
        ) VALUES (
            %(destination_id)s, %(forecast_time)s, %(temperature_c)s, %(precipitation_probability)s,
            %(precipitation_mm)s, %(wind_speed_kmh)s, %(weathercode)s, %(us_aqi)s, %(pm2_5)s, %(pm10)s,
            %(uv_index)s, %(pollen_grass)s, %(pollen_tree)s, %(pollen_weed)s
        )
        ON CONFLICT (destination_id, forecast_time) DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            precipitation_probability = EXCLUDED.precipitation_probability,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            weathercode = EXCLUDED.weathercode,
            us_aqi = EXCLUDED.us_aqi,
            pm2_5 = EXCLUDED.pm2_5,
            pm10 = EXCLUDED.pm10,
            uv_index = EXCLUDED.uv_index,
            pollen_grass = EXCLUDED.pollen_grass,
            pollen_tree = EXCLUDED.pollen_tree,
            pollen_weed = EXCLUDED.pollen_weed,
            fetched_at = now()
        """,
        row,
    )


dbutils.widgets.text("trip_id", "")
TRIP_ID_PARAM = dbutils.widgets.get("trip_id").strip()

if TRIP_ID_PARAM:
    active_destinations = lakebase.run_query(
        """
        SELECT d.id AS destination_id, d.latitude, d.longitude, t.start_date, t.end_date
        FROM destinations d
        JOIN trips t ON t.id = d.trip_id
        WHERE t.id = %s
          AND t.status != 'cancelled'
          AND d.latitude IS NOT NULL
          AND d.longitude IS NOT NULL
        """,
        (int(TRIP_ID_PARAM),),
    )
else:
    active_destinations = lakebase.run_query(
        """
        SELECT d.id AS destination_id, d.latitude, d.longitude, t.start_date, t.end_date
        FROM destinations d
        JOIN trips t ON t.id = d.trip_id
        WHERE t.end_date >= CURRENT_DATE
          AND t.status != 'cancelled'
          AND d.latitude IS NOT NULL
          AND d.longitude IS NOT NULL
        """
    )

if not active_destinations:
    print("No active destinations to refresh.")
else:
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame(
        [
            (
                r["destination_id"], r["latitude"], r["longitude"],
                r["start_date"].isoformat(), r["end_date"].isoformat(),
            )
            for r in active_destinations
        ],
        schema="destination_id int, latitude double, longitude double, start_date string, end_date string",
    )

    forecast_df = input_df.mapInPandas(fetch_partition, schema=OUTPUT_SCHEMA)

    row_count = 0
    for row in forecast_df.collect():
        upsert_weather_row(row.asDict())
        row_count += 1

    print(f"Upserted {row_count} weather snapshot rows across {len(active_destinations)} destinations.")
