-- Lakebase (Postgres) schema for the AI Trip and Outdoor Activity Planner.
-- Apply with: psql "$LAKEBASE_URL" -f sql/schema.sql
-- (or via lakebase.py's get_connection()/run_write() from a notebook/script)

CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------------
-- Core tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    display_name  TEXT,
    preferences   TEXT,                 -- free text: interests, constraints, allergies
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trips (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    start_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    status      TEXT NOT NULL DEFAULT 'planning'
                CHECK (status IN ('planning', 'confirmed', 'completed', 'cancelled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trips_user_id ON trips(user_id);

-- A trip can include multiple stops/destinations (multi-city trips).
CREATE TABLE IF NOT EXISTS destinations (
    id                    SERIAL PRIMARY KEY,
    trip_id               INTEGER NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    name                  TEXT NOT NULL,          -- as typed by the user
    canonical_name        TEXT,                   -- resolved name from geocoding
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    country               TEXT,
    timezone              TEXT,
    description           TEXT,                   -- Wikimedia extract
    wikimedia_page_title  TEXT,
    wikimedia_url         TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_destinations_trip_id ON destinations(trip_id);

CREATE TABLE IF NOT EXISTS activities (
    id                    SERIAL PRIMARY KEY,
    destination_id        INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    name                  TEXT NOT NULL,
    category              TEXT,                   -- e.g. hiking, museum, food, photography
    is_outdoor            BOOLEAN NOT NULL DEFAULT TRUE,
    air_quality_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    description           TEXT,                   -- Wikimedia nearby-attraction extract
    source                TEXT NOT NULL DEFAULT 'wikimedia'
                          CHECK (source IN ('wikimedia', 'user_added', 'agent_added')),
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    wikimedia_page_title  TEXT,
    wikimedia_url         TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_activities_destination_id ON activities(destination_id);

CREATE TABLE IF NOT EXISTS itinerary_items (
    id           SERIAL PRIMARY KEY,
    trip_id      INTEGER NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    activity_id  INTEGER REFERENCES activities(id) ON DELETE SET NULL,
    day_date     DATE NOT NULL,
    start_time   TIME,
    end_time     TIME,
    position     INTEGER NOT NULL DEFAULT 0,   -- ordering within the day
    status       TEXT NOT NULL DEFAULT 'scheduled'
                 CHECK (status IN ('scheduled', 'rescheduled', 'completed', 'cancelled')),
    notes        TEXT,                          -- e.g. agent's reasoning for placement/rescheduling
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_itinerary_items_trip_day ON itinerary_items(trip_id, day_date);

CREATE TABLE IF NOT EXISTS weather_snapshots (
    id                       SERIAL PRIMARY KEY,
    destination_id           INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    forecast_time            TIMESTAMPTZ NOT NULL,
    temperature_c            NUMERIC,
    precipitation_probability NUMERIC,   -- percent, 0-100
    precipitation_mm         NUMERIC,
    wind_speed_kmh           NUMERIC,
    weathercode              SMALLINT,
    us_aqi                   NUMERIC,
    pm2_5                    NUMERIC,
    pm10                     NUMERIC,
    uv_index                 NUMERIC,
    pollen_grass             NUMERIC,
    pollen_tree              NUMERIC,
    pollen_weed              NUMERIC,
    fetched_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (destination_id, forecast_time)
);

CREATE INDEX IF NOT EXISTS idx_weather_snapshots_destination_time ON weather_snapshots(destination_id, forecast_time);

CREATE TABLE IF NOT EXISTS packing_items (
    id          SERIAL PRIMARY KEY,
    trip_id     INTEGER NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    item_name   TEXT NOT NULL,
    reason      TEXT,          -- agent's explanation, e.g. "rain expected day 2"
    category    TEXT,          -- clothing, gear, health, documents, ...
    packed      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_packing_items_trip_id ON packing_items(trip_id);

-- ---------------------------------------------------------------------------
-- Embeddings (context engineering: destination descriptions, attraction info,
-- activity requirements, user notes -> sentence-transformers/all-MiniLM-L6-v2,
-- 384 dimensions, same model as the Day 3 reference repo)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS destination_embeddings (
    id              SERIAL PRIMARY KEY,
    destination_id  INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    embedding       vector(384) NOT NULL,
    model_name      TEXT NOT NULL DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (destination_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_destination_embeddings_vector
    ON destination_embeddings USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS activity_embeddings (
    id            SERIAL PRIMARY KEY,
    activity_id   INTEGER NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    embedding     vector(384) NOT NULL,
    model_name    TEXT NOT NULL DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (activity_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_activity_embeddings_vector
    ON activity_embeddings USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS user_note_embeddings (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    embedding   vector(384) NOT NULL,
    model_name  TEXT NOT NULL DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_user_note_embeddings_vector
    ON user_note_embeddings USING hnsw (embedding vector_cosine_ops);
