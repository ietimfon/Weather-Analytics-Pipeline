-- ============================================================
-- sql/schema.sql  — Star Schema DDL for Weather Analytics
-- ============================================================
-- ERD (text)
--
--   dim_location ──┐
--                  │  (location_id FK)
--   dim_time     ──┼──▶  fact_weather
--                  │  (time_id FK)
--
-- ============================================================

-- ── DIMENSION: Location ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_id   INTEGER PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    country       VARCHAR(100),
    latitude      DOUBLE,
    longitude     DOUBLE
);

-- ── DIMENSION: Time ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_time (
    time_id      INTEGER PRIMARY KEY,
    observed_at  TIMESTAMPTZ NOT NULL,
    date         DATE,
    year         INTEGER,
    month        INTEGER,
    month_name   VARCHAR(20),
    day          INTEGER,
    day_of_week  INTEGER,     -- 0=Monday … 6=Sunday
    day_name     VARCHAR(20),
    hour         INTEGER,
    quarter      INTEGER,
    is_weekend   INTEGER      -- 1 if Saturday/Sunday, else 0
);

-- ── FACT: Weather Observations ───────────────────────────────
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id              INTEGER PRIMARY KEY,
    time_id                 INTEGER REFERENCES dim_time(time_id),
    location_id             INTEGER REFERENCES dim_location(location_id),
    temperature_c           DOUBLE,           -- °C at 2 m height
    relative_humidity_pct   DOUBLE,           -- % 0–100
    precipitation_mm        DOUBLE,           -- mm in the hour
    wind_speed_kmh          DOUBLE,           -- km/h at 10 m height
    wind_direction_deg      DOUBLE,           -- degrees 0–360
    surface_pressure_hpa    DOUBLE,           -- hPa (hectopascals)
    cloud_cover_pct         DOUBLE,           -- % 0–100
    visibility_m            DOUBLE,           -- metres
    temp_range_c            DOUBLE,           -- daily max − min (from dim_daily)
    is_raining              INTEGER,          -- 1 if precipitation_mm > 0
    anomaly_flag            INTEGER,          -- 1 if any sensor reading out of range
    extracted_at            VARCHAR(30)       -- ISO-8601 UTC timestamp of API call
);

-- ── STAGING: Raw API payloads (ELT) ──────────────────────────
CREATE TABLE IF NOT EXISTS stg_weather_raw (
    stg_id          INTEGER,
    location_name   VARCHAR(100),
    country         VARCHAR(100),
    raw_payload     VARCHAR,           -- full JSON blob
    extracted_at    VARCHAR(30),
    loaded_at       TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ── Analytical views ──────────────────────────────────────────

-- Average temperature and precipitation by location and month
CREATE OR REPLACE VIEW vw_monthly_summary AS
SELECT
    l.location_name,
    l.country,
    t.year,
    t.month,
    t.month_name,
    ROUND(AVG(f.temperature_c)::NUMERIC,        2) AS avg_temp_c,
    ROUND(MIN(f.temperature_c)::NUMERIC,        2) AS min_temp_c,
    ROUND(MAX(f.temperature_c)::NUMERIC,        2) AS max_temp_c,
    ROUND(SUM(f.precipitation_mm)::NUMERIC,     2) AS total_precip_mm,
    ROUND(AVG(f.wind_speed_kmh)::NUMERIC,       2) AS avg_wind_kmh,
    ROUND(AVG(f.relative_humidity_pct)::NUMERIC,2) AS avg_humidity_pct,
    COUNT(*)                                        AS observation_count,
    SUM(f.anomaly_flag)                             AS anomaly_count
FROM   fact_weather  f
JOIN   dim_location  l ON f.location_id = l.location_id
JOIN   dim_time      t ON f.time_id     = t.time_id
GROUP  BY l.location_name, l.country, t.year, t.month, t.month_name
ORDER  BY l.location_name, t.year, t.month;

-- Hourly temperature comparison across all locations
CREATE OR REPLACE VIEW vw_hourly_temp_comparison AS
SELECT
    t.observed_at,
    t.date,
    t.hour,
    l.location_name,
    l.country,
    f.temperature_c,
    f.precipitation_mm,
    f.is_raining
FROM   fact_weather  f
JOIN   dim_location  l ON f.location_id = l.location_id
JOIN   dim_time      t ON f.time_id     = t.time_id
ORDER  BY t.observed_at, l.location_name;
