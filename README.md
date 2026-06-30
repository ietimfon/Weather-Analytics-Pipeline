# Weather-Analytics-Pipeline
---

## Overview

A **production-style ETL and ELT pipeline** that extracts weather data from the [Open-Meteo API](https://open-meteo.com/), transforms and validates it, loads it into a star-schema SQL database (DuckDB by default, PostgreSQL optional), and runs automatically every day via Apache Airflow.

---

## Project Structure

```
weather_analytics/
├── config.py                      # Central configuration (API, DB, paths, thresholds)
├── pipeline.py                    # Top-level WeatherPipeline class + CLI entry point
├── demo_run.py                    # Demo runner using mock data (sandbox-safe)
│
├── src/
│   ├── extract/
│   │   └── api_extractor.py       # WeatherAPIExtractor — calls Open-Meteo API
│   ├── transform/
│   │   └── transformer.py         # WeatherTransformer — cleans & shapes data
│   ├── validate/
│   │   └── validator.py           # DataValidator — data quality checks
│   ├── load/
│   │   └── db_loader.py           # DatabaseLoader — ETL & ELT loads to DB
│   └── utils/
│       ├── logger.py              # Loguru-based structured logging
│       └── helpers.py             # Reusable utility functions
│
├── dags/
│   └── weather_analytics_dag.py   # Apache Airflow DAG (daily schedule)
│
├── sql/
│   └── schema.sql                 # Star schema DDL + analytical views
│
├── tests/
│   ├── conftest.py
│   └── test_pipeline.py           # 23 pytest unit tests
│
├── data/
│   ├── raw/                       # Raw JSON from API (per-day files)
│   └── staged/                    # Staged data for ELT
│
├── logs/
│   └── pipeline.log               # Rotating daily log, 30-day retention
│
├── requirements.txt
└── README.md
```

---

## Star Schema Design

```
         dim_location                   dim_time
    ┌──────────────────┐          ┌───────────────────────┐
    │ location_id (PK) │          │ time_id (PK)          │
    │ location_name    │          │ observed_at (UTC)      │
    │ country          │          │ date, year, month      │
    │ latitude         │          │ month_name, day        │
    │ longitude        │          │ day_of_week, day_name  │
    └────────┬─────────┘          │ hour, quarter          │
             │                    │ is_weekend             │
             │ (location_id FK)   └──────────┬────────────┘
             │                               │ (time_id FK)
             └──────────────┬────────────────┘
                            ▼
                      fact_weather
              ┌──────────────────────────────┐
              │ weather_id (PK)              │
              │ time_id (FK)                 │
              │ location_id (FK)             │
              │ temperature_c                │
              │ relative_humidity_pct        │
              │ precipitation_mm             │
              │ wind_speed_kmh               │
              │ wind_direction_deg           │
              │ surface_pressure_hpa         │
              │ cloud_cover_pct              │
              │ visibility_m                 │
              │ temp_range_c (derived)       │
              │ is_raining (derived)         │
              │ anomaly_flag (QA)            │
              │ extracted_at                 │
              └──────────────────────────────┘

              + stg_weather_raw  (ELT staging table)
```

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/<your-username>/weather_analytics.git
cd weather_analytics
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and set values:

```bash
# Database
DB_TYPE=duckdb              # or postgresql
DB_PATH=weather_analytics.duckdb

# For PostgreSQL
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=weather_db
# DB_USER=postgres
# DB_PASSWORD=yourpassword

LOG_LEVEL=INFO
```

### 3. Run the pipeline (ETL mode)

```bash
python pipeline.py --mode etl
```

Available modes:

| Flag | Description |
|------|-------------|
| `--mode etl` | Extract → Transform → Validate → Load |
| `--mode elt` | Extract → Stage raw → In-DB transform → Load |
| `--mode both` | Run ETL then ELT |

### 4. Run with mock data (offline demo)

```bash
python demo_run.py
```

---

## Running Tests

```bash
pytest tests/ -v
```

Expected output: **23 passed**.

Tests cover:
- `TestHelpers` — utility functions
- `TestWeatherTransformer` — all transformation logic
- `TestDataValidator` — all validation rules
- `TestWeatherAPIExtractor` — API calls (mocked with `pytest-mock`)

---

## Airflow Automation

### Install and configure

```bash
export AIRFLOW_HOME=~/airflow
pip install apache-airflow==2.8.1

airflow db init
airflow users create --username admin --password admin \
    --firstname Ime --lastname Eti-mfon \
    --role Admin --email ime@zilstack.com

# Copy DAG
cp dags/weather_analytics_dag.py ~/airflow/dags/
```

### Start Airflow

```bash
airflow scheduler &
airflow webserver --port 8080
```

Visit `http://localhost:8080`, enable `weather_analytics_pipeline`, and trigger a run.

### DAG Task Flow

```
start
  └── extract_weather
        └── validate_raw
              └── transform_weather
                    └── validate_transformed
                          └── load_dimensions
                                └── load_fact_weather
                                      └── elt_stage_raw
                                            └── elt_indb_transform
                                                  └── pipeline_health_check
                                                        └── end
```

**Schedule:** `@daily` (midnight UTC)
**Retries:** 2 attempts with 5-minute delay
**Max active runs:** 1 (prevents overlapping runs)

---

## ETL vs ELT — Design Decisions

| Aspect | ETL | ELT |
|---|---|---|
| Transform location | Python (in-memory) | Staging table → SQL |
| When to use | Clean API data with known schema | Large volumes / raw archiving needed |
| Traceability | Raw JSON saved to disk | Raw JSON persisted in `stg_weather_raw` |
| Re-processing | Re-run pipeline | Re-run SQL transforms on existing staged data |

Both workflows are implemented and toggled via `--mode`.

---

## Data Quality & Validation

The `DataValidator` class enforces:

- Required column presence
- No NULL values in primary/foreign keys
- No duplicate composite keys
- Numeric range checks (temperature −90 to 60°C, humidity 0–100%, etc.)
- Anomaly flagging written to `fact_weather.anomaly_flag`

---

## Software Engineering Highlights

| Requirement | Implementation |
|---|---|
| Modular code | 5 separate modules (extract, transform, load, validate, utils) |
| Class-based pipeline | `WeatherPipeline`, `WeatherAPIExtractor`, `WeatherTransformer`, `DataValidator`, `DatabaseLoader` |
| Logging | Loguru — console + rotating file, structured format |
| Exception handling | Try/catch at every API, DB, and transform boundary |
| Unit tests | 23 pytest tests with fixtures and mocking |
| PEP 8 | Consistent style throughout |
| Documentation | Docstrings on all classes and public methods |
| GitHub-ready | Full repo structure with `.env` support |

---

## Analytical Views

Two SQL views are created alongside the schema:

```sql
-- Average/min/max temperature & precipitation by location and month
SELECT * FROM vw_monthly_summary;

-- Hourly temperature comparison across all locations
SELECT * FROM vw_hourly_temp_comparison;
```

---

## Sample Database Output

```
dim_location (5 rows)
 location_id  location_name         country  latitude  longitude
           1          Lagos         Nigeria      6.52       3.38
           2          Accra           Ghana      5.60      -0.19
           3        Abidjan   Cote D'Ivoire      5.36      -4.01
           4       Windhoek         Namibia    -22.56      17.08
           5         London  United Kingdom     51.51      -0.13

fact_weather (240 rows across 5 locations × 48 hours)
 weather_id  time_id  location_id  temperature_c  precipitation_mm  wind_speed_kmh  is_raining  anomaly_flag
          1        1            1           28.0              0.06            12.6           1             0
          2        2            1           30.4              0.00            10.4           0             0
        ...
```

---

## Locations Covered

| Location | Country | Lat | Lon |
|---|---|---|---|
| Lagos | Nigeria | 6.52 | 3.38 |
| Accra | Ghana | 5.60 | -0.19 |
| Abidjan | Côte d'Ivoire | 5.36 | -4.01 |
| Windhoek | Namibia | -22.56 | 17.08 |
| London | United Kingdom | 51.51 | -0.13 |

---

## Author

**Ime Eti-mfon** | Data Scientist & Engineer  
Founder, [Zilstack](https://zilstack.com) - Full-Stack Data Solutions  

Lagos, Nigeria

