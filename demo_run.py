"""
demo_run.py — Runs the full pipeline with realistic mock data.
This simulates what happens when the Open-Meteo API is reachable.
Run:  python demo_run.py
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import random
from datetime import datetime, timedelta, timezone

# ── Generate realistic mock API responses ─────────────────────

def make_hourly(base_temp, n_hours=48):
    times = []
    temp, hum, precip, wind, wdir, press, cloud, vis = [], [], [], [], [], [], [], []
    start = datetime(2025, 6, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(n_hours):
        ts = start + timedelta(hours=i)
        times.append(ts.strftime("%Y-%m-%dT%H:%M"))
        temp.append(round(base_temp + random.uniform(-3, 3), 1))
        hum.append(round(random.uniform(60, 95), 1))
        precip.append(round(max(0, random.gauss(0.05, 0.15)), 2))
        wind.append(round(random.uniform(5, 25), 1))
        wdir.append(round(random.uniform(0, 360), 0))
        press.append(round(random.uniform(1005, 1015), 1))
        cloud.append(round(random.uniform(20, 90), 0))
        vis.append(round(random.uniform(8000, 12000), 0))
    return {
        "time": times, "temperature_2m": temp, "relative_humidity_2m": hum,
        "precipitation": precip, "wind_speed_10m": wind, "wind_direction_10m": wdir,
        "surface_pressure": press, "cloud_cover": cloud, "visibility": vis,
    }

def make_daily(base_max, base_min, n_days=2):
    times, mx, mn, precip_sum, ws_max, sunrise, sunset = [], [], [], [], [], [], []
    start = datetime(2025, 6, 12)
    for i in range(n_days):
        d = start + timedelta(days=i)
        times.append(d.strftime("%Y-%m-%d"))
        mx.append(round(base_max + random.uniform(-1, 1), 1))
        mn.append(round(base_min + random.uniform(-1, 1), 1))
        precip_sum.append(round(random.uniform(0, 5), 2))
        ws_max.append(round(random.uniform(15, 30), 1))
        sunrise.append(d.strftime("%Y-%m-%dT05:45"))
        sunset.append(d.strftime("%Y-%m-%dT18:30"))
    return {
        "time": times, "temperature_2m_max": mx, "temperature_2m_min": mn,
        "precipitation_sum": precip_sum, "wind_speed_10m_max": ws_max,
        "sunrise": sunrise, "sunset": sunset,
    }

MOCK_LOCATIONS = [
    {"name": "Lagos",    "lat":  6.52, "lon":  3.38, "country": "Nigeria",        "base_max": 33, "base_min": 25},
    {"name": "Accra",    "lat":  5.60, "lon": -0.19, "country": "Ghana",           "base_max": 31, "base_min": 24},
    {"name": "Abidjan",  "lat":  5.36, "lon": -4.01, "country": "Cote D'Ivoire",  "base_max": 32, "base_min": 24},
    {"name": "Windhoek", "lat":-22.56, "lon": 17.08, "country": "Namibia",        "base_max": 22, "base_min": 10},
    {"name": "London",   "lat": 51.51, "lon": -0.13, "country": "United Kingdom", "base_max": 18, "base_min": 12},
]

mock_records = []
for loc in MOCK_LOCATIONS:
    mock_records.append({
        "latitude":  loc["lat"],
        "longitude": loc["lon"],
        "hourly": make_hourly(loc["base_min"] + 4),
        "daily":  make_daily(loc["base_max"], loc["base_min"]),
        "_meta": {
            "location_name": loc["name"],
            "country":       loc["country"],
            "extracted_at":  "2025-06-12T00:05:00Z",
        }
    })

print(f"Generated {len(mock_records)} mock location records.")

# ── Run transform + validate + load ──────────────────────────

from src.transform.transformer import WeatherTransformer
from src.validate.validator    import DataValidator
from src.load.db_loader        import DatabaseLoader
from src.utils.logger          import get_logger

log = get_logger("demo_run")

log.info("=== DEMO RUN: Transform ===")
transformer = WeatherTransformer()
dim_location, dim_time, fact_weather = transformer.transform(mock_records)

log.info("=== DEMO RUN: Validate ===")
validator = DataValidator()
passed    = validator.validate_all(dim_location, dim_time, fact_weather)

log.info("=== DEMO RUN: Load → DuckDB ===")
loader = DatabaseLoader()
with loader.managed_connection():
    loader.create_schema()
    loader.load_dim_location(dim_location)
    loader.load_dim_time(dim_time)
    loader.load_fact_weather(fact_weather)

    print("\n" + "="*60)
    print("SAMPLE OUTPUT — dim_location")
    print("="*60)
    print(loader.sample_table("dim_location", 10).to_string(index=False))

    print("\n" + "="*60)
    print("SAMPLE OUTPUT — dim_time (5 rows)")
    print("="*60)
    print(loader.sample_table("dim_time", 5).to_string(index=False))

    print("\n" + "="*60)
    print("SAMPLE OUTPUT — fact_weather (10 rows)")
    print("="*60)
    fw = loader.sample_table("fact_weather", 10)
    print(fw.to_string(index=False))

    print("\n" + "="*60)
    print(f"TOTALS — fact_weather rows: {len(fact_weather)}  |  dim_time rows: {len(dim_time)}")
    print("="*60)

print("\n✓ DEMO RUN COMPLETE")
