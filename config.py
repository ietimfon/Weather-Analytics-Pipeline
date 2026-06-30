# ============================================================
# config.py — Central configuration for Weather Analytics Pipeline
# ============================================================

import os
from dotenv import load_dotenv

load_dotenv()

# ── API ──────────────────────────────────────────────────────
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Default locations to collect weather data for
LOCATIONS = [
    {"name": "Lagos",    "latitude": 6.5244,  "longitude": 3.3792,  "country": "Nigeria"},
    {"name": "Accra",    "latitude": 5.6037,  "longitude": -0.1870, "country": "Ghana"},
    {"name": "Abidjan",  "latitude": 5.3600,  "longitude": -4.0083, "country": "Cote d'Ivoire"},
    {"name": "Windhoek", "latitude": -22.5597, "longitude": 17.0832, "country": "Namibia"},
    {"name": "London",   "latitude": 51.5074, "longitude": -0.1278, "country": "United Kingdom"},
]

# Hourly variables to extract
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
    "cloud_cover",
    "visibility",
]

# Daily variables to extract
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "sunrise",
    "sunset",
]

API_PARAMS_BASE = {
    "hourly": ",".join(HOURLY_VARIABLES),
    "daily":  ",".join(DAILY_VARIABLES),
    "timezone": "auto",
    "past_days": 1,
    "forecast_days": 1,
}

API_TIMEOUT = 30  # seconds

# ── Database ─────────────────────────────────────────────────
DB_TYPE     = os.getenv("DB_TYPE", "duckdb")          # duckdb | postgresql
DB_PATH     = os.getenv("DB_PATH", "weather_analytics.duckdb")  # for DuckDB
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "weather_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
LOG_DIR        = os.path.join(BASE_DIR, "logs")
RAW_DATA_DIR   = os.path.join(BASE_DIR, "data", "raw")
STAGE_DATA_DIR = os.path.join(BASE_DIR, "data", "staged")

os.makedirs(LOG_DIR,        exist_ok=True)
os.makedirs(RAW_DATA_DIR,   exist_ok=True)
os.makedirs(STAGE_DATA_DIR, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE   = os.path.join(LOG_DIR, "pipeline.log")

# ── Validation thresholds ─────────────────────────────────────
TEMP_MIN_C   = -90.0
TEMP_MAX_C   =  60.0
HUMIDITY_MIN =   0.0
HUMIDITY_MAX = 100.0
WIND_MAX_KMH = 400.0
PRESSURE_MIN = 800.0
PRESSURE_MAX = 1100.0
