# ============================================================
# tests/test_pipeline.py — Unit tests (pytest)
# ============================================================

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure project root is on path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from src.utils.helpers import (
    clean_column_names,
    parse_datetime_column,
    enforce_numeric,
    utc_now_str,
)
from src.validate.validator import DataValidator
from src.transform.transformer import WeatherTransformer


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def sample_api_response():
    """Minimal realistic Open-Meteo API response for one location."""
    return {
        "latitude":  6.5244,
        "longitude": 3.3792,
        "hourly": {
            "time":                 ["2025-06-01T00:00", "2025-06-01T01:00", "2025-06-01T02:00"],
            "temperature_2m":       [28.5, 27.9, 27.1],
            "relative_humidity_2m": [80,   82,   85],
            "precipitation":        [0.0,  0.2,  0.0],
            "wind_speed_10m":       [12.0, 11.5, 10.8],
            "wind_direction_10m":   [180,  185,  190],
            "surface_pressure":     [1010, 1009, 1010],
            "cloud_cover":          [60,   70,   75],
            "visibility":           [10000, 9500, 9000],
        },
        "daily": {
            "time":                 ["2025-06-01"],
            "temperature_2m_max":   [32.0],
            "temperature_2m_min":   [24.0],
            "precipitation_sum":    [0.2],
            "wind_speed_10m_max":   [18.0],
            "sunrise":              ["2025-06-01T05:30"],
            "sunset":               ["2025-06-01T18:45"],
        },
        "_meta": {
            "location_name": "Lagos",
            "country":       "Nigeria",
            "extracted_at":  "2025-06-01T00:05:00Z",
        },
    }


@pytest.fixture
def transformer():
    return WeatherTransformer()


@pytest.fixture
def validator():
    return DataValidator()


@pytest.fixture
def clean_dim_location():
    return pd.DataFrame({
        "location_id":   [1, 2],
        "location_name": ["Lagos", "Accra"],
        "country":       ["Nigeria", "Ghana"],
        "latitude":      [6.52, 5.60],
        "longitude":     [3.38, -0.19],
    })


@pytest.fixture
def clean_dim_time():
    return pd.DataFrame({
        "time_id":     [1, 2],
        "observed_at": pd.to_datetime(["2025-06-01 00:00+00:00", "2025-06-01 01:00+00:00"], utc=True),
        "date":        [pd.Timestamp("2025-06-01").date()] * 2,
        "year":        [2025, 2025],
        "month":       [6, 6],
        "month_name":  ["June", "June"],
        "day":         [1, 1],
        "day_of_week": [6, 6],
        "day_name":    ["Sunday", "Sunday"],
        "hour":        [0, 1],
        "quarter":     [2, 2],
        "is_weekend":  [1, 1],
    })


@pytest.fixture
def clean_fact_weather():
    return pd.DataFrame({
        "weather_id":            [1, 2],
        "time_id":               [1, 2],
        "location_id":           [1, 1],
        "temperature_c":         [28.5, 27.9],
        "relative_humidity_pct": [80.0, 82.0],
        "precipitation_mm":      [0.0, 0.2],
        "wind_speed_kmh":        [12.0, 11.5],
        "wind_direction_deg":    [180.0, 185.0],
        "surface_pressure_hpa":  [1010.0, 1009.0],
        "cloud_cover_pct":       [60.0, 70.0],
        "visibility_m":          [10000.0, 9500.0],
        "temp_range_c":          [8.0, 8.0],
        "is_raining":            [0, 1],
        "anomaly_flag":          [0, 0],
        "extracted_at":          ["2025-06-01T00:05:00Z"] * 2,
    })


# ── Utility tests ─────────────────────────────────────────────

class TestHelpers:
    def test_clean_column_names_lowercases(self):
        df = pd.DataFrame(columns=["Temperature_C", "Wind Speed", "HUMIDITY-PCT"])
        result = clean_column_names(df)
        assert list(result.columns) == ["temperature_c", "wind_speed", "humidity_pct"]

    def test_clean_column_names_strips_whitespace(self):
        df = pd.DataFrame(columns=["  col  ", "another col"])
        result = clean_column_names(df)
        assert "col" in result.columns
        assert "another_col" in result.columns

    def test_parse_datetime_column_valid(self):
        s = pd.Series(["2025-06-01T00:00", "2025-06-01T01:00"])
        result = parse_datetime_column(s)
        assert result.notna().all()
        assert pd.api.types.is_datetime64_any_dtype(result)

    def test_parse_datetime_column_invalid_returns_nat(self):
        s = pd.Series(["not-a-date", "also-bad"])
        result = parse_datetime_column(s)
        assert result.isna().all()

    def test_enforce_numeric_converts_strings(self):
        df = pd.DataFrame({"temp": ["28.5", "abc", "27.1"]})
        result = enforce_numeric(df, ["temp"])
        assert result["temp"].iloc[0] == pytest.approx(28.5)
        assert pd.isna(result["temp"].iloc[1])

    def test_utc_now_str_format(self):
        ts = utc_now_str()
        assert ts.endswith("Z")
        assert len(ts) == 20  # "YYYY-MM-DDTHH:MM:SSZ"


# ── Transformer tests ─────────────────────────────────────────

class TestWeatherTransformer:
    def test_transform_returns_three_dataframes(self, transformer, sample_api_response):
        dim_loc, dim_time, fact = transformer.transform([sample_api_response])
        assert isinstance(dim_loc,  pd.DataFrame)
        assert isinstance(dim_time, pd.DataFrame)
        assert isinstance(fact,     pd.DataFrame)

    def test_dim_location_has_required_columns(self, transformer, sample_api_response):
        dim_loc, _, _ = transformer.transform([sample_api_response])
        required = {"location_id", "location_name", "country", "latitude", "longitude"}
        assert required.issubset(set(dim_loc.columns))

    def test_dim_time_has_temporal_fields(self, transformer, sample_api_response):
        _, dim_time, _ = transformer.transform([sample_api_response])
        required = {"time_id", "observed_at", "year", "month", "day", "hour", "is_weekend"}
        assert required.issubset(set(dim_time.columns))

    def test_fact_weather_row_count_matches_hourly_records(self, transformer, sample_api_response):
        _, _, fact = transformer.transform([sample_api_response])
        n_hourly = len(sample_api_response["hourly"]["time"])
        assert len(fact) == n_hourly

    def test_is_raining_flag_set_correctly(self, transformer, sample_api_response):
        _, _, fact = transformer.transform([sample_api_response])
        # Row 1 has precipitation 0.2 → is_raining=1
        assert fact.loc[fact["precipitation_mm"] > 0, "is_raining"].all() == 1

    def test_no_duplicate_fact_rows(self, transformer, sample_api_response):
        _, _, fact = transformer.transform([sample_api_response])
        dupes = fact.duplicated(subset=["time_id", "location_id"]).sum()
        assert dupes == 0

    def test_empty_input_raises(self, transformer):
        with pytest.raises(RuntimeError):
            transformer.transform([])


# ── Validator tests ───────────────────────────────────────────

class TestDataValidator:
    def test_validate_all_passes_clean_data(
        self, validator, clean_dim_location, clean_dim_time, clean_fact_weather
    ):
        passed = validator.validate_all(clean_dim_location, clean_dim_time, clean_fact_weather)
        assert passed is True

    def test_validate_fact_fails_on_missing_column(self, validator, clean_fact_weather):
        bad = clean_fact_weather.drop(columns=["weather_id"])
        result = validator.validate_fact(bad)
        assert result.passed is False
        assert any("weather_id" in e for e in result.errors)

    def test_validate_fact_fails_on_null_foreign_key(self, validator, clean_fact_weather):
        bad = clean_fact_weather.copy()
        bad.loc[0, "location_id"] = None
        result = validator.validate_fact(bad)
        assert result.passed is False

    def test_validate_dim_location_fails_on_duplicate(self, validator, clean_dim_location):
        bad = pd.concat([clean_dim_location, clean_dim_location], ignore_index=True)
        result = validator.validate_dim_location(bad)
        assert result.passed is False

    def test_validate_dim_location_fails_bad_latitude(self, validator, clean_dim_location):
        bad = clean_dim_location.copy()
        bad.loc[0, "latitude"] = 200  # invalid
        result = validator.validate_dim_location(bad)
        assert any("latitude" in w for w in result.warnings)

    def test_validate_dim_time_passes_clean(self, validator, clean_dim_time):
        result = validator.validate_dim_time(clean_dim_time)
        assert result.passed is True


# ── Extractor tests (mocked) ──────────────────────────────────

class TestWeatherAPIExtractor:
    def test_extract_calls_api_for_each_location(self, sample_api_response):
        from src.extract.api_extractor import WeatherAPIExtractor

        with patch("requests.Session.get") as mock_get:
            mock_resp             = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                **sample_api_response,
                "latitude":  6.52,
                "longitude": 3.38,
            }
            mock_get.return_value = mock_resp

            extractor = WeatherAPIExtractor(
                locations=[{"name": "Lagos", "latitude": 6.52, "longitude": 3.38, "country": "Nigeria"}]
            )
            # Patch file save to avoid filesystem side-effects in tests
            with patch.object(extractor, "_save_raw"):
                results = extractor.extract_all()

            assert len(results) == 1
            assert results[0]["_meta"]["location_name"] == "Lagos"

    def test_extract_handles_api_error_gracefully(self):
        from src.extract.api_extractor import WeatherAPIExtractor
        import requests

        with patch("requests.Session.get", side_effect=requests.exceptions.ConnectionError("offline")):
            extractor = WeatherAPIExtractor(
                locations=[{"name": "Lagos", "latitude": 6.52, "longitude": 3.38, "country": "Nigeria"}]
            )
            with patch.object(extractor, "_save_raw"):
                results = extractor.extract_all()

            assert results == []   # graceful failure, no crash

    def test_validate_response_raises_on_bad_status(self):
        from src.extract.api_extractor import WeatherAPIExtractor
        import requests

        extractor = WeatherAPIExtractor()
        mock_resp             = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text        = "Internal Server Error"

        with pytest.raises(ValueError, match="HTTP 500"):
            extractor._validate_response(mock_resp, "TestCity")

    def test_validate_response_raises_on_missing_keys(self):
        from src.extract.api_extractor import WeatherAPIExtractor

        extractor = WeatherAPIExtractor()
        mock_resp             = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"latitude": 6.52}   # missing hourly, daily, longitude

        with pytest.raises(ValueError, match="missing keys"):
            extractor._validate_response(mock_resp, "TestCity")
