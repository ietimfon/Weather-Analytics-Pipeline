# ============================================================
# dags/weather_analytics_dag.py — Apache Airflow DAG
# ============================================================
#
# Task dependency graph:
#
#   extract_weather
#        │
#   validate_raw
#        │
#   transform_weather
#        │
#   validate_transformed
#        │
#   load_dimensions ─────┐
#        │               │
#   load_fact_weather ◄──┘
#        │
#   elt_stage_raw
#        │
#   elt_indb_transform
#        │
#   pipeline_health_check
#
# ============================================================

from __future__ import annotations

import json
import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty  import EmptyOperator

# Add project root to path so Airflow workers can import our modules
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

log = logging.getLogger("airflow.task")

# ── Default args ──────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "ime_eti_mfon",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "start_date":       datetime(2025, 1, 1),
}

# ── Python callables ──────────────────────────────────────────

def task_extract(**context):
    """Pull weather data from Open-Meteo for all configured locations."""
    from src.extract.api_extractor import WeatherAPIExtractor
    extractor   = WeatherAPIExtractor()
    raw_records = extractor.extract_all()

    if not raw_records:
        raise ValueError("Extraction returned 0 records.")

    # Push to XCom as JSON for downstream tasks
    context["ti"].xcom_push(key="raw_records", value=json.dumps(raw_records, default=str))
    log.info(f"Extracted {len(raw_records)} location record(s).")


def task_validate_raw(**context):
    """Basic sanity check on raw API payloads."""
    raw_json    = context["ti"].xcom_pull(key="raw_records", task_ids="extract_weather")
    raw_records = json.loads(raw_json)

    for rec in raw_records:
        assert "hourly" in rec, f"Missing 'hourly' key in record: {rec.get('_meta')}"
        assert "daily"  in rec, f"Missing 'daily' key in record: {rec.get('_meta')}"

    log.info(f"Raw validation passed for {len(raw_records)} record(s).")


def task_transform(**context):
    """Clean and shape raw data into star-schema DataFrames."""
    import pandas as pd
    from src.transform.transformer import WeatherTransformer

    raw_json    = context["ti"].xcom_pull(key="raw_records", task_ids="extract_weather")
    raw_records = json.loads(raw_json)

    transformer = WeatherTransformer()
    dim_loc, dim_time, fact = transformer.transform(raw_records)

    # Push DataFrames as JSON records
    context["ti"].xcom_push(key="dim_location",  value=dim_loc.to_json(orient="records", date_format="iso"))
    context["ti"].xcom_push(key="dim_time",       value=dim_time.to_json(orient="records", date_format="iso"))
    context["ti"].xcom_push(key="fact_weather",   value=fact.to_json(orient="records",    date_format="iso"))
    log.info(f"Transformation complete. fact_weather rows: {len(fact)}")


def task_validate_transformed(**context):
    """Run DataValidator against all three DataFrames."""
    import pandas as pd
    from src.validate.validator import DataValidator

    ti = context["ti"]

    dim_loc  = pd.read_json(ti.xcom_pull(key="dim_location",  task_ids="transform_weather"), orient="records")
    dim_time = pd.read_json(ti.xcom_pull(key="dim_time",      task_ids="transform_weather"), orient="records")
    fact     = pd.read_json(ti.xcom_pull(key="fact_weather",  task_ids="transform_weather"), orient="records")

    validator = DataValidator()
    passed    = validator.validate_all(dim_loc, dim_time, fact)

    if not passed:
        log.warning("Validation completed with issues — check validator logs.")
    else:
        log.info("All validations passed ✓")


def task_load_dimensions(**context):
    """Load dim_location and dim_time into the warehouse."""
    import pandas as pd
    from src.load.db_loader import DatabaseLoader

    ti       = context["ti"]
    dim_loc  = pd.read_json(ti.xcom_pull(key="dim_location", task_ids="transform_weather"), orient="records")
    dim_time = pd.read_json(ti.xcom_pull(key="dim_time",     task_ids="transform_weather"), orient="records")

    loader = DatabaseLoader()
    with loader.managed_connection():
        loader.create_schema()
        loader.load_dim_location(dim_loc)
        loader.load_dim_time(dim_time)

    log.info("Dimension tables loaded.")


def task_load_fact(**context):
    """Load fact_weather into the warehouse."""
    import pandas as pd
    from src.load.db_loader import DatabaseLoader

    ti   = context["ti"]
    fact = pd.read_json(ti.xcom_pull(key="fact_weather", task_ids="transform_weather"), orient="records")

    loader = DatabaseLoader()
    with loader.managed_connection():
        loader.load_fact_weather(fact)

    log.info(f"fact_weather loaded: {len(fact)} rows.")


def task_elt_stage(**context):
    """ELT Step: load raw payloads into the staging table."""
    from src.load.db_loader import DatabaseLoader

    raw_json    = context["ti"].xcom_pull(key="raw_records", task_ids="extract_weather")
    raw_records = json.loads(raw_json)

    loader = DatabaseLoader()
    with loader.managed_connection():
        loader.create_schema()
        loader.load_staging(raw_records)

    log.info("Raw records staged for ELT.")


def task_elt_indb_transform(**context):
    """ELT Step: trigger in-database transformation from staging."""
    from src.load.db_loader import DatabaseLoader

    loader = DatabaseLoader()
    with loader.managed_connection():
        loader.run_elt_transform()

    log.info("ELT in-database transform step complete.")


def task_health_check(**context):
    """Final task: confirm all tables have data."""
    from src.load.db_loader import DatabaseLoader

    loader = DatabaseLoader()
    with loader.managed_connection():
        for table in ("dim_location", "dim_time", "fact_weather", "stg_weather_raw"):
            sample = loader.sample_table(table, n=1)
            status = "✓" if not sample.empty else "✗ (empty)"
            log.info(f"  {table}: {status}")


# ── DAG definition ────────────────────────────────────────────

with DAG(
    dag_id="weather_analytics_pipeline",
    description="Daily ETL + ELT pipeline: Open-Meteo API → Star Schema (DuckDB/Postgres)",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",          # runs at midnight UTC every day
    catchup=False,
    max_active_runs=1,
    tags=["weather", "etl", "elt", "zilstack"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=task_extract,
    )

    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=task_validate_raw,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=task_transform,
    )

    validate_transformed = PythonOperator(
        task_id="validate_transformed",
        python_callable=task_validate_transformed,
    )

    load_dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    load_fact = PythonOperator(
        task_id="load_fact_weather",
        python_callable=task_load_fact,
    )

    elt_stage = PythonOperator(
        task_id="elt_stage_raw",
        python_callable=task_elt_stage,
    )

    elt_transform = PythonOperator(
        task_id="elt_indb_transform",
        python_callable=task_elt_indb_transform,
    )

    health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=task_health_check,
    )

    end = EmptyOperator(task_id="end")

    # ── Task dependencies ─────────────────────────────────────
    (
        start
        >> extract
        >> validate_raw
        >> transform
        >> validate_transformed
        >> load_dims
        >> load_fact
        >> elt_stage
        >> elt_transform
        >> health_check
        >> end
    )
