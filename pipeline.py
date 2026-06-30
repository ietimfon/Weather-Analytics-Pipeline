# ============================================================
# pipeline.py — Top-level reusable pipeline class
# ============================================================

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.extract.api_extractor import WeatherAPIExtractor
from src.transform.transformer import WeatherTransformer
from src.validate.validator    import DataValidator
from src.load.db_loader        import DatabaseLoader
from src.utils.logger          import get_logger

log = get_logger("pipeline")


class WeatherPipeline:
    """
    Orchestrates the full ETL and ELT pipeline for weather analytics.

    Usage
    -----
    >>> pipeline = WeatherPipeline()
    >>> pipeline.run_etl()      # Extract → Transform → Validate → Load
    >>> pipeline.run_elt()      # Extract → Stage raw → In-DB transform → Load
    """

    def __init__(self):
        self.extractor   = WeatherAPIExtractor()
        self.transformer = WeatherTransformer()
        self.validator   = DataValidator()
        self.loader      = DatabaseLoader()

    # ── ETL ───────────────────────────────────────────────────

    def run_etl(self) -> bool:
        """
        Classic ETL: data is transformed in Python before loading.
        Returns True on success, False on any critical failure.
        """
        log.info("=" * 60)
        log.info("STARTING ETL PIPELINE")
        log.info("=" * 60)

        try:
            # 1. Extract
            log.info("Step 1/4 — Extract")
            raw_records = self.extractor.extract_all()
            if not raw_records:
                log.error("Extraction returned no records. Aborting.")
                return False

            # 2. Transform
            log.info("Step 2/4 — Transform")
            dim_location, dim_time, fact_weather = self.transformer.transform(raw_records)

            # 3. Validate
            log.info("Step 3/4 — Validate")
            passed = self.validator.validate_all(dim_location, dim_time, fact_weather)
            if not passed:
                log.warning("Validation issues detected — proceeding with load (check warnings above).")

            # 4. Load
            log.info("Step 4/4 — Load")
            with self.loader.managed_connection():
                self.loader.create_schema()
                self.loader.load_dim_location(dim_location)
                self.loader.load_dim_time(dim_time)
                self.loader.load_fact_weather(fact_weather)

                # Print sample output
                log.info("Sample — fact_weather (5 rows):")
                sample = self.loader.sample_table("fact_weather", 5)
                log.info(f"\n{sample.to_string(index=False)}")

            log.info("ETL PIPELINE COMPLETED SUCCESSFULLY ✓")
            return True

        except Exception as exc:
            log.error(f"ETL PIPELINE FAILED: {exc}", exc_info=True)
            return False

    # ── ELT ───────────────────────────────────────────────────

    def run_elt(self) -> bool:
        """
        ELT workflow:
          1. Extract raw data from the API.
          2. Load raw data into a staging table.
          3. Transform the staged data inside the pipeline (SQL-based).
          4. Load the final analytical tables.
        """
        log.info("=" * 60)
        log.info("STARTING ELT PIPELINE")
        log.info("=" * 60)

        try:
            # 1. Extract
            log.info("Step 1/4 — Extract raw data")
            raw_records = self.extractor.extract_all()
            if not raw_records:
                log.error("Extraction returned no records. Aborting ELT.")
                return False

            # 2. Load raw into staging
            log.info("Step 2/4 — Load raw into staging table")
            with self.loader.managed_connection():
                self.loader.create_schema()
                self.loader.load_staging(raw_records)

                # 3. In-DB transform
                log.info("Step 3/4 — In-database transform")
                self.loader.run_elt_transform()

                # 4. Load final tables (reuse Python transform for this implementation)
                log.info("Step 4/4 — Load analytical tables")
                dim_location, dim_time, fact_weather = self.transformer.transform(raw_records)
                passed = self.validator.validate_all(dim_location, dim_time, fact_weather)
                if not passed:
                    log.warning("Validation issues — proceeding anyway.")

                self.loader.load_dim_location(dim_location)
                self.loader.load_dim_time(dim_time)
                self.loader.load_fact_weather(fact_weather)

            log.info("ELT PIPELINE COMPLETED SUCCESSFULLY ✓")
            return True

        except Exception as exc:
            log.error(f"ELT PIPELINE FAILED: {exc}", exc_info=True)
            return False


# ── CLI entry point ───────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Weather Analytics Pipeline")
    parser.add_argument(
        "--mode",
        choices=["etl", "elt", "both"],
        default="etl",
        help="Pipeline mode to run (default: etl)",
    )
    args = parser.parse_args()

    p = WeatherPipeline()

    if args.mode in ("etl", "both"):
        p.run_etl()
    if args.mode in ("elt", "both"):
        p.run_elt()
