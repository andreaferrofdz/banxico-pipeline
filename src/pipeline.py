"""
Banxico Data Pipeline — Orchestrator
======================================
Coordinates the full pipeline execution: extract → silver → gold → quality.

Steps run sequentially. If any step raises an exception the pipeline stops
immediately, logs which step failed, and re-raises so Glue marks the job
as failed and SNS triggers the alert.

execution_date is created once and passed to all steps so S3 partition keys
are consistent across Bronze, Silver and Gold within a single pipeline run.

Usage
-----
Daily   : python3 src/pipeline.py
Backfill: python3 src/pipeline.py --mode backfill --start-date 2023-01-01
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from extract import run_extract
from transform import run_silver  # , run_gold  # TODO: uncomment when implemented

# from quality import run_quality              # TODO: uncomment when implemented

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline_orchestration")

# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def run_step_extract(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the Bronze extraction step.

    Calls run_extract which routes to daily or backfill logic based on mode.
    If this step fails the pipeline stops — downstream steps depend on Bronze data.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started. Shared across all steps
        so Bronze, Silver and Gold partitions are consistent within a run.
    """
    run_extract(mode, start_date, execution_date)


def run_step_silver(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the Silver transformation step.

    Reads Bronze JSON payloads, applies cleaning and type casting,
    deduplicates by (serie_id, date), and writes Parquet to Silver.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    run_silver(mode, execution_date)


def run_step_gold(mode: str, start_date: str | None, execution_date: datetime) -> None:
    """
    Execute the Gold aggregation step.

    Reads Silver Parquet, applies business-level aggregations and joins
    across series, and writes the final dataset consumed by Tableau Public.

    Not yet implemented — placeholder returns without action.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    # TODO: implement when gold.py is ready
    pass


def run_step_quality(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the data quality validation step.

    Runs Great Expectations suite against the Gold layer and publishes
    an SNS alert if any expectation fails. Runs last because Gold is
    the consumer-facing layer — Silver issues surface as Gold failures.

    Not yet implemented — placeholder returns without action.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    # TODO: implement when quality.py is ready
    pass


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_pipeline(mode: str, start_date: str | None, execution_date: datetime) -> None:
    """
    Orchestrate the full Banxico pipeline: extract → silver → gold → quality.

    Steps run sequentially. Logging and timing are centralized in the loop —
    step functions contain only business logic. If any step raises an exception
    the pipeline logs which step failed and re-raises — Glue marks the job as
    failed and SNS triggers the alert.

    execution_date is created once in the entry point and passed to all steps
    so S3 partition keys are consistent across Bronze, Silver and Gold
    within a single pipeline run.

    Parameters
    ----------
    mode : str
        'daily'    — extracts rolling window, processes latest data.
        'backfill' — iterates month by month from start_date to yesterday.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started. Created once in __main__
        and shared across all steps.
    """
    logger.info(
        "Pipeline started | mode=%s | start_date=%s",
        mode,
        start_date or "N/A",
    )
    pipeline_start = time.perf_counter()

    steps = [
        ("1/4", "extract", run_step_extract),
        ("2/4", "silver", run_step_silver),
        ("3/4", "gold", run_step_gold),
        ("4/4", "quality", run_step_quality),
    ]

    for step_num, step_name, step_fn in steps:
        logger.info("Step %s: %s → started", step_num, step_name)
        step_start = time.perf_counter()

        try:
            step_fn(mode, start_date, execution_date)
        except Exception as exc:
            # Log the failing step before re-raising so CloudWatch and Glue
            # logs show exactly where the pipeline broke.
            logger.error(
                "Pipeline failed at step '%s' after %.1fs | error: %s",
                step_name,
                time.perf_counter() - pipeline_start,
                exc,
            )
            raise

        logger.info(
            "Step %s: %s → completed in %.1fs",
            step_num,
            step_name,
            time.perf_counter() - step_start,
        )

    logger.info(
        "Pipeline completed in %.1fs",
        time.perf_counter() - pipeline_start,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Banxico data pipeline orchestrator — runs extract, silver, gold and quality steps."
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily: extracts rolling window per series. backfill: iterates month by month.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Backfill start date in YYYY-MM-DD format. Required when --mode=backfill.",
    )
    args = parser.parse_args()

    if args.mode == "backfill" and not args.start_date:
        parser.error("--start-date is required when --mode=backfill")

    run_pipeline(args.mode, args.start_date, datetime.now(tz=timezone.utc))
