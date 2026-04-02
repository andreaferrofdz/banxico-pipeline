import argparse
import logging
import time
from datetime import datetime, timezone

from extract import run_extract
# from quality import run_quality          # TODO: uncomment when implemented
# from transform import run_gold, run_silver  # TODO: uncomment when implemented

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline_orchestration")


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
    logger.info("Step 1/4: extract → started")
    start = time.perf_counter()

    run_extract(mode, start_date, execution_date)

    logger.info("Step 1/4: extract → completed in %.1fs", time.perf_counter() - start)


def run_step_silver(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the Silver transformation step.

    Reads Bronze JSON payloads, applies cleaning and type casting,
    deduplicates by (serie_id, date), and writes Parquet to Silver.

    Not yet implemented — placeholder logs skipped and returns.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    logger.info("Step 2/4: silver → skipped (not implemented)")
    # TODO: implement when silver.py is ready


def run_step_gold(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the Gold aggregation step.

    Reads Silver Parquet, applies business-level aggregations and joins
    across series, and writes the final dataset consumed by Tableau Public.

    Not yet implemented — placeholder logs skipped and returns.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    logger.info("Step 3/4: gold → skipped (not implemented)")
    # TODO: implement when gold.py is ready


def run_step_quality(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Execute the data quality validation step.

    Runs Great Expectations suite against the Gold layer and publishes
    an SNS alert if any expectation fails. Runs last because Gold is
    the consumer-facing layer — Silver issues surface as Gold failures.

    Not yet implemented — placeholder logs skipped and returns.

    Parameters
    ----------
    mode : str
        'daily' or 'backfill'.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime
        Wall-clock time when the pipeline started.
    """
    logger.info("Step 4/4: quality → skipped (not implemented)")
    # TODO: implement when quality.py is ready


def run_pipeline(
    mode: str, start_date: str | None, execution_date: datetime
) -> None:
    """
    Orchestrate the full Banxico pipeline: extract → silver → gold → quality.

    Steps run sequentially. If any step raises an exception the pipeline
    stops immediately — subsequent steps are not executed.

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
    logger.info("Pipeline started | mode=%s | start_date=%s", mode, start_date or "N/A")
    start = time.perf_counter()

    run_step_extract(mode, start_date, execution_date)
    run_step_silver(mode, start_date, execution_date)
    run_step_gold(mode, start_date, execution_date)
    run_step_quality(mode, start_date, execution_date)

    logger.info("Pipeline completed in %.1fs", time.perf_counter() - start)


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