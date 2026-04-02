"""
Banxico SIE API — Bronze Layer Extractor
=========================================
Fetches raw financial series from Banxico and deposits them as JSON
into the Bronze layer of the data lake following an auditable,
immutable partition structure.

Partition structure
-------------------
Daily:
  bronze/source=banxico/dataset=<name>/extraction_type=daily/
    execution_date=<YYYY-MM-DD>/payload.json

Backfill:
  bronze/source=banxico/dataset=<name>/extraction_type=backfill/
    execution_date=<YYYY-MM-DD>/range_start=<YYYY-MM-DD>/range_end=<YYYY-MM-DD>/payload.json

execution_date always reflects when the pipeline ran, not the date
range of the data inside the file.

Daily runs are idempotent — re-running the same day overwrites the file
with identical data. Backfill runs are non-colliding — range_start/range_end
produce a unique path per extracted period, so multiple backfills on the
same execution_date coexist safely.

Payload structure
-----------------
Each file wraps the raw Banxico response with pipeline metadata:
{
  "metadata": {
    "source":           "banxico",
    "dataset":          "tipo_de_cambio",
    "serie_id":         "SF43718",
    "extraction_type":  "daily",
    "execution_date":   "2026-04-01",
    "execution_ts":     "2026-04-01T08:05:01Z",
    "start_date":       "2026-03-25",
    "end_date":         "2026-03-31"
  },
  "data": { ...raw Banxico API response... }
}

Retry policy
------------
fetch_serie retries up to 3 times on transient failures with exponential
backoff (2s → 4s → 8s, capped at 30s). Only server errors (5xx), timeouts,
and connection errors are retried. Client errors (4xx) fail immediately —
they indicate configuration issues, not transient failures.

Series ingested
---------------
- SF43718 : USD/MXN FIX exchange rate  (daily)
- SF61745 : TIIE 28-day interest rate  (daily)
- SP1     : INPC consumer price index  (monthly)

Secret management
-----------------
Local development : .env file via python-dotenv
Production (AWS)  : SSM Parameter Store for BANXICO_TOKEN
                    Glue Job parameters for BUCKET_NAME, AWS_REGION

Usage
-----
Daily   : python3 src/extract/banxico_api.py
Backfill: python3 src/extract/banxico_api.py --mode backfill --start-date 2023-01-01
"""

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

# load_dotenv() is a no-op when variables are already present in the environment
# (Glue, Lambda), so this is safe to call without guards.
load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("banxico_extractor")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BANXICO_BASE_URL = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
BANXICO_TOKEN    = os.getenv("BANXICO_TOKEN")
BUCKET_NAME      = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")

# Save payload locally in addition to S3.
# Useful for local debugging — disable in production (Glue has no persistent filesystem).
SAVE_LOCAL = os.getenv("SAVE_LOCAL", "false").lower() == "true"

SERIES: dict[str, dict] = {
    "tipo_de_cambio": {"id": "SF43718", "frequency": "daily",   "lookback_days": 7},
    "tiie_28":        {"id": "SF61745", "frequency": "daily",   "lookback_days": 7},
    "inpc":           {"id": "SP1",     "frequency": "monthly"},
}

BRONZE_BASE = Path("data/bronze")

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def format_date(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def get_daily_window(data_month: datetime, lookback_days: int = 7) -> tuple[str, str]:
    """Return (start_date, end_date) for a rolling daily window ending yesterday.

    Window ends yesterday to ensure the trading day is fully closed
    before extraction — same-day data from Banxico may be incomplete.
    """
    end_date   = data_month - timedelta(days=1)
    start_date = end_date - timedelta(days=lookback_days - 1)
    return format_date(start_date), format_date(end_date)


def get_full_month_window(data_month: datetime) -> tuple[str, str]:
    """Return (start_date, end_date) covering the entire calendar month of data_month.

    Used during backfill for all series — daily and monthly — so every
    iteration extracts a complete, non-overlapping calendar month.
    """
    first_of_month = data_month.replace(day=1)
    if data_month.month == 12:
        last_of_month = data_month.replace(
            year=data_month.year + 1, month=1, day=1
        ) - timedelta(days=1)
    else:
        last_of_month = data_month.replace(
            month=data_month.month + 1, day=1
        ) - timedelta(days=1)
    return format_date(first_of_month), format_date(last_of_month)


def get_last_closed_month_window(data_month: datetime) -> tuple[str, str]:
    """Return (start_date, end_date) covering the entire previous calendar month.

    Used for monthly series in daily mode only. Targets the last fully closed
    month so SP1 (INPC) is guaranteed to be published before extraction.
    INPC is typically released 10-15 days after month-end.
    """
    first_of_current  = data_month.replace(day=1)
    last_of_previous  = first_of_current - timedelta(days=1)
    first_of_previous = last_of_previous.replace(day=1)
    return format_date(first_of_previous), format_date(last_of_previous)


def resolve_window(
    serie_info: dict,
    data_month: datetime,
    mode: str,
) -> tuple[str, str]:
    """
    Resolve the API date window for a series based on its frequency and execution mode.

    data_month controls which period to extract — it is the execution_date
    in daily mode and the current iteration month in backfill mode.

    Daily mode   : daily series use rolling lookback window,
                   monthly series use previous closed month.
    Backfill mode: all series extract the full calendar month of data_month.
    """
    if mode == "backfill":
        return get_full_month_window(data_month)

    if serie_info["frequency"] == "daily":
        return get_daily_window(data_month, serie_info["lookback_days"])

    if serie_info["frequency"] == "monthly":
        return get_last_closed_month_window(data_month)

    raise ValueError(f"Unknown frequency: {serie_info['frequency']!r}")


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _is_retryable(exc: BaseException) -> bool:
    """Return True only for transient failures worth retrying.

    Server errors (5xx)   : transient — retry.
    Timeout / connection  : transient — retry.
    Client errors (4xx)   : configuration issue — fail immediately.
    """
    if isinstance(exc, requests.HTTPError):
        return exc.response.status_code >= 500
    return isinstance(exc, (requests.Timeout, requests.ConnectionError))


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def fetch_serie(serie_id: str, start_date: str, end_date: str) -> dict:
    """
    Call the Banxico SIE API for a single series and return raw JSON.

    Retries up to 3 times on transient failures with exponential backoff:
    2s → 4s → 8s (capped at 30s). Client errors (4xx) are not retried —
    they indicate bad request parameters, not server instability.

    In production, retrieve BANXICO_TOKEN from SSM Parameter Store
    instead of environment variables.

    Raises
    ------
    requests.HTTPError       after retries if server returns 5xx.
    requests.HTTPError       immediately if server returns 4xx.
    requests.Timeout         after retries if request times out.
    requests.ConnectionError after retries if connection fails.
    """
    url     = f"{BANXICO_BASE_URL}/{serie_id}/datos/{start_date}/{end_date}"

    # Banxico API works without a token but rate-limits aggressively.
    # Always include token when available.
    headers = {"Bmx-Token": BANXICO_TOKEN} if BANXICO_TOKEN else {}

    logger.info("Fetching %s | %s → %s", serie_id, start_date, end_date)
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_payload(
    raw_data: dict,
    dataset: str,
    serie_id: str,
    extraction_type: str,
    execution_date: str,
    execution_ts: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Wrap raw Banxico API response with pipeline metadata.

    Embedding metadata inside the file makes each payload self-describing —
    Silver and any audit process can determine provenance without relying
    solely on the S3 path.
    """
    return {
        "metadata": {
            "source":           "banxico",
            "dataset":          dataset,
            "serie_id":         serie_id,
            "extraction_type":  extraction_type,
            "execution_date":   execution_date,
            "execution_ts":     execution_ts,
            "start_date":       start_date,
            "end_date":         end_date,
        },
        "data": raw_data,
    }


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def build_s3_key(
    dataset: str,
    extraction_type: str,
    execution_date: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Build the S3 key for a Bronze payload.

    Daily:
      bronze/source=banxico/dataset=<name>/extraction_type=daily/
        execution_date=<YYYY-MM-DD>/payload.json

    Backfill:
      bronze/source=banxico/dataset=<name>/extraction_type=backfill/
        execution_date=<YYYY-MM-DD>/range_start=<YYYY-MM-DD>/range_end=<YYYY-MM-DD>/payload.json

    execution_date always reflects when the pipeline ran.
    range_start/range_end follow Hive key=value convention, allow independent
    Athena partition filtering, and prevent key collisions across backfill runs.
    """
    base = (
        f"bronze/source=banxico/"
        f"dataset={dataset}/"
        f"extraction_type={extraction_type}/"
        f"execution_date={execution_date}/"
    )

    if extraction_type == "backfill":
        # Hive-style range partitions prevent collisions when multiple
        # backfills run on the same execution_date with different ranges.
        base += f"range_start={start_date}/range_end={end_date}/"

    return base + "payload.json"


def build_local_path(
    dataset: str,
    extraction_type: str,
    execution_date: str,
    start_date: str,
    end_date: str,
) -> Path:
    """Build the local filesystem path mirroring the S3 structure."""
    path = (
        BRONZE_BASE
        / "source=banxico"
        / f"dataset={dataset}"
        / f"extraction_type={extraction_type}"
        / f"execution_date={execution_date}"
    )
    if extraction_type == "backfill":
        path = path / f"range_start={start_date}" / f"range_end={end_date}"
    return path / "payload.json"


def save_local(payload: dict, local_path: Path) -> None:
    """
    Persist payload as JSON to the local filesystem.
    Only called when SAVE_LOCAL=true — intended for local debugging only.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        # ensure_ascii=False preserves Spanish characters in Banxico responses.
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Saved locally → %s", local_path)


def upload_to_s3(payload: dict, s3_key: str) -> str:
    """
    Serialize payload as UTF-8 JSON and upload directly to S3 from memory.

    Uploading from memory avoids filesystem dependency, making this safe
    to call in AWS Glue where the local filesystem is ephemeral and not
    guaranteed to persist between task attempts.

    Returns the full S3 URI of the uploaded object.
    """
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    s3_client.put_object(
        Bucket      = BUCKET_NAME,
        Key         = s3_key,
        Body        = body,
        ContentType = "application/json",
    )

    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
    logger.info("Uploaded → %s", s3_uri)
    return s3_uri


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def extract_all(
    execution_date: datetime,
    data_month: datetime | None = None,
    mode: str = "daily",
) -> list[str]:
    """
    Extract all configured series and upload to S3 Bronze layer.
    Optionally saves locally when SAVE_LOCAL=true.

    Parameters
    ----------
    execution_date : datetime
        When the pipeline ran. Always passed by run_extract — never created internally.
        Reflects the actual wall-clock time of execution, never the month
        being extracted during backfill.

    data_month : datetime, optional
        Which period to extract data for. Defaults to execution_date.
        In daily mode: same as execution_date.
        In backfill mode: the current month being iterated by run_backfill.

    mode : str
        'daily'    — daily series use rolling lookback window,
                     monthly series extract the previous closed month.
        'backfill' — all series extract the full calendar month of data_month.

    Returns
    -------
    list[str]
        Dataset names successfully uploaded to S3.
    """

    # In daily mode data_month == execution_date.
    # In backfill mode data_month is the iteration month, execution_date is today.
    if data_month is None:
        data_month = execution_date

    execution_date_str = format_date(execution_date)
    execution_ts_str   = execution_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    saved: list[str]   = []
    errors: list[str]  = []

    for dataset, serie_info in SERIES.items():
        serie_id = serie_info["id"]

        try:
            start_date, end_date = resolve_window(serie_info, data_month, mode)

            raw_data = fetch_serie(serie_id, start_date, end_date)

            payload = build_payload(
                raw_data        = raw_data,
                dataset         = dataset,
                serie_id        = serie_id,
                extraction_type = mode,
                execution_date  = execution_date_str,
                execution_ts    = execution_ts_str,
                start_date      = start_date,
                end_date        = end_date,
            )

            s3_key = build_s3_key(dataset, mode, execution_date_str, start_date, end_date)
            upload_to_s3(payload, s3_key)

            if SAVE_LOCAL:
                local_path = build_local_path(dataset, mode, execution_date_str, start_date, end_date)
                save_local(payload, local_path)

            saved.append(dataset)

        except Exception as exc:  # noqa: BLE001
            # Catch-all so one failing series never blocks the others from completing.
            logger.error("Failed to extract %s (%s): %s", dataset, serie_id, exc)
            errors.append(dataset)

    if errors:
        raise RuntimeError(
            f"Extraction completed with errors. Failed: {errors}. "
            f"Succeeded: {len(saved)}/{len(SERIES)}"
        )

    logger.info("Extraction complete. %d series uploaded.", len(saved))
    return saved


def run_backfill(start_date_str: str, execution_date: datetime) -> None:
    """
    Extract full historical range iterating month by month.

    execution_date is passed by run_extract and fixed for the entire backfill run —
    all monthly iterations share the same execution_date so S3 paths are consistent.
    data_month advances each iteration to control which calendar month to extract.

    This separation ensures execution_date in the S3 path always means
    "when the pipeline ran", never "what data is inside the file".

    Parameters
    ----------
    start_date_str : str
        Start date in YYYY-MM-DD format. Example: "2023-01-01"
    execution_date : datetime
        Wall-clock time when the pipeline started. Passed from run_extract
        so all iterations of a backfill share the same execution_date.

    Usage
    -----
        python3 src/pipeline.py --mode backfill --start-date 2023-01-01
    """
    start = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = datetime.now(tz=timezone.utc) - timedelta(days=1)

    if start > end:
        raise ValueError(
            f"start_date ({format_date(start)}) must be before yesterday ({format_date(end)}). "
            f"Cannot backfill future dates."
        )

    logger.info("Starting backfill | %s → %s", format_date(start), format_date(end))

    data_month = start.replace(day=1)
    while data_month <= end:
        logger.info("Backfilling month: %s", format_date(data_month))

        extract_all(
            execution_date = execution_date,
            data_month     = data_month,
            mode           = "backfill",
        )

        # Advance to the 1st of next month.
        if data_month.month == 12:
            data_month = data_month.replace(year=data_month.year + 1, month=1, day=1)
        else:
            data_month = data_month.replace(month=data_month.month + 1, day=1)

    logger.info("Backfill complete.")

def run_extract(mode: str, start_date: str | None, execution_date: datetime | None = None) -> None:
    """
    Public interface for pipeline orchestration.

    Creates execution_date if not provided — this is the single point where
    wall-clock time is captured for the entire pipeline run. Passing execution_date
    explicitly allows pipeline.py to share the same timestamp across all steps.

    Parameters
    ----------
    mode : str
        'daily'    — extracts rolling window for all series.
        'backfill' — extracts full historical range month by month.
    start_date : str or None
        Required when mode='backfill'. YYYY-MM-DD format.
    execution_date : datetime, optional
        When the pipeline started. Defaults to now (UTC) if not provided.
        pipeline.py creates this once and passes it to all steps.

    Raises
    ------
    ValueError
        If mode='backfill' and start_date is None.
    ValueError
        If start_date is after yesterday (future dates cannot be backfilled).
    """
    if execution_date is None:
        execution_date = datetime.now(tz=timezone.utc)

    if mode == "backfill":
        if not start_date:
            raise ValueError("--start-date is required when --mode=backfill")
        run_backfill(start_date, execution_date)
    else:
        extract_all(execution_date)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Banxico Bronze Layer Extractor")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
    )
    args = parser.parse_args()

    # Validation for CLI usage — pipeline.py validates before calling run_extract directly.
    if args.mode == "backfill" and not args.start_date:
        parser.error("--start-date is required when --mode=backfill")

    run_extract(mode=args.mode, start_date=args.start_date)