"""
Banxico SIE API — Bronze Layer Extractor
=========================================
Fetches raw financial series from Banxico and saves them as JSON
following a Hive-style partition structure that mirrors S3.

Local path  : data/bronze/banxico/serie=<ID>/frequency=<freq>/extraction_date=<date>/raw.json
S3 path     : s3://<bucket>/bronze/banxico/serie=<ID>/frequency=<freq>/extraction_date=<date>/raw.json

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

Execution modes
---------------
Daily (default) : extracts rolling lookback window per series (lookback_days)
Backfill        : single API call per series covering the full historical range

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

# Save raw JSON locally in addition to S3.
# Useful for local debugging — disable in production (Glue has no persistent filesystem).
SAVE_LOCAL = os.getenv("SAVE_LOCAL", "false").lower() == "true"

SERIES: dict[str, dict] = {
    "tipo_de_cambio": {"id": "SF43718", "frequency": "daily",   "lookback_days": 7},
    "tiie_28":        {"id": "SF61745", "frequency": "daily",   "lookback_days": 7},
    "inpc":           {"id": "SP1",     "frequency": "monthly"},
}

BRONZE_BASE = Path("data/bronze/banxico")

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def format_date(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def get_daily_window(execution_date: datetime, lookback_days: int = 7) -> tuple[str, str]:
    """Return (start_date, end_date) for a rolling daily window ending yesterday.

    Window ends yesterday to ensure the trading day is fully closed
    before extraction — same-day data from Banxico may be incomplete.
    """
    end_date   = execution_date - timedelta(days=1)
    start_date = end_date - timedelta(days=lookback_days - 1)
    return format_date(start_date), format_date(end_date)


def get_last_closed_month_window(execution_date: datetime) -> tuple[str, str]:
    """Return (start_date, end_date) covering the entire previous calendar month.

    Always targets the last fully closed month so SP1 (INPC) is guaranteed
    to be published before extraction. INPC is typically released 10-15 days
    after month-end — running on the 1st ensures the value is available.
    """
    first_of_current  = execution_date.replace(day=1)
    last_of_previous  = first_of_current - timedelta(days=1)
    first_of_previous = last_of_previous.replace(day=1)
    return format_date(first_of_previous), format_date(last_of_previous)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def fetch_serie(serie_id: str, start_date: str, end_date: str) -> dict:
    """
    Call the Banxico SIE API for a single series.

    Token is optional for short date ranges but enforces strict rate limits
    without it. In production, retrieve BANXICO_TOKEN from SSM Parameter Store.

    Raises
    ------
    requests.HTTPError  if the API returns a non-2xx status code.
    requests.Timeout    if the request takes longer than 30 seconds.
    """
    url = f"{BANXICO_BASE_URL}/{serie_id}/datos/{start_date}/{end_date}"

    # Banxico API works without a token but rate-limits aggressively.
    # Always include token when available.
    headers = {"Bmx-Token": BANXICO_TOKEN} if BANXICO_TOKEN else {}

    logger.info("Fetching %s | %s → %s", serie_id, start_date, end_date)
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def save_bronze_local(
    serie_id: str,
    frequency: str,
    partition_key: str,
    raw_data: dict,
) -> Path:
    """
    Persist raw API response as JSON using a Hive-style partition path.
    Only called when SAVE_LOCAL=true — intended for local debugging only.

    Returns the full path of the saved file.
    """
    output_dir = (
        BRONZE_BASE
        / f"serie={serie_id}"
        / f"frequency={frequency}"
        / f"extraction_date={partition_key}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        # ensure_ascii=False preserves Spanish characters in Banxico responses.
        json.dump(raw_data, f, ensure_ascii=False, indent=2)

    logger.info("Saved locally → %s", output_file)
    return output_file


def upload_to_s3(
    raw_data: dict,
    serie_id: str,
    frequency: str,
    partition_key: str,
) -> str:
    """
    Serialize and upload JSON directly to S3 from memory — no local file needed.

    Uploading from memory avoids filesystem dependency, making this safe
    to call in AWS Glue where the local filesystem is ephemeral and not
    guaranteed to persist between task attempts.

    The partition_key distinguishes daily runs (YYYY-MM-DD) from backfill
    loads (backfill_YYYY-MM-DD_YYYY-MM-DD), allowing Silver to apply
    different processing logic per file type.

    Returns the full S3 URI of the uploaded object.
    """
    s3_key = (
        f"bronze/banxico/"
        f"serie={serie_id}/"
        f"frequency={frequency}/"
        f"extraction_date={partition_key}/"
        f"raw.json"
    )

    body = json.dumps(raw_data, ensure_ascii=False, indent=2).encode("utf-8")

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

def extract_all(execution_date: datetime | None = None) -> list[str]:
    """
    Extract all configured series and upload to S3 Bronze layer.
    Optionally saves locally when SAVE_LOCAL=true.

    Parameters
    ----------
    execution_date : datetime, optional
        Defaults to today (UTC). Override for testing or backfill scenarios:
            extract_all(execution_date=datetime(2025, 1, 15, tzinfo=timezone.utc))

    Returns
    -------
    list[str]
        Serie IDs successfully uploaded to S3.
    """
    if execution_date is None:
        execution_date = datetime.now(tz=timezone.utc)

    partition_key    = format_date(execution_date)
    saved_series: list[str] = []
    errors: list[str] = []

    for serie_name, serie_info in SERIES.items():
        serie_id  = serie_info["id"]
        frequency = serie_info["frequency"]

        try:
            if frequency == "daily":
                start_date, end_date = get_daily_window(
                    execution_date, serie_info["lookback_days"]
                )
            elif frequency == "monthly":
                start_date, end_date = get_last_closed_month_window(execution_date)
            else:
                raise ValueError(f"Unknown frequency: {frequency!r}")

            raw_data = fetch_serie(serie_id, start_date, end_date)

            # Always upload to S3 — primary storage.
            # Local save is optional, controlled by SAVE_LOCAL env variable.
            upload_to_s3(raw_data, serie_id, frequency, partition_key)

            if SAVE_LOCAL:
                save_bronze_local(serie_id, frequency, partition_key, raw_data)

            saved_series.append(serie_id)

        except Exception as exc:  # noqa: BLE001
            # Catch-all so one failing series never blocks the others from completing.
            logger.error("Failed to extract %s (%s): %s", serie_name, serie_id, exc)
            errors.append(serie_name)

    if errors:
        raise RuntimeError(
            f"Extraction completed with errors. Failed series: {errors}. "
            f"Successfully uploaded: {len(saved_series)}/{len(SERIES)}"
        )

    logger.info("Extraction complete. %d series uploaded.", len(saved_series))
    return saved_series


def run_backfill(start_date_str: str) -> None:
    """
    Extract full historical range in a single API call per series.

    Banxico SIE API supports arbitrary date ranges — no need to iterate
    month by month. One request per series covers the entire backfill window,
    reducing 72 HTTP requests (3 series × 24 months) to just 3.

    The partition key uses the format backfill_<start>_<end> so Silver
    can distinguish backfill files from daily incremental files and apply
    the appropriate processing logic.

    Parameters
    ----------
    start_date_str : str
        Start date in YYYY-MM-DD format. Example: "2023-01-01"

    Usage
    -----
        python3 src/extract/banxico_api.py --mode backfill --start-date 2023-01-01
    """
    start = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = datetime.now(tz=timezone.utc) - timedelta(days=1)

    start_date    = format_date(start)
    end_date      = format_date(end)
    partition_key = f"backfill_{start_date}_{end_date}"

    logger.info("Starting backfill | %s → %s", start_date, end_date)

    saved_series: list[str] = []
    errors: list[str] = []

    for serie_name, serie_info in SERIES.items():
        serie_id  = serie_info["id"]
        frequency = serie_info["frequency"]

        try:
            # Single API call covers the entire historical range.
            # For monthly series, Banxico returns one row per month automatically.
            raw_data = fetch_serie(serie_id, start_date, end_date)

            upload_to_s3(raw_data, serie_id, frequency, partition_key)

            if SAVE_LOCAL:
                save_bronze_local(serie_id, frequency, partition_key, raw_data)

            saved_series.append(serie_id)
            logger.info("Backfill complete for %s (%s)", serie_name, serie_id)

        except Exception as exc:  # noqa: BLE001
            # Catch-all so one failing series never blocks the others from completing.
            logger.error("Failed to backfill %s (%s): %s", serie_name, serie_id, exc)
            errors.append(serie_name)

    if errors:
        raise RuntimeError(
            f"Backfill completed with errors. Failed series: {errors}. "
            f"Successfully uploaded: {len(saved_series)}/{len(SERIES)}"
        )

    logger.info("Backfill complete. %d series uploaded.", len(saved_series))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Banxico Bronze Layer Extractor")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily: extracts rolling window per series. backfill: single call per series for full history.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Backfill start date in YYYY-MM-DD format. Required when --mode=backfill.",
    )
    args = parser.parse_args()

    if args.mode == "backfill":
        if not args.start_date:
            parser.error("--start-date is required when --mode=backfill")
        run_backfill(start_date_str=args.start_date)
    else:
        extract_all()