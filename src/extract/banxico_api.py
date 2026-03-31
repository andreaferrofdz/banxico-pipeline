"""
Extraction of data from Banxico SIE API.
Deposits raw JSON into the Bronze layer of the data lake (S3).

Series:
    SF43718 — USD/MXN exchange rate (Fix)
    SF61745 — TIIE 28-day rate
    SP1     — INPC general inflation

Secret management:
    Local development : .env file via python-dotenv
    Production (AWS)  : SSM Parameter Store for sensitive values (BANXICO_TOKEN)
                        Glue Job parameters for non-sensitive config (BUCKET_NAME)
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import requests
from dotenv import load_dotenv

# ─── Environment ──────────────────────────────────────────────────────────────

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

BANXICO_BASE_URL = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"
BANXICO_TOKEN    = os.getenv("BANXICO_TOKEN", "")

SERIES = {
    "tipo_de_cambio": "SF43718",
    "tiie_28":        "SF61745",
    "inpc":           "SP1",
}

BUCKET_NAME = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")
AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")

# ─── AWS client ───────────────────────────────────────────────────────────────

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ─── Functions ────────────────────────────────────────────────────────────────

def get_date_range(days_back: int = 30) -> tuple[str, str]:
    """
    Returns (start_date, end_date) as YYYY-MM-DD strings.
    Both dates are inclusive in the Banxico API request.
    """
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def fetch_serie(serie_id: str, start_date: str, end_date: str) -> dict:
    """
    Fetches a single series from Banxico SIE API and returns raw JSON.

    Token is optional for short date ranges but required for full history.
    In production, retrieve BANXICO_TOKEN from SSM Parameter Store
    instead of environment variables.
    """
    url = f"{BANXICO_BASE_URL}/{serie_id}/datos/{start_date}/{end_date}"

    # Banxico API works without a token but enforces strict rate limits.
    # Always include token when available.
    headers = {"Bmx-Token": BANXICO_TOKEN} if BANXICO_TOKEN else {}

    logger.info(f"Fetching serie {serie_id} | {start_date} → {end_date}")

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def build_s3_key(serie_name: str, serie_id: str, extraction_date: str) -> str:
    """
    Builds a Hive-style partitioned S3 path for the Bronze layer.

    Partitioning by serie/year/month/day enables Athena partition pruning,
    which reduces scanned data volume and query cost on large date ranges.

    Example:
        bronze/serie=tipo_de_cambio/year=2026/month=03/day=30/SF43718_20260330.json
    """
    dt    = datetime.strptime(extraction_date, "%Y-%m-%d")
    year  = dt.strftime("%Y")
    month = dt.strftime("%m")
    day   = dt.strftime("%d")

    return (
        f"bronze/"
        f"serie={serie_name}/"
        f"year={year}/month={month}/day={day}/"
        f"{serie_id}_{dt.strftime('%Y%m%d')}.json"
    )


def upload_to_s3(data: dict, s3_key: str) -> None:
    """
    Serializes dict as UTF-8 JSON and uploads to S3 Bronze layer.
    Preserves Spanish characters in Banxico response with ensure_ascii=False.
    """
    body = json.dumps(data, ensure_ascii=False, indent=2)

    s3_client.put_object(
        Bucket      = BUCKET_NAME,
        Key         = s3_key,
        Body        = body.encode("utf-8"),
        ContentType = "application/json",
    )

    logger.info(f"Uploaded → s3://{BUCKET_NAME}/{s3_key}")


def extract_all(days_back: int = 30) -> None:
    """
    Orchestrates extraction of all configured series into Bronze.

    Processes series independently so a single failure does not block others.
    Raises RuntimeError at the end if any series failed — this surfaces
    as a Glue job failure and triggers the SNS alert pipeline.
    """
    start_date, end_date = get_date_range(days_back)
    extraction_date      = end_date
    results              = {"success": [], "failed": []}

    for serie_name, serie_id in SERIES.items():
        try:
            data   = fetch_serie(serie_id, start_date, end_date)
            s3_key = build_s3_key(serie_name, serie_id, extraction_date)
            upload_to_s3(data, s3_key)
            results["success"].append(serie_name)

        except requests.HTTPError as e:
            logger.error(f"HTTP error fetching {serie_id}: {e}")
            results["failed"].append(serie_name)

        except Exception as e:
            logger.error(f"Unexpected error for {serie_id}: {e}")
            results["failed"].append(serie_name)

    logger.info(
        f"Extraction complete | "
        f"success={results['success']} | "
        f"failed={results['failed']}"
    )

    # Raise after processing all series so partial successes are still uploaded.
    if results["failed"]:
        raise RuntimeError(f"Failed series: {results['failed']}")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    extract_all(days_back=30)