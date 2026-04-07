"""
Checkpoint Store
===================================
Manages per-dataset processing state for the Silver layer.

Each dataset maintains an independent checkpoint in S3, storing the last
execution_date successfully processed. This allows Silver to resume from
where it left off without reprocessing already-transformed data.

Checkpoint structure
--------------------
S3 path : silver/source=banxico/_checkpoints/<dataset>.json
Content :
  {
    "dataset":                       "tipo_de_cambio",
    "last_processed_execution_date": "2026-04-01",
    "updated_at":                    "2026-04-01T08:05:01Z"
  }

Usage
-----
  from checkpoints import read_checkpoint, write_checkpoint, delete_checkpoint

  last_run = read_checkpoint("tipo_de_cambio")   # None on first run
  write_checkpoint("tipo_de_cambio", "2026-04-01")
  delete_checkpoint("tipo_de_cambio")             # force full reprocessing
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("checkpoints")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_checkpoint_key(dataset: str) -> str:
    """
    Build the S3 key for a dataset's checkpoint file.

    Checkpoints live inside the Silver prefix so they are co-located
    with the data they describe and included in Silver-level access policies.
    """
    return f"silver/source=banxico/_checkpoints/{dataset}.json"


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def read_checkpoint(dataset: str) -> str | None:
    """
    Read the last successfully processed execution_date for a dataset.

    Returns None on the first run when no checkpoint exists — Silver
    will process all available Bronze partitions in that case.

    Parameters
    ----------
    dataset : str
        Dataset name matching the Bronze partition key. Example: "tipo_de_cambio".

    Returns
    -------
    str or None
        Last processed execution_date in YYYY-MM-DD format, or None if
        no checkpoint exists.
    """
    s3_key = get_checkpoint_key(dataset)

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        last_processed_execution_date = data.get("last_processed_execution_date")

        logger.info(
            "Checkpoint read | dataset=%s | last_processed=%s",
            dataset,
            last_processed_execution_date,
        )

        return last_processed_execution_date

    except s3_client.exceptions.NoSuchKey:
        return None


def write_checkpoint(dataset: str, execution_date: str) -> None:
    """
    Persist the last successfully processed execution_date for a dataset.

    Called by Silver after successfully transforming and uploading a dataset.
    Overwrites any existing checkpoint for the dataset.

    Parameters
    ----------
    dataset : str
        Dataset name matching the Bronze partition key. Example: "tipo_de_cambio".
    execution_date : str
        Last processed execution_date in YYYY-MM-DD format.
    """
    s3_key = get_checkpoint_key(dataset)

    payload = {
        "dataset": dataset,
        "last_processed_execution_date": execution_date,
        "updated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Checkpoint written | dataset=%s | execution_date=%s", dataset, execution_date
    )


def delete_checkpoint(dataset: str) -> None:
    """
    Delete the checkpoint for a dataset to force full reprocessing.

    After deletion, the next Silver run will treat the dataset as a first
    run and process all available Bronze partitions from scratch.

    Parameters
    ----------
    dataset : str
        Dataset name matching the Bronze partition key. Example: "tipo_de_cambio".
    """
    s3_key = get_checkpoint_key(dataset)
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)

    logger.info("Checkpoint deleted | dataset=%s", dataset)
