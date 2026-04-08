import json
import logging
import os
import re
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv

from checkpoints import read_checkpoint, write_checkpoint

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
logger = logging.getLogger("silver")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET_NAME = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")
AWS_REGION  = os.getenv("AWS_REGION",  "us-east-1")

SERIES = {
    "tipo_de_cambio": {"id": "SF43718", "frequency": "daily"},
    "tiie_28":        {"id": "SF60648", "frequency": "daily"},
    "inpc":           {"id": "SP1",     "frequency": "monthly"},
}

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Extract helpers
# ---------------------------------------------------------------------------

def list_bronze_files(
    dataset: str,
    mode: str,
    execution_date: str,
    after_date: str | None = None,
) -> list[str]:
    """
    List S3 keys for Bronze payloads of a given dataset and extraction type.

    Backfill: scopes to exact execution_date partition — lists all range sub-partitions
    within that single backfill run.

    Daily: uses StartAfter to return only execution_dates after the checkpoint,
    avoiding reprocessing of already-transformed files.

    Paginates automatically for buckets with >1000 objects.

    Parameters
    ----------
    dataset        : Bronze dataset name. Example: "tipo_de_cambio".
    mode           : "daily" or "backfill".
    execution_date : YYYY-MM-DD string of the pipeline run date.
    after_date     : YYYY-MM-DD checkpoint date for daily mode. Pass "0000-00-00"
                     to return all files (first run). Ignored in backfill mode.
    """
    base = f"bronze/source=banxico/dataset={dataset}/extraction_type={mode}"

    if mode == "backfill":
        prefix      = f"{base}/execution_date={execution_date}"
        start_after = ""
    else:
        prefix      = base
        start_after = f"{base}/execution_date={after_date}"

    files              = []
    continuation_token = None

    while True:
        kwargs = dict(Bucket=BUCKET_NAME, Prefix=prefix)
        if start_after:
            kwargs["StartAfter"] = start_after
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)
        files.extend(content["Key"] for content in response.get("Contents", []))

        if not response.get("IsTruncated"):
            break

        continuation_token = response["NextContinuationToken"]

    return files


def read_bronze_payload(s3_key: str) -> dict:
    """Read a Bronze payload from S3 and return the full dict (metadata + data)."""
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    content  = response["Body"].read().decode("utf-8")
    return json.loads(content)


def parse_bronze_records(payload: dict) -> pd.DataFrame:
    """
    Extract records from a Bronze payload into a raw DataFrame.

    Adds metadata columns (source, dataset, serie_id, processed_at) so each
    row is self-describing without needing the S3 path for context.

    Returns an empty DataFrame if the payload contains no data — caller
    decides whether to skip or raise.
    """
    series   = payload.get("data", {}).get("bmx", {}).get("series", [])
    metadata = payload.get("metadata", {})

    if not series or "datos" not in series[0]:
        logger.warning(
            "No data found | %s → %s",
            metadata.get("start_date"),
            metadata.get("end_date"),
        )
        return pd.DataFrame()

    processed_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []

    for serie in series:
        for d in serie.get("datos", []):
            rows.append({
                "fecha":        d["fecha"],
                "dato":         d["dato"],
                "titulo":       serie["titulo"],
                "source":       metadata["source"],
                "dataset":      metadata["dataset"],
                "serie_id":     metadata["serie_id"],
                "processed_at": processed_at,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all Silver transformations to a raw Bronze DataFrame.

    - Parses Banxico date format (dd/mm/yyyy) to datetime64
    - Casts value strings to float64 — N/E → NaN via errors="coerce"
    - Normalizes title whitespace (strip + collapse internal spaces)
    - Renames columns to final Silver schema
    """
    df = df.copy()

    df["fecha"]  = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")
    df["dato"]   = pd.to_numeric(df["dato"], errors="coerce")
    df["titulo"] = df["titulo"].str.strip().apply(lambda x: re.sub(r"\s+", " ", x))

    df = df.rename(columns={
        "fecha":  "date",
        "dato":   "value",
        "titulo": "title",
    })

    return df[["date", "source", "dataset", "serie_id", "processed_at", "value", "title"]]


def build_silver_dataframe(file_list: list[str]) -> pd.DataFrame:
    """
    Read, parse and transform all Bronze files in file_list into a single DataFrame.

    Skips empty payloads with a warning. Deduplicates by (serie_id, date) keeping
    the latest processed_at — handles overlaps from daily rolling windows.

    Returns an empty DataFrame if all payloads are empty or file_list is empty.
    """
    dfs = []

    for s3_key in file_list:
        payload = read_bronze_payload(s3_key)
        raw_df  = parse_bronze_records(payload)

        if raw_df.empty:
            logger.warning("No data in payload | %s | skipping", s3_key)
            continue

        dfs.append(transform(raw_df))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    df = df.sort_values("processed_at", ascending=False)
    df = df.drop_duplicates(subset=["serie_id", "date"], keep="first")

    return df.sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def process_dataset_to_silver(
    dataset: str,
    mode: str,
    file_list: list[str],
    execution_date: datetime,
) -> None:
    """
    Orchestrate Silver processing for a single dataset.

    Reads all Bronze files in file_list, transforms them, and writes the
    result to Silver. Updates the checkpoint after a successful write.

    Parameters
    ----------
    dataset        : Dataset name. Example: "tipo_de_cambio".
    mode           : "daily" or "backfill".
    file_list      : S3 keys of Bronze files to process.
    execution_date : Wall-clock time when the pipeline started.
    """
    silver_df = build_silver_dataframe(file_list)

    if silver_df.empty:
        logger.warning("No data to write | dataset=%s", dataset)
        return

    # TODO: write_silver_parquet(silver_df, dataset, execution_date)
    # TODO: write_checkpoint(dataset, execution_date.strftime("%Y-%m-%d"))

    logger.info(
        "Silver processing complete | dataset=%s | rows=%d",
        dataset,
        len(silver_df),
    )


def run_silver(mode: str, start_date: str | None, execution_date: datetime) -> None:
    """
    Entry point for the Silver transformation step.

    Daily mode   : reads checkpoint per dataset to process only new Bronze files.
                   If no checkpoint exists (first run), processes all available files.
    Backfill mode: processes all Bronze files for the exact execution_date of this run.
                   Checkpoint is not read or updated — backfill is an explicit reprocess.

    Parameters
    ----------
    mode           : "daily" or "backfill".
    start_date     : Backfill start date in YYYY-MM-DD format. Not used for listing —
                     Bronze files are scoped by execution_date.
    execution_date : Wall-clock time when the pipeline started.
    """
    execution_date_str = execution_date.strftime("%Y-%m-%d")

    for dataset in SERIES:
        if mode == "backfill":
            files = list_bronze_files(dataset, mode, execution_date_str)
        else:
            checkpoint = read_checkpoint(dataset)
            after_date = checkpoint if checkpoint else "0000-00-00"
            files      = list_bronze_files(dataset, mode, execution_date_str, after_date)

        logger.info(
            "Files to process | dataset=%s | mode=%s | count=%d",
            dataset,
            mode,
            len(files),
        )

        process_dataset_to_silver(dataset, mode, files, execution_date)