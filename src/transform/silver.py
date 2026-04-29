"""Silver layer transformation pipeline for Banxico macroeconomic data.

Reads raw JSON payloads from the Bronze layer in S3, applies schema normalization
and type casting, and writes partitioned Parquet files to the Silver layer.

Supports two execution modes:
    - daily: incremental processing using per-dataset checkpoints.
    - backfill: full reprocess of a specific execution_date partition.

Bronze → Silver transformations applied:
    - Banxico date format (dd/mm/yyyy) parsed to date32.
    - String values cast to float64 — N/E sentinel → NaN.
    - Title whitespace normalized (strip + collapse).
    - Columns renamed to final Silver schema.

Output partition structure:
    s3://<bucket>/silver/source=banxico/dataset=<name>/year=<YYYY>/month=<MM>/data.parquet

Glue Data Catalog partitions are registered programmatically after each write —
no MSCK REPAIR TABLE or Glue Crawler required.

Usage:
    python silver.py  # runs daily mode with current UTC timestamp
"""
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

SERIES = {
    "tipo_de_cambio": {"id": "SF43718", "frequency": "daily"},
    "tiie_28": {"id": "SF60648", "frequency": "daily"},
    "inpc": {"id": "SP1", "frequency": "monthly"},
}

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Extract helpers
# ---------------------------------------------------------------------------


def _list_bronze_files(
    dataset: str,
    mode: str,
    execution_date: str,
    after_date: Optional[str] = None,
) -> list[str]:
    """List S3 keys for Bronze payloads of a given dataset and extraction type.

    Backfill: scopes to exact execution_date partition — lists all range sub-partitions
    within that single backfill run.

    Daily: uses StartAfter to return only execution_dates after the checkpoint,
    avoiding reprocessing of already-transformed files.

    Paginates automatically for buckets with >1000 objects.

    Args:
        dataset: Bronze dataset name. Example: "tipo_de_cambio".
        mode: Extraction mode — "daily" or "backfill".
        execution_date: YYYY-MM-DD string of the pipeline run date.
        after_date: YYYY-MM-DD checkpoint date for daily mode. Pass "0000-00-00"
            to return all files (first run). Ignored in backfill mode.

    Returns:
        List of S3 keys matching the given prefix and mode filters.
    """
    base = f"bronze/source=banxico/dataset={dataset}/extraction_type={mode}"

    if mode == "backfill":
        prefix = f"{base}/execution_date={execution_date}"
        start_after = ""
    else:
        prefix = base
        start_after = f"{base}/execution_date={after_date}"

    files = []
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


def _read_bronze_payload(s3_key: str) -> dict:
    """Read a Bronze payload from S3 and return the full dict (metadata + data).

    Args:
        s3_key: Full S3 object key of the Bronze JSON file.

    Returns:
        Parsed JSON payload as a dictionary with keys 'metadata' and 'data'.
    """
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def _parse_bronze_records(payload: dict) -> pd.DataFrame:
    """Extract records from a Bronze payload into a raw DataFrame.

    Adds metadata columns (source, dataset, serie_id, processed_at) so each
    row is self-describing without needing the S3 path for context.

    Args:
        payload: Parsed Bronze JSON dict with keys 'metadata' and 'data'.

    Returns:
        Raw DataFrame with columns: fecha, dato, titulo, source, dataset,
        serie_id, processed_at. Empty DataFrame if the payload contains no data.
    """
    series = payload.get("data", {}).get("bmx", {}).get("series", [])
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
            rows.append(
                {
                    "fecha": d["fecha"],
                    "dato": d["dato"],
                    "titulo": serie["titulo"],
                    "source": metadata["source"],
                    "dataset": metadata["dataset"],
                    "serie_id": metadata["serie_id"],
                    "processed_at": processed_at,
                }
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all Silver transformations to a raw Bronze DataFrame.

    - Parses Banxico date format (dd/mm/yyyy) to datetime64.
    - Casts value strings to float64 — N/E → NaN via errors='coerce'.
    - Normalizes title whitespace (strip + collapse internal spaces).
    - Renames columns to final Silver schema.

    Args:
        df: Raw Bronze DataFrame as returned by _parse_bronze_records.

    Returns:
        Transformed DataFrame with columns: date, source, dataset, serie_id,
        processed_at, value, title.
    """
    df = df.copy()

    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")
    df["dato"] = pd.to_numeric(df["dato"], errors="coerce")
    df["titulo"] = df["titulo"].str.strip().apply(lambda x: re.sub(r"\s+", " ", x))

    df = df.rename(
        columns={
            "fecha": "date",
            "dato": "value",
            "titulo": "title",
        }
    )

    return df[
        ["date", "source", "dataset", "serie_id", "processed_at", "value", "title"]
    ]


def _build_silver_dataframe(file_list: list[str]) -> pd.DataFrame:
    """Read, parse and transform all Bronze files into a single Silver DataFrame.

    Skips empty payloads with a warning. Deduplicates by (serie_id, date) keeping
    the latest processed_at — handles overlaps from daily rolling windows.

    Args:
        file_list: S3 keys of Bronze JSON files to process.

    Returns:
        Consolidated Silver DataFrame sorted by date. Empty DataFrame if all
        payloads are empty or file_list is empty.
    """
    dfs = []

    for s3_key in file_list:
        payload = _read_bronze_payload(s3_key)
        raw_df = _parse_bronze_records(payload)

        if raw_df.empty:
            logger.warning("No data in payload | %s | skipping", s3_key)
            continue

        dfs.append(_transform(raw_df))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    df = df.sort_values("processed_at", ascending=False)
    df = df.drop_duplicates(subset=["serie_id", "date"], keep="first")

    return df.sort_values("date").reset_index(drop=True)


def _register_silver_partition(dataset: str, year: str, month: str) -> None:
    """Register a Silver partition in Glue Data Catalog after writing Parquet.

    Eliminates the need for MSCK REPAIR TABLE or a Glue Crawler.
    AlreadyExistsException is silently ignored — the call is idempotent
    so re-running the pipeline for the same period is safe.

    Args:
        dataset: Dataset name. Example: "tipo_de_cambio".
        year: Partition year as string. Example: "2024".
        month: Partition month as zero-padded string. Example: "01".
    """
    database_name = os.getenv("GLUE_DATABASE", "banxico-pipeline-dev")

    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=f"silver_{dataset}",
            PartitionInput={
                "Values": [year, month],
                "StorageDescriptor": {
                    "Location": (
                        f"s3://{BUCKET_NAME}/silver/source=banxico/"
                        f"dataset={dataset}/year={year}/month={month}/"
                    ),
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                },
            },
        )
        logger.info(
            "Partition registered | table=silver_%s | year=%s | month=%s",
            dataset,
            year,
            month,
        )

    except glue_client.exceptions.AlreadyExistsException:
        # Partition already registered — idempotent, safe to ignore.
        pass


def _write_silver_parquet(
    df: pd.DataFrame,
    dataset: str,
    execution_date: datetime,
) -> None:
    """Write a Silver DataFrame to S3 as Parquet, partitioned by year and month of the data.

    Partitions by business date (year/month of the data records), not by pipeline
    execution_date — Silver partitions represent when the data occurred, not when
    the pipeline ran. This enables efficient Athena partition pruning on date ranges.

    Each partition is overwritten on every run — idempotent by design. If Silver
    already has data for year=2024/month=01, it is replaced with the latest version.

    Partition structure:
        silver/source=banxico/dataset=<name>/year=<YYYY>/month=<MM>/data.parquet

    Args:
        df: Silver DataFrame to write. Must contain a 'date' column.
        dataset: Dataset name. Example: "tipo_de_cambio".
        execution_date: Wall-clock time when the pipeline started — used for logging only.
    """
    execution_date_str = execution_date.strftime("%Y-%m-%d")

    df = df.copy()
    df["year"] = df["date"].dt.year.astype(str)
    df["month"] = df["date"].dt.month.astype(str).str.zfill(2)

    partitions = df.groupby(["year", "month"])

    schema = pa.schema(
        [
            pa.field("date", pa.date32()),
            pa.field("source", pa.string()),
            pa.field("dataset", pa.string()),
            pa.field("serie_id", pa.string()),
            pa.field("processed_at", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("title", pa.string()),
        ]
    )

    for (year, month), partition_df in partitions:
        partition_df = partition_df.drop(columns=["year", "month"])

        partition_df = partition_df.copy()
        partition_df["date"] = partition_df["date"].dt.date

        table = pa.Table.from_pandas(partition_df, schema=schema, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        s3_key = (
            f"silver/source=banxico/"
            f"dataset={dataset}/"
            f"year={year}/"
            f"month={month}/"
            f"data.parquet"
        )

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        _register_silver_partition(dataset, str(year), str(month))

        logger.info(
            "Written Silver Parquet | dataset=%s | year=%s | month=%s | rows=%d",
            dataset,
            year,
            month,
            len(partition_df),
        )

    logger.info(
        "Silver write complete | dataset=%s | execution_date=%s | partitions=%d | total_rows=%d",
        dataset,
        execution_date_str,
        len(partitions),
        len(df),
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _process_dataset_to_silver(
    dataset: str,
    file_list: list[str],
    execution_date: datetime,
) -> None:
    """Orchestrate Silver processing for a single dataset.

    Reads all Bronze files in file_list, transforms them, and writes the
    result to Silver. Updates the checkpoint after a successful write.

    Args:
        dataset: Dataset name. Example: "tipo_de_cambio".
        file_list: S3 keys of Bronze files to process.
        execution_date: Wall-clock time when the pipeline started.
    """
    silver_df = _build_silver_dataframe(file_list)

    if silver_df.empty:
        logger.warning("No data to write | dataset=%s", dataset)
        return

    _write_silver_parquet(silver_df, dataset, execution_date)
    write_checkpoint(dataset, execution_date.strftime("%Y-%m-%d"))

    logger.info(
        "Silver processing complete | dataset=%s | rows=%d",
        dataset,
        len(silver_df),
    )


def run_silver(mode: str, execution_date: datetime) -> None:
    """Entry point for the Silver transformation step.

    Daily mode: reads checkpoint per dataset to process only new Bronze files.
        If no checkpoint exists (first run), processes all available files.
    Backfill mode: processes all Bronze files for the exact execution_date of this run.
        Checkpoint is not read or updated — backfill is an explicit reprocess.

    Args:
        mode: Extraction mode — "daily" or "backfill".
        execution_date: Wall-clock time when the pipeline started.
    """
    execution_date_str = execution_date.strftime("%Y-%m-%d")

    for dataset in SERIES:
        if mode == "backfill":
            files = _list_bronze_files(dataset, mode, execution_date_str)
        else:
            checkpoint = read_checkpoint(dataset)
            after_date = checkpoint if checkpoint else "0000-00-00"
            files = _list_bronze_files(dataset, mode, execution_date_str, after_date)

        logger.info(
            "Files to process | dataset=%s | mode=%s | count=%d",
            dataset,
            mode,
            len(files),
        )

        _process_dataset_to_silver(dataset, files, execution_date)

if __name__ == "__main__":
    execution_date = datetime.now(tz=timezone.utc)
    run_silver(mode="daily", execution_date=execution_date)