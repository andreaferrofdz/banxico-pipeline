"""Gold layer aggregator for Banxico macroeconomic data.

Reads Silver Parquet datasets, aggregates to monthly frequency, joins the three
series into a single macro indicators dataset and writes to S3 Gold layer.

Gold is always fully regenerated on every run — no incremental logic needed
given the small dataset size (~40 rows). Each run writes a new partition keyed
by execution_date for auditability.

Partition structure:
    gold/source=banxico/dataset=macro_indicators/execution_date=<YYYY-MM-DD>/data.parquet

Gold schema:
    date            : date   — month-start date
    fx_rate         : float  — monthly average USD/MXN FIX exchange rate
    fx_volatility   : float  — monthly std deviation of daily FX (exchange rate risk proxy)
    fx_mom_pct      : float  — FX month-over-month change (%)
    tiie_28         : float  — monthly average TIIE 28-day interbank rate
    tiie_mom_change : float  — TIIE absolute change month-over-month (basis points)
    inpc            : float  — INPC consumer price index value
    inpc_mom_pct    : float  — monthly inflation (%)
    inpc_yoy_pct    : float  — year-over-year inflation (%)
    processed_at    : string — UTC timestamp when Gold was written

Join strategy:
    Inner join across all three series — only months where all three have data
    are included. INPC has a 2-month publication lag so the most recent months
    are excluded automatically.

NaN policy:
    First-period NaN values (mom_pct, yoy_pct) are preserved as null.
    Filling with 0 would imply no change, which is semantically incorrect.

Usage:
    python gold.py  # runs daily mode with current UTC timestamp
"""

import io
import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
logger = logging.getLogger("gold")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET_NAME = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------

s3_client = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# Silver readers
# ---------------------------------------------------------------------------


def _read_silver_dataset(dataset: str) -> pd.DataFrame:
    """Read all Silver Parquet partitions for a given dataset from S3.

    Lists all year/month partitions and concatenates them into a single
    DataFrame indexed by date, sorted ascending.

    Args:
        dataset: Silver dataset name. Example: "tipo_de_cambio".

    Returns:
        DataFrame indexed by date with Silver columns. Empty DataFrame
        if no Parquet files found.
    """
    prefix = f"silver/source=banxico/dataset={dataset}/"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not keys:
        logger.warning("No Parquet files found | dataset=%s", dataset)
        return pd.DataFrame()

    dfs = []
    for key in keys:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        buffer = io.BytesIO(obj["Body"].read())
        dfs.append(pd.read_parquet(buffer))

    df = pd.concat(dfs, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").set_index("date")


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def _aggregate_fx(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily FX series to monthly frequency.

    Args:
        df: Daily FX DataFrame indexed by date (datetime).

    Returns:
        Monthly DataFrame with columns: fx_rate (mean USD/MXN),
        fx_volatility (std deviation — exchange rate risk proxy),
        fx_mom_pct (month-over-month change %). First month is NaN by design.
    """
    monthly = df.groupby(pd.Grouper(freq="MS")).agg(
        fx_rate=("value", "mean"),
        fx_volatility=("value", "std"),
    )
    monthly["fx_mom_pct"] = monthly["fx_rate"].pct_change() * 100
    return monthly


def _aggregate_tiie(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily TIIE 28-day series to monthly frequency.

    TIIE includes weekends with the same value as the preceding Friday.
    Monthly averaging naturally smooths this out.

    Args:
        df: Daily TIIE DataFrame indexed by date (datetime).

    Returns:
        Monthly DataFrame with columns: tiie_28 (mean rate),
        tiie_mom_change (absolute change month-over-month). First month is NaN by design.
    """
    monthly = df.groupby(pd.Grouper(freq="MS")).agg(
        tiie_28=("value", "mean"),
    )
    monthly["tiie_mom_change"] = monthly["tiie_28"].diff()
    return monthly


def _aggregate_inpc(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived inflation metrics from monthly INPC index.

    INPC publication lag: Banxico publishes ~10-15 days after month-end.
    The 2 most recent months are typically unavailable and excluded via inner join.

    Args:
        df: Monthly INPC DataFrame indexed by date (datetime).

    Returns:
        Monthly DataFrame with columns: inpc (raw index), inpc_mom_pct
        (month-over-month %), inpc_yoy_pct (year-over-year %).
        First month NaN for mom, first 12 months NaN for yoy — by design.
    """
    monthly = df[["value"]].copy()
    monthly = monthly.rename(columns={"value": "inpc"})
    monthly["inpc_mom_pct"] = monthly["inpc"].pct_change() * 100
    monthly["inpc_yoy_pct"] = monthly["inpc"].pct_change(periods=12) * 100
    return monthly


# ---------------------------------------------------------------------------
# Gold builder
# ---------------------------------------------------------------------------


def _build_gold_dataframe(
    fx: pd.DataFrame,
    tiie: pd.DataFrame,
    inpc: pd.DataFrame,
) -> pd.DataFrame:
    """Join the three monthly-aggregated series into a single Gold DataFrame.

    Uses inner join so only months where all three series have data are included.
    INPC publication lag means the 2 most recent months are excluded automatically.

    NaN values from first-period calculations are preserved — not filled with 0.
    A NaN month-over-month change is semantically different from a 0% change.

    Args:
        fx: Monthly FX DataFrame indexed by date.
        tiie: Monthly TIIE DataFrame indexed by date.
        inpc: Monthly INPC DataFrame indexed by date.

    Returns:
        Unified Gold DataFrame indexed by date, sorted ascending.
    """
    df = fx.join(tiie, how="inner").join(inpc, how="inner")
    return df.sort_index()


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
def _register_gold_partition(execution_date: str) -> None:
    """Register a Gold partition in Glue Data Catalog after writing Parquet.

    Eliminates the need for MSCK REPAIR TABLE or a Glue Crawler.
    AlreadyExistsException is silently ignored — the call is idempotent
    so re-running the pipeline for the same execution_date is safe.

    Args:
        execution_date: Pipeline run date in YYYY-MM-DD format.
    """
    database_name = os.getenv("GLUE_DATABASE", "banxico-pipeline-dev")

    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName="gold_macro_indicators",
            PartitionInput={
                "Values": [execution_date],
                "StorageDescriptor": {
                    "Location": (
                        f"s3://{BUCKET_NAME}/gold/source=banxico/"
                        f"dataset=macro_indicators/"
                        f"execution_date={execution_date}/"
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
            "Partition registered | table=gold_macro_indicators | execution_date=%s",
            execution_date,
        )

    except glue_client.exceptions.AlreadyExistsException:
        pass


def _write_gold_parquet(
    df: pd.DataFrame,
    execution_date: datetime,
) -> None:
    """Write the Gold DataFrame to S3 as Parquet, partitioned by execution_date.

    Each pipeline run writes a new partition — preserves full audit history
    without over-partitioning by business date given the small dataset size.

    Args:
        df: Gold DataFrame indexed by date. Must match Gold schema.
        execution_date: Wall-clock time when the pipeline started.
    """
    execution_date_str = execution_date.strftime("%Y-%m-%d")

    df = df.copy().reset_index()
    df["processed_at"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Convert date index to Python date — required for pa.date32() compatibility with Athena.
    df["date"] = df["date"].dt.date

    schema = pa.schema(
        [
            pa.field("date", pa.date32()),
            pa.field("fx_rate", pa.float64()),
            pa.field("fx_volatility", pa.float64()),
            pa.field("fx_mom_pct", pa.float64()),
            pa.field("tiie_28", pa.float64()),
            pa.field("tiie_mom_change", pa.float64()),
            pa.field("inpc", pa.float64()),
            pa.field("inpc_mom_pct", pa.float64()),
            pa.field("inpc_yoy_pct", pa.float64()),
            pa.field("processed_at", pa.string()),
        ]
    )

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    s3_key = (
        f"gold/source=banxico/"
        f"dataset=macro_indicators/"
        f"execution_date={execution_date_str}/"
        f"data.parquet"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    _register_gold_partition(execution_date_str)

    logger.info(
        "Gold write complete | execution_date=%s | rows=%d",
        execution_date_str,
        len(df),
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_gold(mode: str, execution_date: datetime) -> None:
    """Entry point for the Gold aggregation step.

    Reads all Silver datasets, aggregates to monthly frequency, joins into
    a single macro indicators dataset and writes to S3 Gold layer.

    Gold is always fully regenerated regardless of mode — the dataset is small
    (~40 rows) and aggregation is fast. No incremental logic needed.

    Args:
        mode: "daily" or "backfill" — passed by pipeline for consistency.
            Gold ignores mode and always regenerates the full dataset.
        execution_date: Wall-clock time when the pipeline started.
    """
    logger.info(
        "Reading Silver datasets | execution_date=%s",
        execution_date.strftime("%Y-%m-%d"),
    )

    fx_raw = _read_silver_dataset("tipo_de_cambio")
    tiie_raw = _read_silver_dataset("tiie_28")
    inpc_raw = _read_silver_dataset("inpc")

    if any(df.empty for df in [fx_raw, tiie_raw, inpc_raw]):
        logger.error("One or more Silver datasets are empty — aborting Gold generation")
        raise RuntimeError("Cannot build Gold: one or more Silver datasets are empty")

    fx_monthly = _aggregate_fx(fx_raw)
    tiie_monthly = _aggregate_tiie(tiie_raw)
    inpc_monthly = _aggregate_inpc(inpc_raw)

    gold = _build_gold_dataframe(fx_monthly, tiie_monthly, inpc_monthly)

    logger.info(
        "Gold DataFrame built | rows=%d | %s → %s",
        len(gold),
        gold.index.min().date(),
        gold.index.max().date(),
    )

    _write_gold_parquet(gold, execution_date)


if __name__ == "__main__":
    execution_date = datetime.now(tz=timezone.utc)
    run_gold(mode="daily", execution_date=execution_date)