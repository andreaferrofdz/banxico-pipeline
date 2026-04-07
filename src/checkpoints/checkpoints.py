import json
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "banxico-pipeline-dev-datalake")

s3_client = boto3.client("s3", region_name=AWS_REGION)


def get_checkpoint_key(dataset: str) -> str:
    """
    Get S3 key for a dataset's checkpoint.
    """
    return f"silver/source=banxico/_checkpoints/{dataset}.json"


def read_checkpoint(dataset: str) -> str | None:
    """
    Read last processed execution_date for a dataset.
    Returns None if checkpoint does not exist (first run).
    """
    s3_key = get_checkpoint_key(dataset)

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        return data.get("last_processed_execution_date")
    except s3_client.exceptions.NoSuchKey:
        return None


def write_checkpoint(dataset: str, execution_date: str) -> None:
    """
    Write last processed execution_date for a dataset.
    Called after Silver successfully processes a dataset.
    """

    s3_key = get_checkpoint_key(dataset)

    data = {
        "dataset": dataset,
        "last_processed_execution_date": execution_date,
        "updated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )


def delete_checkpoint(dataset: str) -> None:
    """
    Delete checkpoint to force full reprocessing of a dataset.
    """
    s3_key = get_checkpoint_key(dataset)
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
