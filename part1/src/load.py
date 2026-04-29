from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from src.schemas import FACEBOOK_ADS_BIGQUERY_SCHEMA

logger = logging.getLogger(__name__)


def write_jsonl(rows: list[dict[str, Any]], output_path: str | Path) -> None:
    """
    Write normalized records to JSONL file.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info(f"Wrote {len(rows)} records to {path}")

def load_to_bigquery_stub(rows: list[dict[str, Any]], settings: dict[str, Any]) -> None:
    """
    Load normalized records into BigQuery table.
    
    Note: This is an example of how the loading function could look like.

    Requires google-cloud-bigquery library and proper authentication setup.
    """
    from google.cloud import bigquery

    bq_settings = settings["bigquery"]

    project_id = bq_settings["project_id"]
    dataset_id = bq_settings["dataset_id"]
    table_id = bq_settings["table_id"]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                field["name"],
                field["type"],
                mode=field.get("mode", "NULLABLE"),
            )
            for field in FACEBOOK_ADS_BIGQUERY_SCHEMA
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config,
    )

    load_job.result()

    logger.info("Loaded %s rows into BigQuery table %s", len(rows), table_ref)