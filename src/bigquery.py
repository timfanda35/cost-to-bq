import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from google.cloud import bigquery


@dataclass
class SchemaConfig:
    schema_path: Path
    partition_field: str
    cluster_fields: list[str]


CUR2_SCHEMA = SchemaConfig(
    schema_path=Path(__file__).parent / "bq_schema" / "aws-cur-2.0-parquet.json",
    partition_field="bill_billing_period_start_date",
    cluster_fields=["line_item_usage_start_date", "line_item_usage_account_id"],
)

FOCUS12_SCHEMA = SchemaConfig(
    schema_path=Path(__file__).parent / "bq_schema" / "aws-focus-1.2-parquet.json",
    partition_field="BillingPeriodStart",
    cluster_fields=["BillingAccountId"],
)

SCHEMA_MAP: dict[str, SchemaConfig] = {
    "cur2": CUR2_SCHEMA,
    "focus1.2": FOCUS12_SCHEMA,
}

logger = logging.getLogger(__name__)


def run_load_job(
    gcs_uri: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    partition_date: date | None = None,
    *,
    schema_config: SchemaConfig,
    run_id: str = "",
    export_name: str = "",
    partition_label: str = "",
    kms_key_name: str | None = None,
) -> None:
    """Load parquet file(s) from GCS into BigQuery (WRITE_TRUNCATE).

    When partition_date is given the load targets that specific date partition
    using a BigQuery partition decorator (table$YYYYMMDD), replacing only that
    partition rather than the entire table.
    """
    client = bigquery.Client(project=project_id)
    if partition_date:
        table_ref = f"{project_id}.{dataset_id}.{table_id}${partition_date.strftime('%Y%m')}"
    else:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

    schema = client.schema_from_json(schema_config.schema_path)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=schema_config.partition_field,
        ),
        clustering_fields=schema_config.cluster_fields,
    )
    if kms_key_name:
        job_config.destination_encryption_configuration = bigquery.EncryptionConfiguration(
            kms_key_name=kms_key_name
        )

    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    logger.info("bq.job.submitted", extra={
        "log_event": "bq.job.submitted",
        "run_id": run_id,
        "export_name": export_name,
        "partition": partition_label,
        "job_id": job.job_id,
        "gcs_uri": gcs_uri,
        "bq_table": table_ref,
    })

    job.result(timeout=3300)  # blocks until complete

    if job.errors:
        logger.error("bq.job.failed", extra={
            "log_event": "bq.job.failed",
            "run_id": run_id,
            "export_name": export_name,
            "partition": partition_label,
            "job_id": job.job_id,
            "errors": job.errors,
        })
        raise RuntimeError(f"BigQuery load job failed: {job.errors}")

    logger.info("bq.job.complete", extra={
        "log_event": "bq.job.complete",
        "run_id": run_id,
        "export_name": export_name,
        "partition": partition_label,
        "job_id": job.job_id,
        "output_rows": job.output_rows,
        "output_bytes": job.output_bytes,
    })
