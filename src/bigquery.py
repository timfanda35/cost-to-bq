from datetime import date
from google.cloud import bigquery


def run_load_job(
    gcs_uri: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    partition_date: date | None = None,
) -> None:
    """Load parquet file(s) from GCS into BigQuery (WRITE_TRUNCATE).

    When partition_date is given the load targets that specific date partition
    using a BigQuery partition decorator (table$YYYYMMDD), replacing only that
    partition rather than the entire table.
    """
    client = bigquery.Client(project=project_id)
    if partition_date:
        table_ref = f"{project_id}.{dataset_id}.{table_id}${partition_date.strftime('%Y%m%d')}"
    else:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result(timeout=3300)  # blocks until complete

    if job.errors:
        raise RuntimeError(f"BigQuery load job failed: {job.errors}")
