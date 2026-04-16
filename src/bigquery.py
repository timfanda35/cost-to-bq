from google.cloud import bigquery


def run_load_job(gcs_uri: str, project_id: str, dataset_id: str, table_id: str) -> None:
    """Load a parquet file from GCS into BigQuery (WRITE_TRUNCATE)."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()  # blocks until complete

    if job.errors:
        raise RuntimeError(f"BigQuery load job failed: {job.errors}")
