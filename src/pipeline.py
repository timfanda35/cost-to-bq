from src.config import Config
from src.sources.s3 import S3Source
from src.gcs import upload_to_gcs
from src.bigquery import run_load_job


def run_pipeline() -> dict:
    cfg = Config()

    source = S3Source(
        bucket=cfg.source_bucket,
        prefix=cfg.source_prefix,
        region=cfg.aws_region,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
    )

    # Discover & download latest file
    meta = source.find_latest()
    buf = source.download(meta.key)

    # Derive destination blob name (strip leading path, keep filename)
    filename = meta.key.split("/")[-1]
    dest_blob_name = f"{cfg.gcs_destination_prefix.rstrip('/')}/{filename}" if cfg.gcs_destination_prefix else filename

    # Upload to GCS
    gcs_uri = upload_to_gcs(buf, gcs_bucket=cfg.gcs_bucket, dest_blob_name=dest_blob_name)

    # Load into BigQuery
    run_load_job(
        gcs_uri=gcs_uri,
        project_id=cfg.bq_project_id,
        dataset_id=cfg.bq_dataset_id,
        table_id=cfg.bq_table_id,
    )

    return {
        "source_key": meta.key,
        "last_modified": meta.last_modified.isoformat(),
        "gcs_uri": gcs_uri,
        "bq_table": f"{cfg.bq_project_id}.{cfg.bq_dataset_id}.{cfg.bq_table_id}",
    }
