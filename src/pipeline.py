import logging
from datetime import date, datetime, timezone

from src.config import Config
from src.sources.s3 import S3Source
from src.gcs import upload_to_gcs
from src.bigquery import run_load_job

logger = logging.getLogger(__name__)


def billing_periods(today: date | None = None) -> list[date]:
    """Return the first day of the current month and the previous two months."""
    if today is None:
        today = date.today()
    periods = []
    for offset in (2, 1, 0):
        month = today.month - offset
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        periods.append(date(year, month, 1))
    return periods


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


def run_pipeline(export_name: str | None = None, partition: str | None = None) -> dict:
    cfg = Config()
    resolved_export_name = export_name or cfg.export_name

    source = S3Source(
        bucket=cfg.source_bucket,
        prefix=cfg.source_prefix,
        region=cfg.aws_region,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
    )

    now = datetime.now(timezone.utc)
    run_id = f"{now.strftime('%Y%m%d')}-{int(now.timestamp())}"

    if partition is not None:
        parsed = datetime.strptime(partition, "%Y-%m").date().replace(day=1)
        periods = [parsed]
    else:
        periods = billing_periods(now.date())

    results = []
    for period in periods:
        period_label = f"BILLING_PERIOD={period.strftime('%Y-%m')}"
        s3_prefix = _join(cfg.source_prefix, resolved_export_name, "data", period_label) + "/"
        gcs_base = _join(cfg.gcs_destination_prefix, resolved_export_name, "data", run_id, period_label)

        try:
            objects = source.list_partition(s3_prefix)
        except FileNotFoundError:
            logger.warning("No files found for month %s in S3; skipping", period_label)
            continue
        gcs_uris = []
        for obj in objects:
            filename = obj.key.rsplit("/", 1)[-1]
            dest_blob = f"{gcs_base}/{filename}"
            gcs_uri = upload_to_gcs(source.stream(obj.key), cfg.gcs_bucket, dest_blob)
            gcs_uris.append(gcs_uri)

        wildcard = f"gs://{cfg.gcs_bucket}/{gcs_base}/*.parquet"
        run_load_job(
            gcs_uri=wildcard,
            project_id=cfg.bq_project_id,
            dataset_id=cfg.bq_dataset_id,
            table_id=cfg.bq_table_id,
            partition_date=period,
        )
        results.append({
            "partition": period_label,
            "files": len(gcs_uris),
            "gcs_uris": gcs_uris,
        })

    return {
        "run_id": run_id,
        "export_name": resolved_export_name,
        "periods": results,
        "bq_table": f"{cfg.bq_project_id}.{cfg.bq_dataset_id}.{cfg.bq_table_id}",
    }
