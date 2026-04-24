import logging
import time
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
        endpoint_url=cfg.s3_endpoint_url,
    )

    now = datetime.now(timezone.utc)
    run_id = f"{now.strftime('%Y%m%d')}-{int(now.timestamp())}"
    start_time = time.monotonic()

    if partition is not None:
        parsed = datetime.strptime(partition, "%Y-%m").date().replace(day=1)
        periods = [parsed]
    else:
        periods = billing_periods(now.date())

    logger.info("pipeline.started", extra={
        "log_event": "pipeline.started",
        "run_id": run_id,
        "export_name": resolved_export_name,
        "periods": [p.strftime("%Y-%m") for p in periods],
    })

    periods_loaded = 0
    periods_skipped = 0
    results = []

    try:
        for period in periods:
            period_label = f"BILLING_PERIOD={period.strftime('%Y-%m')}"
            partition_str = period.strftime("%Y-%m")
            s3_prefix = _join(cfg.source_prefix, resolved_export_name, "data", period_label) + "/"
            gcs_base = _join(cfg.gcs_destination_prefix, resolved_export_name, "data", run_id, period_label)
            period_start = time.monotonic()

            logger.info("period.started", extra={
                "log_event": "period.started",
                "run_id": run_id,
                "export_name": resolved_export_name,
                "partition": partition_str,
                "s3_prefix": s3_prefix,
            })

            try:
                objects = source.list_partition(s3_prefix)
            except FileNotFoundError:
                logger.warning("period.skipped", extra={
                    "log_event": "period.skipped",
                    "run_id": run_id,
                    "export_name": resolved_export_name,
                    "partition": partition_str,
                    "reason": "no_parquet_files",
                })
                periods_skipped += 1
                continue

            logger.info("period.files_listed", extra={
                "log_event": "period.files_listed",
                "run_id": run_id,
                "export_name": resolved_export_name,
                "partition": partition_str,
                "file_count": len(objects),
            })

            gcs_uris = []
            for obj in objects:
                filename = obj.key.rsplit("/", 1)[-1]
                dest_blob = f"{gcs_base}/{filename}"
                gcs_uri = upload_to_gcs(
                    source.stream(obj.key), cfg.gcs_bucket, dest_blob,
                    run_id=run_id, export_name=resolved_export_name,
                    partition=partition_str, s3_key=obj.key,
                )
                gcs_uris.append(gcs_uri)

            wildcard = f"gs://{cfg.gcs_bucket}/{gcs_base}/*.parquet"
            run_load_job(
                gcs_uri=wildcard,
                project_id=cfg.bq_project_id,
                dataset_id=cfg.bq_dataset_id,
                table_id=cfg.bq_table_id,
                partition_date=period,
                run_id=run_id,
                export_name=resolved_export_name,
                partition_label=partition_str,
            )

            periods_loaded += 1
            logger.info("period.complete", extra={
                "log_event": "period.complete",
                "run_id": run_id,
                "export_name": resolved_export_name,
                "partition": partition_str,
                "files": len(gcs_uris),
                "duration_seconds": round(time.monotonic() - period_start, 2),
            })

            results.append({
                "partition": period_label,
                "files": len(gcs_uris),
                "gcs_uris": gcs_uris,
            })

        logger.info("pipeline.complete", extra={
            "log_event": "pipeline.complete",
            "run_id": run_id,
            "export_name": resolved_export_name,
            "periods_loaded": periods_loaded,
            "periods_skipped": periods_skipped,
            "duration_seconds": round(time.monotonic() - start_time, 2),
        })

        return {
            "run_id": run_id,
            "export_name": resolved_export_name,
            "periods": results,
            "bq_table": f"{cfg.bq_project_id}.{cfg.bq_dataset_id}.{cfg.bq_table_id}",
        }

    except Exception as exc:
        logger.error("pipeline.failed", extra={
            "log_event": "pipeline.failed",
            "run_id": run_id,
            "export_name": resolved_export_name,
            "error": str(exc),
            "duration_seconds": round(time.monotonic() - start_time, 2),
        })
        raise
