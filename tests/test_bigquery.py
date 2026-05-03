import logging
from datetime import date
from unittest.mock import MagicMock, patch
import pytest
from google.cloud import bigquery
from src.bigquery import run_load_job, CUR2_SCHEMA, FOCUS12_SCHEMA


def _mock_job(error=None):
    job = MagicMock()
    job.job_id = "test-job-id-123"
    job.output_rows = 42000
    job.output_bytes = 1048576
    job.result.return_value = None
    job.errors = [{"message": error}] if error else []
    return job


def test_load_job_succeeds():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client) as mock_client_cls:
        run_load_job(
            gcs_uri="gs://bucket/billing/2024-04-15.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            schema_config=CUR2_SCHEMA,
        )
    mock_client_cls.assert_called_once_with(project="my-project")

    args, kwargs = bq_client.load_table_from_uri.call_args
    assert args[0] == "gs://bucket/billing/2024-04-15.parquet"
    assert args[1] == "my-project.billing.daily"
    assert kwargs["job_config"].source_format == bigquery.SourceFormat.PARQUET
    assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert kwargs["job_config"].time_partitioning.type_ == bigquery.TimePartitioningType.MONTH
    assert kwargs["job_config"].time_partitioning.field == "bill_billing_period_start_date"
    assert kwargs["job_config"].clustering_fields == ["line_item_usage_start_date", "line_item_usage_account_id"]
    bq_client.schema_from_json.assert_called_once_with(CUR2_SCHEMA.schema_path)
    job.result.assert_called_once_with(timeout=3300)


def test_load_job_with_partition_date_uses_decorator():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/billing/BILLING_PERIOD=2024-02/**",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            partition_date=date(2024, 2, 1),
            schema_config=CUR2_SCHEMA,
        )

    args, _ = bq_client.load_table_from_uri.call_args
    assert args[1] == "my-project.billing.daily$202402"


def test_load_job_raises_on_error():
    job = _mock_job(error="Bad schema")
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        with pytest.raises(RuntimeError, match="BigQuery load job failed"):
            run_load_job(
                gcs_uri="gs://bucket/billing/2024-04-15.parquet",
                project_id="my-project",
                dataset_id="billing",
                table_id="daily",
                schema_config=CUR2_SCHEMA,
            )
    job.result.assert_called_once()


def test_load_job_logs_submitted_and_complete(caplog):
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with caplog.at_level(logging.INFO, logger="src.bigquery"), \
         patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/path/*.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            run_id="20260423-123",
            export_name="my-export",
            partition_label="2026-04",
            schema_config=CUR2_SCHEMA,
        )

    submitted = [r for r in caplog.records if getattr(r, "log_event", None) == "bq.job.submitted"]
    assert len(submitted) == 1
    assert submitted[0].job_id == "test-job-id-123"
    assert submitted[0].run_id == "20260423-123"

    complete = [r for r in caplog.records if getattr(r, "log_event", None) == "bq.job.complete"]
    assert len(complete) == 1
    assert complete[0].output_rows == 42000
    assert complete[0].output_bytes == 1048576


def test_load_job_sets_cmek_when_provided():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job
    kms_key = "projects/my-project/locations/us/keyRings/ring/cryptoKeys/key"

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/billing/2024-04-15.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            kms_key_name=kms_key,
            schema_config=CUR2_SCHEMA,
        )

    _, kwargs = bq_client.load_table_from_uri.call_args
    enc = kwargs["job_config"].destination_encryption_configuration
    assert enc.kms_key_name == kms_key


def test_load_job_no_cmek_by_default():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/billing/2024-04-15.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            schema_config=CUR2_SCHEMA,
        )

    _, kwargs = bq_client.load_table_from_uri.call_args
    assert kwargs["job_config"].destination_encryption_configuration is None


def test_load_job_logs_failed_on_error(caplog):
    job = _mock_job(error="Bad schema")
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with caplog.at_level(logging.ERROR, logger="src.bigquery"), \
         patch("src.bigquery.bigquery.Client", return_value=bq_client):
        with pytest.raises(RuntimeError):
            run_load_job(
                gcs_uri="gs://bucket/path/*.parquet",
                project_id="my-project",
                dataset_id="billing",
                table_id="daily",
                run_id="20260423-123",
                export_name="my-export",
                partition_label="2026-04",
                schema_config=CUR2_SCHEMA,
            )

    failed = [r for r in caplog.records if getattr(r, "log_event", None) == "bq.job.failed"]
    assert len(failed) == 1
    assert failed[0].job_id == "test-job-id-123"
    assert any("Bad schema" in str(e) for e in failed[0].errors)


def test_load_job_focus12_uses_correct_schema():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/billing/2024-04-15.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
            schema_config=FOCUS12_SCHEMA,
        )

    _, kwargs = bq_client.load_table_from_uri.call_args
    assert kwargs["job_config"].time_partitioning.field == "BillingPeriodStart"
    assert kwargs["job_config"].clustering_fields == ["BillingAccountId"]
    bq_client.schema_from_json.assert_called_once_with(FOCUS12_SCHEMA.schema_path)
