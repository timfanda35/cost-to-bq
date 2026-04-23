from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
from google.cloud import bigquery
from src.bigquery import run_load_job, _SCHEMA_PATH


def _mock_job(error=None):
    job = MagicMock()
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
        )
    mock_client_cls.assert_called_once_with(project="my-project")

    args, kwargs = bq_client.load_table_from_uri.call_args
    assert args[0] == "gs://bucket/billing/2024-04-15.parquet"
    assert args[1] == "my-project.billing.daily"
    assert kwargs["job_config"].source_format == bigquery.SourceFormat.PARQUET
    assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert kwargs["job_config"].time_partitioning.type_ == bigquery.TimePartitioningType.MONTH
    assert kwargs["job_config"].time_partitioning.field == "bill_billing_period_start_date"
    assert kwargs["job_config"].clustering_fields == ["line_item_usage_start_date"]
    bq_client.schema_from_json.assert_called_once_with(_SCHEMA_PATH)
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
            )
    job.result.assert_called_once()
