from unittest.mock import MagicMock, patch
import pytest
from src.bigquery import run_load_job


def _mock_job(error=None):
    job = MagicMock()
    job.result.return_value = None
    if error:
        job.errors = [{"message": error}]
    else:
        job.errors = []
    return job


def test_load_job_succeeds():
    job = _mock_job()
    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value = job

    with patch("src.bigquery.bigquery.Client", return_value=bq_client):
        run_load_job(
            gcs_uri="gs://bucket/billing/2024-04-15.parquet",
            project_id="my-project",
            dataset_id="billing",
            table_id="daily",
        )

    bq_client.load_table_from_uri.assert_called_once()
    args, kwargs = bq_client.load_table_from_uri.call_args
    assert args[0] == "gs://bucket/billing/2024-04-15.parquet"
    assert args[1] == "my-project.billing.daily"
    job.result.assert_called_once()


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
