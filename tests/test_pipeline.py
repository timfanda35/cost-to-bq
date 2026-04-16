import io
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from src.pipeline import run_pipeline
from src.sources.base import ObjectMeta


def _make_env():
    return {
        "SOURCE_TYPE": "s3",
        "SOURCE_BUCKET": "src-bucket",
        "SOURCE_PREFIX": "exports/",
        "GCS_BUCKET": "dest-bucket",
        "GCS_DESTINATION_PREFIX": "billing/",
        "BQ_PROJECT_ID": "my-project",
        "BQ_DATASET_ID": "billing",
        "BQ_TABLE_ID": "daily",
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "id",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }


def test_pipeline_s3_success(monkeypatch):
    for k, v in _make_env().items():
        monkeypatch.setenv(k, v)

    meta = ObjectMeta(key="exports/b.parquet",
                      last_modified=datetime(2024, 4, 15, tzinfo=timezone.utc))
    buf = io.BytesIO(b"PAR1")

    s3_source = MagicMock()
    s3_source.find_latest.return_value = meta
    s3_source.download.return_value = buf

    with patch("src.pipeline.S3Source", return_value=s3_source) as mock_s3, \
         patch("src.pipeline.upload_to_gcs", return_value="gs://dest-bucket/billing/b.parquet") as mock_gcs, \
         patch("src.pipeline.run_load_job") as mock_bq:
        result = run_pipeline()

    mock_s3.assert_called_once()
    s3_source.find_latest.assert_called_once()
    s3_source.download.assert_called_once_with("exports/b.parquet")
    mock_gcs.assert_called_once_with(buf, gcs_bucket="dest-bucket", dest_blob_name="billing/b.parquet")
    mock_bq.assert_called_once_with(
        gcs_uri="gs://dest-bucket/billing/b.parquet",
        project_id="my-project",
        dataset_id="billing",
        table_id="daily",
    )
    assert result["source_key"] == "exports/b.parquet"
    assert result["gcs_uri"] == "gs://dest-bucket/billing/b.parquet"
    assert result["last_modified"] == "2024-04-15T00:00:00+00:00"
    assert result["bq_table"] == "my-project.billing.daily"


def test_pipeline_azure_success(monkeypatch):
    env = _make_env()
    env["SOURCE_TYPE"] = "azure"
    env["AZURE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=https;..."
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_REGION", raising=False)

    meta = ObjectMeta(key="exports/b.parquet",
                      last_modified=datetime(2024, 4, 15, tzinfo=timezone.utc))
    buf = io.BytesIO(b"PAR1")

    az_source = MagicMock()
    az_source.find_latest.return_value = meta
    az_source.download.return_value = buf

    with patch("src.pipeline.AzureSource", return_value=az_source), \
         patch("src.pipeline.upload_to_gcs", return_value="gs://dest-bucket/billing/b.parquet"), \
         patch("src.pipeline.run_load_job"):
        result = run_pipeline()

    az_source.find_latest.assert_called_once()
    az_source.download.assert_called_once_with("exports/b.parquet")
    assert result["source_key"] == "exports/b.parquet"
    assert result["bq_table"] == "my-project.billing.daily"
