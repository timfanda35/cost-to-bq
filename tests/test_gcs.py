import io
from unittest.mock import MagicMock, patch
from src.gcs import upload_to_gcs


def test_upload_returns_gcs_uri():
    buf = io.BytesIO(b"PAR1\x00\x00")
    blob_mock = MagicMock()
    bucket_mock = MagicMock()
    bucket_mock.blob.return_value = blob_mock
    gcs_client_mock = MagicMock()
    gcs_client_mock.bucket.return_value = bucket_mock

    with patch("src.gcs.storage.Client", return_value=gcs_client_mock):
        uri = upload_to_gcs(buf, gcs_bucket="dest-bucket", dest_blob_name="billing/2024-04-15.parquet")

    blob_mock.upload_from_file.assert_called_once_with(buf, rewind=True)
    assert uri == "gs://dest-bucket/billing/2024-04-15.parquet"


def test_upload_rewinds_buffer():
    buf = io.BytesIO(b"PAR1\x00\x00")
    buf.read()  # advance position
    blob_mock = MagicMock()
    bucket_mock = MagicMock()
    bucket_mock.blob.return_value = blob_mock
    gcs_client_mock = MagicMock()
    gcs_client_mock.bucket.return_value = bucket_mock

    with patch("src.gcs.storage.Client", return_value=gcs_client_mock):
        upload_to_gcs(buf, gcs_bucket="dest-bucket", dest_blob_name="billing/x.parquet")

    # rewind=True in the call is sufficient; validate the call was made
    _, kwargs = blob_mock.upload_from_file.call_args
    assert kwargs.get("rewind") is True
