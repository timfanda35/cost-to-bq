import io
from unittest.mock import MagicMock, patch
from src.sources.s3 import S3Source


def _make_source():
    return S3Source(
        bucket="my-bucket",
        prefix="exports/",
        region="us-east-1",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
    )


def test_find_latest_returns_newest_key():
    source = _make_source()
    objects = [
        {"Key": "exports/a.parquet", "LastModified": __import__("datetime").datetime(2024, 4, 10)},
        {"Key": "exports/b.parquet", "LastModified": __import__("datetime").datetime(2024, 4, 15)},
        {"Key": "exports/c.parquet", "LastModified": __import__("datetime").datetime(2024, 4, 5)},
    ]
    with patch.object(source._client, "list_objects_v2", return_value={"Contents": objects}):
        meta = source.find_latest()
    assert meta.key == "exports/b.parquet"


def test_find_latest_raises_when_no_objects():
    source = _make_source()
    with patch.object(source._client, "list_objects_v2", return_value={}):
        import pytest
        with pytest.raises(FileNotFoundError, match="No objects"):
            source.find_latest()


def test_download_returns_bytesio():
    source = _make_source()
    body_mock = MagicMock()
    body_mock.read.return_value = b"PAR1\x00\x00"
    with patch.object(source._client, "get_object", return_value={"Body": body_mock}):
        buf = source.download("exports/b.parquet")
    assert isinstance(buf, io.BytesIO)
    assert buf.read() == b"PAR1\x00\x00"
