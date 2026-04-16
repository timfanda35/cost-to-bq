import io
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from src.sources.azure import AzureSource


def _make_source():
    return AzureSource(
        container="my-container",
        prefix="exports/",
        connection_string="DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;EndpointSuffix=core.windows.net",
    )


def _blob(name, modified):
    b = MagicMock()
    b.name = name
    b.last_modified = modified
    b.size = 100
    return b


def test_find_latest_returns_newest_key():
    source = _make_source()
    blobs = [
        _blob("exports/a.parquet", datetime(2024, 4, 10, tzinfo=timezone.utc)),
        _blob("exports/b.parquet", datetime(2024, 4, 15, tzinfo=timezone.utc)),
        _blob("exports/c.parquet", datetime(2024, 4, 5, tzinfo=timezone.utc)),
    ]
    with patch.object(source._container_client, "list_blobs", return_value=iter(blobs)):
        meta = source.find_latest()
    assert meta.key == "exports/b.parquet"


def test_find_latest_raises_when_empty():
    source = _make_source()
    with patch.object(source._container_client, "list_blobs", return_value=iter([])):
        with pytest.raises(FileNotFoundError, match="No blobs"):
            source.find_latest()


def test_download_returns_bytesio():
    source = _make_source()
    downloader = MagicMock()
    downloader.readall.return_value = b"PAR1\x00\x00"
    blob_client = MagicMock()
    blob_client.download_blob.return_value = downloader
    with patch.object(source._container_client, "get_blob_client", return_value=blob_client):
        buf = source.download("exports/b.parquet")
    assert isinstance(buf, io.BytesIO)
    assert buf.read() == b"PAR1\x00\x00"
