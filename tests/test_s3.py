import io
import pytest
from datetime import datetime
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
        {"Key": "exports/a.parquet", "LastModified": datetime(2024, 4, 10)},
        {"Key": "exports/b.parquet", "LastModified": datetime(2024, 4, 15)},
        {"Key": "exports/c.parquet", "LastModified": datetime(2024, 4, 5)},
    ]
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{"Contents": objects}])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        meta = source.find_latest()
    assert meta.key == "exports/b.parquet"


def test_find_latest_raises_when_no_objects():
    source = _make_source()
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{}])  # page with no Contents
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        with pytest.raises(FileNotFoundError, match="No objects"):
            source.find_latest()


def test_find_latest_handles_pagination():
    source = _make_source()
    page1 = {"Contents": [
        {"Key": "exports/a.parquet", "LastModified": datetime(2024, 4, 10)},
    ]}
    page2 = {"Contents": [
        {"Key": "exports/b.parquet", "LastModified": datetime(2024, 4, 15)},
    ]}
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([page1, page2])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        meta = source.find_latest()
    assert meta.key == "exports/b.parquet"  # newest from page 2


def test_download_returns_bytesio():
    source = _make_source()
    body_mock = MagicMock()
    body_mock.read.return_value = b"PAR1\x00\x00"
    with patch.object(source._client, "get_object", return_value={"Body": body_mock}):
        buf = source.download("exports/b.parquet")
    assert isinstance(buf, io.BytesIO)
    assert buf.read() == b"PAR1\x00\x00"


def test_init_raises_when_key_id_given_without_secret():
    with pytest.raises(ValueError, match="aws_secret_access_key"):
        S3Source(bucket="b", prefix="p", region="us-east-1", aws_access_key_id="id", aws_secret_access_key=None)
