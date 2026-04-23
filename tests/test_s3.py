import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.sources.s3 import S3Source
from src.sources.base import ObjectMeta


def _make_source():
    return S3Source(
        bucket="my-bucket",
        prefix="exports/",
        region="us-east-1",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
    )


def test_list_partition_returns_parquet_files():
    source = _make_source()
    objects = [
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/part-0.parquet", "LastModified": datetime(2024, 4, 10), "Size": 100},
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/part-1.parquet", "LastModified": datetime(2024, 4, 11), "Size": 200},
    ]
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{"Contents": objects}])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        result = source.list_partition("exports/my-export/data/BILLING_PERIOD=2024-04/")
    assert len(result) == 2
    assert all(isinstance(o, ObjectMeta) for o in result)
    assert result[0].key.endswith("part-0.parquet")
    assert result[1].key.endswith("part-1.parquet")


def test_list_partition_skips_non_parquet_files():
    source = _make_source()
    objects = [
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/manifest.json", "LastModified": datetime(2024, 4, 10), "Size": 50},
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/part-0.parquet", "LastModified": datetime(2024, 4, 10), "Size": 100},
    ]
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{"Contents": objects}])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        result = source.list_partition("exports/my-export/data/BILLING_PERIOD=2024-04/")
    assert len(result) == 1
    assert result[0].key.endswith(".parquet")


def test_list_partition_raises_when_no_parquet_files():
    source = _make_source()
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{"Contents": [
        {"Key": "exports/manifest.json", "LastModified": datetime(2024, 4, 10), "Size": 10},
    ]}])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        with pytest.raises(FileNotFoundError, match="No parquet files"):
            source.list_partition("exports/my-export/data/BILLING_PERIOD=2024-04/")


def test_list_partition_raises_when_empty():
    source = _make_source()
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([{}])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        with pytest.raises(FileNotFoundError):
            source.list_partition("exports/my-export/data/BILLING_PERIOD=2024-04/")


def test_list_partition_handles_pagination():
    source = _make_source()
    page1 = {"Contents": [
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/part-0.parquet", "LastModified": datetime(2024, 4, 10), "Size": 100},
    ]}
    page2 = {"Contents": [
        {"Key": "exports/my-export/data/BILLING_PERIOD=2024-04/part-1.parquet", "LastModified": datetime(2024, 4, 11), "Size": 200},
    ]}
    page_mock = MagicMock()
    page_mock.paginate.return_value = iter([page1, page2])
    with patch.object(source._client, "get_paginator", return_value=page_mock):
        result = source.list_partition("exports/my-export/data/BILLING_PERIOD=2024-04/")
    assert len(result) == 2


def test_stream_returns_streaming_body():
    source = _make_source()
    body_mock = MagicMock()
    with patch.object(source._client, "get_object", return_value={"Body": body_mock}):
        result = source.stream("exports/my-export/data/BILLING_PERIOD=2024-04/part-0.parquet")
    assert result is body_mock


def test_init_raises_when_key_id_given_without_secret():
    with pytest.raises(ValueError, match="aws_secret_access_key"):
        S3Source(bucket="b", prefix="p", region="us-east-1", aws_access_key_id="id", aws_secret_access_key=None)
