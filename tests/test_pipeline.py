import logging
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, call, patch
from src.pipeline import run_pipeline, billing_periods
from src.sources.base import ObjectMeta


def _make_env():
    return {
        "SOURCE_TYPE": "s3",
        "SOURCE_BUCKET": "src-bucket",
        "SOURCE_PREFIX": "exports",
        "EXPORT_NAME": "my-export",
        "GCS_BUCKET": "dest-bucket",
        "GCS_DESTINATION_PREFIX": "billing",
        "BQ_PROJECT_ID": "my-project",
        "BQ_DATASET_ID": "billing",
        "BQ_TABLE_ID": "daily",
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "id",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }


# ── billing_periods unit tests ────────────────────────────────────────────────

def test_billing_periods_returns_three_months():
    result = billing_periods(date(2026, 4, 23))
    assert result == [date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)]


def test_billing_periods_wraps_year_boundary():
    result = billing_periods(date(2026, 1, 15))
    assert result == [date(2025, 11, 1), date(2025, 12, 1), date(2026, 1, 1)]


def test_billing_periods_defaults_to_today():
    # just verify it returns a list of 3 dates without error
    result = billing_periods()
    assert len(result) == 3
    assert all(d.day == 1 for d in result)


# ── run_pipeline integration tests ───────────────────────────────────────────

def _make_obj(partition: str, filename: str) -> ObjectMeta:
    return ObjectMeta(
        key=f"exports/my-export/data/{partition}/{filename}",
        last_modified=datetime(2024, 4, 10, tzinfo=timezone.utc),
        size=100,
    )


def test_pipeline_processes_three_billing_periods(monkeypatch):
    for k, v in _make_env().items():
        monkeypatch.setenv(k, v)

    fixed_now = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    expected_run_id = f"20260423-{int(fixed_now.timestamp())}"

    partitions = [
        "BILLING_PERIOD=2026-02",
        "BILLING_PERIOD=2026-03",
        "BILLING_PERIOD=2026-04",
    ]
    stream_mock = MagicMock()
    s3_source = MagicMock()
    s3_source.list_partition.side_effect = [
        [_make_obj(partitions[0], "part-0.parquet")],
        [_make_obj(partitions[1], "part-0.parquet")],
        [_make_obj(partitions[2], "part-0.parquet")],
    ]
    s3_source.stream.return_value = stream_mock

    def gcs_side_effect(stream, gcs_bucket, dest_blob_name, **kwargs):
        return f"gs://{gcs_bucket}/{dest_blob_name}"

    with patch("src.pipeline.datetime") as mock_dt, \
         patch("src.pipeline.S3Source", return_value=s3_source), \
         patch("src.pipeline.upload_to_gcs", side_effect=gcs_side_effect) as mock_gcs, \
         patch("src.pipeline.run_load_job") as mock_bq:
        mock_dt.now.return_value = fixed_now
        result = run_pipeline()

    assert result["run_id"] == expected_run_id
    assert result["bq_table"] == "my-project.billing.daily"
    assert len(result["periods"]) == 3

    # verify list_partition called with correct S3 prefixes
    assert s3_source.list_partition.call_count == 3
    list_calls = [c.args[0] for c in s3_source.list_partition.call_args_list]
    assert list_calls[0] == f"exports/my-export/data/BILLING_PERIOD=2026-02/"
    assert list_calls[1] == f"exports/my-export/data/BILLING_PERIOD=2026-03/"
    assert list_calls[2] == f"exports/my-export/data/BILLING_PERIOD=2026-04/"

    # verify GCS destination paths include run_id and partition subfolder
    gcs_calls = [c.args[2] for c in mock_gcs.call_args_list]
    for i, partition in enumerate(partitions):
        assert f"my-export/data/{expected_run_id}/{partition}/part-0.parquet" in gcs_calls[i]

    # verify BigQuery called with wildcard + partition decorator dates + context fields
    assert mock_bq.call_count == 3
    bq_calls = mock_bq.call_args_list
    assert bq_calls[0].kwargs["partition_date"] == date(2026, 2, 1)
    assert bq_calls[1].kwargs["partition_date"] == date(2026, 3, 1)
    assert bq_calls[2].kwargs["partition_date"] == date(2026, 4, 1)
    for c in bq_calls:
        assert c.kwargs["gcs_uri"].endswith("/*.parquet")
        assert c.kwargs["run_id"] == expected_run_id
        assert c.kwargs["export_name"] == "my-export"


def test_pipeline_result_contains_per_period_info(monkeypatch):
    for k, v in _make_env().items():
        monkeypatch.setenv(k, v)

    fixed_now = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    s3_source = MagicMock()
    s3_source.list_partition.return_value = [
        _make_obj("BILLING_PERIOD=2026-02", "part-0.parquet"),
        _make_obj("BILLING_PERIOD=2026-02", "part-1.parquet"),
    ]

    with patch("src.pipeline.datetime") as mock_dt, \
         patch("src.pipeline.S3Source", return_value=s3_source), \
         patch("src.pipeline.upload_to_gcs", return_value="gs://dest-bucket/some/path"), \
         patch("src.pipeline.run_load_job"):
        mock_dt.now.return_value = fixed_now
        result = run_pipeline()

    # each period entry reports correct file count
    for period_result in result["periods"]:
        assert period_result["files"] == 2
        assert len(period_result["gcs_uris"]) == 2
        assert "partition" in period_result


def test_pipeline_logs_started_and_complete(monkeypatch, caplog):
    for k, v in _make_env().items():
        monkeypatch.setenv(k, v)

    fixed_now = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    expected_run_id = f"20260423-{int(fixed_now.timestamp())}"
    s3_source = MagicMock()
    s3_source.list_partition.return_value = [_make_obj("BILLING_PERIOD=2026-04", "part-0.parquet")]

    with caplog.at_level(logging.INFO, logger="src.pipeline"), \
         patch("src.pipeline.datetime") as mock_dt, \
         patch("src.pipeline.S3Source", return_value=s3_source), \
         patch("src.pipeline.upload_to_gcs", return_value="gs://dest-bucket/some/path"), \
         patch("src.pipeline.run_load_job"):
        mock_dt.now.return_value = fixed_now
        run_pipeline()

    started = [r for r in caplog.records if getattr(r, "log_event", None) == "pipeline.started"]
    assert len(started) == 1
    assert started[0].run_id == expected_run_id
    assert "2026-02" in started[0].periods
    assert "2026-04" in started[0].periods

    complete = [r for r in caplog.records if getattr(r, "log_event", None) == "pipeline.complete"]
    assert len(complete) == 1
    assert complete[0].periods_loaded == 3
    assert complete[0].periods_skipped == 0


def test_pipeline_logs_period_skipped(monkeypatch, caplog):
    for k, v in _make_env().items():
        monkeypatch.setenv(k, v)

    fixed_now = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
    s3_source = MagicMock()
    s3_source.list_partition.side_effect = FileNotFoundError("no files")

    with caplog.at_level(logging.WARNING, logger="src.pipeline"), \
         patch("src.pipeline.datetime") as mock_dt, \
         patch("src.pipeline.S3Source", return_value=s3_source), \
         patch("src.pipeline.upload_to_gcs"), \
         patch("src.pipeline.run_load_job"):
        mock_dt.now.return_value = fixed_now
        run_pipeline()

    skipped = [r for r in caplog.records if getattr(r, "log_event", None) == "period.skipped"]
    assert len(skipped) == 3
    assert all(r.reason == "no_parquet_files" for r in skipped)
    assert {r.partition for r in skipped} == {"2026-02", "2026-03", "2026-04"}
