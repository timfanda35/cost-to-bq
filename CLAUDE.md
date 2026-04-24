# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run the server locally
python main.py

# Run all tests
pytest

# Run a single test file
pytest tests/test_pipeline.py

# Run a specific test
pytest tests/test_pipeline.py::test_pipeline_processes_three_billing_periods
```

## Architecture

This is a FastAPI service that runs as a Cloud Run job, triggered daily by Cloud Scheduler. It implements a single ETL pipeline:

```
S3 (CUR Hive partitions)  →  GCS (staging)  →  BigQuery (partitioned WRITE_TRUNCATE)
```

**Request flow:**
- `POST /run` → `main.py` → `src/pipeline.py::run_pipeline()` → `billing_periods()` → per period: `S3Source.list_partition()` → `S3Source.stream()` → `upload_to_gcs()` → `run_load_job(partition_date=...)`
- `Config` reads all env vars at pipeline start and fails fast if any required var is missing

**Key behaviors:**
- Only `SOURCE_TYPE=s3` is supported; `Config.__init__` raises `ValueError` for any other value (Azure support was removed)
- By default each run loads **3 billing periods**: the current month and the previous two, computed by `billing_periods()` in `src/pipeline.py`
- `POST /run` accepts an optional JSON body: `export_name` (overrides `EXPORT_NAME` env var) and `partition` (`YYYY-MM` string; when set, only that single period is processed; missing files are silently skipped with a warning, same as the default multi-period run)
- S3 paths follow the AWS CUR Hive-partition layout: `{SOURCE_PREFIX}/{EXPORT_NAME}/data/BILLING_PERIOD=YYYY-MM/`; all `.parquet` files in each partition are loaded
- BigQuery loads use an explicit schema (`src/bq_schema/aws-cur2-parquet.json`), target a month partition decorator (`table$YYYYMM`) with `WRITE_TRUNCATE`, MONTH partitioning on `bill_billing_period_start_date`, and clustering on `["line_item_usage_start_date", "line_item_usage_account_id"]`
- GCS staging path includes a `run_id` timestamp component: `{GCS_DESTINATION_PREFIX}/{EXPORT_NAME}/data/{run_id}/BILLING_PERIOD=YYYY-MM/`
- `S3Source` uses instance role credentials if `AWS_ACCESS_KEY_ID` is not set
- `S3Source` passes `endpoint_url` to `boto3.client()` when `S3_ENDPOINT_URL` is set, enabling AWS VPC/PrivateLink endpoints; omitting the var uses the default public AWS endpoint

**Source abstraction:** `src/sources/base.py` defines `ObjectMeta` and was designed for multiple source types. Only `S3Source` is implemented.

## Logging

Structured JSON logging is emitted to stdout via `python-json-logger`. Cloud Run captures it automatically in Google Cloud Logging where `jsonPayload` fields are queryable.

Every log line includes `log_event` (dotted name), `run_id`, and `export_name`. Key events:

| `log_event` | Level | When |
|---|---|---|
| `request.received` | INFO | Start of `POST /run` |
| `pipeline.started` | INFO | After run_id generated; includes `periods` list |
| `period.started` / `period.files_listed` / `period.complete` | INFO | Per billing period |
| `period.skipped` | WARNING | S3 partition has no parquet files; includes `reason: "no_parquet_files"` |
| `gcs.file.uploaded` | INFO | After each file uploaded; includes `s3_key`, `gcs_uri` |
| `bq.job.submitted` | INFO | Immediately after BQ job created; includes `job_id` |
| `bq.job.complete` | INFO | After `job.result()` returns; includes `output_rows`, `output_bytes` |
| `bq.job.failed` | ERROR | Before `RuntimeError` is raised; includes `job_id`, `errors` |
| `pipeline.complete` | INFO | After all periods; includes `periods_loaded`, `periods_skipped`, `duration_seconds` |
| `pipeline.failed` | ERROR | Any unhandled exception; re-raises after logging |

Useful Cloud Logging filters:
```
# Full timeline for one run
jsonPayload.run_id="20260423-xxx"

# Audit: every BQ partition loaded (rows, bytes)
jsonPayload.log_event="bq.job.complete"

# All skipped periods
jsonPayload.log_event="period.skipped"
```

Set `LOG_LEVEL=DEBUG` to lower the root log level (default `INFO`).

## Notes

- Tests use `fastapi.testclient.TestClient` (requires `httpx` from `requirements-dev.txt`) and mock at the module boundary (`patch("main.run_pipeline")`)
- The `PORT` env var controls the listen port (default `8080`); the Dockerfile passes it via `--port` to uvicorn
