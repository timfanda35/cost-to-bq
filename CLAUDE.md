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

**Source abstraction:** `src/sources/base.py` defines `ObjectMeta` and was designed for multiple source types. Only `S3Source` is implemented.

## Notes

- Tests use `fastapi.testclient.TestClient` (requires `httpx` from `requirements-dev.txt`) and mock at the module boundary (`patch("main.run_pipeline")`)
- The `PORT` env var controls the listen port (default `8080`); the Dockerfile passes it via `--port` to uvicorn
