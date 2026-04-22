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
pytest tests/test_pipeline.py::test_run_pipeline_success
```

## Architecture

This is a FastAPI service that runs as a Cloud Run job, triggered daily by Cloud Scheduler. It implements a single ETL pipeline:

```
S3  →  GCS (staging)  →  BigQuery (WRITE_TRUNCATE)
```

**Request flow:**
- `POST /run` → `main.py` → `src/pipeline.py::run_pipeline()` → `S3Source.find_latest()` + `S3Source.download()` → `upload_to_gcs()` → `run_load_job()`
- `Config` reads all env vars at pipeline start and fails fast if any required var is missing

**Key behaviors:**
- Only `SOURCE_TYPE=s3` is supported; `Config.__init__` raises `ValueError` for any other value (Azure support was removed)
- BigQuery loads use `WRITE_TRUNCATE` + `autodetect=True` — the entire target table is replaced on each run
- GCS acts purely as a staging area; the most recently modified file in the S3 prefix is always loaded
- `S3Source` uses instance role credentials if `AWS_ACCESS_KEY_ID` is not set

**Source abstraction:** `src/sources/base.py` defines `ObjectMeta` and was designed for multiple source types. Only `S3Source` is implemented.

## Notes

- The README still mentions Flask/gunicorn — the service was migrated to FastAPI/uvicorn; README is outdated in that regard
- Tests use `fastapi.testclient.TestClient` (requires `httpx` from `requirements-dev.txt`) and mock at the module boundary (`patch("main.run_pipeline")`)
- The `PORT` env var controls the listen port (default `8080`); the Dockerfile passes it via `--port` to uvicorn
