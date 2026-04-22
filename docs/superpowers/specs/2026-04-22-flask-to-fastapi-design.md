# Flask to FastAPI Refactor — Design Spec

**Date:** 2026-04-22
**Approach:** Option A — Minimal in-place swap

## Overview

Replace Flask with FastAPI across `main.py`, `requirements.txt`, `Dockerfile`, and `tests/test_main.py`. No changes to `src/`. Business logic (`run_pipeline`) remains synchronous.

## main.py

Replace `Flask` import and app construction with `FastAPI`. Routes map 1:1:

- `GET /health` — returns `{"status": "ok"}` with HTTP 200 (FastAPI default).
- `POST /run` — calls `run_pipeline()`, returns the result dict on success (HTTP 200), or `JSONResponse(status_code=500, content={"error": str(exc)})` on exception.

FastAPI handles JSON serialization automatically for dict return values. `jsonify` is removed.

## requirements.txt

Remove:
- `Flask==3.0.3`
- `gunicorn==21.2.0`

Add:
- `fastapi==0.111.0`
- `uvicorn[standard]==0.29.0`

`uvicorn[standard]` includes `httptools` and `uvloop` for production performance. All other dependencies (`boto3`, `google-cloud-storage`, `google-cloud-bigquery`) remain unchanged.

## Dockerfile

Change `CMD` from gunicorn to uvicorn:

```dockerfile
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--timeout-keep-alive", "300"]
```

`--timeout-keep-alive 300` preserves the existing 300-second timeout behavior.

## tests/test_main.py

Replace Flask's `app.test_client()` fixture with FastAPI's `TestClient`:

```python
from fastapi.testclient import TestClient
from main import app

@pytest.fixture
def client():
    return TestClient(app)
```

The three test cases (`test_run_returns_200_on_success`, `test_run_returns_500_on_error`, `test_health_check`) keep their assertions and `patch` targets unchanged. Only the fixture and imports change.

## requirements-dev.txt

Add `httpx` as an explicit dependency — FastAPI's `TestClient` requires it.

## Out of Scope

- No async conversion of `run_pipeline` or `src/` modules.
- No `APIRouter`, lifespan handlers, or Pydantic response models.
- No changes to `src/bigquery.py`, `src/gcs.py`, `src/pipeline.py`, `src/sources/`.
