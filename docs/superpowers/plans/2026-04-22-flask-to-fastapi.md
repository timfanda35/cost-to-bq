# Flask to FastAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Flask with FastAPI and gunicorn with uvicorn across the entire app with no behavioral changes to the two existing endpoints.

**Architecture:** Minimal in-place swap — `main.py` is rewritten using FastAPI idioms, `requirements.txt` and `requirements-dev.txt` are updated, tests are migrated to FastAPI's `TestClient` (backed by `httpx`), and the Dockerfile CMD is updated to use uvicorn directly.

**Tech Stack:** FastAPI 0.111.0, uvicorn[standard] 0.29.0, httpx 0.27.0, pytest 8.1.1, pytest-mock 3.14.0

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `requirements.txt` | Modify | Remove Flask + gunicorn, add fastapi + uvicorn |
| `requirements-dev.txt` | Modify | Add httpx for TestClient |
| `tests/test_main.py` | Modify | Migrate to FastAPI TestClient; write failing tests |
| `main.py` | Modify | Rewrite with FastAPI; makes tests pass |
| `Dockerfile` | Modify | Switch CMD from gunicorn to uvicorn |

---

### Task 1: Update dependencies

**Files:**
- Modify: `requirements.txt`
- Modify: `requirements-dev.txt`

- [ ] **Step 1: Replace requirements.txt**

Full file content:
```
fastapi==0.111.0
boto3==1.34.87
google-cloud-storage==2.16.0
google-cloud-bigquery==3.19.0
uvicorn[standard]==0.29.0
```

- [ ] **Step 2: Replace requirements-dev.txt**

Full file content:
```
-r requirements.txt
pytest==8.1.1
pytest-mock==3.14.0
httpx==0.27.0
```

- [ ] **Step 3: Install updated dependencies**

```bash
pip install -r requirements-dev.txt
```

Expected: All packages install successfully. Flask and gunicorn are no longer in the environment.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt requirements-dev.txt
git commit -m "chore: replace Flask/gunicorn with FastAPI/uvicorn deps"
```

---

### Task 2: Update tests to use FastAPI TestClient (will fail — main.py still has Flask)

**Files:**
- Modify: `tests/test_main.py`

- [ ] **Step 1: Rewrite tests/test_main.py**

Full file content:
```python
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_run_returns_200_on_success(client):
    result = {
        "source_key": "exports/b.parquet",
        "last_modified": "2024-04-15T00:00:00+00:00",
        "gcs_uri": "gs://dest/billing/b.parquet",
        "bq_table": "proj.ds.tbl",
    }
    with patch("main.run_pipeline", return_value=result):
        resp = client.post("/run")
    assert resp.status_code == 200
    assert resp.json() == result


def test_run_returns_500_on_error(client):
    with patch("main.run_pipeline", side_effect=RuntimeError("something broke")):
        resp = client.post("/run")
    assert resp.status_code == 500
    data = resp.json()
    assert data["error"] == "something broke"


def test_health_check(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
```

Note: `resp.get_json()` → `resp.json()` because httpx's response object uses `.json()`, not Flask's `.get_json()`.

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_main.py -v
```

Expected: FAIL — `ImportError: cannot import name 'Flask' from 'flask'` (or similar) because `main.py` still imports Flask which is now uninstalled.

---

### Task 3: Rewrite main.py with FastAPI (makes tests pass)

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Rewrite main.py**

Full file content:
```python
import logging
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from src.pipeline import run_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run():
    try:
        result = run_pipeline()
        logger.info("Pipeline complete: %s", result)
        return result
    except Exception as exc:
        logger.exception("Pipeline failed")
        return JSONResponse(status_code=500, content={"error": str(exc)})


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
```

- [ ] **Step 2: Run tests to verify all three pass**

```bash
pytest tests/test_main.py -v
```

Expected:
```
tests/test_main.py::test_run_returns_200_on_success PASSED
tests/test_main.py::test_run_returns_500_on_error PASSED
tests/test_main.py::test_health_check PASSED
3 passed
```

- [ ] **Step 3: Run the full test suite to check for regressions**

```bash
pytest -v
```

Expected: All tests pass (no regressions in `test_bigquery.py`, `test_config.py`, `test_gcs.py`, `test_pipeline.py`, `test_s3.py`).

- [ ] **Step 4: Commit**

```bash
git add main.py tests/test_main.py
git commit -m "feat: migrate Flask to FastAPI with uvicorn"
```

---

### Task 4: Update Dockerfile

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Replace the CMD line in Dockerfile**

Full file content:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--timeout-keep-alive", "300"]
```

`--timeout-keep-alive 300` preserves the existing 300-second timeout behavior from gunicorn.

- [ ] **Step 2: Commit**

```bash
git add Dockerfile
git commit -m "chore: update Dockerfile to use uvicorn"
```
