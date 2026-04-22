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
