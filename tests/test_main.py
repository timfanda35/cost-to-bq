import pytest
from unittest.mock import patch


@pytest.fixture
def client():
    from main import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


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
    data = resp.get_json()
    assert data["source_key"] == "exports/b.parquet"


def test_run_returns_500_on_error(client):
    with patch("main.run_pipeline", side_effect=RuntimeError("something broke")):
        resp = client.post("/run")
    assert resp.status_code == 500
    data = resp.get_json()
    assert "error" in data


def test_health_check(client):
    resp = client.get("/health")
    assert resp.status_code == 200
