import os
import pytest
import src.config as cfg


def _set_common(extra=None):
    base = {
        "SOURCE_TYPE": "s3",
        "SOURCE_BUCKET": "my-bucket",
        "SOURCE_PREFIX": "exports/",
        "EXPORT_NAME": "my-export",
        "GCS_BUCKET": "gcs-bucket",
        "GCS_DESTINATION_PREFIX": "billing/",
        "BQ_PROJECT_ID": "my-project",
        "BQ_DATASET_ID": "billing",
        "BQ_TABLE_ID": "daily",
    }
    if extra:
        base.update(extra)
    return base


def test_config_loads_s3(monkeypatch):
    for k, v in _set_common({"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "id", "AWS_SECRET_ACCESS_KEY": "secret"}).items():
        monkeypatch.setenv(k, v)
    c = cfg.Config()
    assert c.source_type == "s3"
    assert c.aws_region == "us-east-1"
    assert c.export_name == "my-export"


def test_missing_export_name_raises(monkeypatch):
    env = _set_common({"AWS_REGION": "us-east-1"})
    env.pop("EXPORT_NAME")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("EXPORT_NAME", raising=False)
    with pytest.raises(ValueError, match="EXPORT_NAME"):
        cfg.Config()


def test_missing_source_type_raises(monkeypatch):
    monkeypatch.delenv("SOURCE_TYPE", raising=False)
    with pytest.raises(ValueError, match="SOURCE_TYPE"):
        cfg.Config()


def test_invalid_source_type_raises(monkeypatch):
    for k, v in _set_common({"SOURCE_TYPE": "gcs", "AWS_REGION": "us-east-1"}).items():
        monkeypatch.setenv(k, v)
    with pytest.raises(ValueError, match="SOURCE_TYPE must be"):
        cfg.Config()


def test_s3_missing_aws_region_raises(monkeypatch):
    env = _set_common()
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_REGION", raising=False)
    with pytest.raises(ValueError, match="AWS_REGION"):
        cfg.Config()


def test_repr_does_not_expose_credentials(monkeypatch):
    for k, v in _set_common({"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "myid", "AWS_SECRET_ACCESS_KEY": "topsecret"}).items():
        monkeypatch.setenv(k, v)
    c = cfg.Config()
    r = repr(c)
    assert "topsecret" not in r
    assert "myid" not in r
