import os
import pytest


def _set_common(extra=None):
    base = {
        "SOURCE_TYPE": "s3",
        "SOURCE_BUCKET": "my-bucket",
        "SOURCE_PREFIX": "exports/",
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
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    c = cfg.Config()
    assert c.source_type == "s3"
    assert c.aws_region == "us-east-1"


def test_config_loads_azure(monkeypatch):
    env = _set_common({"SOURCE_TYPE": "azure", "AZURE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;..."})
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    c = cfg.Config()
    assert c.source_type == "azure"
    assert c.azure_connection_string.startswith("DefaultEndpoints")


def test_missing_source_type_raises(monkeypatch):
    monkeypatch.delenv("SOURCE_TYPE", raising=False)
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    with pytest.raises(ValueError, match="SOURCE_TYPE"):
        cfg.Config()


def test_invalid_source_type_raises(monkeypatch):
    for k, v in _set_common({"SOURCE_TYPE": "gcs"}).items():
        monkeypatch.setenv(k, v)
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    with pytest.raises(ValueError, match="SOURCE_TYPE must be"):
        cfg.Config()


def test_s3_missing_aws_region_raises(monkeypatch):
    env = _set_common()
    env.pop("SOURCE_TYPE", None)
    env["SOURCE_TYPE"] = "s3"
    # no AWS_REGION
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_REGION", raising=False)
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    with pytest.raises(ValueError, match="AWS_REGION"):
        cfg.Config()
