import os


class Config:
    def __init__(self):
        self.source_type = self._require("SOURCE_TYPE")
        if self.source_type != "s3":
            raise ValueError("SOURCE_TYPE must be 's3'")

        self.source_bucket = self._require("SOURCE_BUCKET")
        self.source_prefix = os.environ.get("SOURCE_PREFIX", "")
        self.export_name = self._require("EXPORT_NAME")

        self.gcs_bucket = self._require("GCS_BUCKET")
        self.gcs_destination_prefix = os.environ.get("GCS_DESTINATION_PREFIX", "")

        self.bq_project_id = self._require("BQ_PROJECT_ID")
        self.bq_dataset_id = self._require("BQ_DATASET_ID")
        self.bq_table_id = self._require("BQ_TABLE_ID")

        self.aws_region = self._require("AWS_REGION")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL")
        self.bq_cmek_key_name = os.environ.get("BQ_CMEK_KEY_NAME")

    def __repr__(self) -> str:
        return (
            f"Config(source_type={self.source_type!r}, "
            f"source_bucket={self.source_bucket!r}, "
            f"gcs_bucket={self.gcs_bucket!r})"
        )

    @staticmethod
    def _require(name: str) -> str:
        val = os.environ.get(name)
        if not val:
            raise ValueError(f"Required env var {name!r} is not set or is empty")
        return val
