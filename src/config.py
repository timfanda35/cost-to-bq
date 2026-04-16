import os


class Config:
    def __init__(self):
        self.source_type = self._require("SOURCE_TYPE")
        if self.source_type not in ("s3", "azure"):
            raise ValueError("SOURCE_TYPE must be 's3' or 'azure'")

        self.source_bucket = self._require("SOURCE_BUCKET")
        self.source_prefix = os.environ.get("SOURCE_PREFIX", "")

        self.gcs_bucket = self._require("GCS_BUCKET")
        self.gcs_destination_prefix = os.environ.get("GCS_DESTINATION_PREFIX", "")

        self.bq_project_id = self._require("BQ_PROJECT_ID")
        self.bq_dataset_id = self._require("BQ_DATASET_ID")
        self.bq_table_id = self._require("BQ_TABLE_ID")

        if self.source_type == "s3":
            self.aws_region = self._require("AWS_REGION")
            self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        else:
            self.azure_connection_string = self._require("AZURE_CONNECTION_STRING")

    @staticmethod
    def _require(name: str) -> str:
        val = os.environ.get(name)
        if not val:
            raise ValueError(f"Required env var {name!r} is not set")
        return val
