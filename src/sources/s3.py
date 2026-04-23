from __future__ import annotations

import boto3
from .base import ObjectMeta


class S3Source:
    def __init__(self, bucket: str, prefix: str, region: str,
                 aws_access_key_id: str | None = None,
                 aws_secret_access_key: str | None = None):
        self._bucket = bucket
        self._prefix = prefix
        kwargs = {"region_name": region}
        if aws_access_key_id is not None:
            if aws_secret_access_key is None:
                raise ValueError("aws_secret_access_key must be provided with aws_access_key_id")
            kwargs["aws_access_key_id"] = aws_access_key_id
            kwargs["aws_secret_access_key"] = aws_secret_access_key
        self._client = boto3.client("s3", **kwargs)

    def list_partition(self, partition_prefix: str) -> list[ObjectMeta]:
        paginator = self._client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=partition_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    objects.append(ObjectMeta(
                        key=obj["Key"],
                        last_modified=obj["LastModified"],
                        size=obj.get("Size", 0),
                    ))
        if not objects:
            raise FileNotFoundError(
                f"No parquet files found under s3://{self._bucket}/{partition_prefix}"
            )
        return objects

    def stream(self, key: str):
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        return resp["Body"]
