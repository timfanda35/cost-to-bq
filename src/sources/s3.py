from __future__ import annotations

import io
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

    def find_latest(self) -> ObjectMeta:
        resp = self._client.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix)
        contents = resp.get("Contents")
        if not contents:
            raise FileNotFoundError(f"No objects found under s3://{self._bucket}/{self._prefix}")
        newest = max(contents, key=lambda o: o["LastModified"])
        return ObjectMeta(key=newest["Key"], last_modified=newest["LastModified"],
                          size=newest.get("Size", 0))

    def download(self, key: str) -> io.BytesIO:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        data = resp["Body"].read()
        return io.BytesIO(data)
