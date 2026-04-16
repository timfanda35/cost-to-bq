import io
from azure.storage.blob import BlobServiceClient
from .base import ObjectMeta


class AzureSource:
    def __init__(self, container: str, prefix: str, connection_string: str):
        self._prefix = prefix
        client = BlobServiceClient.from_connection_string(connection_string)
        self._container_client = client.get_container_client(container)

    def find_latest(self) -> ObjectMeta:
        blobs = list(self._container_client.list_blobs(name_starts_with=self._prefix))
        if not blobs:
            raise FileNotFoundError(f"No blobs found under prefix '{self._prefix}'")
        newest = max(blobs, key=lambda b: b.last_modified)
        return ObjectMeta(key=newest.name, last_modified=newest.last_modified, size=newest.size or 0)

    def download(self, key: str) -> io.BytesIO:
        blob_client = self._container_client.get_blob_client(key)
        data = blob_client.download_blob().readall()
        return io.BytesIO(data)
