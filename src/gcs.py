from google.cloud import storage


def upload_to_gcs(stream, gcs_bucket: str, dest_blob_name: str) -> str:
    """Stream upload to GCS and return the gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_file(stream, rewind=False)
    return f"gs://{gcs_bucket}/{dest_blob_name}"
