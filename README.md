# billing-loader

A Flask service that extracts the latest billing file from AWS S3 or Azure Blob Storage, stages it in Google Cloud Storage (GCS), and loads it into BigQuery. Designed to run on Cloud Run, triggered daily by Cloud Scheduler.

## Architecture

```
S3 / Azure Blob  →  GCS (staging)  →  BigQuery (WRITE_TRUNCATE)
```

Each run finds the most recently modified file in the source, uploads it to GCS, then replaces the target BigQuery table entirely.

## Prerequisites

- Python 3.11+
- A GCP project with the following APIs enabled: Cloud Run, Cloud Scheduler, Cloud Storage, BigQuery
- A GCP service account with these roles:
  - `roles/storage.objectAdmin` on the GCS staging bucket
  - `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the BQ project

## Configuration

Copy `.env.example` to `.env` and fill in the values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `SOURCE_TYPE` | Yes | — | `s3` or `azure` |
| `SOURCE_BUCKET` | Yes | — | S3 bucket name or Azure container name |
| `SOURCE_PREFIX` | No | `""` | Path prefix to filter source objects |
| `GCS_BUCKET` | Yes | — | GCS staging bucket name |
| `GCS_DESTINATION_PREFIX` | No | `""` | Path prefix in GCS (e.g. `billing/`) |
| `BQ_PROJECT_ID` | Yes | — | GCP project for BigQuery |
| `BQ_DATASET_ID` | Yes | — | BigQuery dataset name |
| `BQ_TABLE_ID` | Yes | — | BigQuery table name |
| `AWS_REGION` | Yes (S3) | — | AWS region (e.g. `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | No | — | AWS key ID; uses instance role if omitted |
| `AWS_SECRET_ACCESS_KEY` | No | — | Required if `AWS_ACCESS_KEY_ID` is set |
| `AZURE_CONNECTION_STRING` | Yes (Azure) | — | Full Azure Blob Storage connection string |
| `PORT` | No | `8080` | HTTP port for the Flask server |

## Local Development

```bash
pip install -r requirements.txt

# Copy and fill in environment variables
cp .env.example .env

# Run the server
python main.py
```

Test the endpoints:

```bash
curl http://localhost:8080/health
# {"status": "ok"}

curl -X POST http://localhost:8080/run
# {"source_key": "...", "last_modified": "...", "gcs_uri": "gs://...", "bq_table": "project.dataset.table"}
```

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## Deployment to Cloud Run

**1. Store secrets in Secret Manager** (first deploy only):

```bash
echo -n "YOUR_AWS_KEY_ID" | gcloud secrets create billing-loader-aws-key-id --data-file=-
echo -n "YOUR_AWS_SECRET" | gcloud secrets create billing-loader-aws-secret-key --data-file=-
echo -n "YOUR_AZURE_CONN_STR" | gcloud secrets create billing-loader-azure-connection-string --data-file=-

# Grant the service account access to each secret
for SECRET in billing-loader-aws-key-id billing-loader-aws-secret-key billing-loader-azure-connection-string; do
  gcloud secrets add-iam-policy-binding $SECRET \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor"
done
```

**2. Build and deploy:**

```bash
IMAGE="gcr.io/${GCP_PROJECT_ID}/billing-loader"

gcloud builds submit --tag "${IMAGE}" .

gcloud run deploy billing-loader \
  --image "${IMAGE}" \
  --platform managed \
  --region "${GCP_REGION:-us-central1}" \
  --no-allow-unauthenticated \
  --service-account "${SERVICE_ACCOUNT}" \
  --set-env-vars "SOURCE_TYPE=${SOURCE_TYPE},SOURCE_BUCKET=${SOURCE_BUCKET},SOURCE_PREFIX=${SOURCE_PREFIX:-},GCS_BUCKET=${GCS_BUCKET},GCS_DESTINATION_PREFIX=${GCS_DESTINATION_PREFIX:-},BQ_PROJECT_ID=${BQ_PROJECT_ID},BQ_DATASET_ID=${BQ_DATASET_ID},BQ_TABLE_ID=${BQ_TABLE_ID},AWS_REGION=${AWS_REGION:-}" \
  --set-secrets "AWS_ACCESS_KEY_ID=billing-loader-aws-key-id:latest,AWS_SECRET_ACCESS_KEY=billing-loader-aws-secret-key:latest,AZURE_CONNECTION_STRING=billing-loader-azure-connection-string:latest"
```

**3. Create the Cloud Scheduler job:**

```bash
SERVICE_URL=$(gcloud run services describe billing-loader \
  --platform managed --region "${GCP_REGION:-us-central1}" \
  --format "value(status.url)")

gcloud scheduler jobs create http billing-loader-daily \
  --schedule "${CRON_SCHEDULE:-0 6 * * *}" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email "${SERVICE_ACCOUNT}" \
  --location "${GCP_REGION:-us-central1}"
```

Trigger a manual run:

```bash
gcloud scheduler jobs run billing-loader-daily --location "${GCP_REGION:-us-central1}"
```

## API Endpoints

### `GET /health`

Returns service health status.

```json
{"status": "ok"}
```

### `POST /run`

Runs the ETL pipeline. Returns metadata about the loaded file on success.

```json
{
  "source_key": "exports/2024-01-15-billing.parquet",
  "last_modified": "2024-01-15T06:00:00+00:00",
  "gcs_uri": "gs://my-staging-bucket/billing/2024-01-15-billing.parquet",
  "bq_table": "my-project.billing.daily_costs"
}
```

Returns `500` with `{"error": "..."}` if the pipeline fails.
