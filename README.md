# billing-loader

A FastAPI service that extracts billing files from AWS S3 (Cost and Usage Reports in Hive-partitioned format), stages them in Google Cloud Storage (GCS), and loads them into BigQuery. Designed to run on Cloud Run, triggered daily by Cloud Scheduler.

## Architecture

```
S3 (CUR Hive partitions)  →  GCS (staging)  →  BigQuery (partitioned WRITE_TRUNCATE)
```

Each run loads **3 billing periods** (current month + previous two). For each period it finds all `.parquet` files under the CUR Hive partition path, uploads them to GCS, then replaces that month's BigQuery partition.

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
| `SOURCE_TYPE` | Yes | — | Must be `s3` |
| `SOURCE_BUCKET` | Yes | — | S3 bucket name |
| `SOURCE_PREFIX` | No | `""` | Path prefix in the bucket before the export name |
| `EXPORT_NAME` | Yes | — | CUR export name; forms the Hive path `{SOURCE_PREFIX}/{EXPORT_NAME}/data/BILLING_PERIOD=YYYY-MM/` |
| `GCS_BUCKET` | Yes | — | GCS staging bucket name |
| `GCS_DESTINATION_PREFIX` | No | `""` | Path prefix in GCS (e.g. `billing/`) |
| `BQ_PROJECT_ID` | Yes | — | GCP project for BigQuery |
| `BQ_DATASET_ID` | Yes | — | BigQuery dataset name |
| `BQ_TABLE_ID` | Yes | — | BigQuery table name (must be date-partitioned) |
| `AWS_REGION` | Yes | — | AWS region (e.g. `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | No | — | AWS key ID; uses instance role if omitted |
| `AWS_SECRET_ACCESS_KEY` | No | — | Required if `AWS_ACCESS_KEY_ID` is set |
| `PORT` | No | `8080` | HTTP port for the uvicorn server |

## Local Development

```bash
pip install -r requirements-dev.txt

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
# {"run_id": "20240115-1705300800", "periods": [...], "bq_table": "project.dataset.table"}
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

# Grant the service account access to each secret
for SECRET in billing-loader-aws-key-id billing-loader-aws-secret-key; do
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
  --set-env-vars "SOURCE_TYPE=s3,SOURCE_BUCKET=${SOURCE_BUCKET},SOURCE_PREFIX=${SOURCE_PREFIX:-},EXPORT_NAME=${EXPORT_NAME},GCS_BUCKET=${GCS_BUCKET},GCS_DESTINATION_PREFIX=${GCS_DESTINATION_PREFIX:-},BQ_PROJECT_ID=${BQ_PROJECT_ID},BQ_DATASET_ID=${BQ_DATASET_ID},BQ_TABLE_ID=${BQ_TABLE_ID},AWS_REGION=${AWS_REGION}" \
  --set-secrets "AWS_ACCESS_KEY_ID=billing-loader-aws-key-id:latest,AWS_SECRET_ACCESS_KEY=billing-loader-aws-secret-key:latest"
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

Runs the ETL pipeline for the current and previous two billing months. Returns a summary per period.

```json
{
  "run_id": "20240115-1705300800",
  "periods": [
    {
      "partition": "BILLING_PERIOD=2023-11",
      "files": 3,
      "gcs_uris": ["gs://my-bucket/billing/my-export/data/.../file.parquet"]
    },
    {"partition": "BILLING_PERIOD=2023-12", "files": 3, "gcs_uris": ["..."]},
    {"partition": "BILLING_PERIOD=2024-01", "files": 3, "gcs_uris": ["..."]}
  ],
  "bq_table": "my-project.billing.daily_costs"
}
```

Returns `500` with `{"error": "..."}` if the pipeline fails.
