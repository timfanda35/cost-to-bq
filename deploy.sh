#!/usr/bin/env bash
# deploy.sh — deploy to Cloud Run and create/update Cloud Scheduler job
set -euo pipefail

# Prerequisites — create secrets in Secret Manager before first deploy:
#   gcloud secrets create billing-loader-aws-key-id --data-file=-       <<< "YOUR_KEY_ID"
#   gcloud secrets create billing-loader-aws-secret-key --data-file=-   <<< "YOUR_SECRET_KEY"
#   gcloud secrets create billing-loader-azure-connection-string --data-file=- <<< "YOUR_CONN_STR"
# Grant access: gcloud secrets add-iam-policy-binding SECRETNAME \
#   --member="serviceAccount:${SA}" --role="roles/secretmanager.secretAccessor"

PROJECT_ID="${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="billing-loader"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
SCHEDULER_JOB="billing-loader-daily"
SCHEDULE="${CRON_SCHEDULE:-0 6 * * *}"   # 06:00 UTC daily
SA="${SERVICE_ACCOUNT:?set SERVICE_ACCOUNT}"  # e.g. billing-loader@PROJECT.iam.gserviceaccount.com

echo "Building and pushing image..."
gcloud builds submit --tag "${IMAGE}" .

echo "Deploying to Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --platform managed \
  --region "${REGION}" \
  --no-allow-unauthenticated \
  --service-account "${SA}" \
  --set-env-vars "SOURCE_TYPE=${SOURCE_TYPE},\
SOURCE_BUCKET=${SOURCE_BUCKET},\
SOURCE_PREFIX=${SOURCE_PREFIX:-},\
GCS_BUCKET=${GCS_BUCKET},\
GCS_DESTINATION_PREFIX=${GCS_DESTINATION_PREFIX:-},\
BQ_PROJECT_ID=${BQ_PROJECT_ID:-${PROJECT_ID}},\
BQ_DATASET_ID=${BQ_DATASET_ID},\
BQ_TABLE_ID=${BQ_TABLE_ID},\
AWS_REGION=${AWS_REGION:-}" \
  --set-secrets "AWS_ACCESS_KEY_ID=billing-loader-aws-key-id:latest,\
AWS_SECRET_ACCESS_KEY=billing-loader-aws-secret-key:latest,\
AZURE_CONNECTION_STRING=billing-loader-azure-connection-string:latest"

SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --platform managed --region "${REGION}" \
  --format "value(status.url)")

echo "Service URL: ${SERVICE_URL}"

echo "Creating/updating Cloud Scheduler job..."
gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
  --schedule "${SCHEDULE}" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email "${SA}" \
  --location "${REGION}" 2>/dev/null \
|| gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
  --schedule "${SCHEDULE}" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email "${SA}" \
  --location "${REGION}"

echo "Done. Trigger manually: gcloud scheduler jobs run ${SCHEDULER_JOB} --location ${REGION}"
