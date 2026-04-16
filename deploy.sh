#!/usr/bin/env bash
# deploy.sh — deploy to Cloud Run and create/update Cloud Scheduler job
set -euo pipefail

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

# TODO: For production, move AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
# and AZURE_CONNECTION_STRING to Secret Manager and use --set-secrets instead.

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
AWS_REGION=${AWS_REGION:-},\
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-},\
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-},\
AZURE_CONNECTION_STRING=${AZURE_CONNECTION_STRING:-}"

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
