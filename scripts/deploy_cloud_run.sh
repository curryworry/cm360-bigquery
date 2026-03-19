#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   GCP_PROJECT_ID
#   GCP_REGION (e.g. australia-southeast1)
#   SERVICE_NAME (e.g. gmail-bq-importer)
#   APP_PASSWORD
# Optional:
#   AUTO_BQ_PROJECT_ID
#   AUTO_BQ_DATASET
#   PROJECT_STORE_BACKEND (firestore/json) default firestore
#   PROJECT_STORE_COLLECTION default import_projects
#   APP_SESSION_SECRET
#   DISPATCH_TOKEN (optional token for /api/dispatch-due-projects)
#   GMAIL_DELEGATED_USER (recommended for service-account domain-wide delegation)
#   RUNTIME_SERVICE_ACCOUNT (default: gmail-bq-ingestor@<project>.iam.gserviceaccount.com)
#   AR_REPOSITORY (default: cloud-run-images)

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required}"
: "${GCP_REGION:?GCP_REGION is required}"
: "${SERVICE_NAME:?SERVICE_NAME is required}"
: "${APP_PASSWORD:?APP_PASSWORD is required}"

AR_REPOSITORY="${AR_REPOSITORY:-cloud-run-images}"
IMAGE="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${AR_REPOSITORY}/${SERVICE_NAME}:$(date +%Y%m%d-%H%M%S)"

echo "Building image: ${IMAGE}"

if ! gcloud artifacts repositories describe "${AR_REPOSITORY}" \
  --project "${GCP_PROJECT_ID}" \
  --location "${GCP_REGION}" >/dev/null 2>&1; then
  echo "Creating Artifact Registry repo: ${AR_REPOSITORY}"
  gcloud artifacts repositories create "${AR_REPOSITORY}" \
    --project "${GCP_PROJECT_ID}" \
    --location "${GCP_REGION}" \
    --repository-format docker
fi

gcloud builds submit --project "${GCP_PROJECT_ID}" --tag "${IMAGE}" .

PROJECT_STORE_BACKEND="${PROJECT_STORE_BACKEND:-firestore}"
PROJECT_STORE_COLLECTION="${PROJECT_STORE_COLLECTION:-import_projects}"
AUTO_BQ_PROJECT_ID="${AUTO_BQ_PROJECT_ID:-$GCP_PROJECT_ID}"
AUTO_BQ_DATASET="${AUTO_BQ_DATASET:-gmail_ingestion}"
APP_SESSION_SECRET="${APP_SESSION_SECRET:-replace-me-in-prod}"
DISPATCH_TOKEN="${DISPATCH_TOKEN:-}"
GMAIL_DELEGATED_USER="${GMAIL_DELEGATED_USER:-}"
RUNTIME_SERVICE_ACCOUNT="${RUNTIME_SERVICE_ACCOUNT:-gmail-bq-ingestor@${GCP_PROJECT_ID}.iam.gserviceaccount.com}"

echo "Deploying Cloud Run service: ${SERVICE_NAME}"
gcloud run deploy "${SERVICE_NAME}" \
  --project "${GCP_PROJECT_ID}" \
  --region "${GCP_REGION}" \
  --image "${IMAGE}" \
  --platform managed \
  --allow-unauthenticated \
  --service-account "${RUNTIME_SERVICE_ACCOUNT}" \
  --set-env-vars "APP_PASSWORD=${APP_PASSWORD}" \
  --set-env-vars "APP_SESSION_SECRET=${APP_SESSION_SECRET}" \
  --set-env-vars "AUTO_BQ_PROJECT_ID=${AUTO_BQ_PROJECT_ID}" \
  --set-env-vars "AUTO_BQ_DATASET=${AUTO_BQ_DATASET}" \
  --set-env-vars "PROJECT_STORE_BACKEND=${PROJECT_STORE_BACKEND}" \
  --set-env-vars "PROJECT_STORE_COLLECTION=${PROJECT_STORE_COLLECTION}" \
  --set-env-vars "DISPATCH_TOKEN=${DISPATCH_TOKEN}" \
  --set-env-vars "GMAIL_DELEGATED_USER=${GMAIL_DELEGATED_USER}"

echo "Deployed."
