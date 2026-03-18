#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   GCP_PROJECT_ID
#   GCP_REGION
#   SERVICE_NAME
#   JOB_NAME
# Optional:
#   CRON_SCHEDULE (default: every 15 minutes)
#   DISPATCH_TOKEN (if set, sent as x-dispatch-token header)

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required}"
: "${GCP_REGION:?GCP_REGION is required}"
: "${SERVICE_NAME:?SERVICE_NAME is required}"
: "${JOB_NAME:?JOB_NAME is required}"

CRON_SCHEDULE="${CRON_SCHEDULE:-*/15 * * * *}"

SERVICE_URL="$(gcloud run services describe "${SERVICE_NAME}" --project "${GCP_PROJECT_ID}" --region "${GCP_REGION}" --format='value(status.url)')"
DISPATCH_URL="${SERVICE_URL}/api/dispatch-due-projects"

echo "Creating/updating Cloud Scheduler job ${JOB_NAME} -> ${DISPATCH_URL}"

HEADERS="Content-Type=application/json"
if [[ -n "${DISPATCH_TOKEN:-}" ]]; then
  HEADERS="${HEADERS},x-dispatch-token=${DISPATCH_TOKEN}"
fi

if gcloud scheduler jobs describe "${JOB_NAME}" --project "${GCP_PROJECT_ID}" --location "${GCP_REGION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${JOB_NAME}" \
    --project "${GCP_PROJECT_ID}" \
    --location "${GCP_REGION}" \
    --schedule "${CRON_SCHEDULE}" \
    --uri "${DISPATCH_URL}" \
    --http-method POST \
    --headers "${HEADERS}" \
    --message-body '{}'
else
  gcloud scheduler jobs create http "${JOB_NAME}" \
    --project "${GCP_PROJECT_ID}" \
    --location "${GCP_REGION}" \
    --schedule "${CRON_SCHEDULE}" \
    --uri "${DISPATCH_URL}" \
    --http-method POST \
    --headers "${HEADERS}" \
    --message-body '{}'
fi

echo "Scheduler job ready."
