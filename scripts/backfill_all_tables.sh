#!/usr/bin/env bash
set -euo pipefail

# One-off recovery backfill for active Gmail ingestion projects.
#
# This intentionally uses the CLI-only --attachment-order=oldest_first flag so
# historical files are merged before newer files. Scheduled/UI runs are not
# affected by this script.

PROJECT="${GCP_PROJECT_ID:-gmail-bigquery-importer}"
REGION="${GCP_REGION:-us-east1}"
SERVICE="${SERVICE_NAME:-gmail-bq-importer}"
DATASET="${AUTO_BQ_DATASET:-gmail_ingestion}"
COLLECTION="${PROJECT_STORE_COLLECTION:-import_projects}"
LOOKBACK_DAYS="${GMAIL_LOOKBACK_DAYS:-150}"
MAX_MESSAGES="${GMAIL_MAX_MESSAGES:-500}"
DELEGATED_USER="${GMAIL_DELEGATED_USER:-hi@ash.gdn}"
SERVICE_ACCOUNT="${RUNTIME_SERVICE_ACCOUNT:-gmail-bq-ingestor@${PROJECT}.iam.gserviceaccount.com}"
IMAGE="${IMAGE:-}"
SUFFIX="${BACKFILL_JOB_SUFFIX:-$(date +%Y%m%d%H%M%S)}"

if [[ -z "${IMAGE}" ]]; then
  IMAGE="$(
    gcloud run services describe "${SERVICE}" \
      --project "${PROJECT}" \
      --region "${REGION}" \
      --format='value(spec.template.spec.containers[0].image)'
  )"
fi

TOKEN="$(gcloud auth print-access-token)"
PROJECTS_FILE="$(mktemp)"
trap 'rm -f "${PROJECTS_FILE}"' EXIT

curl -sS \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://firestore.googleapis.com/v1/projects/${PROJECT}/databases/(default)/documents/${COLLECTION}" \
  | jq -r '
      .documents[]
      | .fields as $f
      | select(($f.status.stringValue // "") == "ACTIVE")
      | [
          $f.target_table.stringValue,
          $f.subject_contains.stringValue,
          ($f.header_row_number.integerValue // "1"),
          ($f.skip_leading_rows.integerValue // "0")
        ]
      | @tsv
    ' \
  | sort > "${PROJECTS_FILE}"

while IFS=$'\t' read -r table subject header_row skip_rows; do
  [[ -n "${table}" ]] || continue

  job="bf-${table//_/-}-${SUFFIX}"
  job="${job:0:63}"
  args=(
    "--subject-contains=${subject}"
    "--header-row-number=${header_row}"
    "--skip-leading-rows=${skip_rows}"
    "--ingestion-mode=all_matches"
    "--attachment-order=oldest_first"
    "--target-project-id=${PROJECT}"
    "--target-dataset=${DATASET}"
    "--target-table=${table}"
  )
  args_csv="$(IFS=,; echo "${args[*]}")"

  echo "Creating backfill job ${job} for ${table} (${subject})"
  gcloud run jobs create "${job}" \
    --project "${PROJECT}" \
    --region "${REGION}" \
    --image "${IMAGE}" \
    --service-account "${SERVICE_ACCOUNT}" \
    --tasks 1 \
    --max-retries 0 \
    --task-timeout 3600s \
    --set-env-vars "GMAIL_DELEGATED_USER=${DELEGATED_USER},GMAIL_LOOKBACK_DAYS=${LOOKBACK_DAYS},GMAIL_MAX_MESSAGES=${MAX_MESSAGES},GMAIL_API_MAX_ATTEMPTS=8,GMAIL_API_BASE_SLEEP_SECONDS=2,GMAIL_API_MAX_SLEEP_SECONDS=60,AUTO_BQ_PROJECT_ID=${PROJECT},AUTO_BQ_DATASET=${DATASET}" \
    --command ingest-run \
    --args "${args_csv}"

  echo "Executing ${job}"
  gcloud run jobs execute "${job}" \
    --project "${PROJECT}" \
    --region "${REGION}" \
    --wait
done < "${PROJECTS_FILE}"
