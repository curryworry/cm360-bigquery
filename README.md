# Gmail Attachment -> BigQuery Importer

Internal tool for recurring Gmail CSV/ZIP ingestion into BigQuery.

## What it does

1. Finds Gmail attachments by subject.
2. Supports CSV or ZIP containing CSV.
3. Detects header row candidates.
4. Lets users confirm header row in UI.
5. Creates/updates BigQuery table.
6. Maintains ongoing import projects with status controls.

## Core UI flows

1. `/` Import Tool:
- ad-hoc import and header detection
- "Use This Row" quick-fill
- save as ongoing project

2. `/projects` Ongoing Projects:
- create projects
- run now
- pause/resume/stop
- delete project
- optional delete BigQuery table on project delete
- view recent run logs

## Auth

Simple shared password gate:

```bash
APP_PASSWORD=your-password
APP_SESSION_SECRET=long-random-secret
```

If `APP_PASSWORD` is empty, auth is disabled.

## Project persistence

Two backends:

- `PROJECT_STORE_BACKEND=firestore` (recommended for Cloud Run)
- `PROJECT_STORE_BACKEND=json` (local dev only)

Firestore collection defaults to `import_projects`.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/gmail.readonly

uvicorn src.main:app --env-file .env --reload
```

## Required env vars

```bash
AUTO_BQ_PROJECT_ID=gmail-bigquery-importer
AUTO_BQ_DATASET=gmail_ingestion
GMAIL_LOOKBACK_DAYS=30
GMAIL_MAX_MESSAGES=20
INGESTION_MODE=latest_only

APP_PASSWORD=change-me
APP_SESSION_SECRET=change-me-too

PROJECT_STORE_BACKEND=firestore
PROJECT_STORE_COLLECTION=import_projects
```

Optional:

```bash
DISPATCH_TOKEN=shared-machine-token
```

## Cloud Run deployment

Use the provided script:

```bash
export GCP_PROJECT_ID="gmail-bigquery-importer"
export GCP_REGION="australia-southeast1"
export SERVICE_NAME="gmail-bq-importer"
export APP_PASSWORD="your-password"
export APP_SESSION_SECRET="long-random-secret"
export AUTO_BQ_PROJECT_ID="gmail-bigquery-importer"
export AUTO_BQ_DATASET="gmail_ingestion"
export PROJECT_STORE_BACKEND="firestore"
export PROJECT_STORE_COLLECTION="import_projects"

./scripts/deploy_cloud_run.sh
```

## Scheduler endpoint

Endpoint to run all `ACTIVE` projects:

`POST /api/dispatch-due-projects`

Optional protection with `DISPATCH_TOKEN` header:

`x-dispatch-token: <token>`

Create/update scheduler job:

```bash
export GCP_PROJECT_ID="gmail-bigquery-importer"
export GCP_REGION="australia-southeast1"
export SERVICE_NAME="gmail-bq-importer"
export JOB_NAME="gmail-bq-daily"
export CRON_SCHEDULE="0 9 * * *"

./scripts/create_scheduler_job.sh
```

If `DISPATCH_TOKEN` is set, configure scheduler header `x-dispatch-token`.
