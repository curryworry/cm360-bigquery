# Gmail Attachment -> BigQuery Ingestion

Internal tool where the end user only enters a Gmail subject line.

The service automatically:

1. Finds Gmail messages with matching subject + attachments
2. Downloads CSV or ZIP->CSV attachments
3. Detects and normalizes CSV column names
4. Creates dataset/table in BigQuery if missing
5. Upserts rows into a subject-based table

If files include metadata rows before the real header, users can control:
- `skip_leading_rows`
- `header_row_number`

## Quick Start

### 1) Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2) Authenticate and set environment variables

```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/gmail.readonly
export AUTO_BQ_PROJECT_ID="your-gcp-project-id"   # optional if ADC has default project
export AUTO_BQ_DATASET="gmail_ingestion"          # optional
export GMAIL_LOOKBACK_DAYS="30"                   # optional
export GMAIL_MAX_MESSAGES="20"                    # optional
```

Optional service-account mode (instead of user ADC):
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/abs/path/to/service-account.json"
export GMAIL_DELEGATED_USER="reporting@yourdomain.com"
```

BigQuery permissions need dataset/table create + job execution.

### 3) Run via UI

```bash
uvicorn src.main:app --reload
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

### 4) Run via CLI

```bash
ingest-run --subject-contains "CM360 Delivery Report"
ingest-run --subject-contains "CM360 Delivery Report" --dry-run
ingest-run --subject-contains "CM360 Delivery Report" --skip-leading-rows 3 --header-row-number 5
```

## Table Behavior

- BigQuery table name is derived from subject text.
- Dataset defaults to `gmail_ingestion` (override with `AUTO_BQ_DATASET`).
- Column names are auto-normalized from CSV headers.
- Upsert key strategy is automatic:
  - Preferred: `date + *_id` columns
  - Fallback: all `*_id` columns
  - Final fallback: full-row hash

In fallback mode, changed rows may insert as new rows because there is no stable business key in the CSV.

## Expected Flow

1. Query Gmail with `subject:"<input>" has:attachment`.
2. Pull matching attachments.
3. If ZIP, extract all CSV files.
4. Parse CSV into dictionaries.
5. Ensure dataset + target table exist.
6. Upsert to target table via staging + `MERGE`.

## Recommended Next Step

Add a Cloud Scheduler job hitting `/api/run` for each pipeline to run this automatically.
