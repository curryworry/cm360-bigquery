"""One-time date/key migration for Wildfoods, retaining its full history."""

from google.cloud import bigquery

from .project_store import ProjectStore


PROJECT_ID = "6bf4878a8c80"
TABLE = "gmail-bigquery-importer.gmail_ingestion.wildfoods_floodlight_cm360"

REPAIR_SQL = f"""
BEGIN TRANSACTION;
ASSERT (
  SELECT COUNTIF(_record_key != LOWER(TO_HEX(SHA256(CONCAT(
    COALESCE(activity, ''), '|', COALESCE(date, '')
  ))))) = 0 FROM `{TABLE}`
) AS 'Unexpected existing merge keys; aborting repair';

CREATE TEMP TABLE repaired AS
WITH normalized AS (
  SELECT * REPLACE (
    COALESCE(FORMAT_DATE('%Y-%m-%d', COALESCE(
      SAFE.PARSE_DATE('%Y-%m-%d', date), SAFE.PARSE_DATE('%d/%m/%Y', date)
    )), date) AS date
  ) FROM `{TABLE}`
), keyed AS (
  SELECT * REPLACE (
    LOWER(TO_HEX(SHA256(CONCAT(COALESCE(activity, ''), '|', COALESCE(date, ''))))) AS _record_key
  ) FROM normalized
)
SELECT * FROM keyed
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _record_key
  ORDER BY _ingested_at DESC, _message_id DESC, _attachment_name DESC
) = 1;

ASSERT (SELECT COUNT(*) > 0 FROM repaired) AS 'Refusing empty repair';
DELETE FROM `{TABLE}` WHERE TRUE;
INSERT INTO `{TABLE}` (activity, date, total_conversions, _record_key, _message_id, _attachment_name, _ingested_at)
SELECT activity, date, total_conversions, _record_key, _message_id, _attachment_name, _ingested_at FROM repaired;
COMMIT TRANSACTION;
"""


def main() -> None:
    store = ProjectStore()
    project = store.get_project(PROJECT_ID)
    if not project or project.get("target_table") != TABLE.split(".")[-1]:
        raise ValueError("Wildfoods project configuration does not match the repair target")
    client = bigquery.Client(project="gmail-bigquery-importer")
    expected = {"activity", "date", "total_conversions", "_record_key", "_message_id", "_attachment_name", "_ingested_at"}
    if {f.name for f in client.get_table(TABLE).schema} != expected:
        raise ValueError("Unexpected Wildfoods schema; aborting repair")
    store.update_project(PROJECT_ID, {"status": "PAUSED"})
    # On failure keep the project paused until settings and data are consistent.
    client.query(REPAIR_SQL).result()
    store.update_project(PROJECT_ID, {
        "normalize_dates": True,
        "status": project.get("status", "ACTIVE"),
    })
    print("Wildfoods dates repaired; normalization enabled; prior project status restored.")


if __name__ == "__main__":
    main()
