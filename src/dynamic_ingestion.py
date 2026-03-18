from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import uuid
from typing import Any

import google.auth
from google.cloud import bigquery

from .attachment_parser import (
    extract_csv_files,
    parse_csv,
    parse_csv_with_header_row,
    suggest_header_row,
)
from .gmail_client import GmailClient
from .models import CsvConfig


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    if not slug:
        slug = "report"
    if slug[0].isdigit():
        slug = f"r_{slug}"
    return slug[:80]


def _safe_col(col: Any) -> str:
    text = "" if col is None else str(col)
    col = re.sub(r"[^a-zA-Z0-9_]+", "_", text.strip().lower()).strip("_")
    if not col:
        col = "col"
    if col[0].isdigit():
        col = f"c_{col}"
    return col[:120]


def _normalize_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, str | None]], dict[str, str]]:
    col_map: dict[str, str] = {}
    used: set[str] = set()
    out: list[dict[str, str | None]] = []

    for row in rows:
        clean: dict[str, str | None] = {}
        for raw_key, raw_val in row.items():
            key_token = "__unnamed__" if raw_key is None else str(raw_key)
            if key_token not in col_map:
                base = _safe_col(raw_key)
                name = base
                i = 2
                while name in used:
                    name = f"{base}_{i}"
                    i += 1
                used.add(name)
                col_map[key_token] = name
            key = col_map[key_token]

            if isinstance(raw_val, list):
                # DictReader stores extra columns under key None as a list.
                joined = ",".join([str(v).strip() for v in raw_val if str(v).strip() != ""])
                val = joined if joined else None
            else:
                val = None if raw_val is None else str(raw_val).strip()
                val = val if val != "" else None
            clean[key] = val
        out.append(clean)
    return out, col_map


METRIC_PATTERNS = [
    "impression",
    "click",
    "conversion",
    "conv",
    "revenue",
    "cost",
    "spend",
    "amount",
    "total",
    "count",
    "ctr",
    "cpc",
    "cpm",
    "cp",
    "rate",
    "ratio",
    "roas",
    "value",
]


def _is_metric_column(col: str) -> bool:
    c = col.lower()
    if "%" in c:
        return True
    return any(p in c for p in METRIC_PATTERNS)


def _select_key_columns(columns: list[str]) -> tuple[list[str], str]:
    dim_cols = [c for c in columns if not _is_metric_column(c)]
    if dim_cols:
        # Stable ordering helps keep key material deterministic.
        return sorted(dim_cols), "dimension_columns"
    return [], "row_hash_fallback"


def _row_key(row: dict[str, Any], key_columns: list[str]) -> str:
    if key_columns:
        material = "|".join([(row.get(c) or "") for c in key_columns])
    else:
        material = "|".join([f"{k}={row.get(k) or ''}" for k in sorted(row.keys()) if not k.startswith("_")])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _build_schema(columns: list[str]) -> list[bigquery.SchemaField]:
    schema = [bigquery.SchemaField(c, "STRING", mode="NULLABLE") for c in columns]
    schema.extend(
        [
            bigquery.SchemaField("_record_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("_message_id", "STRING"),
            bigquery.SchemaField("_attachment_name", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ]
    )
    return schema


def _ensure_dataset(client: bigquery.Client, project_id: str, dataset: str) -> None:
    ds = bigquery.Dataset(f"{project_id}.{dataset}")
    client.create_dataset(ds, exists_ok=True)


def _ensure_table_and_evolve(
    client: bigquery.Client,
    full_table_id: str,
    desired_schema: list[bigquery.SchemaField],
) -> None:
    try:
        table = client.get_table(full_table_id)
    except Exception:
        client.create_table(bigquery.Table(full_table_id, schema=desired_schema), exists_ok=True)
        return

    existing = {f.name for f in table.schema}
    missing = [f for f in desired_schema if f.name not in existing]
    if missing:
        alter = ", ".join([f"ADD COLUMN `{f.name}` {f.field_type}" for f in missing])
        client.query(f"ALTER TABLE `{full_table_id}` {alter}").result()


def _load_and_merge(
    client: bigquery.Client,
    full_table_id: str,
    schema: list[bigquery.SchemaField],
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    staging = f"{full_table_id}_staging_{uuid.uuid4().hex[:8]}"
    client.create_table(bigquery.Table(staging, schema=schema), exists_ok=True)
    try:
        client.load_table_from_json(
            rows,
            staging,
            job_config=bigquery.LoadJobConfig(schema=schema),
        ).result()

        cols = [f.name for f in schema]
        updatable = [c for c in cols if c != "_record_key"]
        update_set = ", ".join([f"`{c}` = S.`{c}`" for c in updatable])
        insert_cols = ", ".join([f"`{c}`" for c in cols])
        insert_vals = ", ".join([f"S.`{c}`" for c in cols])
        sql = f"""
        MERGE `{full_table_id}` T
        USING (
          SELECT * EXCEPT(_rn)
          FROM (
            SELECT
              S.*,
              ROW_NUMBER() OVER (
                PARTITION BY S._record_key
                ORDER BY S._ingested_at DESC, S._message_id DESC, S._attachment_name DESC
              ) AS _rn
            FROM `{staging}` S
          )
          WHERE _rn = 1
        ) S
        ON T._record_key = S._record_key
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        client.query(sql).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
    return len(rows)


def run_subject_ingestion(
    subject_contains: str,
    dry_run: bool = False,
    header_row_number: int | None = None,
    skip_leading_rows: int = 0,
    target_project_id: str | None = None,
    target_dataset: str | None = None,
    target_table: str | None = None,
    ingestion_mode: str | None = None,
) -> dict[str, Any]:
    if not subject_contains.strip():
        raise ValueError("subject_contains is required.")

    default_project = target_project_id or os.getenv("AUTO_BQ_PROJECT_ID")
    if not default_project:
        _, default_project = google.auth.default()
    if not default_project:
        raise ValueError("Unable to determine GCP project. Set AUTO_BQ_PROJECT_ID.")

    dataset = target_dataset or os.getenv("AUTO_BQ_DATASET", "gmail_ingestion")
    lookback_days = int(os.getenv("GMAIL_LOOKBACK_DAYS", "30"))
    max_messages = int(os.getenv("GMAIL_MAX_MESSAGES", "20"))
    delegated_user = os.getenv("GMAIL_DELEGATED_USER")
    mode = (ingestion_mode or os.getenv("INGESTION_MODE", "latest_only")).lower().strip()
    if mode not in {"all_matches", "latest_only"}:
        raise ValueError("ingestion_mode must be one of: all_matches, latest_only")

    table = target_table or _slugify(subject_contains)
    full_table_id = f"{default_project}.{dataset}.{table}"

    gmail = GmailClient(delegated_user=delegated_user)
    query = f'subject:"{subject_contains}" has:attachment newer_than:{lookback_days}d'
    attachments = gmail.fetch_attachments_by_query(query=query, max_results=max_messages)
    if mode == "latest_only" and attachments:
        attachments = attachments[:1]

    raw_rows: list[dict[str, Any]] = []
    raw_rows_source: list[dict[str, str]] = []
    per_attachment: list[dict[str, Any]] = []
    first_csv_hint: dict[str, Any] | None = None
    now = dt.datetime.utcnow().isoformat()
    for att in attachments:
        csv_files = extract_csv_files(att, unzip=True)
        row_count = 0
        for csv_name, content in csv_files:
            csv_cfg = CsvConfig(skip_leading_rows=max(skip_leading_rows, 0))
            if first_csv_hint is None:
                hint = suggest_header_row(content, cfg=csv_cfg)
                first_csv_hint = {
                    "filename": csv_name,
                    **hint,
                }
            if header_row_number is not None:
                rows = parse_csv_with_header_row(
                    content,
                    cfg=csv_cfg,
                    header_row_number=header_row_number,
                )
            else:
                rows = parse_csv(content, cfg=csv_cfg)
            raw_rows.extend(rows)
            raw_rows_source.extend(
                [{"message_id": att.message_id, "filename": att.filename} for _ in rows]
            )
            row_count += len(rows)
        per_attachment.append(
            {
                "message_id": att.message_id,
                "filename": att.filename,
                "rows_parsed": row_count,
            }
        )

    if not raw_rows:
        return {
            "subject_contains": subject_contains,
            "query": query,
            "table": full_table_id,
            "attachments_seen": len(attachments),
            "rows_parsed": 0,
            "rows_loaded": 0,
            "rows_estimated": 0,
            "status": "no_rows_found",
            "ingestion_mode": mode,
            "header_row_number_used": header_row_number,
            "skip_leading_rows": skip_leading_rows,
            "header_detection": first_csv_hint,
            "results": per_attachment,
        }

    if not dry_run and header_row_number is None:
        return {
            "subject_contains": subject_contains,
            "query": query,
            "table": full_table_id,
            "attachments_seen": len(attachments),
            "rows_parsed": len(raw_rows),
            "rows_loaded": 0,
            "rows_estimated": len(raw_rows),
            "dry_run": dry_run,
            "status": "header_confirmation_required",
            "message": "Confirm header row number and rerun import.",
            "ingestion_mode": mode,
            "header_row_number_used": None,
            "skip_leading_rows": skip_leading_rows,
            "header_detection": first_csv_hint,
            "results": per_attachment,
        }

    normalized, col_map = _normalize_rows(raw_rows)
    cols = sorted({k for row in normalized for k in row.keys()})
    key_columns, key_strategy = _select_key_columns(cols)

    enriched: list[dict[str, Any]] = []
    for i, row in enumerate(normalized):
        source = raw_rows_source[i] if i < len(raw_rows_source) else {}
        new_row = {c: row.get(c) for c in cols}
        new_row["_record_key"] = _row_key(new_row, key_columns=key_columns)
        new_row["_message_id"] = source.get("message_id")
        new_row["_attachment_name"] = source.get("filename")
        new_row["_ingested_at"] = now
        enriched.append(new_row)

    schema = _build_schema(cols)
    loaded = 0
    if not dry_run:
        client = bigquery.Client(project=default_project)
        _ensure_dataset(client, default_project, dataset)
        _ensure_table_and_evolve(client, full_table_id, schema)
        loaded = _load_and_merge(client, full_table_id, schema, enriched)

    return {
        "subject_contains": subject_contains,
        "query": query,
        "table": full_table_id,
        "attachments_seen": len(attachments),
        "rows_parsed": len(raw_rows),
        "rows_loaded": loaded,
        "rows_estimated": len(raw_rows),
        "dry_run": dry_run,
        "ingestion_mode": mode,
        "header_row_number_used": header_row_number,
        "skip_leading_rows": skip_leading_rows,
        "header_detection": first_csv_hint,
        "key_columns": key_columns,
        "key_strategy": key_strategy,
        "column_name_map": col_map,
        "results": per_attachment,
    }
