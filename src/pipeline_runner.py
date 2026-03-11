from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any

from .attachment_parser import attachment_hash, extract_csv_files, parse_csv
from .bq_loader import BigQueryLoader
from .gmail_client import GmailClient
from .models import PipelineConfig


def run_pipeline(
    pipeline: PipelineConfig,
    subject_contains: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    delegated_user = os.getenv("GMAIL_DELEGATED_USER")
    gmail = GmailClient(delegated_user=delegated_user)
    bq = BigQueryLoader(project_id=pipeline.target.project_id)
    bq.ensure_dataset(pipeline)
    bq.ensure_target_table(pipeline)

    query_override = None
    if subject_contains:
        query_override = f'subject:"{subject_contains}" has:attachment'

    attachments = gmail.fetch_matching_attachments(pipeline, query_override=query_override)
    results: list[dict[str, Any]] = []
    total_rows = 0

    for att in attachments:
        file_hash = attachment_hash(att)
        if bq.already_processed(pipeline, att.message_id, file_hash):
            results.append(
                {
                    "message_id": att.message_id,
                    "filename": att.filename,
                    "status": "skipped_already_processed",
                }
            )
            continue

        csv_files = extract_csv_files(att, unzip=pipeline.attachment.unzip)
        if not csv_files:
            results.append(
                {
                    "message_id": att.message_id,
                    "filename": att.filename,
                    "status": "skipped_no_csv",
                }
            )
            continue

        rows_for_message = 0
        for csv_name, content in csv_files:
            rows = parse_csv(content, cfg=pipeline.csv)
            if dry_run:
                rows_for_message += len(rows)
                continue
            rows_for_message += bq.load_rows(pipeline, rows)

        if not dry_run:
            bq.mark_processed(pipeline, att.message_id, file_hash)
        total_rows += rows_for_message
        results.append(
            {
                "message_id": att.message_id,
                "filename": att.filename,
                "status": "processed",
                "rows_loaded": rows_for_message,
            }
        )

    return {
        "pipeline_id": pipeline.id,
        "dry_run": dry_run,
        "attachments_seen": len(attachments),
        "rows_loaded": total_rows,
        "results": results,
        "pipeline": asdict(pipeline),
    }
