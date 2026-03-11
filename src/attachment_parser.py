from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from typing import Any

from .models import AttachmentPayload, CsvConfig


def attachment_hash(payload: AttachmentPayload) -> str:
    return hashlib.sha256(payload.raw_bytes).hexdigest()


def extract_csv_files(payload: AttachmentPayload, unzip: bool = True) -> list[tuple[str, bytes]]:
    if unzip and payload.filename.lower().endswith(".zip"):
        files: list[tuple[str, bytes]] = []
        with zipfile.ZipFile(io.BytesIO(payload.raw_bytes)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    files.append((name, zf.read(name)))
        return files
    if payload.filename.lower().endswith(".csv"):
        return [(payload.filename, payload.raw_bytes)]
    return []


def parse_csv(content: bytes, cfg: CsvConfig) -> list[dict[str, str]]:
    text = content.decode(cfg.encoding, errors="replace")
    first = text.splitlines()[0] if text.splitlines() else ""
    delimiter = cfg.delimiter
    if delimiter is None:
        delimiter = "\t" if "\t" in first else ","

    buf = io.StringIO(text)
    for _ in range(cfg.skip_leading_rows):
        next(buf, None)

    reader = csv.DictReader(
        buf,
        delimiter=delimiter,
    )
    return [dict(row) for row in reader]


def _csv_rows(content: bytes, cfg: CsvConfig) -> list[list[str]]:
    text = content.decode(cfg.encoding, errors="replace")
    first = text.splitlines()[0] if text.splitlines() else ""
    delimiter = cfg.delimiter
    if delimiter is None:
        delimiter = "\t" if "\t" in first else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    return [list(r) for r in reader]


def suggest_header_row(
    content: bytes,
    cfg: CsvConfig,
    max_scan_rows: int = 25,
) -> dict[str, Any]:
    rows = _csv_rows(content, cfg)
    start = min(cfg.skip_leading_rows, len(rows))
    scan = rows[start : start + max_scan_rows]
    if not scan:
        return {"suggested_row_number": None, "candidates": [], "preview": []}

    scored: list[tuple[float, int, list[str]]] = []
    for i, row in enumerate(scan):
        cells = [c.strip() for c in row]
        non_empty = [c for c in cells if c]
        if not non_empty:
            continue
        unique_ratio = len(set(non_empty)) / max(len(non_empty), 1)
        numericish = sum([1 for c in non_empty if c.replace(".", "", 1).isdigit()])
        numeric_ratio = numericish / max(len(non_empty), 1)
        score = len(non_empty) * 1.2 + unique_ratio - (numeric_ratio * 0.9)
        scored.append((score, i, row))

    scored.sort(reverse=True, key=lambda x: x[0])
    best_rel = scored[0][1] if scored else 0
    top = scored[:5]
    candidates = [
        {"row_number": start + rel + 1, "row_values": row}
        for _, rel, row in top
    ]
    preview = [
        {"row_number": start + i + 1, "row_values": row}
        for i, row in enumerate(scan[:10])
    ]
    return {
        "suggested_row_number": start + best_rel + 1,
        "candidates": candidates,
        "preview": preview,
    }


def parse_csv_with_header_row(
    content: bytes,
    cfg: CsvConfig,
    header_row_number: int,
) -> list[dict[str, str | list[str] | None]]:
    rows = _csv_rows(content, cfg)
    if not rows:
        return []

    if header_row_number < 1 or header_row_number > len(rows):
        raise ValueError(
            f"header_row_number {header_row_number} is out of bounds for CSV with {len(rows)} rows."
        )
    header_idx = header_row_number - 1
    headers = [h.strip() for h in rows[header_idx]]
    data_rows = rows[header_idx + 1 :]

    out: list[dict[str, str | list[str] | None]] = []
    for row in data_rows:
        rec: dict[str, str | list[str] | None] = {}
        for i, key in enumerate(headers):
            val = row[i].strip() if i < len(row) else ""
            rec[key] = val or None
        if len(row) > len(headers):
            rec[None] = row[len(headers) :]
        out.append(rec)
    return out
