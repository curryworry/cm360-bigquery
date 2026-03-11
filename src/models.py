from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SchemaFieldConfig:
    name: str
    type: str
    mode: str = "NULLABLE"


@dataclass
class AttachmentConfig:
    filename_regex: str | None = None
    unzip: bool = True


@dataclass
class CsvConfig:
    delimiter: str | None = None
    skip_leading_rows: int = 0
    allow_quoted_newlines: bool = True
    encoding: str = "utf-8-sig"


@dataclass
class TargetConfig:
    project_id: str
    dataset: str
    table: str
    write_mode: str = "upsert"
    key_columns: list[str] = field(default_factory=list)
    schema: list[SchemaFieldConfig] = field(default_factory=list)
    time_partitioning_field: str | None = None


@dataclass
class PipelineConfig:
    id: str
    gmail_query: str
    target: TargetConfig
    description: str | None = None
    subject_contains: str | None = None
    attachment: AttachmentConfig = field(default_factory=AttachmentConfig)
    csv: CsvConfig = field(default_factory=CsvConfig)


@dataclass
class AttachmentPayload:
    message_id: str
    filename: str
    raw_bytes: bytes
    headers: dict[str, Any]

