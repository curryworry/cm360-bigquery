from __future__ import annotations

import os
from pathlib import Path

import yaml

from .models import (
    AttachmentConfig,
    CsvConfig,
    PipelineConfig,
    SchemaFieldConfig,
    TargetConfig,
)


def _as_schema(fields: list[dict]) -> list[SchemaFieldConfig]:
    return [SchemaFieldConfig(**field) for field in fields]


def _as_pipeline(raw: dict) -> PipelineConfig:
    target = TargetConfig(
        project_id=raw["target"]["project_id"],
        dataset=raw["target"]["dataset"],
        table=raw["target"]["table"],
        write_mode=raw["target"].get("write_mode", "upsert"),
        key_columns=raw["target"].get("key_columns", []),
        schema=_as_schema(raw["target"].get("schema", [])),
        time_partitioning_field=raw["target"].get("time_partitioning_field"),
    )

    return PipelineConfig(
        id=raw["id"],
        description=raw.get("description"),
        gmail_query=raw["gmail_query"],
        subject_contains=raw.get("subject_contains"),
        attachment=AttachmentConfig(**raw.get("attachment", {})),
        csv=CsvConfig(**raw.get("csv", {})),
        target=target,
    )


def load_pipelines(config_path: str | None = None) -> dict[str, PipelineConfig]:
    if config_path is None:
        config_path = os.getenv("PIPELINE_CONFIG_PATH", "configs/pipelines.yaml")

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    pipelines = {}
    for pipeline_raw in raw.get("pipelines", []):
        pipeline = _as_pipeline(pipeline_raw)
        pipelines[pipeline.id] = pipeline

    if not pipelines:
        raise ValueError("No pipelines found in config.")

    return pipelines

