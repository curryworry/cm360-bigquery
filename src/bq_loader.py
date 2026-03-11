from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

from google.cloud import bigquery

from .models import PipelineConfig


class BigQueryLoader:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def _dataset_ref(self, pipeline: PipelineConfig) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(pipeline.target.project_id, pipeline.target.dataset)

    def _table_ref(self, pipeline: PipelineConfig) -> bigquery.TableReference:
        ds = self._dataset_ref(pipeline)
        return ds.table(pipeline.target.table)

    def ensure_dataset(self, pipeline: PipelineConfig) -> None:
        ds_ref = self._dataset_ref(pipeline)
        dataset = bigquery.Dataset(ds_ref)
        self.client.create_dataset(dataset, exists_ok=True)

    def ensure_target_table(self, pipeline: PipelineConfig) -> None:
        table_ref = self._table_ref(pipeline)
        schema = [
            bigquery.SchemaField(field.name, field.type, mode=field.mode)
            for field in pipeline.target.schema
        ]
        table = bigquery.Table(table_ref, schema=schema)
        if pipeline.target.time_partitioning_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=pipeline.target.time_partitioning_field,
            )
        self.client.create_table(table, exists_ok=True)

    def ensure_state_table(self, pipeline: PipelineConfig) -> str:
        table_name = "_ingestion_runs"
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{pipeline.target.project_id}.{pipeline.target.dataset}.{table_name}` (
          pipeline_id STRING,
          message_id STRING,
          attachment_hash STRING,
          processed_at TIMESTAMP
        )
        """
        self.client.query(sql).result()
        return table_name

    def already_processed(
        self, pipeline: PipelineConfig, message_id: str, file_hash: str
    ) -> bool:
        state_table = self.ensure_state_table(pipeline)
        sql = f"""
        SELECT 1
        FROM `{pipeline.target.project_id}.{pipeline.target.dataset}.{state_table}`
        WHERE pipeline_id = @pipeline_id
          AND message_id = @message_id
          AND attachment_hash = @attachment_hash
        LIMIT 1
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("pipeline_id", "STRING", pipeline.id),
                    bigquery.ScalarQueryParameter("message_id", "STRING", message_id),
                    bigquery.ScalarQueryParameter("attachment_hash", "STRING", file_hash),
                ]
            ),
        )
        return any(True for _ in job.result())

    def mark_processed(self, pipeline: PipelineConfig, message_id: str, file_hash: str) -> None:
        state_table = self.ensure_state_table(pipeline)
        sql = f"""
        INSERT INTO `{pipeline.target.project_id}.{pipeline.target.dataset}.{state_table}`
        (pipeline_id, message_id, attachment_hash, processed_at)
        VALUES (@pipeline_id, @message_id, @attachment_hash, @processed_at)
        """
        self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("pipeline_id", "STRING", pipeline.id),
                    bigquery.ScalarQueryParameter("message_id", "STRING", message_id),
                    bigquery.ScalarQueryParameter("attachment_hash", "STRING", file_hash),
                    bigquery.ScalarQueryParameter("processed_at", "TIMESTAMP", dt.datetime.utcnow()),
                ]
            ),
        ).result()

    def _normalize_value(self, type_name: str, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return None
        else:
            v = value

        t = type_name.upper()
        if t in {"STRING", "GEOGRAPHY"}:
            return str(v)
        if t in {"INT64", "INTEGER"}:
            return int(v)
        if t in {"FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC"}:
            return float(Decimal(str(v)))
        if t in {"BOOL", "BOOLEAN"}:
            if isinstance(v, bool):
                return v
            return str(v).lower() in {"1", "true", "t", "yes", "y"}
        return v

    def _normalize_rows(self, pipeline: PipelineConfig, rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            clean = {}
            for field in pipeline.target.schema:
                clean[field.name] = self._normalize_value(field.type, row.get(field.name))
            out.append(clean)
        return out

    def load_rows(self, pipeline: PipelineConfig, rows: list[dict]) -> int:
        if not rows:
            return 0

        rows = self._normalize_rows(pipeline, rows)
        staging_table = (
            f"{pipeline.target.project_id}.{pipeline.target.dataset}."
            f"_staging_{pipeline.target.table}_{uuid.uuid4().hex[:8]}"
        )
        schema = [
            bigquery.SchemaField(field.name, field.type, mode=field.mode)
            for field in pipeline.target.schema
        ]
        staging = bigquery.Table(staging_table, schema=schema)
        self.client.create_table(staging, exists_ok=True)

        try:
            load_job = self.client.load_table_from_json(
                rows,
                staging_table,
                job_config=bigquery.LoadJobConfig(schema=schema),
            )
            load_job.result()

            target_table = (
                f"{pipeline.target.project_id}.{pipeline.target.dataset}.{pipeline.target.table}"
            )

            if pipeline.target.write_mode == "replace":
                sql = f"""
                TRUNCATE TABLE `{target_table}`;
                INSERT INTO `{target_table}`
                SELECT * FROM `{staging_table}`
                """
                self.client.query(sql).result()
            else:
                if not pipeline.target.key_columns:
                    raise ValueError(
                        f"Pipeline {pipeline.id} is in upsert mode but has no key_columns."
                    )
                all_cols = [field.name for field in pipeline.target.schema]
                on_clause = " AND ".join(
                    [f"T.{col} = S.{col}" for col in pipeline.target.key_columns]
                )
                update_cols = [col for col in all_cols if col not in pipeline.target.key_columns]
                matched_clause = ""
                if update_cols:
                    update_set = ", ".join([f"{col} = S.{col}" for col in update_cols])
                    matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
                insert_cols = ", ".join(all_cols)
                insert_vals = ", ".join([f"S.{col}" for col in all_cols])
                sql = f"""
                MERGE `{target_table}` T
                USING `{staging_table}` S
                ON {on_clause}
                {matched_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({insert_cols}) VALUES ({insert_vals})
                """
                self.client.query(sql).result()
        finally:
            self.client.delete_table(staging_table, not_found_ok=True)
        return len(rows)
