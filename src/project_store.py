from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import firestore


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProjectStore:
    def __init__(self) -> None:
        self.backend = os.getenv("PROJECT_STORE_BACKEND", "json").lower()
        self.json_path = Path(os.getenv("PROJECT_STORE_JSON_PATH", "data/projects.json"))
        self.firestore_collection = os.getenv("PROJECT_STORE_COLLECTION", "import_projects")

        self.fs_client = None
        if self.backend == "firestore":
            self.fs_client = firestore.Client()

    def _read_json(self) -> dict[str, Any]:
        if not self.json_path.exists():
            return {"projects": [], "runs": {}}
        with self.json_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _write_json(self, payload: dict[str, Any]) -> None:
        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        with self.json_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def list_projects(self) -> list[dict[str, Any]]:
        if self.backend == "firestore" and self.fs_client:
            docs = self.fs_client.collection(self.firestore_collection).stream()
            out = []
            for d in docs:
                row = d.to_dict() or {}
                row["id"] = d.id
                out.append(row)
            out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
            return out
        payload = self._read_json()
        return sorted(payload.get("projects", []), key=lambda x: x.get("created_at", ""), reverse=True)

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        if self.backend == "firestore" and self.fs_client:
            doc = self.fs_client.collection(self.firestore_collection).document(project_id).get()
            if not doc.exists:
                return None
            row = doc.to_dict() or {}
            row["id"] = doc.id
            return row
        payload = self._read_json()
        for p in payload.get("projects", []):
            if p["id"] == project_id:
                return p
        return None

    def create_project(self, data: dict[str, Any]) -> dict[str, Any]:
        project_id = data.get("id") or uuid.uuid4().hex[:12]
        row = {
            "id": project_id,
            "name": data["name"],
            "subject_contains": data["subject_contains"],
            "header_row_number": data.get("header_row_number"),
            "skip_leading_rows": data.get("skip_leading_rows", 0),
            "target_project_id": data["target_project_id"],
            "target_dataset": data["target_dataset"],
            "target_table": data["target_table"],
            "ingestion_mode": data.get("ingestion_mode", "latest_only"),
            "schedule_time_utc": data.get("schedule_time_utc", "09:00"),
            "timezone": data.get("timezone", "Pacific/Auckland"),
            "status": data.get("status", "ACTIVE"),
            "last_run_at": data.get("last_run_at"),
            "last_status": data.get("last_status"),
            "last_error": data.get("last_error"),
            "last_rows_loaded": data.get("last_rows_loaded", 0),
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }
        if self.backend == "firestore" and self.fs_client:
            self.fs_client.collection(self.firestore_collection).document(project_id).set(row)
            return row
        payload = self._read_json()
        payload.setdefault("projects", []).append(row)
        payload.setdefault("runs", {})[project_id] = []
        self._write_json(payload)
        return row

    def update_project(self, project_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        patch = {**patch, "updated_at": utc_now()}
        if self.backend == "firestore" and self.fs_client:
            ref = self.fs_client.collection(self.firestore_collection).document(project_id)
            snap = ref.get()
            if not snap.exists:
                return None
            ref.update(patch)
            updated = ref.get().to_dict() or {}
            updated["id"] = project_id
            return updated
        payload = self._read_json()
        for i, p in enumerate(payload.get("projects", [])):
            if p["id"] == project_id:
                payload["projects"][i] = {**p, **patch}
                self._write_json(payload)
                return payload["projects"][i]
        return None

    def delete_project(self, project_id: str) -> bool:
        if self.backend == "firestore" and self.fs_client:
            ref = self.fs_client.collection(self.firestore_collection).document(project_id)
            if not ref.get().exists:
                return False
            ref.delete()
            return True
        payload = self._read_json()
        before = len(payload.get("projects", []))
        payload["projects"] = [p for p in payload.get("projects", []) if p["id"] != project_id]
        payload.setdefault("runs", {}).pop(project_id, None)
        self._write_json(payload)
        return len(payload["projects"]) < before

    def append_run(self, project_id: str, run: dict[str, Any]) -> None:
        run = {**run, "created_at": utc_now()}
        if self.backend == "firestore" and self.fs_client:
            self.fs_client.collection(self.firestore_collection).document(project_id).collection("runs").add(run)
            return
        payload = self._read_json()
        payload.setdefault("runs", {}).setdefault(project_id, []).append(run)
        payload["runs"][project_id] = payload["runs"][project_id][-50:]
        self._write_json(payload)

    def list_runs(self, project_id: str, limit: int = 20) -> list[dict[str, Any]]:
        if self.backend == "firestore" and self.fs_client:
            runs = (
                self.fs_client.collection(self.firestore_collection)
                .document(project_id)
                .collection("runs")
                .order_by("created_at", direction=firestore.Query.DESCENDING)
                .limit(limit)
                .stream()
            )
            return [r.to_dict() or {} for r in runs]
        payload = self._read_json()
        rows = payload.setdefault("runs", {}).get(project_id, [])
        return list(reversed(rows[-limit:]))

    def find_active_conflict_by_target(
        self,
        target_project_id: str,
        target_dataset: str,
        target_table: str,
    ) -> dict[str, Any] | None:
        t_project = target_project_id.strip().lower()
        t_dataset = target_dataset.strip().lower()
        t_table = target_table.strip().lower()
        for p in self.list_projects():
            if (p.get("status") or "").upper() != "ACTIVE":
                continue
            if (
                (p.get("target_project_id", "").strip().lower() == t_project)
                and (p.get("target_dataset", "").strip().lower() == t_dataset)
                and (p.get("target_table", "").strip().lower() == t_table)
            ):
                return p
        return None
