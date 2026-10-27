import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.dynamic_ingestion import _normalize_rows, _row_key, _select_key_columns
from src.project_store import ProjectStore


class DateNormalizationTests(unittest.TestCase):
    def test_date_formats_share_key_only_when_enabled(self):
        rows = [
            {"Date": "14/10/2026", "Activity": "Buy Tickets", "Total Conversions": "9"},
            {"Date": "2026-10-14", "Activity": "Buy Tickets", "Total Conversions": "11"},
        ]
        legacy, _ = _normalize_rows(rows)
        normalized, _ = _normalize_rows(rows, normalize_dates=True)
        keys, _ = _select_key_columns(list(normalized[0]))
        self.assertNotEqual(_row_key(legacy[0], keys), _row_key(legacy[1], keys))
        self.assertEqual(_row_key(normalized[0], keys), _row_key(normalized[1], keys))
        self.assertEqual(normalized[1]["total_conversions"], "11")

    def test_day_first_and_non_dates(self):
        rows = [{"Date": v} for v in ("08/10/2026", "2026-10-08", "Grand Total:", "31/02/2026", None)]
        normalized, _ = _normalize_rows(rows, normalize_dates=True)
        self.assertEqual([r["date"] for r in normalized],
                         ["2026-10-08", "2026-10-08", "Grand Total:", "31/02/2026", None])

    def test_new_project_default_does_not_modify_existing_records(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"PROJECT_STORE_BACKEND": "json"}):
            store = ProjectStore()
            store.json_path = Path(tmp) / "projects.json"
            store._write_json({"projects": [{"id": "old", "created_at": "2026-01-01"}], "runs": {}})
            data = dict(name="New", subject_contains="New", target_project_id="p",
                        target_dataset="d", target_table="t")
            self.assertTrue(store.create_project(data)["normalize_dates"])
            self.assertNotIn("normalize_dates", store.get_project("old"))
            self.assertFalse(store.create_project({**data, "normalize_dates": False})["normalize_dates"])

    def test_scheduled_project_missing_setting_keeps_legacy_behavior(self):
        from src import main
        project = dict(subject_contains="Old")
        for setting, expected in (({}, False), ({"normalize_dates": True}, True)):
            with self.subTest(setting=setting), patch.object(main, "store") as store, \
                    patch.object(main, "run_subject_ingestion", return_value={}) as ingest:
                store.get_project.return_value = {**project, **setting}
                main._run_project_internal("project")
                self.assertIs(ingest.call_args.kwargs["normalize_dates"], expected)

    def test_first_import_and_saved_setting_agree_for_new_and_reused_tables(self):
        from google.api_core.exceptions import NotFound
        from src import main
        for exists, expected in ((False, True), (True, False)):
            with self.subTest(exists=exists), patch.object(main, "store") as store, \
                    patch.object(main, "_is_logged_in", return_value=True), \
                    patch.object(main.bigquery, "Client") as client, \
                    patch.object(main, "run_subject_ingestion", return_value={}) as ingest:
                store.find_active_conflict_by_target.return_value = None
                store.list_projects.return_value = []
                if not exists:
                    client.return_value.get_table.side_effect = NotFound("missing")
                response = main.create_project(
                    request=None, name="New", subject_contains="New", header_row_number=13,
                    skip_leading_rows=0, target_project_id="p", target_dataset="d",
                    target_table="t", ingestion_mode="latest_only", schedule_time_utc="09:00",
                )
                self.assertEqual(response.status_code, 302)
                self.assertIs(ingest.call_args.kwargs["normalize_dates"], expected)
                self.assertIs(store.create_project.call_args.args[0]["normalize_dates"], expected)
