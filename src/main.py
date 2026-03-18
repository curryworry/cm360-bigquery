from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from pydantic import BaseModel

from .auth import PasswordAuth
from .dynamic_ingestion import run_subject_ingestion
from .project_store import ProjectStore

app = FastAPI(title="Gmail Attachment Ingestion")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))
auth = PasswordAuth()
store = ProjectStore()


class RunRequest(BaseModel):
    subject_contains: str
    dry_run: bool = False
    header_row_number: int | None = None
    skip_leading_rows: int = 0
    ingestion_mode: str = "latest_only"


def _require_auth(request: Request) -> None:
    if not auth.enabled():
        return
    token = request.cookies.get(auth.cookie_name)
    if not auth.validate_cookie_value(token):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _is_logged_in(request: Request) -> bool:
    if not auth.enabled():
        return True
    return auth.validate_cookie_value(request.cookies.get(auth.cookie_name))


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_hhmm(value: str | None, fallback: tuple[int, int] = (9, 0)) -> tuple[int, int]:
    if not value:
        return fallback
    parts = value.strip().split(":")
    if len(parts) != 2:
        return fallback
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return fallback
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return fallback
    return (hour, minute)


def _project_due_now(project: dict, now_utc: datetime) -> tuple[bool, str]:
    # We intentionally run schedules in fixed EST/ET for all projects.
    tz_name = project.get("timezone") or "America/New_York"
    tz = ZoneInfo(tz_name)
    now_local = now_utc.astimezone(tz)

    hour, minute = _parse_hhmm(project.get("schedule_time_utc"), fallback=(9, 0))
    due_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if now_local < due_local:
        return (False, "before_scheduled_time")

    last_run_at = _parse_iso(project.get("last_run_at"))
    if last_run_at is None:
        return (True, "never_run")

    last_local = last_run_at.astimezone(tz)
    if last_local.date() == now_local.date() and last_local >= due_local:
        return (False, "already_ran_today")

    return (True, "due")


def _run_project_internal(project_id: str, dry_run: bool = False) -> dict:
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")

    try:
        result = run_subject_ingestion(
            subject_contains=project["subject_contains"],
            dry_run=dry_run,
            header_row_number=project.get("header_row_number"),
            skip_leading_rows=project.get("skip_leading_rows", 0),
            target_project_id=project.get("target_project_id"),
            target_dataset=project.get("target_dataset"),
            target_table=project.get("target_table"),
            ingestion_mode=project.get("ingestion_mode", "latest_only"),
        )
        store.update_project(
            project_id,
            {
                "last_run_at": datetime.now(timezone.utc).isoformat(),
                "last_status": "ok",
                "last_error": None,
                "last_rows_loaded": result.get("rows_loaded", 0),
            },
        )
        store.append_run(
            project_id,
            {
                "status": "ok",
                "dry_run": dry_run,
                "rows_loaded": result.get("rows_loaded", 0),
                "rows_parsed": result.get("rows_parsed", 0),
                "message": result.get("status", "ok"),
            },
        )
        return {"ok": True, "project_id": project_id, "result": result}
    except Exception as exc:
        store.update_project(
            project_id,
            {
                "last_run_at": datetime.now(timezone.utc).isoformat(),
                "last_status": "error",
                "last_error": str(exc),
            },
        )
        store.append_run(project_id, {"status": "error", "dry_run": dry_run, "message": str(exc)})
        return {"ok": False, "project_id": project_id, "error": str(exc)}


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    if _is_logged_in(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})


@app.post("/login", response_class=HTMLResponse)
def login_submit(request: Request, password: str = Form(...)) -> HTMLResponse:
    if not auth.enabled():
        return RedirectResponse(url="/", status_code=302)
    if not auth.verify_password(password):
        return templates.TemplateResponse(
            request=request, name="login.html", context={"error": "Invalid password."}
        )
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie(
        auth.cookie_name,
        auth.issue_cookie_value(),
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=60 * 60 * 12,
    )
    return resp


@app.post("/logout")
def logout() -> RedirectResponse:
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie(auth.cookie_name)
    return resp


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "result": None,
            "error": None,
            "projects": store.list_projects(),
            "form_data": {
                "subject_contains": "",
                "skip_leading_rows": 0,
                "header_row_number": None,
                "ingestion_mode": "latest_only",
                "run_action": "setup",
            },
        },
    )


@app.post("/run", response_class=HTMLResponse)
def run_from_form(
    request: Request,
    subject_contains: str = Form(...),
    run_action: str = Form(default="setup"),
    header_row_number: int | None = Form(default=None),
    skip_leading_rows: int = Form(default=0),
    ingestion_mode: str = Form(default="latest_only"),
) -> HTMLResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    if not subject_contains.strip():
        raise HTTPException(status_code=400, detail="subject_contains is required.")
    dry_run = True
    if run_action == "import" and not header_row_number:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "result": None,
                "error": "Select a header row before Import. Use Setup Run first, then choose 'Use This Row'.",
                "projects": store.list_projects(),
                "form_data": {
                    "subject_contains": subject_contains.strip(),
                    "skip_leading_rows": skip_leading_rows,
                    "header_row_number": header_row_number,
                    "ingestion_mode": ingestion_mode,
                    "run_action": run_action,
                },
            },
        )

    try:
        result = run_subject_ingestion(
            subject_contains=subject_contains.strip(),
            dry_run=dry_run,
            header_row_number=header_row_number,
            skip_leading_rows=skip_leading_rows,
            ingestion_mode=ingestion_mode,
        )
        error = None
    except Exception as exc:
        result = None
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "result": result,
            "error": error,
            "projects": store.list_projects(),
            "form_data": {
                "subject_contains": subject_contains.strip(),
                "skip_leading_rows": skip_leading_rows,
                "header_row_number": header_row_number,
                "ingestion_mode": ingestion_mode,
                "run_action": run_action,
            },
        },
    )


@app.post("/api/run")
def run_api(payload: RunRequest) -> JSONResponse:
    if not payload.subject_contains.strip():
        raise HTTPException(status_code=400, detail="subject_contains is required.")
    result = run_subject_ingestion(
        subject_contains=payload.subject_contains,
        dry_run=payload.dry_run,
        header_row_number=payload.header_row_number,
        skip_leading_rows=payload.skip_leading_rows,
        ingestion_mode=payload.ingestion_mode,
    )
    return JSONResponse(content=result)


@app.get("/projects", response_class=HTMLResponse)
def list_projects_page(request: Request) -> HTMLResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    projects = store.list_projects()
    for p in projects:
        p["runs"] = store.list_runs(p["id"], limit=5)
    return templates.TemplateResponse(
        request=request,
        name="projects.html",
        context={
            "projects": projects,
            "error": request.query_params.get("error"),
        },
    )


@app.post("/projects")
def create_project(
    request: Request,
    name: str = Form(...),
    subject_contains: str = Form(...),
    header_row_number: int = Form(...),
    skip_leading_rows: int = Form(default=0),
    target_project_id: str = Form(...),
    target_dataset: str = Form(...),
    target_table: str = Form(...),
    ingestion_mode: str = Form(default="latest_only"),
    schedule_time_utc: str = Form(default="09:00"),
) -> RedirectResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    conflict = store.find_active_conflict_by_target(
        target_project_id=target_project_id,
        target_dataset=target_dataset,
        target_table=target_table,
    )
    if conflict:
        msg = (
            "Conflict: active project "
            f"'{conflict.get('name', conflict.get('id'))}' already targets "
            f"{target_project_id}.{target_dataset}.{target_table}. "
            "Pause/stop it or reuse that project."
        )
        return RedirectResponse(
            url=f"/projects?error={quote_plus(msg)}",
            status_code=302,
        )

    # First materialize data in BigQuery once, then save as ongoing project.
    try:
        result = run_subject_ingestion(
            subject_contains=subject_contains.strip(),
            dry_run=False,
            header_row_number=header_row_number,
            skip_leading_rows=max(skip_leading_rows, 0),
            target_project_id=target_project_id.strip(),
            target_dataset=target_dataset.strip(),
            target_table=target_table.strip(),
            ingestion_mode=ingestion_mode.strip() or "latest_only",
        )
    except Exception as exc:
        return RedirectResponse(url=f"/projects?error={quote_plus(str(exc))}", status_code=302)

    store.create_project(
        {
            "name": name.strip(),
            "subject_contains": subject_contains.strip(),
            "header_row_number": header_row_number,
            "skip_leading_rows": max(skip_leading_rows, 0),
            "target_project_id": target_project_id.strip(),
            "target_dataset": target_dataset.strip(),
            "target_table": target_table.strip(),
            "ingestion_mode": ingestion_mode.strip() or "latest_only",
            "schedule_time_utc": schedule_time_utc.strip() or "09:00",
            "timezone": "America/New_York",
            "status": "ACTIVE",
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "last_status": "ok",
            "last_rows_loaded": result.get("rows_loaded", 0),
            "last_error": None,
        }
    )
    return RedirectResponse(url="/projects", status_code=302)


@app.post("/projects/{project_id}/status")
def set_project_status(request: Request, project_id: str, status: str = Form(...)) -> RedirectResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    allowed = {"ACTIVE", "PAUSED", "STOPPED"}
    if status not in allowed:
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
    row = store.update_project(project_id, {"status": status})
    if not row:
        raise HTTPException(status_code=404, detail="Project not found.")
    return RedirectResponse(url="/projects", status_code=302)


@app.post("/projects/{project_id}/run")
def run_project_now(request: Request, project_id: str, dry_run: bool = Form(default=False)) -> RedirectResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    _run_project_internal(project_id, dry_run=dry_run)
    return RedirectResponse(url="/projects", status_code=302)


@app.post("/projects/{project_id}/delete")
def delete_project(
    request: Request,
    project_id: str,
    delete_table: bool = Form(default=False),
) -> RedirectResponse:
    if not _is_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")
    if delete_table:
        full_table = (
            f"{project['target_project_id']}."
            f"{project['target_dataset']}."
            f"{project['target_table']}"
        )
        client = bigquery.Client(project=project["target_project_id"])
        client.delete_table(full_table, not_found_ok=True)
    store.delete_project(project_id)
    return RedirectResponse(url="/projects", status_code=302)


@app.post("/api/run-project/{project_id}")
def run_project_api(project_id: str, dry_run: bool = False) -> JSONResponse:
    return JSONResponse(content=_run_project_internal(project_id, dry_run=dry_run))


@app.post("/api/dispatch-due-projects")
def dispatch_due_projects(request: Request) -> JSONResponse:
    # Optional machine token for scheduler endpoint.
    required = os.getenv("DISPATCH_TOKEN")
    if required:
        got = request.headers.get("x-dispatch-token", "")
        if got != required:
            raise HTTPException(status_code=401, detail="Invalid dispatch token.")

    now_utc = datetime.now(timezone.utc)
    results = []
    skipped = []
    for p in store.list_projects():
        if p.get("status") != "ACTIVE":
            skipped.append({"project_id": p.get("id"), "reason": "not_active"})
            continue
        due, reason = _project_due_now(p, now_utc)
        if not due:
            skipped.append({"project_id": p.get("id"), "reason": reason})
            continue
        results.append(_run_project_internal(p["id"], dry_run=False))
    return JSONResponse(
        content={
            "checked_at": now_utc.isoformat(),
            "ran_count": len(results),
            "skipped_count": len(skipped),
            "results": results,
            "skipped": skipped,
        }
    )


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="Run ingestion from a Gmail subject line.")
    parser.add_argument("--subject-contains", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--header-row-number", type=int, default=None)
    parser.add_argument("--skip-leading-rows", type=int, default=0)
    parser.add_argument("--ingestion-mode", default="latest_only")
    parser.add_argument("--target-project-id", default=None)
    parser.add_argument("--target-dataset", default=None)
    parser.add_argument("--target-table", default=None)
    args = parser.parse_args()

    result = run_subject_ingestion(
        subject_contains=args.subject_contains,
        dry_run=args.dry_run,
        header_row_number=args.header_row_number,
        skip_leading_rows=args.skip_leading_rows,
        ingestion_mode=args.ingestion_mode,
        target_project_id=args.target_project_id,
        target_dataset=args.target_dataset,
        target_table=args.target_table,
    )
    print(json.dumps(result, indent=2))
