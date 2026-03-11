from __future__ import annotations

import argparse
import json
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .dynamic_ingestion import run_subject_ingestion

app = FastAPI(title="Gmail Attachment Ingestion")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


class RunRequest(BaseModel):
    subject_contains: str
    dry_run: bool = False
    header_row_number: int | None = None
    skip_leading_rows: int = 0


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "result": None,
            "error": None,
            "form_data": {
                "subject_contains": "",
                "skip_leading_rows": 0,
                "header_row_number": None,
                "dry_run": False,
            },
        },
    )


@app.post("/run", response_class=HTMLResponse)
def run_from_form(
    request: Request,
    subject_contains: str = Form(...),
    dry_run: bool = Form(default=False),
    header_row_number: int | None = Form(default=None),
    skip_leading_rows: int = Form(default=0),
) -> HTMLResponse:
    if not subject_contains.strip():
        raise HTTPException(status_code=400, detail="subject_contains is required.")

    try:
        result = run_subject_ingestion(
            subject_contains=subject_contains.strip(),
            dry_run=dry_run,
            header_row_number=header_row_number,
            skip_leading_rows=skip_leading_rows,
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
            "form_data": {
                "subject_contains": subject_contains.strip(),
                "skip_leading_rows": skip_leading_rows,
                "header_row_number": header_row_number,
                "dry_run": dry_run,
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
    )
    return JSONResponse(content=result)


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="Run ingestion from a Gmail subject line.")
    parser.add_argument("--subject-contains", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--header-row-number", type=int, default=None)
    parser.add_argument("--skip-leading-rows", type=int, default=0)
    args = parser.parse_args()

    result = run_subject_ingestion(
        subject_contains=args.subject_contains,
        dry_run=args.dry_run,
        header_row_number=args.header_row_number,
        skip_leading_rows=args.skip_leading_rows,
    )
    print(json.dumps(result, indent=2))
