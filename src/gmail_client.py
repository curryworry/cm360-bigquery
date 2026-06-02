from __future__ import annotations

import base64
import os
import re
import time
import random
from dataclasses import dataclass
from typing import Any

import google.auth
from google.auth import iam
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .models import AttachmentPayload, PipelineConfig

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


@dataclass
class GmailClient:
    delegated_user: str | None = None

    def __post_init__(self) -> None:
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path:
            creds = service_account.Credentials.from_service_account_file(
                creds_path, scopes=GMAIL_SCOPES
            )
            if not self.delegated_user:
                raise ValueError(
                    "GMAIL_DELEGATED_USER is required when using GOOGLE_APPLICATION_CREDENTIALS."
                )
            creds = creds.with_subject(self.delegated_user)
        else:
            # On Cloud Run, ADC is usually compute/service-account credentials that do not
            # directly support with_subject(). Build a keyless signer-backed service account
            # credential so Gmail domain-wide delegation works without JSON keys.
            base_creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            if self.delegated_user:
                request = Request()
                if not base_creds.valid:
                    base_creds.refresh(request)
                sa_email = getattr(base_creds, "service_account_email", None)
                if not sa_email:
                    raise ValueError(
                        "Unable to determine runtime service account email for delegated Gmail auth."
                    )
                signer = iam.Signer(request, base_creds, sa_email)
                creds = service_account.Credentials(
                    signer=signer,
                    service_account_email=sa_email,
                    token_uri="https://oauth2.googleapis.com/token",
                    scopes=GMAIL_SCOPES,
                    subject=self.delegated_user,
                )
            else:
                creds = base_creds
        self.service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        self.retry_max_attempts = max(int(os.getenv("GMAIL_API_MAX_ATTEMPTS", "5")), 1)
        self.retry_base_sleep = max(float(os.getenv("GMAIL_API_BASE_SLEEP_SECONDS", "1")), 0)
        self.retry_max_sleep = max(float(os.getenv("GMAIL_API_MAX_SLEEP_SECONDS", "30")), 0)

    def _is_retryable_http_error(self, exc: HttpError) -> bool:
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status in {429, 500, 502, 503, 504}:
            return True
        reason = str(exc).lower()
        return "ratelimitexceeded" in reason or "user-rate limit exceeded" in reason

    def _execute(self, request: Any) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1, self.retry_max_attempts + 1):
            try:
                return request.execute()
            except HttpError as exc:
                last_exc = exc
                if attempt >= self.retry_max_attempts or not self._is_retryable_http_error(exc):
                    raise
                backoff = min(self.retry_max_sleep, self.retry_base_sleep * (2 ** (attempt - 1)))
                sleep_for = backoff + random.uniform(0, min(1.0, backoff))
                time.sleep(sleep_for)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Gmail API request failed without returning a response.")

    def list_messages(self, query: str, max_results: int = 10) -> list[dict[str, Any]]:
        response = (
            self._execute(
                self.service.users()
                .messages()
                .list(userId="me", q=query, maxResults=max_results)
            )
        )
        return response.get("messages", [])

    def _get_subject_from_payload(self, payload: dict[str, Any]) -> str:
        headers = payload.get("headers", [])
        for header in headers:
            if header.get("name", "").lower() == "subject":
                return header.get("value", "")
        return ""

    def get_message_metadata(self, message_id: str) -> dict[str, Any]:
        return self._execute(
            self.service.users()
            .messages()
            .get(userId="me", id=message_id, format="metadata", metadataHeaders=["Subject"])
        )

    def fetch_attachments_from_message(
        self,
        message_id: str,
        filename_regex: str = r".*\.(csv|zip)$",
    ) -> list[AttachmentPayload]:
        compiled = re.compile(filename_regex) if filename_regex else None
        message = self._execute(
            self.service.users()
            .messages()
            .get(userId="me", id=message_id, format="full")
        )
        payload = message.get("payload", {})

        attachments: list[AttachmentPayload] = []
        for att in self._extract_attachments(message_id=message_id, payload=payload):
            if compiled and not compiled.search(att.filename):
                continue
            attachments.append(att)
        return attachments

    def find_latest_messages_by_subjects(
        self,
        subject_filters: dict[str, str],
        lookback_days: int,
        max_results: int,
    ) -> dict[str, dict[str, Any]]:
        query = f"has:attachment newer_than:{lookback_days}d"
        messages = self.list_messages(query=query, max_results=max_results)
        remaining = {
            project_id: subject.strip().lower()
            for project_id, subject in subject_filters.items()
            if subject and subject.strip()
        }
        matches: dict[str, dict[str, Any]] = {}

        for msg in messages:
            if not remaining:
                break
            metadata = self.get_message_metadata(msg["id"])
            payload = metadata.get("payload", {})
            subject = self._get_subject_from_payload(payload)
            if not subject:
                continue
            subject_lower = subject.lower()
            matched_project_ids = [
                project_id
                for project_id, needle in remaining.items()
                if needle in subject_lower
            ]
            for project_id in matched_project_ids:
                matches[project_id] = {
                    "message_id": msg["id"],
                    "subject": subject,
                }
                remaining.pop(project_id, None)
        return matches

    def _extract_attachments(self, message_id: str, payload: dict[str, Any]) -> list[AttachmentPayload]:
        out: list[AttachmentPayload] = []
        stack = [payload]

        while stack:
            part = stack.pop()
            body = part.get("body", {})
            filename = part.get("filename")
            attachment_id = body.get("attachmentId")

            if filename and attachment_id:
                att = self._execute(
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId="me", messageId=message_id, id=attachment_id)
                )
                raw_data = att.get("data", "")
                file_bytes = base64.urlsafe_b64decode(raw_data.encode("utf-8"))
                headers = {h["name"]: h["value"] for h in payload.get("headers", [])}
                out.append(
                    AttachmentPayload(
                        message_id=message_id,
                        filename=filename,
                        raw_bytes=file_bytes,
                        headers=headers,
                    )
                )

            for child in part.get("parts", []):
                stack.append(child)

        return out

    def fetch_matching_attachments(
        self, pipeline: PipelineConfig, query_override: str | None = None
    ) -> list[AttachmentPayload]:
        query = query_override or pipeline.gmail_query
        messages = self.list_messages(query=query)
        compiled = (
            re.compile(pipeline.attachment.filename_regex)
            if pipeline.attachment.filename_regex
            else None
        )

        attachments: list[AttachmentPayload] = []
        for msg in messages:
            message_id = msg["id"]
            for att in self.fetch_attachments_from_message(message_id=message_id, filename_regex=""):
                if compiled and not compiled.search(att.filename):
                    continue
                attachments.append(att)

        return attachments

    def fetch_attachments_by_query(
        self,
        query: str,
        filename_regex: str = r".*\.(csv|zip)$",
        max_results: int = 20,
        latest_only: bool = False,
    ) -> list[AttachmentPayload]:
        messages = self.list_messages(query=query, max_results=max_results)
        if latest_only and messages:
            messages = messages[:1]

        attachments: list[AttachmentPayload] = []
        for msg in messages:
            message_id = msg["id"]
            attachments.extend(
                self.fetch_attachments_from_message(
                    message_id=message_id,
                    filename_regex=filename_regex,
                )
            )

        return attachments
