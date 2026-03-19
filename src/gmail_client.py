from __future__ import annotations

import base64
import os
import re
from dataclasses import dataclass
from typing import Any

import google.auth
from google.auth import iam
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build

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

    def list_messages(self, query: str, max_results: int = 10) -> list[dict[str, Any]]:
        response = (
            self.service.users()
            .messages()
            .list(userId="me", q=query, maxResults=max_results)
            .execute()
        )
        return response.get("messages", [])

    def _extract_attachments(self, message_id: str, payload: dict[str, Any]) -> list[AttachmentPayload]:
        out: list[AttachmentPayload] = []
        stack = [payload]

        while stack:
            part = stack.pop()
            body = part.get("body", {})
            filename = part.get("filename")
            attachment_id = body.get("attachmentId")

            if filename and attachment_id:
                att = (
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId="me", messageId=message_id, id=attachment_id)
                    .execute()
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
            message = (
                self.service.users()
                .messages()
                .get(userId="me", id=message_id, format="full")
                .execute()
            )
            payload = message.get("payload", {})
            for att in self._extract_attachments(message_id=message_id, payload=payload):
                if compiled and not compiled.search(att.filename):
                    continue
                attachments.append(att)

        return attachments

    def fetch_attachments_by_query(
        self,
        query: str,
        filename_regex: str = r".*\.(csv|zip)$",
        max_results: int = 20,
    ) -> list[AttachmentPayload]:
        messages = self.list_messages(query=query, max_results=max_results)
        compiled = re.compile(filename_regex) if filename_regex else None

        attachments: list[AttachmentPayload] = []
        for msg in messages:
            message_id = msg["id"]
            message = (
                self.service.users()
                .messages()
                .get(userId="me", id=message_id, format="full")
                .execute()
            )
            payload = message.get("payload", {})
            for att in self._extract_attachments(message_id=message_id, payload=payload):
                if compiled and not compiled.search(att.filename):
                    continue
                attachments.append(att)

        return attachments
