from __future__ import annotations

import hmac
import os
from dataclasses import dataclass

from itsdangerous import BadSignature, URLSafeSerializer


@dataclass
class PasswordAuth:
    cookie_name: str = "cm360_session"

    def __post_init__(self) -> None:
        self.password = os.getenv("APP_PASSWORD", "")
        self.secret = os.getenv("APP_SESSION_SECRET", "dev-session-secret-change-me")
        self.serializer = URLSafeSerializer(self.secret, salt="cm360-auth")

    def enabled(self) -> bool:
        return bool(self.password.strip())

    def verify_password(self, plain_text: str) -> bool:
        return hmac.compare_digest(plain_text or "", self.password)

    def issue_cookie_value(self) -> str:
        return self.serializer.dumps({"ok": True})

    def validate_cookie_value(self, value: str | None) -> bool:
        if not value:
            return False
        try:
            data = self.serializer.loads(value)
            return bool(data.get("ok"))
        except BadSignature:
            return False

