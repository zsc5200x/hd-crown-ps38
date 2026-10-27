"""Encrypted persistence for the local PS3838 account credentials."""
from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from pinnacle_session import SessionStore


DEFAULT_CREDENTIALS_PATH = Path(__file__).with_name("data") / "pinnacle_credentials.enc"


class PinnacleCredentialStore:
    """Store username and password in a Fernet-encrypted local file."""

    def __init__(self, path: str | Path = DEFAULT_CREDENTIALS_PATH):
        self._store = SessionStore(path)
        self._lock = threading.RLock()

    @property
    def path(self) -> Path:
        return self._store.path

    def save(self, username: str, password: str) -> None:
        if not isinstance(username, str) or not username.strip():
            raise ValueError("平博账号不能为空")
        if not isinstance(password, str) or not password:
            raise ValueError("平博密码不能为空")
        with self._lock:
            self._store.save({"username": username.strip(), "password": password})

    def load(self) -> dict[str, str] | None:
        with self._lock:
            payload: dict[str, Any] | None = self._store.load()
        if not isinstance(payload, dict):
            return None
        username = payload.get("username")
        password = payload.get("password")
        if not isinstance(username, str) or not username.strip():
            return None
        if not isinstance(password, str) or not password:
            return None
        return {"username": username.strip(), "password": password}
