"""Strict Base32 handling and RFC 6238 code generation for account login."""
from __future__ import annotations

import base64
import binascii
import re
import time
from datetime import datetime, timezone

import pyotp


_BASE32 = re.compile(r"[A-Z2-7]+\Z")
_MAX_SECRET_LENGTH = 128
TOTP_INTERVAL = 30


def normalize_secret(value: str) -> str:
    """Normalize a pasted authenticator key without accepting arbitrary text."""
    if not isinstance(value, str) or len(value) > _MAX_SECRET_LENGTH:
        raise ValueError("二次验证密钥格式不正确，请输入 Base32 密钥")
    compact = "".join(value.split()).replace("-", "").upper()
    unpadded = compact.rstrip("=")
    supplied_padding = compact[len(unpadded):]
    if (not unpadded or not _BASE32.fullmatch(unpadded)
            or len(unpadded) % 8 in (1, 3, 6)
            or supplied_padding not in ("", "=" * ((-len(unpadded)) % 8))):
        raise ValueError("二次验证密钥格式不正确，请输入 Base32 密钥")
    padded = unpadded + "=" * ((-len(unpadded)) % 8)
    try:
        decoded = base64.b32decode(padded, casefold=False)
    except (binascii.Error, ValueError):
        raise ValueError("二次验证密钥格式不正确，请输入 Base32 密钥") from None
    if not decoded or len(decoded) > 64:
        raise ValueError("二次验证密钥格式不正确，请输入 Base32 密钥")
    return unpadded


def generate_code(secret: str, timestamp: float | None = None) -> tuple[str, int, float]:
    """Return (code, time-step counter, seconds remaining in this code window)."""
    normalized = normalize_secret(secret)
    instant = time.time() if timestamp is None else float(timestamp)
    counter = int(instant // TOTP_INTERVAL)
    remaining = TOTP_INTERVAL - instant % TOTP_INTERVAL
    code = pyotp.TOTP(normalized, interval=TOTP_INTERVAL).at(
        datetime.fromtimestamp(instant, timezone.utc)
    )
    return code, counter, remaining
