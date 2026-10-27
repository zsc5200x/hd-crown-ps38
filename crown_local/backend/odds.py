"""Shared odds-format conversion helpers.

The upstream APIs can return several display formats, while this project
calculates combined water in Hong Kong odds. Keep the conversion in one
place so the report, quote worker, and Telegram output use the same rules.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import math


_FORMAT_ALIASES = {
    "H": "HK",
    "HONGKONG": "HK",
    "HONG KONG": "HK",
    "MALAY": "MY",
    "INDONESIAN": "ID",
    "INDONESIA": "ID",
}


def to_hk_water(value, odds_format: str | None = "HK") -> float | None:
    """Convert a supported odds value to a positive Hong Kong water."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    text = str(value).strip()
    if len(text) > 40:
        return None
    try:
        number = Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not number.is_finite() or number == 0:
        return None

    fmt = str(odds_format or "HK").strip().upper()
    fmt = _FORMAT_ALIASES.get(fmt, fmt)
    if fmt == "HK":
        converted = number if number > 0 else None
    elif fmt == "EU":
        converted = number - 1 if number > 1 else None
    elif fmt == "MY":
        converted = (number if number > 0 else -1 / number) if abs(number) <= 1 else None
    elif fmt == "ID":
        converted = (number if number > 0 else -1 / number) if abs(number) >= 1 else None
    else:
        return None

    if converted is None or not converted.is_finite() or converted <= 0:
        return None
    try:
        result = float(converted)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None
