"""Shared filters for provider event feeds."""
from __future__ import annotations

import unicodedata


# These feeds expose virtual/e-soccer fixtures as football leagues. They are
# not real-world fixtures and must stay out of matching and persistence.
_EXCLUDED_LEAGUE_MARKERS = ("特别投注", "电竞足球", "足球电竞", "电子足球")


def is_excluded_league(league_name) -> bool:
    """Whether a provider league should be omitted from event processing."""
    normalized = unicodedata.normalize("NFKC", str(league_name or ""))
    return any(marker in normalized for marker in _EXCLUDED_LEAGUE_MARKERS)
