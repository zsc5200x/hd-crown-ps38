"""Persistent manual Crown-to-Pinnacle event mapping decisions."""
from __future__ import annotations

import json
import os
from pathlib import Path
import threading
from datetime import datetime, timezone


_PATH = Path(__file__).resolve().parent / "data" / "event_mappings.json"
_LOCK = threading.RLock()


def _read():
    try:
        payload = json.loads(_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"confirmed": [], "rejected": []}
    except (OSError, json.JSONDecodeError):
        return {"confirmed": [], "rejected": []}
    if not isinstance(payload, dict):
        return {"confirmed": [], "rejected": []}
    confirmed = payload.get("confirmed")
    rejected = payload.get("rejected")
    return {
        "confirmed": [row for row in confirmed if isinstance(row, dict)] if isinstance(confirmed, list) else [],
        "rejected": [row for row in rejected if isinstance(row, dict)] if isinstance(rejected, list) else [],
    }


def _write(payload):
    _PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = _PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    os.replace(temp, _PATH)


def list_mappings():
    with _LOCK:
        return _read()


def confirm(crown_id, pinnacle_id, crown_event, pinnacle_event):
    crown_id, pinnacle_id = str(crown_id), str(pinnacle_id)
    if not crown_id or not pinnacle_id:
        raise ValueError("缺少比赛 ID")
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = {
        "crown_id": crown_id,
        "pinnacle_id": pinnacle_id,
        "home": str(crown_event.get("home") or ""),
        "away": str(crown_event.get("away") or ""),
        "pinnacle_home": str(pinnacle_event.get("home") or ""),
        "pinnacle_away": str(pinnacle_event.get("away") or ""),
        "confirmed_at": now,
    }
    with _LOCK:
        payload = _read()
        confirmed = payload["confirmed"]
        for old in confirmed:
            if old.get("crown_id") == crown_id or old.get("pinnacle_id") == pinnacle_id:
                if old.get("crown_id") == crown_id and old.get("pinnacle_id") == pinnacle_id:
                    old.update(row)
                    payload["rejected"] = [item for item in payload["rejected"]
                                           if item.get("crown_id") != crown_id
                                           and item.get("pinnacle_id") != pinnacle_id]
                    _write(payload)
                    return old
                raise ValueError("该比赛已绑定到另一场比赛，请先解除原映射")
        confirmed.append(row)
        payload["rejected"] = [item for item in payload["rejected"]
                               if item.get("crown_id") != crown_id
                               and item.get("pinnacle_id") != pinnacle_id]
        _write(payload)
    return row


def reject(crown_id, pinnacle_id):
    crown_id, pinnacle_id = str(crown_id), str(pinnacle_id)
    row = {"crown_id": crown_id, "pinnacle_id": pinnacle_id}
    with _LOCK:
        payload = _read()
        payload["rejected"] = [item for item in payload["rejected"]
                               if not (item.get("crown_id") == crown_id
                                       and item.get("pinnacle_id") == pinnacle_id)]
        payload["rejected"].append(row)
        _write(payload)
    return row


def unconfirm(crown_id):
    crown_id = str(crown_id)
    with _LOCK:
        payload = _read()
        before = len(payload["confirmed"])
        payload["confirmed"] = [item for item in payload["confirmed"]
                                if item.get("crown_id") != crown_id]
        if before != len(payload["confirmed"]):
            _write(payload)
        return before != len(payload["confirmed"])
