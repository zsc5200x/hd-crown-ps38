"""Use a visible Google Chrome session for the PS3838 page and its WebSocket feed."""
from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from pinnacle_ws_parser import (
    UnsupportedOddsUpdate,
    parse_complete_odds_update,
    parse_incremental_odds_update,
)


SPORTS_URL = "https://www.ps3838.com/zh-cn/sports/soccer"
WEBSOCKET_HOST = "www.ps3838.com"
WEBSOCKET_PATH = "/sports-websocket/ws"


def parse_odds_frame(
    frame: str | bytes,
    parse_response: Callable[[Mapping[str, Any]], Any],
) -> tuple[Any, Any] | None:
    """Return parsed events only for a complete UPDATE_ODDS snapshot."""
    return parse_odds_message(decode_odds_frame(frame), parse_response)


def decode_odds_frame(frame: str | bytes) -> dict[str, Any] | None:
    """Decode one WebSocket frame once, ignoring non-object and malformed JSON."""
    try:
        text = frame.decode("utf-8") if isinstance(frame, bytes) else frame
        message = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None
    return message if isinstance(message, dict) else None


def parse_odds_message(
    message: Mapping[str, Any] | None,
    parse_response: Callable[[Mapping[str, Any]], Any],
    diagnostics: dict[str, Any] | None = None,
) -> tuple[Any, Any] | None:
    """Parse a decoded UPDATE_ODDS message when it contains a full snapshot."""
    if not isinstance(message, Mapping):
        return None
    odds = message.get("odds")
    if message.get("type") != "UPDATE_ODDS" or not isinstance(odds, dict):
        return None
    try:
        if odds.get("refreshAll") is True:
            return parse_complete_odds_update(message, parse_response)
        return parse_incremental_odds_update(message, parse_response, diagnostics)
    except UnsupportedOddsUpdate as exc:
        if diagnostics is not None:
            diagnostics["parser_rejection"] = str(exc)
        return None


def summarize_odds_frame(frame: str | bytes) -> dict[str, Any] | None:
    """Return structural metadata only; never retain event or credential values."""
    return summarize_odds_message(decode_odds_frame(frame))


def summarize_odds_message(message: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Summarize an already decoded frame without retaining value data."""
    if not isinstance(message, Mapping) or message.get("type") != "UPDATE_ODDS":
        return None

    odds = message.get("odds")
    if not isinstance(odds, dict):
        return {"type": "UPDATE_ODDS", "odds_kind": type(odds).__name__}

    def value_shape(value: Any, depth: int = 0) -> dict[str, Any]:
        if isinstance(value, dict):
            return {"kind": "object", "size": len(value)}
        if isinstance(value, list):
            shape: dict[str, Any] = {"kind": "array", "length": len(value)}
            if value and depth < 2:
                shape["sample_item_shapes"] = [
                    value_shape(item, depth + 1) for item in value[:3]
                ]
            return shape
        if isinstance(value, str):
            return {"kind": "string", "length": len(value)}
        if value is None:
            return {"kind": "null"}
        return {"kind": type(value).__name__}

    def tree_summary(value: Any, present: bool) -> dict[str, Any]:
        summary: dict[str, Any] = {"present": present, "kind": type(value).__name__}
        if not isinstance(value, list):
            return summary

        leagues = 0
        matches = 0
        for sport in value:
            if not isinstance(sport, list) or len(sport) < 3 or not isinstance(sport[2], list):
                continue
            for league in sport[2]:
                if not isinstance(league, list) or len(league) < 3:
                    continue
                leagues += 1
                if isinstance(league[2], list):
                    matches += len(league[2])
        summary.update({"sports": len(value), "leagues": leagues, "matches": matches})
        return summary

    def update_row_summary(value: Any) -> dict[str, Any]:
        counts: dict[str, int] = {}
        samples: list[dict[str, Any]] = []
        sample_keys: set[tuple[Any, ...]] = set()
        groups = 0
        records = 0
        if isinstance(value, list):
            for group in value:
                if not isinstance(group, list) or len(group) < 2 or not isinstance(group[1], list):
                    continue
                groups += 1
                for row in group[1]:
                    if not isinstance(row, list):
                        continue
                    records += 1
                    length_key = str(len(row))
                    counts[length_key] = counts.get(length_key, 0) + 1
                    field_types = tuple(
                        "null" if field is None else
                        "object" if isinstance(field, dict) else
                        "array" if isinstance(field, list) else
                        "boolean" if isinstance(field, bool) else
                        "number" if isinstance(field, (int, float)) else
                        "string" if isinstance(field, str) else
                        "other"
                        for field in row
                    )
                    key = (len(row), field_types)
                    if key in sample_keys or len(samples) >= 8:
                        continue
                    sample_keys.add(key)
                    samples.append({"length": len(row), "field_types": list(field_types)})
        return {
            "groups": groups,
            "records": records,
            "record_length_counts": counts,
            "sample_record_shapes": samples,
        }

    return {
        "type": "UPDATE_ODDS",
        "refresh_all": odds.get("refreshAll") is True,
        "refresh_all_kind": _safe_kind(odds.get("refreshAll")) if "refreshAll" in odds else "missing",
        "fields": sorted(str(key) for key in odds),
        "auxiliary_shapes": {
            str(key): value_shape(value)
            for key, value in odds.items()
            if key not in {"refreshAll", "n", "l", "u"}
        },
        "n": tree_summary(odds.get("n"), "n" in odds),
        "l": tree_summary(odds.get("l"), "l" in odds),
        "u": value_shape(odds.get("u")),
        "u_rows": update_row_summary(odds.get("u")),
    }


def _safe_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "other"


def summarize_other_protocol_message(message: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Summarize non-odds WebSocket message type and shape without values."""
    if not isinstance(message, Mapping) or message.get("type") == "UPDATE_ODDS":
        return None
    raw_type = message.get("type")
    if not isinstance(raw_type, str):
        message_type = "non_string"
    elif re.fullmatch(r"[A-Za-z0-9_.:-]{1,48}", raw_type):
        message_type = raw_type
    else:
        message_type = "other"

    def shape(value: Any) -> dict[str, Any]:
        result = {"kind": _safe_kind(value)}
        if isinstance(value, (list, dict)):
            result["size"] = len(value)
        if isinstance(value, list) and value:
            result["sample_item_kinds"] = sorted({_safe_kind(item) for item in value[:8]})
        return result

    return {
        "protocol_message": True,
        "message_type": message_type,
        "fields": sorted(str(key) for key in message),
        "field_shapes": {
            str(key): shape(value) for key, value in message.items() if key != "type"
        },
    }


def summarize_incremental_tree_parse(
    frame: str | bytes,
    parse_response: Callable[[Mapping[str, Any]], Any],
) -> dict[str, Any] | None:
    """Report whether incremental n/l trees yield events without retaining event data."""
    message = decode_odds_frame(frame)
    summary = summarize_odds_message(message)
    if summary is None or summary.get("refresh_all") is True:
        return summary
    try:
        odds = message.get("odds")
        if not isinstance(odds, dict):
            return summary
        parsed = parse_response({
            "n": odds.get("n") if isinstance(odds.get("n"), list) else [],
            "l": odds.get("l") if isinstance(odds.get("l"), list) else [],
        })
        events, _leagues = parsed
        summary["incremental_tree_parsed"] = True
        summary["tree_parsed_events"] = len(events)
        summary["tree_parsed_prematch"] = sum(
            isinstance(event, Mapping) and not bool(event.get("is_live"))
            for event in events
        )
        summary["tree_parsed_live"] = sum(
            isinstance(event, Mapping) and bool(event.get("is_live"))
            for event in events
        )
    except Exception:
        summary["incremental_tree_parsed"] = False
    return summary


def is_odds_websocket_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except (TypeError, ValueError):
        return False
    return parsed.hostname == WEBSOCKET_HOST and parsed.path == WEBSOCKET_PATH


class Ps3838BrowserController:
    """Own a headed, persistent Chrome process on one Playwright worker thread."""

    def __init__(
        self,
        profile_dir: str | Path,
        parse_response: Callable[[Mapping[str, Any]], Any],
        on_odds_update: Callable[[Any, Any, Mapping[str, Any]], None],
        on_status: Callable[[str], None] | None = None,
        *,
        playwright_factory: Callable[[], Any] | None = None,
        on_frame_diagnostic: Callable[[Mapping[str, Any]], None] | None = None,
    ):
        self.profile_dir = Path(profile_dir)
        self.parse_response = parse_response
        self.on_odds_update = on_odds_update
        self.on_status = on_status or (lambda _status: None)
        self.playwright_factory = playwright_factory
        self.on_frame_diagnostic = on_frame_diagnostic or (lambda _summary: None)
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._credentials: dict[str, str] | None = None
        self._focus_requested = threading.Event()
        self._reload_requested = threading.Event()
        self._stop_requested = threading.Event()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return bool(self._thread and self._thread.is_alive())

    @property
    def running(self) -> bool:
        return self.is_running

    def start(self, username: str, password: str) -> bool:
        """Start Chrome and submit saved credentials once; return if newly started."""
        username = str(username or "").strip()
        password = str(password or "")
        if not username or not password:
            raise ValueError("平博账号和密码不能为空")

        with self._lock:
            if self._thread and self._thread.is_alive():
                self._focus_requested.set()
                return False
            self._credentials = {"username": username, "password": password}
            self._stop_requested.clear()
            self._focus_requested.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="ps3838-chrome",
                daemon=True,
            )
            self._thread.start()
            return True

    def stop(self, timeout: float = 5) -> None:
        """Request a clean browser shutdown, primarily for tests and app exit."""
        self._stop_requested.set()
        with self._lock:
            thread = self._thread
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(0, timeout))

    def refresh(self) -> bool:
        """Reload the current sports page on its owning Playwright thread."""
        with self._lock:
            if not self._thread or not self._thread.is_alive():
                return False
            self._reload_requested.set()
            self._focus_requested.set()
            return True

    def _run(self) -> None:
        playwright = None
        context = None
        try:
            if self.playwright_factory is None:
                from playwright.sync_api import sync_playwright

                playwright = sync_playwright().start()
            else:
                playwright = self.playwright_factory()
                if hasattr(playwright, "start"):
                    playwright = playwright.start()

            self.profile_dir.mkdir(parents=True, exist_ok=True)
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.profile_dir),
                channel="chrome",
                headless=False,
                viewport=None,
                locale="zh-CN",
            )
            self.on_status("browser_open")
            page = context.pages[0] if context.pages else context.new_page()
            page.on("websocket", self._attach_websocket)
            page.goto(SPORTS_URL, wait_until="domcontentloaded")
            page.bring_to_front()
            self._submit_credentials_once(page)
            today_soccer_selected = False

            while not self._stop_requested.is_set() and not page.is_closed():
                page.wait_for_timeout(200)
                if not today_soccer_selected:
                    today_soccer_selected = self._select_today_soccer(page)
                if self._reload_requested.is_set():
                    self._reload_requested.clear()
                    today_soccer_selected = False
                    page.reload(wait_until="domcontentloaded")
                if self._focus_requested.is_set():
                    self._focus_requested.clear()
                    page.bring_to_front()
            if page.is_closed():
                self.on_status("browser_closed")
        except Exception as exc:
            # Keep errors free of exception text, which can include request URLs.
            self.on_status(f"browser_error:{type(exc).__name__}")
        finally:
            if context is not None:
                try:
                    context.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass
            with self._lock:
                self._credentials = None
                self._thread = None

    def _select_today_soccer(self, page: Any) -> bool:
        """Select today's soccer feed, which includes live and same-day fixtures."""
        try:
            tab = page.locator(".SportMenuItemTabComponent .market-tab").filter(
                has_text=re.compile(r"今天")
            ).first
            if tab.count() == 0 or not tab.is_visible():
                return False
            if "active" not in (tab.get_attribute("class") or ""):
                tab.click(timeout=1000)
                page.wait_for_timeout(500)
            if "active" not in (tab.get_attribute("class") or ""):
                return False
            self.on_status("today_subscription_selected")
            return True
        except Exception:
            return False

    def _submit_credentials_once(self, page: Any) -> None:
        with self._lock:
            credentials = self._credentials
            self._credentials = None
        if not credentials:
            return

        username = page.locator("#loginId")
        password = page.locator("#pass")
        try:
            if not username.is_visible() or not password.is_visible():
                login_button = page.get_by_role(
                    "button", name=re.compile(r"登录|Log in", re.IGNORECASE)
                ).first
                if login_button.count() == 0 or not login_button.is_visible():
                    login_button = page.get_by_text(
                        re.compile(r"登录|Log in", re.IGNORECASE)
                    ).first
                if login_button.count() == 0 or not login_button.is_visible():
                    guest_banner = page.get_by_text(
                        re.compile(r"访客.*赔率.*延迟|guest.*odds.*delay", re.IGNORECASE)
                    ).first
                    if guest_banner.count() and guest_banner.is_visible():
                        self.on_status("guest_odds_delay_detected")
                    else:
                        self.on_status("login_state_unverified")
                    return
                login_button.click(timeout=2000)

            username.wait_for(state="visible", timeout=20000)
            password.wait_for(state="visible", timeout=20000)
            username.fill(credentials["username"])
            password.fill(credentials["password"])
            form = username.locator("xpath=ancestor::form")
            form.get_by_role("button", name="登录").click()
            self.on_status("login_submitted")

            try:
                username.wait_for(state="hidden", timeout=15000)
                login_button = page.get_by_role(
                    "button", name=re.compile(r"登录|Log in", re.IGNORECASE)
                ).first
                guest_banner = page.get_by_text(
                    re.compile(r"访客.*赔率.*延迟|guest.*odds.*delay", re.IGNORECASE)
                ).first
                if (
                    login_button.count() and login_button.is_visible()
                ) or (
                    guest_banner.count() and guest_banner.is_visible()
                ):
                    self.on_status("login_needs_manual_action:LoginStillVisible")
                else:
                    self.on_status("login_confirmed")
            except Exception:
                self.on_status("login_needs_manual_action:LoginNotConfirmed")
        except Exception as exc:
            # The page remains open so the user can handle changed forms or challenges.
            self.on_status(f"login_needs_manual_action:{type(exc).__name__}")
        finally:
            credentials.clear()

    def _attach_websocket(self, websocket: Any) -> None:
        if not is_odds_websocket_url(websocket.url):
            return
        self.on_status("websocket_detected")
        frame_seen = False

        def on_frame(frame: str | bytes) -> None:
            nonlocal frame_seen
            started = time.perf_counter()
            frame_received_monotonic = time.monotonic()
            frame_received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if not frame_seen:
                frame_seen = True
                self.on_status("websocket_frame_received")

            decode_started = time.perf_counter()
            message = decode_odds_frame(frame)
            decode_ms = (time.perf_counter() - decode_started) * 1000
            if message is None:
                return
            if message.get("type") != "UPDATE_ODDS":
                other_diagnostic = summarize_other_protocol_message(message)
                if other_diagnostic is not None:
                    other_diagnostic.update({
                        "frame_received_at": frame_received_at,
                        "_frame_received_monotonic": frame_received_monotonic,
                        "frame_decode_ms": round(decode_ms, 3),
                        "frame_processing_ms": round((time.perf_counter() - started) * 1000, 3),
                    })
                    self._report_frame_diagnostic(other_diagnostic)
                return

            summary_started = time.perf_counter()
            diagnostic = summarize_odds_message(message)
            summary_ms = (time.perf_counter() - summary_started) * 1000
            if diagnostic is not None:
                diagnostic["frame_received_at"] = frame_received_at
                diagnostic["_frame_received_monotonic"] = frame_received_monotonic

            parse_started = time.perf_counter()
            try:
                parsed = parse_odds_message(message, self.parse_response, diagnostic)
            except Exception:
                parse_ms = (time.perf_counter() - parse_started) * 1000
                if diagnostic is not None:
                    diagnostic["parser_accepted"] = False
                    diagnostic.update({
                        "frame_decode_ms": round(decode_ms, 3),
                        "frame_summary_ms": round(summary_ms, 3),
                        "frame_parse_ms": round(parse_ms, 3),
                        "frame_processing_ms": round((time.perf_counter() - started) * 1000, 3),
                    })
                    self._report_frame_diagnostic(diagnostic)
                self.on_status("odds_frame_processing_error")
                return
            parse_ms = (time.perf_counter() - parse_started) * 1000

            if diagnostic is not None:
                diagnostic["parser_accepted"] = parsed is not None
                if parsed is not None:
                    events, _leagues = parsed
                    diagnostic["parsed_events"] = len(events)
                    diagnostic["parsed_prematch"] = sum(
                        isinstance(event, Mapping) and not bool(event.get("is_live"))
                        for event in events
                    )
                    diagnostic["parsed_live"] = sum(
                        isinstance(event, Mapping) and bool(event.get("is_live"))
                        for event in events
                    )
            callback_ms = 0.0
            try:
                if parsed is not None:
                    callback_started = time.perf_counter()
                    self.on_odds_update(*parsed, diagnostic or {})
                    callback_ms = (time.perf_counter() - callback_started) * 1000
            except Exception:
                self.on_status("odds_frame_processing_error")
            if diagnostic is not None:
                diagnostic.update({
                    "frame_decode_ms": round(decode_ms, 3),
                    "frame_summary_ms": round(summary_ms, 3),
                    "frame_parse_ms": round(parse_ms, 3),
                    "state_commit_ms": round(callback_ms, 3),
                    "frame_processing_ms": round((time.perf_counter() - started) * 1000, 3),
                })
                self._report_frame_diagnostic(diagnostic)

        websocket.on("framereceived", on_frame)
        websocket.on("close", lambda _socket: self.on_status("websocket_closed"))

    def _report_frame_diagnostic(self, summary: Mapping[str, Any]) -> None:
        try:
            self.on_frame_diagnostic(summary)
        except Exception:
            pass
