import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from crown_local.backend.crown_markets import normalize_line
from crown_local.backend.odds import to_hk_water
from event_filters import is_excluded_league
from event_mapping_store import confirm, list_mappings, reject, unconfirm
from match_engine import match_events, parse_crown_start, parse_pinnacle_start
from pinnacle_credentials import PinnacleCredentialStore
from ps3838_browser import Ps3838BrowserController

from crown_runtime import (
    account_action,
    add_account,
    initialize as initialize_crown,
    lock as crown_runtime_lock,
    public_accounts,
    refresh_once,
    state as crown_state,
    submit_otp,
)

HOST = "127.0.0.1"
PORT = 8887
PINNACLE_ODDS_FORMAT = os.environ.get("PINNACLE_ODDS_FORMAT", "HK").strip().upper()
if PINNACLE_ODDS_FORMAT not in {"HK", "EU", "MY", "ID"}:
    PINNACLE_ODDS_FORMAT = "HK"
CROWN_EVENT_TIMEZONE = os.environ.get("CROWN_EVENT_TIMEZONE", "America/New_York")
lock = threading.Lock()
_combined_cache_lock = threading.Lock()
_combined_cache = {"key": None, "created_at": 0.0, "payload": None}
_COMBINED_CACHE_TTL = 2.0
state = {
    "events": [],
    "leagues": [],
    "updated_at": None,
    "received_at": None,
    "request_duration_ms": None,
    "upstream_http_date": None,
    "error": None,
    "fetching": False,
    "browser_status": "尚未启动 Chrome",
    "browser_login_status": "尚未检查",
    "websocket_diagnostics": {
        "odds_frames_total": 0,
        "other_frames_total": 0,
        "full_snapshots": 0,
        "incremental_odds_frames": 0,
        "last_odds_frame": None,
        "last_other_frame": None,
        "last_full_snapshot": None,
        "last_incremental_update": None,
        "last_full_snapshot_interval_ms": None,
    },
}
_ps3838_browser_lock = threading.RLock()
_pinnacle_credentials = PinnacleCredentialStore()
_last_full_snapshot_monotonic = None
_pinnacle_section_events = {"n": [], "l": []}


def _on_browser_odds_update(events, _leagues, frame_summary=None):
    committed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    received_at = (
        frame_summary.get("frame_received_at")
        if isinstance(frame_summary, dict) and frame_summary.get("frame_received_at")
        else committed_at
    )
    with lock:
        if isinstance(frame_summary, dict) and frame_summary.get("refresh_all") is False:
            cached_ids = {
                str(event.get("id") or "")
                for section_events in _pinnacle_section_events.values()
                for event in section_events
                if isinstance(event, dict) and event.get("id")
            }
            update_ids = {
                str(event.get("id") or "")
                for event in events
                if isinstance(event, dict) and event.get("id")
            }
            frame_summary["cache_events_before"] = len(cached_ids)
            frame_summary["incremental_cache_matches"] = len(cached_ids & update_ids)
            frame_summary["incremental_cache_misses"] = len(update_ids - cached_ids)
            changed = False
            applicable = False
            for section in ("n", "l"):
                is_live_section = section == "l"
                current = {
                    str(event.get("id") or ""): event
                    for event in _pinnacle_section_events[section]
                    if isinstance(event, dict) and event.get("id")
                }
                section_changed = False
                for update in events:
                    if not isinstance(update, dict):
                        continue
                    if "is_live" in update and bool(update.get("is_live")) != is_live_section:
                        continue
                    event_id = str(update.get("id") or "")
                    previous = current.get(event_id)
                    if not event_id or previous is None:
                        continue
                    applicable = applicable or _incremental_patch_is_applicable(previous, update)
                    merged_event = _merge_incremental_event(previous, update)
                    if merged_event != previous:
                        current[event_id] = merged_event
                        section_changed = True
                if section_changed:
                    _pinnacle_section_events[section] = list(current.values())
                    changed = True

            if applicable:
                state["received_at"] = received_at
                state["error"] = None
                state["browser_status"] = "Chrome 已收到平博增量赔率"

            if not changed:
                if update_ids - cached_ids and not cached_ids:
                    login_note = "（登录状态未确认）" if state.get("browser_login_status") == "状态未确认" else ""
                    state["browser_status"] = f"收到赔率增量但缺少赛事基础列表{login_note}"
                return

            merged = {str(event.get("id") or ""): event for event in _pinnacle_section_events["n"]}
            merged.update({str(event.get("id") or ""): event for event in _pinnacle_section_events["l"]})
            state["events"] = [event for event_id, event in merged.items() if event_id]
            state["leagues"] = sorted({event["league"] for event in state["events"] if event.get("league")})
            state["updated_at"] = committed_at
            state["received_at"] = received_at
            state["request_duration_ms"] = None
            state["upstream_http_date"] = None
            state["error"] = None
            state["browser_status"] = "Chrome 已实时更新赔率"
            return

        if isinstance(frame_summary, dict):
            for section in ("n", "l"):
                shape = frame_summary.get(section)
                if not isinstance(shape, dict) or shape.get("kind") != "list":
                    continue
                is_live_section = section == "l"
                section_events = [
                    event for event in events
                    if isinstance(event, dict) and bool(event.get("is_live")) == is_live_section
                ]
                _pinnacle_section_events[section] = (
                    section_events if is_live_section else _pinnacle_today_events(section_events)
                )
        else:
            _pinnacle_section_events["n"] = _pinnacle_today_events([
                event for event in events if isinstance(event, dict) and not bool(event.get("is_live"))
            ])
            _pinnacle_section_events["l"] = [
                event for event in events if isinstance(event, dict) and bool(event.get("is_live"))
            ]

        merged = {str(event.get("id") or ""): event for event in _pinnacle_section_events["n"]}
        merged.update({str(event.get("id") or ""): event for event in _pinnacle_section_events["l"]})
        state["events"] = [event for event_id, event in merged.items() if event_id]
        state["leagues"] = sorted({event["league"] for event in state["events"] if event.get("league")})
        state["updated_at"] = committed_at
        state["received_at"] = received_at
        state["request_duration_ms"] = None
        state["upstream_http_date"] = None
        state["error"] = None
        state["browser_status"] = "Chrome 已收到赔率快照"


def _merge_incremental_event(previous: dict, update: dict) -> dict:
    """Merge verified fields from one partial event without erasing absent markets."""
    merged = dict(previous)
    for key in ("league_id", "league", "home", "away", "score", "minute", "period", "start_ms"):
        value = update.get(key)
        if value not in (None, ""):
            merged[key] = value

    previous_markets = previous.get("markets") if isinstance(previous.get("markets"), dict) else {}
    update_markets = update.get("markets") if isinstance(update.get("markets"), dict) else {}
    markets = dict(previous_markets)
    for market_type in ("handicap", "total"):
        rows = update_markets.get(market_type)
        if not isinstance(rows, list) or not rows:
            continue
        if market_type == "handicap" and any(isinstance(row, dict) and "side_line" in row for row in rows):
            combined = _merge_incremental_handicap_rows(markets.get(market_type), rows)
        elif market_type == "total" and any(isinstance(row, dict) and "price" in row for row in rows):
            combined = _merge_incremental_total_rows(markets.get(market_type), rows)
        else:
            combined = _merge_incremental_market_rows(markets.get(market_type), rows)
        if combined != markets.get(market_type):
            markets[market_type] = combined

    moneyline = update_markets.get("moneyline")
    if isinstance(moneyline, list) and any(valid_price(value) for value in moneyline):
        old_moneyline = markets.get("moneyline")
        combined = list(old_moneyline) if isinstance(old_moneyline, list) else []
        if len(combined) < len(moneyline):
            combined.extend([""] * (len(moneyline) - len(combined)))
        for index, value in enumerate(moneyline):
            if valid_price(value):
                combined[index] = str(value)
        if combined != old_moneyline:
            markets["moneyline"] = combined

    if markets != previous_markets:
        merged["markets"] = markets

    old_quotes = previous.get("match_quotes")
    new_quotes = list(update.get("match_quotes") or []) if isinstance(update.get("match_quotes"), list) else []
    new_quotes.extend(_quotes_from_incremental_market_rows(previous_markets, update_markets))
    if new_quotes:
        quotes_by_key = {
            (quote.get("market_type"), quote.get("period"), quote.get("selection"), quote.get("line")): dict(quote)
            for quote in old_quotes if isinstance(quote, dict)
        } if isinstance(old_quotes, list) else {}
        for quote in new_quotes:
            if not isinstance(quote, dict) or not valid_price(quote.get("hk_odds")):
                continue
            if not _incremental_quote_matches_market(quote, markets):
                continue
            key = (quote.get("market_type"), quote.get("period"), quote.get("selection"), quote.get("line"))
            quotes_by_key[key] = dict(quote)
        combined_quotes = list(quotes_by_key.values())
        if combined_quotes != old_quotes:
            merged["match_quotes"] = combined_quotes

    return merged


def _incremental_patch_is_applicable(previous, update):
    old_markets = previous.get("markets") if isinstance(previous.get("markets"), dict) else {}
    new_markets = update.get("markets") if isinstance(update.get("markets"), dict) else {}
    moneyline = new_markets.get("moneyline")
    if isinstance(moneyline, list) and any(valid_price(value) for value in moneyline):
        return True

    old_handicap = old_markets.get("handicap")
    for item in new_markets.get("handicap") or []:
        if not isinstance(item, dict):
            continue
        if "side_line" not in item and (valid_price(item.get("a")) or valid_price(item.get("b"))):
            return True
        line = _line_text(item.get("side_line"))
        if line is not None and _unique_handicap_row_index(
            old_handicap, line, item.get("selection")
        ) is not None:
            return True

    old_total = old_markets.get("total")
    for item in new_markets.get("total") or []:
        if not isinstance(item, dict):
            continue
        if "price" not in item and (valid_price(item.get("a")) or valid_price(item.get("b"))):
            return True
        line = _line_value(item.get("line"))
        if line is not None and isinstance(old_total, list) and any(
            isinstance(row, dict) and _line_value(row.get("line")) == line
            for row in old_total
        ):
            return True
    return False


def _merge_incremental_market_rows(previous_rows, updates):
    old_rows = [dict(row) for row in previous_rows if isinstance(row, dict)] if isinstance(previous_rows, list) else []
    rows_by_key = {str(row.get("line") or ""): row for row in old_rows}
    update_keys = []
    for update in updates:
        if not isinstance(update, dict) or not (valid_price(update.get("a")) or valid_price(update.get("b"))):
            continue
        key = str(update.get("line") or "")
        if not key:
            continue
        current = dict(rows_by_key.get(key, {}))
        for field in ("line", "home_line", "away_line"):
            if update.get(field) not in (None, ""):
                current[field] = update[field]
        for side in ("a", "b"):
            if valid_price(update.get(side)):
                current[side] = str(update[side])
        rows_by_key[key] = current
        if key not in update_keys:
            update_keys.append(key)

    if not update_keys:
        return old_rows
    ordered_keys = update_keys + [str(row.get("line") or "") for row in old_rows if str(row.get("line") or "") not in update_keys]
    return [rows_by_key[key] for key in ordered_keys if key in rows_by_key]


def _merge_incremental_handicap_rows(previous_rows, updates):
    rows = [dict(row) for row in previous_rows if isinstance(row, dict)] if isinstance(previous_rows, list) else []
    for update in updates:
        if not isinstance(update, dict) or not valid_price(update.get("price")):
            continue
        side_line = _line_text(update.get("side_line"))
        selection = update.get("selection")
        row_index = _unique_handicap_row_index(rows, side_line, selection)
        if row_index is None:
            continue
        side_field = {"home": "a", "away": "b"}[selection]
        rows[row_index][side_field] = str(update["price"])
    return rows


def _unique_handicap_row_index(rows, line, selection):
    """Find a cached row by selected side and that side's line; reject ambiguity."""
    side_field = {"home": "home_line", "away": "away_line"}.get(selection)
    target_line = _line_value(line)
    if side_field is None or target_line is None or not isinstance(rows, list):
        return None
    matches = [
        index for index, row in enumerate(rows)
        if isinstance(row, dict) and _line_value(row.get(side_field)) == target_line
    ]
    return matches[0] if len(matches) == 1 else None


def _merge_incremental_total_rows(previous_rows, updates):
    rows = [dict(row) for row in previous_rows if isinstance(row, dict)] if isinstance(previous_rows, list) else []
    for update in updates:
        if not isinstance(update, dict) or not valid_price(update.get("price")):
            continue
        update_line = _line_value(update.get("line"))
        if update_line is None:
            continue
        side_field = {"over": "a", "under": "b"}.get(update.get("selection"))
        if side_field is None:
            continue
        for row in rows:
            if _line_value(row.get("line")) == update_line:
                row[side_field] = str(update["price"])
                break
    return rows


def _incremental_quote_matches_market(quote, markets):
    market_type = quote.get("market_type")
    if market_type == "moneyline":
        return True
    rows = markets.get(market_type)
    if not isinstance(rows, list):
        return False
    quote_line = _line_value(quote.get("line"))
    if quote_line is None:
        return False
    if market_type == "handicap":
        return _unique_handicap_row_index(rows, quote_line, quote.get("selection")) is not None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if market_type == "total" and quote_line == _line_value(row.get("line")):
            return True
    return False


def _quotes_from_incremental_market_rows(previous_markets, update_markets):
    quotes = []
    handicap_rows = update_markets.get("handicap")
    previous_handicap = previous_markets.get("handicap")
    if isinstance(handicap_rows, list) and isinstance(previous_handicap, list):
        for update in handicap_rows:
            if not isinstance(update, dict) or not valid_price(update.get("price")):
                continue
            side_line = _line_value(update.get("side_line"))
            selection = update.get("selection")
            if _unique_handicap_row_index(previous_handicap, side_line, selection) is None:
                continue
            price = str(update["price"])
            quotes.append({
                "market_type": "handicap", "period": "full", "selection": selection,
                "line": format(side_line, "f"), "hk_odds": float(price), "raw_odds": price,
            })

    total_rows = update_markets.get("total")
    previous_total = previous_markets.get("total")
    if isinstance(total_rows, list) and isinstance(previous_total, list):
        for update in total_rows:
            if not isinstance(update, dict) or not valid_price(update.get("price")):
                continue
            line = _line_value(update.get("line"))
            selection = update.get("selection")
            if line is None or selection not in {"over", "under"}:
                continue
            price = str(update["price"])
            quotes.append({
                "market_type": "total", "period": "full", "selection": selection,
                "line": format(line, "f"), "hk_odds": float(price), "raw_odds": price,
            })
    return quotes


_ps3838_browser = None


def _get_ps3838_browser():
    global _ps3838_browser
    with _ps3838_browser_lock:
        if _ps3838_browser is None:
            _ps3838_browser = Ps3838BrowserController(
                Path(__file__).with_name("data") / "ps3838_chrome",
                parse_response,
                _on_browser_odds_update,
                _on_browser_status,
                on_frame_diagnostic=_on_browser_frame_diagnostic,
            )
        return _ps3838_browser


def _public_pinnacle_credentials() -> dict:
    saved = _pinnacle_credentials.load()
    return {
        "configured": bool(saved),
        "username": saved["username"] if saved else "",
    }


def _pinnacle_today_events(events, *, today=None):
    """Keep live events and pre-match events scheduled for the local calendar day."""
    if today is None:
        today = datetime.now().astimezone().date()
    visible = []
    for event in events or []:
        if not isinstance(event, dict):
            continue
        if event.get("is_live"):
            visible.append(event)
            continue
        start_ms = event.get("start_ms")
        if isinstance(start_ms, bool) or not isinstance(start_ms, (int, float)):
            continue
        try:
            event_day = datetime.fromtimestamp(float(start_ms) / 1000).date()
        except (OSError, OverflowError, TypeError, ValueError):
            continue
        if event_day == today:
            visible.append(event)
    return visible


def _save_pinnacle_credentials(payload: dict) -> dict:
    username = payload.get("username")
    password = payload.get("password")
    if not isinstance(username, str) or not isinstance(password, str):
        raise ValueError("平博账号和密码不能为空")
    _pinnacle_credentials.save(username, password)
    return _public_pinnacle_credentials()


def _open_ps3838_browser() -> dict:
    saved = _pinnacle_credentials.load()
    if not saved:
        raise ValueError("请先保存平博账号和密码")
    started = _get_ps3838_browser().start(saved["username"], saved["password"])
    return {"ok": True, "started": bool(started)}


def _on_browser_status(status: str) -> None:
    labels = {
        "browser_open": "Chrome 已打开，正在等待平博页面",
        "login_submitted": "已在 Chrome 提交登录表单",
        "login_confirmed": "Chrome 登录窗口已关闭",
        "login_already_authenticated": "Chrome 登录状态已确认",
        "login_state_unverified": "Chrome 登录状态暂未确认",
        "guest_odds_delay_detected": "平博页面仍显示访客赔率延迟，请确认登录",
        "websocket_detected": "已检测到平博赔率 WebSocket",
        "websocket_frame_received": "Chrome 已收到平博服务器数据",
        "today_subscription_selected": "Chrome 已切换到足球今日赛事，等待滚球与今日赔率",
        "websocket_closed": "平博赔率 WebSocket 已断开",
        "browser_closed": "Chrome 已关闭",
        "login_needs_manual_action": "请在 Chrome 完成登录或验证",
        "odds_frame_processing_error": "收到赔率帧，但解析失败",
    }
    if status.startswith("browser_error:"):
        label = f"Chrome 启动失败（{status.partition(':')[2]}）"
    elif status.startswith("login_needs_manual_action:"):
        label = "请在 Chrome 完成登录或验证"
    else:
        label = labels.get(status, "Chrome 状态更新")
    with lock:
        state["browser_status"] = label
        if status == "websocket_frame_received" and state.get("browser_login_status") == "状态未确认":
            state["browser_status"] = "WebSocket 已连接，但登录状态未确认"
        if status in {"login_confirmed", "login_already_authenticated"}:
            state["browser_login_status"] = "已验证登录"
        elif status == "login_state_unverified":
            state["browser_login_status"] = "状态未确认"
        elif status == "guest_odds_delay_detected":
            state["browser_login_status"] = "访客模式，赔率有平台延迟"
        elif status == "login_submitted":
            state["browser_login_status"] = "已提交，等待平台确认"
        elif status.startswith("login_needs_manual_action:"):
            state["browser_login_status"] = "需要在 Chrome 完成验证或检查账号密码"


def _on_browser_frame_diagnostic(summary) -> None:
    global _last_full_snapshot_monotonic
    received_at = summary.get("frame_received_at") or datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    now = summary.get("_frame_received_monotonic")
    if not isinstance(now, (int, float)):
        now = time.monotonic()
    record = {
        key: value for key, value in summary.items()
        if key != "_frame_received_monotonic"
    }
    record["received_at"] = received_at

    with lock:
        diagnostics = state["websocket_diagnostics"]
        if summary.get("protocol_message") is True:
            diagnostics["other_frames_total"] += 1
            diagnostics["last_other_frame"] = record
            return
        diagnostics["odds_frames_total"] += 1
        diagnostics["last_odds_frame"] = record

        if summary.get("refresh_all") is True:
            diagnostics["full_snapshots"] += 1
            if _last_full_snapshot_monotonic is not None:
                diagnostics["last_full_snapshot_interval_ms"] = round(
                    (now - _last_full_snapshot_monotonic) * 1000, 1
                )
            _last_full_snapshot_monotonic = now
            diagnostics["last_full_snapshot"] = record
        else:
            diagnostics["incremental_odds_frames"] += 1
            diagnostics["last_incremental_update"] = record


def _pinnacle_feed_status() -> dict:
    """Return freshness metadata for the last recognized browser odds update."""
    with lock:
        updated_at = state.get("updated_at")
        received_at = state.get("received_at")
        request_duration_ms = state.get("request_duration_ms")
        upstream_http_date = state.get("upstream_http_date")
        error = state.get("error")
        visible_events = _pinnacle_today_events(state.get("events") or [])
        event_count = len(visible_events)
        browser_status = state.get("browser_status")
        browser_login_status = state.get("browser_login_status")
        has_live_events = any(bool(event.get("is_live")) for event in visible_events)
        websocket_diagnostics = dict(state.get("websocket_diagnostics") or {})
        for key in ("last_odds_frame", "last_other_frame", "last_full_snapshot", "last_incremental_update"):
            if isinstance(websocket_diagnostics.get(key), dict):
                websocket_diagnostics[key] = dict(websocket_diagnostics[key])

    freshness_at = received_at or updated_at
    age_seconds = None
    if freshness_at:
        try:
            parsed = datetime.fromisoformat(str(freshness_at).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            age_seconds = max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            age_seconds = None
    stale_after = 15.0
    feed_stale = bool(error) or not freshness_at or age_seconds is None or age_seconds > stale_after
    if not updated_at:
        feed_status = "等待 Chrome 赔率数据"
    elif feed_stale:
        feed_status = "Chrome 赔率数据滞后"
    else:
        feed_status = "Chrome 赔率数据新鲜"
    return {
        "feed_status": feed_status,
        "feed_stale": feed_stale,
        "feed_age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "feed_updated_at": updated_at,
        "feed_received_at": received_at,
        "feed_request_duration_ms": request_duration_ms,
        "feed_upstream_http_date": upstream_http_date,
        "feed_error": error,
        "feed_event_count": event_count,
        "feed_has_live_events": has_live_events,
        "browser_status": browser_status,
        "browser_login_status": browser_login_status,
        "websocket_diagnostics": websocket_diagnostics,
    }
CROWN_REFRESH_SECONDS = 5
def crown_refresh_loop():
    while True:
        refresh_started = time.monotonic()
        refresh_once()
        remaining = CROWN_REFRESH_SECONDS - (time.monotonic() - refresh_started)
        if remaining > 0:
            time.sleep(remaining)


def _hk_display(value, odds_format):
    converted = to_hk_water(value, odds_format)
    if converted is None:
        return ""
    try:
        rounded = (Decimal(str(converted)) * 100).to_integral_value(rounding=ROUND_DOWN) / 100
    except (InvalidOperation, ValueError):
        return ""
    return f"{rounded:.2f}"


def _hk_number(value, odds_format=PINNACLE_ODDS_FORMAT):
    """Return a numeric HK value for matching while keeping display rounding."""
    text = _hk_display(value, odds_format)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _line_value(value):
    normalized = normalize_line(value)
    return normalized


def _line_text(value):
    normalized = _line_value(value)
    if normalized is None:
        return None
    return format(normalized.normalize(), "f")


def _asian_line_display(value):
    text = re.sub(r"\s+", "", str(value or "").strip())
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    if not text:
        return ""
    if "/" in text:
        return "/".join(text.split("/"))
    if text.startswith("-"):
        rest = text[1:]
        divider = rest.find("-")
        if divider >= 0:
            return f"-{rest[:divider]}/{rest[divider + 1:]}"
        return text
    divider = text.find("-")
    if divider > 0:
        return f"{text[:divider]}/{text[divider + 1:]}"
    return text


def _handicap_display_label(item, side, display_line):
    side_line = str(item.get(f"{side}_line") or "").strip()
    if side_line:
        signed_line = _asian_line_display(side_line)
        signed_value = normalize_line(signed_line)
        if signed_value is not None:
            if signed_value == 0:
                return "0（平手）"
            magnitude = "/".join(part.lstrip("+-") for part in display_line.split("/"))
            return f"{'-' if signed_value < 0 else '+'}{magnitude or display_line}"

    signed_line = _asian_line_display(item.get("line"))
    signed_value = normalize_line(signed_line)
    if signed_value == 0:
        return "0（平手）"
    if signed_line.startswith("-"):
        direction = "让" if side == "home" else "受让"
    elif signed_line.startswith("+"):
        direction = "受让" if side == "home" else "让"
    else:
        direction = "让" if side == "home" else "受让"
    magnitude = "/".join(part.lstrip("+-") for part in display_line.split("/"))
    if not magnitude:
        return display_line or "盘口未知"
    return f"{'-' if direction == '让' else '+'}{magnitude}"


def _pinnacle_display_event(event):
    result = dict(event)
    markets = event.get("markets") if isinstance(event.get("markets"), dict) else {}
    display = {"handicap": [], "total": [], "moneyline": []}
    for market_name in ("handicap", "total"):
        source_items = markets.get(market_name)
        if not isinstance(source_items, list):
            continue
        for index, item in enumerate(source_items):
            if not isinstance(item, dict):
                continue
            raw_a, raw_b = str(item.get("a") or ""), str(item.get("b") or "")
            a_hk, b_hk = _hk_display(raw_a, PINNACLE_ODDS_FORMAT), _hk_display(raw_b, PINNACLE_ODDS_FORMAT)
            display_line = _asian_line_display(item.get("line"))
            converted_item = {
                **item, "raw_a": raw_a, "raw_b": raw_b,
                "a": a_hk, "b": b_hk, "a_hk": a_hk, "b_hk": b_hk,
                "odds_format": PINNACLE_ODDS_FORMAT,
                "display_line": display_line,
                "market_role": item.get("market_role") or ("main" if index == 0 else "alternate"),
            }
            if market_name == "handicap":
                converted_item["home_display_line"] = _handicap_display_label(item, "home", display_line)
                converted_item["away_display_line"] = _handicap_display_label(item, "away", display_line)
            display[market_name].append(converted_item)
    source_money = markets.get("moneyline")
    if isinstance(source_money, list):
        display["moneyline"] = [_hk_display(value, PINNACLE_ODDS_FORMAT) for value in source_money]
    result["display_markets"] = display
    result["odds_format"] = PINNACLE_ODDS_FORMAT
    result["odds_format_label"] = "港赔（HK）"
    return result


def _pinnacle_display_events(events):
    return [_pinnacle_display_event(event) for event in events if isinstance(event, dict)]


def _snapshot_crown():
    with crown_runtime_lock:
        return {
            "events": list(crown_state.get("events") or []),
            "leagues": list(crown_state.get("leagues") or []),
            "updated_at": crown_state.get("updated_at"),
            "status": crown_state.get("status"),
            "error": crown_state.get("error"),
            "source": crown_state.get("source"),
            "diagnostics": dict(crown_state.get("diagnostics") or {}),
        }


def _combined_payload():
    with lock:
        pinnacle_events = _pinnacle_today_events(state.get("events") or [])
        pinnacle_updated_at = state.get("updated_at")
        pinnacle_error = state.get("error")
    crown = _snapshot_crown()
    mappings = list_mappings()
    mapping_signature = json.dumps(mappings, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    cache_key = (
        crown.get("updated_at"), crown.get("status"), crown.get("error"), len(crown["events"]),
        pinnacle_updated_at, pinnacle_error,
        tuple(str(event.get("id") or "") for event in pinnacle_events), mapping_signature,
        CROWN_EVENT_TIMEZONE, PINNACLE_ODDS_FORMAT,
    )
    with _combined_cache_lock:
        now_monotonic = time.monotonic()
        if (_combined_cache["key"] == cache_key
                and now_monotonic - _combined_cache["created_at"] < _COMBINED_CACHE_TTL
                and _combined_cache["payload"] is not None):
            return _combined_cache["payload"]
        payload = _build_combined_payload(
            crown, pinnacle_events, pinnacle_updated_at, pinnacle_error, mappings,
        )
        _combined_cache.update({"key": cache_key, "created_at": time.monotonic(), "payload": payload})
        return payload


def _build_combined_payload(crown, pinnacle_events, pinnacle_updated_at, pinnacle_error, mappings):
    reference = datetime.now(timezone.utc)
    pinnacle_feed = _pinnacle_feed_status()
    matched = match_events(
        crown["events"], pinnacle_events,
        confirmed_mappings=mappings["confirmed"],
        rejected_mappings=mappings["rejected"],
        now=reference,
        crown_timezone=CROWN_EVENT_TIMEZONE,
    )

    def time_debug(crown_event, pinnacle_event):
        """Expose the provider timestamps in a common, inspectable format."""
        crown_utc = parse_crown_start(
            (crown_event or {}).get("start_label"),
            now=reference,
            crown_timezone=CROWN_EVENT_TIMEZONE,
        ) if crown_event else None
        pinnacle_utc = parse_pinnacle_start(
            (pinnacle_event or {}).get("start_ms")
        ) if pinnacle_event else None
        try:
            crown_zone = ZoneInfo(CROWN_EVENT_TIMEZONE)
        except (ZoneInfoNotFoundError, TypeError):
            crown_zone = None
        crown_local = crown_utc.astimezone(crown_zone) if crown_utc and crown_zone else None
        delta = (abs((crown_utc - pinnacle_utc).total_seconds()) / 60
                 if crown_utc and pinnacle_utc else None)
        return {
            "crown_start_local": crown_local.isoformat(timespec="minutes") if crown_local else None,
            "crown_start_utc": crown_utc.isoformat(timespec="minutes") if crown_utc else None,
            "pinnacle_start_utc": pinnacle_utc.isoformat(timespec="minutes") if pinnacle_utc else None,
            "time_delta_minutes": round(delta, 1) if delta is not None else None,
            "crown_timezone": CROWN_EVENT_TIMEZONE,
        }

    def convert_row(row, *, candidate=False):
        converted = dict(row)
        profile = row.get("match_profile")
        if isinstance(profile, dict):
            # Keep the auditable scores, but do not duplicate the full provider
            # event inside the public JSON payload.
            converted["match_profile"] = {
                key: value for key, value in profile.items() if key != "event"
            }
        converted.update(time_debug(row.get("crown"), row.get("pinnacle")))
        if row.get("crown"):
            converted["crown"] = _crown_display_event(row["crown"])
        if row.get("pinnacle"):
            converted["pinnacle"] = _pinnacle_display_event(row["pinnacle"])
        evidence = row.get("evidence") or []
        converted["manual"] = "人工确认映射" in evidence
        confidence = row.get("confidence")
        converted["confidence_label"] = "人工确认" if converted["manual"] else (
            f"置信度 {round(float(confidence) * 100)}%" if confidence else "需人工核对")
        return converted

    confirmed = [convert_row(row) for row in matched["confirmed"]]
    all_events = []
    for row in matched["rows"]:
        converted = convert_row(row)
        # Keep one row per Crown fixture. Candidate Pinnacle events remain in
        # the dedicated review tab and are not exposed as a confirmed pairing.
        converted.pop("candidates", None)
        all_events.append(converted)
    candidates = []
    for row in matched["candidates"]:
        for candidate in row.get("candidates", []):
            candidates.append(convert_row({
                "crown": row.get("crown"), "pinnacle": candidate.get("event"),
                "status": "candidate", "confidence": candidate.get("confidence"),
                "evidence": candidate.get("evidence", []),
                "matcher_version": candidate.get("matcher_version"),
                "match_profile": candidate,
            }, candidate=True))
    unmatched = [convert_row(row) for row in matched["unmatched"]]
    conflicts = [convert_row(row) for row in matched["conflicts"]]
    all_rows = confirmed + candidates + unmatched + conflicts
    deltas = [row["time_delta_minutes"] for row in all_rows
              if row.get("time_delta_minutes") is not None]
    return {
        "crown_total": len(crown["events"]),
        "pinnacle_total": len(pinnacle_events),
        "all_events": all_events,
        "confirmed": confirmed,
        "candidates": candidates,
        "unmatched": unmatched,
        "conflicts": conflicts,
        "counts": matched["counts"],
        "crown_updated_at": crown.get("updated_at"),
        "pinnacle_updated_at": pinnacle_updated_at,
        "pinnacle_feed": pinnacle_feed,
        "crown_status": crown.get("status"),
        "crown_error": crown.get("error"),
        "crown_stale": bool(crown.get("error") and crown["events"]),
        "pinnacle_error": pinnacle_error,
        "crown_timezone": matched.get("crown_timezone"),
        "pinnacle_timezone": "UTC",
        "timezone_check": {
            "crown_timezone": CROWN_EVENT_TIMEZONE,
            "pinnacle_timezone": "UTC",
            "checked_pairs": len(deltas),
            "aligned_pairs": sum(delta <= 1 for delta in deltas),
            "max_delta_minutes": max(deltas) if deltas else None,
        },
        "odds_format": "HK",
        "odds_format_warning": ("平博输入格式：" + PINNACLE_ODDS_FORMAT)
            if PINNACLE_ODDS_FORMAT != "HK" else None,
        "manual_conflicts": matched.get("manual_conflicts", []),
        "matcher_version": matched.get("matcher_version"),
    }


def _crown_display_event(event):
    result = dict(event)
    markets = event.get("markets") if isinstance(event.get("markets"), dict) else {}
    display = {"handicap": [], "total": [], "moneyline": []}
    for market_name in ("handicap", "total"):
        items = markets.get(market_name)
        if isinstance(items, list):
            display[market_name] = [dict(item) for item in items if isinstance(item, dict)]
    moneyline = markets.get("moneyline")
    if isinstance(moneyline, list):
        display["moneyline"] = [_hk_display(value, "HK") for value in moneyline]
    result["display_markets"] = display
    result.setdefault("odds_format_label", "皇冠港赔（HK）")
    return result

def get_pair(value):
    if isinstance(value, list) and len(value) >= 2:
        return value[0], value[1]
    return None, None

def valid_price(value):
    if value is None or value == "":
        return False
    try:
        return float(value) > 0
    except (ValueError, TypeError):
        return False

def extract_markets(match):
    result = {"handicap": [], "total": [], "moneyline": [], "match_quotes": []}
    if len(match) <= 8 or not isinstance(match[8], dict):
        return result

    # "0" is the main-period market group in the observed response.
    group = match[8].get("0")
    if not isinstance(group, list):
        return result

    # Asian handicap: [line, opposite line, label, price A, price B, ...]
    if len(group) > 0 and isinstance(group[0], list):
        for row in group[0]:
            if isinstance(row, list) and len(row) >= 5:
                a, b = row[3], row[4]
                if valid_price(a) or valid_price(b):
                    result["handicap"].append({
                        "line": str(row[2] if row[2] not in (None, "") else row[0]),
                        "home_line": "" if row[0] is None else str(row[0]),
                        "away_line": "" if row[1] is None else str(row[1]),
                        "a": str(a or ""),
                        "b": str(b or ""),
                    })
                    # Pinnacle provides the two signed side lines in row[0]
                    # and row[1].  Only retain a comparable quote when those
                    # lines parse and are exact opposites; never infer the
                    # side from the display order alone.
                    home_line = _line_value(row[0])
                    away_line = _line_value(row[1])
                    if (home_line is not None and away_line is not None
                            and home_line == -away_line):
                        line_pairs = (("home", home_line, a), ("away", away_line, b))
                        for selection, line, raw_price in line_pairs:
                            hk_odds = _hk_number(raw_price)
                            if hk_odds is None:
                                continue
                            result["match_quotes"].append({
                                "market_type": "handicap",
                                "period": "full",
                                "selection": selection,
                                "line": format(line.normalize(), "f"),
                                "hk_odds": hk_odds,
                                "raw_odds": str(raw_price or ""),
                            })

    # Totals: [display line, numeric line, over price, under price, ...]
    if len(group) > 1 and isinstance(group[1], list):
        for row in group[1]:
            if isinstance(row, list) and len(row) >= 4:
                a, b = row[2], row[3]
                if valid_price(a) or valid_price(b):
                    result["total"].append({
                        "line": str(row[0] or row[1]),
                        "a": str(a or ""),
                        "b": str(b or ""),
                    })
                    line = _line_text(row[1] if len(row) > 1 else row[0])
                    if line is not None:
                        for selection, raw_price in (("over", a), ("under", b)):
                            hk_odds = _hk_number(raw_price)
                            if hk_odds is None:
                                continue
                            result["match_quotes"].append({
                                "market_type": "total",
                                "period": "full",
                                "selection": selection,
                                "line": line,
                                "hk_odds": hk_odds,
                                "raw_odds": str(raw_price or ""),
                            })

    # Three-way prices, when supplied.
    if len(group) > 2 and isinstance(group[2], list):
        row = group[2]
        if len(row) >= 3 and any(valid_price(x) for x in row[:3]):
            result["moneyline"] = [
                str(row[0] or ""), str(row[1] or ""), str(row[2] or "")
            ]
            for selection, raw_price in zip(("home", "draw", "away"), row[:3]):
                hk_odds = _hk_number(raw_price)
                if hk_odds is None:
                    continue
                result["match_quotes"].append({
                    "market_type": "moneyline",
                    "period": "full",
                    "selection": selection,
                    "line": None,
                    "hk_odds": hk_odds,
                    "raw_odds": str(raw_price or ""),
                })

    return result

def parse_response(data):
    events_by_id = {}
    league_names = set()

    # 同时读取赛前 n 和滚球 l
    for section_name in ("n", "l"):
        sports = data.get(section_name, [])

        for sport in sports if isinstance(sports, list) else []:
            if not isinstance(sport, list) or len(sport) < 3:
                continue
            if sport[0] != 29 and sport[1] not in ("Soccer", "足球"):
                continue

            for league in sport[2] or []:
                if not isinstance(league, list) or len(league) < 3:
                    continue

                league_id = league[0]
                league_name = str(league[1] or "Unknown league")
                if is_excluded_league(league_name):
                    continue
                league_names.add(league_name)

                for m in league[2] or []:
                    if not isinstance(m, list) or len(m) < 5:
                        continue
                    if not isinstance(m[0], (int, float)):
                        continue

                    event_id = str(m[0])
                    home_score, away_score = get_pair(
                        m[9] if len(m) > 9 else None
                    )
                    minute = m[15] if len(m) > 15 else None
                    period = m[16] if len(m) > 16 else None
                    start_ms = (
                        m[4] if isinstance(m[4], (int, float)) else None
                    )

                    event = {
                        "id": event_id,
                        "sport": "soccer",
                        "league_id": str(league_id),
                        "league": league_name,
                        "is_live": section_name == "l",
                        "home": str(m[1] or ""),
                        "away": str(m[2] or ""),
                        "start_ms": start_ms,
                        "score": (
                            f"{home_score} - {away_score}"
                            if isinstance(home_score, (int, float))
                            and isinstance(away_score, (int, float))
                            else ""
                        ),
                        "minute": str(minute or ""),
                        "period": str(period or ""),
                        "markets": extract_markets(m),
                    }
                    event["match_quotes"] = event["markets"].pop("match_quotes", [])

                    # 同一比赛重复出现时，滚球区 l 优先
                    if event_id not in events_by_id or section_name == "l":
                        events_by_id[event_id] = event

    events = list(events_by_id.values())
    events.sort(key=lambda e: (e["league"].lower(), e["home"].lower()))
    return events, sorted(league_names)


HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>平博足球赛事</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Public+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style data-source-style>
:root{color-scheme:light;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
*{box-sizing:border-box}
body{margin:0;background:#eef0ef;color:#202124}
header{position:sticky;top:0;z-index:5;background:#eef0ef;padding:10px 18px;border-bottom:0}
.header-row{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}
h1{font-size:21px;margin:0 0 6px;font-weight:750}
.sub{font-size:12px;color:#737a80}
.page-status{display:none!important}
.header-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.login-button{display:inline-flex;align-items:center;gap:7px;min-height:34px;padding:7px 12px;border:1px solid #0f766e;border-radius:7px;background:#0f766e;color:#fff;font-size:13px;font-weight:700;text-decoration:none;cursor:pointer}
.login-button:hover{background:#115e59}
.login-button .dot{width:7px;height:7px;border-radius:50%;background:#a7f3d0}
.pinnacle-config-button{border-color:#cbd5d1;background:#fff;color:#34413b}
.pinnacle-config-button:hover{background:#f3f6f4}
.pinnacle-login{margin-left:10px}
.settings-modal,.pinnacle-modal{position:fixed;inset:0;z-index:20;display:none;align-items:flex-start;justify-content:center;padding:72px 16px 24px;background:rgba(9,18,32,.52);backdrop-filter:blur(8px)}
.settings-modal.open,.pinnacle-modal.open{display:flex}
.settings-panel{width:min(520px,100%);background:#edf2f7;border:1px solid #9ab4d3;border-radius:18px;box-shadow:0 22px 60px rgba(4,14,29,.38),inset 0 1px rgba(255,255,255,.72);overflow:hidden}
.settings-panel-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;background:linear-gradient(135deg,#dfeaf6,#f6f9fc);border-bottom:1px solid #c3d2e2;color:#172b45;font-weight:760}
.settings-panel-body{display:grid;gap:10px;padding:14px}
.settings-source-card{display:grid;gap:10px;padding:13px 14px;border:1px solid #c8d5e2;border-radius:12px;background:rgba(255,255,255,.72);box-shadow:inset 0 1px rgba(255,255,255,.8)}
.settings-source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;color:#1f3651;font-size:13px;font-weight:760}
.settings-source-copy{color:#6b7b8c;font-size:11px;line-height:1.5}
.settings-source-actions{display:flex;gap:8px;flex-wrap:wrap}
.settings-source-actions a,.settings-source-actions button{display:inline-flex;align-items:center;justify-content:center;min-height:32px;padding:6px 11px;border:1px solid #9cb5d1;border-radius:8px;background:#fff;color:#274666;text-decoration:none;font:inherit;font-size:12px;font-weight:700;cursor:pointer}
.settings-source-actions a:hover,.settings-source-actions button:hover{background:#eaf2fb;border-color:#6d9bd2}
.settings-close{width:30px;height:30px;border:1px solid #a9bfd8;border-radius:50%;background:#eaf1f8;color:#47617c;font-size:19px;line-height:1;cursor:pointer}
.nav-row{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:0}
.source-nav{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));width:min(100%,684px);padding:4px;gap:0;margin-top:0;border:1px solid rgba(116,157,204,.42);border-radius:999px;background:linear-gradient(135deg,rgba(17,39,68,.96),rgba(27,50,82,.9));box-shadow:inset 0 1px rgba(255,255,255,.1),inset 0 -8px 18px rgba(5,18,38,.24),0 8px 22px rgba(15,35,61,.12);backdrop-filter:blur(12px)}
.source-nav a{display:flex;align-items:center;justify-content:center;min-width:0;min-height:42px;padding:7px 16px;border:0;border-radius:999px;background:transparent;color:#b9c8da;text-decoration:none;font-size:13px;font-weight:650;white-space:nowrap;transition:background .24s ease,color .24s ease,box-shadow .24s ease,transform .24s ease}
.source-nav a:hover{color:#edf5ff;background:rgba(112,153,201,.16)}
.source-nav a.active{background:linear-gradient(135deg,#5aa0f6,#3985e8);color:#fff;box-shadow:inset 0 1px rgba(255,255,255,.36),0 0 16px rgba(61,139,240,.34);transform:translateY(-1px)}
.settings-button{display:inline-flex;align-items:center;justify-content:center;flex:0 0 44px;width:44px;height:44px;padding:0;border:1px solid rgba(126,164,207,.58);border-radius:50%;background:linear-gradient(145deg,rgba(27,52,84,.96),rgba(15,35,62,.95));color:#c3d3e5;box-shadow:inset 0 1px rgba(255,255,255,.14),0 7px 18px rgba(10,28,52,.18);cursor:pointer;transition:background .24s ease,color .24s ease,border-color .24s ease,box-shadow .24s ease,transform .24s ease}
.settings-button:hover{border-color:#8cb8ed;background:linear-gradient(145deg,#2b588e,#1d426f);color:#fff;box-shadow:inset 0 1px rgba(255,255,255,.22),0 0 15px rgba(78,148,235,.28);transform:translateY(-1px)}
.settings-button:focus-visible{outline:2px solid #8fc1ff;outline-offset:3px}
.settings-button svg{width:18px;height:18px}
.pinnacle-modal.open{display:flex}
.pinnacle-panel{width:min(480px,100%);background:#fff;border:1px solid #d8dfdc;border-radius:10px;box-shadow:0 18px 50px rgba(20,35,29,.22);overflow:hidden}
.pinnacle-panel-head{display:flex;align-items:center;justify-content:space-between;padding:15px 16px;border-bottom:1px solid #e2e7e4;font-weight:750}
.pinnacle-panel-body{padding:16px;display:grid;gap:10px}
.pinnacle-panel-body label{display:grid;gap:5px;font-size:12px;color:#56615b;font-weight:650}
.pinnacle-panel-body input{border-radius:7px;padding:9px 10px;font-size:13px}
.pinnacle-save-button{min-height:36px;padding:8px 12px;border:1px solid #0f766e;border-radius:7px;background:#0f766e;color:#fff;font-size:13px;font-weight:700;cursor:pointer}
.pinnacle-save-button:hover{background:#115e59}
.pinnacle-save-button:disabled{opacity:.65;cursor:wait}
.pinnacle-close{width:30px;height:30px;border:0;border-radius:6px;background:#f1f4f2;color:#5d6661;font-size:20px;line-height:1;cursor:pointer}
main{max-width:1500px;margin:auto;padding:12px}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:12px}
.stat{background:white;border:1px solid #e1e4e3;border-radius:9px;padding:10px 13px}
.stat small{display:block;font-size:12px;color:#72787d}
.stat strong{display:block;font-size:20px;margin-top:3px}
.controls{display:grid;grid-template-columns:minmax(150px,1fr) 200px;gap:8px;margin-bottom:12px}
input,select{width:100%;min-width:0;border:1px solid #d2d8d6;background:white;border-radius:9px;padding:11px;font-size:14px;color:#202124}
.league{margin-bottom:10px;background:#fff;border:1px solid #dfe3e1;border-radius:9px;overflow:hidden}
.league-head{cursor:pointer;user-select:none}
.league-head:hover{background:#f5f7f6}
.league-head .toggle{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;background:#f0f2f1;color:#4d5552;font-size:18px;transition:transform .18s}
.league.collapsed .league-head .toggle{transform:rotate(-90deg)}
.league.collapsed .match{display:none}
.league-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px 14px;background:#fff;border-bottom:1px solid #e6e8e7;font-weight:750}
.league-name{display:flex;align-items:center;gap:9px}
.flag{font-size:19px}
.league-count{font-size:12px;color:#737a80;font-weight:500;white-space:nowrap}
.match{padding:12px 14px;border-bottom:1px solid #e9ebea}
.match:last-child{border-bottom:0}
.match-top{display:grid;grid-template-columns:minmax(180px,1.2fr) minmax(100px,.55fr) minmax(180px,1.2fr);align-items:center;gap:10px;margin-bottom:10px}
.team{font-size:14px;font-weight:650;overflow-wrap:anywhere}
.away{text-align:right}
.score{text-align:center;font-size:13px;color:#5f666b;white-space:nowrap}
.time{display:block;font-size:11px;color:#777f84;font-weight:400;margin-top:4px}
.markets{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px}
.market{min-width:0;border:1px solid #e1e4e3;border-radius:5px;overflow:hidden}
.market-title{background:#f7f8f8;color:#687078;text-align:center;font-size:12px;padding:7px 4px;border-bottom:1px solid #e6e8e7}
.quote-row{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
.quote{padding:7px 5px;text-align:center;font-size:12px;border-right:1px solid #eceeed;overflow-wrap:anywhere}
.quote:last-child{border-right:0}
.line{display:block;color:#62696f;font-size:11px;margin-bottom:3px}
.price{font-size:13px;font-weight:750;color:#d52c1e}
.price.odds-changed{border-radius:3px;animation:price-change 1.2s ease-out}
.price.odds-up{--price-flash:#d8f3e2;--price-flash-text:#14733d}
.price.odds-down{--price-flash:#ffe2dd;--price-flash-text:#b42318}
@keyframes price-change{0%{background:var(--price-flash);color:var(--price-flash-text);box-shadow:0 0 0 4px var(--price-flash)}100%{background:transparent;color:#d52c1e;box-shadow:0 0 0 0 transparent}}
.empty-price{padding:12px 4px;text-align:center;color:#999;font-size:12px}
.notice{padding:18px;background:white;border-radius:9px;color:#777;text-align:center}
.event-groups{display:grid;gap:14px}
.event-group{border:1px solid #dfe3e1;border-radius:9px;overflow:hidden;background:#f8faf9}
.event-group-head{display:flex;width:100%;min-height:48px;align-items:center;justify-content:space-between;gap:12px;padding:12px 14px;border:0;background:#fff;color:inherit;text-align:left;cursor:pointer}
.event-group-head:hover{background:#f5f7f6}
.event-group-head:focus-visible{outline:2px solid #2563eb;outline-offset:-2px}
.event-group.collapsed .event-group-head{border-bottom:0}
.event-group.collapsed .event-group-body{display:none}
.event-group-title{display:flex;align-items:center;gap:9px;font-size:16px;font-weight:750}
.event-group-title::before{content:"";width:8px;height:8px;border-radius:50%;background:#7a8580}
.event-group.live .event-group-title::before{background:#c83c2f}
.event-group-count{font-size:12px;color:#737a80;white-space:nowrap}
.event-group-toggle{display:inline-flex;width:24px;height:24px;align-items:center;justify-content:center;border-radius:6px;background:#f0f2f1;color:#4d5552;transition:transform .18s}
.event-group.collapsed .event-group-toggle{transform:rotate(-90deg)}
.event-group-body{padding:10px}
.event-group-body .league{margin-bottom:10px}
.event-group-body .league:last-child{margin-bottom:0}
@media(min-width:1100px){
 .match{display:grid;grid-template-columns:minmax(260px,.8fr) minmax(0,2fr);align-items:center;gap:18px}
 .match-top{margin:0;grid-template-columns:minmax(0,1fr) 65px minmax(0,1fr)}
}
@media(max-width:600px){
 header{padding:12px}
 h1{font-size:19px}
 .nav-row{gap:7px}
 .source-nav a{min-height:40px;padding-inline:8px;font-size:12px}
 .settings-button{flex-basis:40px;width:40px;height:40px}
 main{padding:7px}
 .stats{grid-template-columns:repeat(2,minmax(0,1fr))}
 .controls{grid-template-columns:1fr}
 .league-head{padding:11px 10px}
 .match{padding:11px 9px}
 .match-top{grid-template-columns:minmax(0,1fr) 48px minmax(0,1fr);gap:5px}
 .team{font-size:13px}
 .markets{grid-template-columns:repeat(3,minmax(0,1fr));gap:4px}
 .market-title{font-size:11px;padding:7px 1px}
 .quote{padding:7px 2px;font-size:11px}
 .price{font-size:12px}
 .line{font-size:10px}
}

/* Pinnodds-inspired gray operations theme */
:root{
  color-scheme:dark;
  --page-bg:#151a20;
  --surface:#1d242c;
  --surface-raised:#242d36;
  --surface-soft:#28323c;
  --surface-deep:#181e25;
  --line:#394651;
  --line-soft:#303b46;
  --text:#e8edf3;
  --text-strong:#f4f7fa;
  --muted:#9aa8b7;
  --muted-2:#788696;
  --accent:#4da3ff;
  --positive:#35d39a;
}
html{background:#151a20;min-height:100%;}
body{
  min-height:100vh;
  background:radial-gradient(circle at 50% -15%,rgba(64,92,122,.18),transparent 42%),linear-gradient(180deg,#151a20 0%,#171d24 46%,#12171d 100%);
  color:var(--text);
}
header{
  background:rgba(21,27,34,.92);
  border-bottom:1px solid rgba(111,133,154,.28);
  box-shadow:0 10px 26px rgba(0,0,0,.16);
}
main{background:transparent}
.stat,.league,.event-group,.event,.notice,.empty-state,.comparison-group{
  background:var(--surface);
  border-color:var(--line);
  box-shadow:0 8px 24px rgba(0,0,0,.14);
}
.stat small,.league-count,.event-group-count,.sub,.comparison-group-title small,.comparison-status,.comparison-footnote{
  color:var(--muted);
}
.stat strong,.event-group-title,.league-name,.team,.quote-name,.quote-line,.comparison-line-head,.comparison-group-title{
  color:var(--text-strong);
}
.stat{background:linear-gradient(145deg,#222b34,#1c232b)}
.league-head,.event-group-head{
  background:linear-gradient(180deg,#28323c,#232c35);
  color:var(--text-strong);
  border-bottom-color:var(--line);
}
.league-head:hover,.event-group-head:hover{background:#303b46}
.event-group-body{background:rgba(17,22,28,.48)}
.match,.event{background:#1b222a;border-bottom-color:var(--line-soft)}
.match-status,.time-debug,.evidence{color:var(--muted)}
.time-debug{background:#202831;border-bottom-color:var(--line-soft)}
.raw-detail{background:#161c22;border-top-color:var(--line-soft);color:#a9b7c5}
input,select,.tabs,.action{
  background:#222b34;
  color:var(--text);
  border-color:#43515e;
}
input::placeholder{color:#718091}
input:focus,select:focus{border-color:var(--accent);box-shadow:0 0 0 2px rgba(77,163,255,.18);outline:0}
.tabs{background:#202831}
.tabs button{color:#a9b6c3;border-right-color:#3a4652}
.tabs button[aria-pressed="true"]{background:#31506c;color:#fff}
.market,.crown-market-card,.crown-moneyline{
  background:var(--surface-deep);
  border-color:var(--line);
}
.market-title,.crown-market-card .market-title{
  background:#28323b;
  color:var(--text-strong);
  border-color:var(--line);
}
.quote,.crown-market-row,.crown-moneyline-cell{
  background:#202831;
  border-color:#34414c;
  color:var(--text);
}
.quote-side{background:transparent;border-bottom-color:#34414c}
.line,.quote-name,.quote-line,.crown-market-label,.crown-moneyline-cell span{color:#c0cbd5}
.market-extra,.crown-market-footer{background:#1a2128;border-color:#34414c;color:var(--muted)}
.empty-price,.crown-market-empty,.empty{color:var(--muted-2)}
.status-label{background:rgba(53,211,154,.16);color:#65e5b5}
.status-label.candidate{background:rgba(239,181,93,.16);color:#f2c77e}
.status-label.conflict{background:rgba(239,102,102,.16);color:#ff9f9f}
.status-label.unmatched{background:#2b343e;color:#aab7c4}
.action:not(.primary){background:#242e38;color:#d8e0e7;border-color:#43515e}
.action:not(.primary):hover{background:#2d3945}
.settings-panel,.pinnacle-panel,.login-panel{
  background:#202831;
  border-color:#43515e;
  color:var(--text);
}
.settings-panel-head{
  background:linear-gradient(135deg,#293746,#202a34);
  border-bottom-color:#43515e;
  color:var(--text-strong);
}
.settings-source-card{background:#252f39;border-color:#43515e;box-shadow:inset 0 1px rgba(255,255,255,.04)}
.settings-source-head{color:var(--text-strong)}
.settings-source-copy,.login-status-copy,.login-help{color:var(--muted)}
.settings-source-actions a,.settings-source-actions button,.pinnacle-config-button,.action-button{
  background:#27323c;
  color:#dbe5ed;
  border-color:#4a5967;
}
.settings-source-actions a:hover,.settings-source-actions button:hover,.pinnacle-config-button:hover,.action-button:hover{background:#31404d;border-color:#6795be}
.settings-close,.pinnacle-close,.close-button{
  background:#2a3540;
  color:#c4d0db;
  border-color:#4b5a68;
}
.pinnacle-panel-body input,.account-form input,.otp-form input{
  background:#1a222a;
  color:var(--text);
  border-color:#43515e;
}
.login-status{background:#252f39;border-color:#43515e}
.account-row{background:#242e38;border-color:#43515e}
.account-row-meta,.account-row-status,.account-list-title,.login-panel-body label,.account-form label{color:#c0cbd5}
.comparison-line{border-top-color:#34414c}
.comparison-group-title{background:#29343e}
.comparison-line-detail{color:#9aa8b7}
.source{background:#1b2229}
.source+.source{border-left-color:#394651}
.notice,.empty-state{color:#b6c2cd}
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{transition-duration:.01ms!important;animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important}
}

/* Taste Skill · data-terminal: restrained navy surfaces, rules and aligned market data */
:root{
  color-scheme:dark;
  --ground:#0c1c2d;
  --ground-raised:#10243a;
  --surface:#142b43;
  --surface-raised:#193550;
  --surface-quiet:#10243a;
  --border:#2b455f;
  --rule:#263e57;
  --ink:#eef4fb;
  --ink-soft:#d2deeb;
  --muted:#aabbd0;
  --muted-2:#91a5bb;
  --blue:#4f9df8;
  --blue-soft:#b8d9ff;
  --positive:#59c5a0;
  --negative:#ed7a86;
  --warning:#e3bb70;
  --font-ui:"Public Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  --font-data:"IBM Plex Mono","SFMono-Regular",Consolas,monospace;
}
html{min-height:100%;background:var(--ground)}
body{
  min-width:320px;
  min-height:100vh;
  margin:0;
  background:linear-gradient(155deg,#132b43 0%,#0c1c2d 38%,#102238 100%);
  color:var(--ink);
  font-family:var(--font-ui);
  font-size:14px;
  line-height:1.45;
}
header{
  position:sticky;
  top:0;
  z-index:10;
  padding:10px 22px;
  background:rgba(12,28,45,.97);
  border-bottom:1px solid var(--border);
  box-shadow:none;
}
.nav-row{
  display:grid;
  grid-template-columns:minmax(190px,1fr) auto minmax(44px,1fr);
  align-items:center;
  gap:20px;
  width:min(100%,1540px);
  margin:0 auto;
}
.brand-lockup{display:flex;align-items:center;gap:10px;min-width:0;color:var(--ink);text-decoration:none}
.brand-mark{position:relative;display:grid;width:30px;height:30px;flex:none;place-items:center;border:1px solid #528bc2;border-radius:50%}
.brand-mark::before{content:"";width:12px;height:12px;border:1px solid #83b8ec;border-radius:50%}
.brand-copy{display:grid;gap:1px;line-height:1.15}
.brand-copy strong{font-size:13px;font-weight:650;letter-spacing:.01em}
.brand-copy small{color:var(--muted-2);font:500 11px/1.3 var(--font-ui);letter-spacing:0}
.source-nav{
  display:flex;
  align-items:center;
  gap:4px;
  width:auto;
  max-width:100%;
  padding:4px;
  border:1px solid var(--border);
  border-radius:999px;
  background:#10243a;
  box-shadow:none;
  backdrop-filter:none;
}
.source-nav a{
  display:flex;
  align-items:center;
  justify-content:center;
  min-height:36px;
  padding:7px 15px;
  border:0;
  border-radius:999px;
  background:transparent;
  color:var(--muted);
  font-size:12px;
  font-weight:550;
  text-decoration:none;
  white-space:nowrap;
  transition:background-color .16s ease,color .16s ease;
}
.source-nav a:hover{background:#1a3551;color:var(--ink)}
.source-nav a.active{background:#244d75;color:#fff;box-shadow:none;transform:none}
.settings-button{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  justify-self:end;
  width:38px;
  height:38px;
  padding:0;
  border:1px solid var(--border);
  border-radius:999px;
  background:transparent;
  color:var(--ink-soft);
  box-shadow:none;
  cursor:pointer;
  transition:background-color .16s ease,border-color .16s ease,color .16s ease;
}
.settings-button:hover{border-color:#4d739b;background:#18334e;color:#fff;box-shadow:none;transform:none}
.settings-button svg{width:17px;height:17px}
main{width:min(100% - 40px,1480px);margin:0 auto;padding:20px 0 40px;background:transparent}
.page-heading{
  display:flex;
  align-items:flex-end;
  justify-content:space-between;
  gap:18px;
  min-width:0;
  padding:2px 0 14px;
  border-bottom:1px solid var(--rule);
}
.page-heading h1{margin:0;color:var(--ink);font-size:20px;font-weight:600;letter-spacing:-.02em;line-height:1.2}
.page-status{
  display:block!important;
  max-width:min(68%,780px);
  color:var(--muted);
  font-size:11px;
  line-height:1.5;
  text-align:right;
  overflow-wrap:anywhere;
}
.stats{
  display:grid;
  grid-template-columns:minmax(190px,1.25fr) repeat(2,minmax(125px,.72fr)) minmax(175px,1fr);
  gap:0;
  margin:0 0 14px;
  overflow:hidden;
  border:1px solid var(--border);
  border-top:0;
  border-radius:0 0 10px 10px;
  background:rgba(16,36,58,.72);
}
.stat{
  display:flex;
  min-width:0;
  min-height:58px;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:9px 15px;
  border:0;
  border-right:1px solid var(--rule);
  border-radius:0;
  background:transparent;
  box-shadow:none;
}
.stat:last-child{border-right:0}
.stat small{color:var(--muted);font-size:11px;line-height:1.35}
.stat strong{
  display:block;
  margin:0;
  color:var(--ink);
  font:500 15px/1.2 var(--font-data);
  font-variant-numeric:tabular-nums lining-nums;
  text-align:right;
  white-space:nowrap;
}
.stat:first-child strong{font-size:19px}
.controls{
  display:grid;
  grid-template-columns:minmax(0,1fr) 210px;
  gap:9px;
  margin:0 0 16px;
}
input,select{
  width:100%;
  min-width:0;
  min-height:40px;
  padding:9px 12px;
  border:1px solid var(--border);
  border-radius:8px;
  background:#10243a;
  color:var(--ink);
  font:400 13px/1.4 var(--font-ui);
}
input::placeholder{color:#91a5bb;opacity:1}
input:focus,select:focus{border-color:var(--blue);outline:2px solid rgba(79,157,248,.24);outline-offset:1px;box-shadow:none}
button:focus-visible,a:focus-visible{outline:2px solid #8ec4ff;outline-offset:2px}
.event-groups{display:grid;gap:14px}
.event-group{
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:10px;
  background:var(--surface-quiet);
  box-shadow:none;
}
.event-group-head{
  display:flex;
  width:100%;
  min-height:48px;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:10px 15px;
  border:0;
  border-bottom:1px solid var(--border);
  background:#152c44;
  color:var(--ink);
  text-align:left;
  cursor:pointer;
}
.event-group-head:hover{background:#1a3551}
.event-group-title{display:flex;align-items:center;gap:9px;color:var(--ink);font-size:14px;font-weight:600}
.event-group-title::before{content:"";width:7px;height:7px;flex:none;border-radius:50%;background:var(--blue)}
.event-group.live .event-group-title::before{background:var(--negative)}
.event-group-count{color:var(--muted);font:400 11px/1.4 var(--font-data);white-space:nowrap}
.event-group-toggle,.toggle{
  display:inline-flex;
  width:23px;
  height:23px;
  flex:none;
  align-items:center;
  justify-content:center;
  border:1px solid var(--border);
  border-radius:50%;
  background:transparent;
  color:var(--muted);
  font-size:16px;
  transition:transform .16s ease;
}
.event-group.collapsed .event-group-toggle,.league.collapsed .toggle{transform:rotate(-90deg)}
.event-group.collapsed .event-group-body{display:none}
.event-group-body{padding:9px;background:transparent}
.league{
  margin:0 0 8px;
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:8px;
  background:var(--surface);
  box-shadow:none;
}
.league:last-child{margin-bottom:0}
.league-head{
  display:flex;
  width:100%;
  min-height:42px;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  padding:8px 12px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:#19334e;
  color:var(--ink);
  font:600 12px/1.4 var(--font-ui);
  text-align:left;
  cursor:pointer;
}
.league-head:hover{background:#1d3b59}
.league.collapsed .league-head{border-bottom-color:transparent}
.league.collapsed .league-matches{display:none}
.league-name{display:flex;min-width:0;align-items:center;gap:9px;color:var(--ink)}
.league-name>span:last-child{overflow-wrap:anywhere}
.flag{display:inline-flex;width:20px;justify-content:center;color:var(--blue-soft);font:500 11px/1 var(--font-data)}
.league-count{color:var(--muted);font:400 11px/1.4 var(--font-data);white-space:nowrap}
.match{
  display:grid;
  grid-template-columns:minmax(245px,.78fr) minmax(0,2fr);
  align-items:center;
  gap:16px;
  padding:12px 14px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:var(--surface);
}
.match:last-child{border-bottom:0}
.match-top{
  display:grid;
  grid-template-columns:minmax(0,1fr) 56px minmax(0,1fr);
  align-items:center;
  gap:9px;
  min-width:0;
  margin:0;
}
.team{min-width:0;color:var(--ink-soft);font-size:13px;font-weight:550;line-height:1.35;overflow-wrap:anywhere}
.away{text-align:right}
.score{color:var(--ink);font:500 14px/1.2 var(--font-data);text-align:center;white-space:nowrap}
.time{display:block;margin-top:4px;color:var(--muted);font:400 11px/1.35 var(--font-data);white-space:normal}
.markets{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;min-width:0}
.market,.crown-market-card,.crown-moneyline{
  min-width:0;
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:7px;
  background:#10243a;
}
.market-title,.crown-market-card .market-title{
  display:flex;
  min-height:33px;
  align-items:center;
  justify-content:space-between;
  gap:8px;
  padding:7px 9px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:#18324c;
  color:var(--ink-soft);
  font-size:11px;
  font-weight:600;
  text-align:left;
}
.quote-row{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);border-bottom:1px solid var(--rule)}
.quote-row:last-child{border-bottom:0}
.handicap-side-head{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));background:#142b41;color:var(--muted);font-size:10px;font-weight:650;text-align:center}
.handicap-side-head span{padding:4px 6px}
.handicap-side-head span+span{border-left:1px solid var(--rule)}
.quote{
  display:grid;
  min-width:0;
  grid-template-columns:minmax(0,1fr) auto;
  align-items:center;
  gap:7px;
  padding:7px 9px;
  border:0;
  border-right:1px solid var(--rule);
  background:transparent;
  color:var(--ink-soft);
  text-align:left;
  overflow-wrap:anywhere;
}
.quote:last-child{border-right:0}
.line,.crown-market-label{
  min-width:0;
  margin:0;
  color:var(--muted);
  font-size:11px;
  line-height:1.4;
  overflow-wrap:anywhere;
}
.price,.crown-market-price,.crown-moneyline-cell strong{
  min-width:6ch;
  color:var(--ink);
  font:500 13px/1.2 var(--font-data);
  font-variant-numeric:tabular-nums lining-nums;
  text-align:right;
  white-space:nowrap;
}
.market-extra,.crown-market-footer{padding:5px 9px;border-top:1px solid var(--rule);background:transparent;color:var(--muted);font:400 11px/1.45 var(--font-data);text-align:left;overflow-wrap:anywhere}
.price.odds-changed{border-radius:3px;animation:price-change 1.2s ease-out}
.price.odds-up{--price-flash:rgba(89,197,160,.2);--price-flash-text:var(--positive)}
.price.odds-down{--price-flash:rgba(237,122,134,.2);--price-flash-text:var(--negative)}
@keyframes price-change{0%{background:var(--price-flash);color:var(--price-flash-text)}100%{background:transparent;color:var(--ink)}}
.crown-markets{display:grid;grid-template-columns:1fr;gap:7px;min-width:0}
.crown-market-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}
.crown-market-card .market-title small{color:var(--muted);font:400 11px/1.3 var(--font-data)}
.crown-market-row{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:9px;padding:7px 9px;border-bottom:1px solid var(--rule);background:transparent}
.crown-market-row:last-of-type{border-bottom:0}
.crown-market-label{font-size:11px}
.crown-market-price{flex:none}
.crown-moneyline-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}
.crown-moneyline-cell{display:grid;min-width:0;gap:4px;padding:8px 10px;border-right:1px solid var(--rule);background:transparent;text-align:center}
.crown-moneyline-cell:last-child{border-right:0}
.crown-moneyline-cell span{color:var(--muted);font-size:11px;line-height:1.35;overflow-wrap:anywhere}
.crown-moneyline-cell strong{justify-self:center}
.crown-market-empty,.empty-price{padding:11px 9px;color:var(--muted);font-size:11px;text-align:center}
.notice,.empty-state{
  padding:18px;
  border:1px dashed #395675;
  border-radius:8px;
  background:#10243a;
  color:var(--muted);
  text-align:center;
  line-height:1.6;
}
.settings-modal,.pinnacle-modal,.modal-backdrop{
  position:fixed;
  inset:0;
  z-index:30;
  display:none;
  align-items:center;
  justify-content:center;
  padding:20px;
  background:rgba(5,15,27,.78);
  backdrop-filter:none;
}
.settings-modal.open,.pinnacle-modal.open,.modal-backdrop.open{display:flex}
.settings-panel,.pinnacle-panel,.login-panel{
  width:min(560px,100%);
  max-height:min(88vh,900px);
  overflow:auto;
  border:1px solid #35516e;
  border-radius:12px;
  background:#142b43;
  color:var(--ink);
  box-shadow:0 18px 48px rgba(0,0,0,.3);
}
.pinnacle-panel{width:min(480px,100%)}
.login-panel{width:min(560px,100%)}
.settings-panel-head,.pinnacle-panel-head,.login-panel-head{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:14px 16px;
  border-bottom:1px solid var(--border);
  background:#19334e;
  color:var(--ink);
  font-size:14px;
  font-weight:600;
}
.settings-panel-body{display:grid;gap:9px;padding:13px}
.settings-source-card{
  display:grid;
  gap:9px;
  padding:12px;
  border:1px solid var(--border);
  border-radius:8px;
  background:#10243a;
  box-shadow:none;
}
.settings-source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;color:var(--ink);font-size:12px;font-weight:600}
.settings-source-copy,.login-status-copy,.login-help{color:var(--muted);font-size:11px;line-height:1.6}
.settings-source-actions{display:flex;gap:8px;flex-wrap:wrap}
.settings-source-actions a,.settings-source-actions button,.pinnacle-config-button,.action-button,.login-button,.pinnacle-save-button{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:34px;
  padding:7px 11px;
  border:1px solid #3a5877;
  border-radius:999px;
  background:#19334e;
  color:var(--ink-soft);
  font:550 12px/1.3 var(--font-ui);
  text-decoration:none;
  cursor:pointer;
}
.settings-source-actions a:hover,.settings-source-actions button:hover,.pinnacle-config-button:hover,.action-button:hover{border-color:#5782af;background:#203e5c;color:#fff}
.login-button,.action-button.primary,.pinnacle-save-button{
  border-color:#397fca;
  background:#286fb3;
  color:#fff;
}
.login-button:hover,.action-button.primary:hover,.pinnacle-save-button:hover{border-color:#5a9fe8;background:#347fc4;color:#fff}
.login-button .dot{width:6px;height:6px;border-radius:50%;background:#a9d7ff}
.settings-close,.pinnacle-close,.close-button{
  display:inline-grid;
  width:30px;
  height:30px;
  place-items:center;
  border:1px solid var(--border);
  border-radius:50%;
  background:transparent;
  color:var(--ink-soft);
  font-size:19px;
  line-height:1;
  cursor:pointer;
}
.pinnacle-panel-body,.login-panel-body{display:grid;gap:10px;padding:15px}
.pinnacle-panel-body label,.account-form label{
  display:grid;
  gap:6px;
  color:var(--muted);
  font-size:11px;
  font-weight:550;
}
.pinnacle-panel-body input,.account-form input,.otp-form input{
  width:100%;
  min-height:38px;
  border:1px solid var(--border);
  border-radius:7px;
  background:#0f2237;
  color:var(--ink);
}
.pinnacle-save-button:disabled,.action-button:disabled{opacity:.6;cursor:wait}
.login-status{display:flex;align-items:flex-start;gap:10px;padding:11px;border:1px solid var(--border);border-radius:8px;background:#10243a}
.login-status-indicator{width:8px;height:8px;flex:none;margin-top:5px;border-radius:50%;background:var(--muted-2)}
.login-status-indicator.connected{background:var(--positive)}
.login-status-indicator.error{background:var(--warning)}
.login-status-title{color:var(--ink);font-size:12px;font-weight:600}
.login-actions{display:flex;gap:8px;flex-wrap:wrap}
.account-form{display:grid;gap:9px;margin-top:12px;padding-top:12px;border-top:1px solid var(--rule)}
.account-form-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.account-form-status{min-height:18px;color:var(--muted);font-size:11px}
.account-list{display:grid;gap:7px;margin-top:12px}
.account-list-title{color:var(--ink-soft);font-size:12px;font-weight:600}
.account-row{display:grid;gap:8px;padding:10px;border:1px solid var(--border);border-radius:8px;background:#10243a}
.account-row-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.account-row-name{color:var(--ink);font-size:12px;font-weight:600;overflow-wrap:anywhere}
.account-row-meta,.account-row-status{color:var(--muted);font-size:11px;line-height:1.5;overflow-wrap:anywhere}
.account-row-status{white-space:nowrap}
.account-row-actions{display:flex;gap:6px;flex-wrap:wrap}
.account-row-actions .action-button{min-height:30px;padding:5px 9px;font-size:11px}
.otp-form{display:flex;align-items:center;gap:7px;flex-wrap:wrap}
.otp-form input{width:min(180px,100%);padding:7px 9px}
.account-empty{padding:10px;border:1px dashed #395675;border-radius:7px;color:var(--muted);font-size:11px;text-align:center}
.login-help{margin:10px 0 0}
@media(max-width:760px){
  header{padding:9px 12px}
  .nav-row{grid-template-columns:minmax(0,1fr) 40px;gap:9px}
  .brand-lockup{grid-column:1;grid-row:1}
  .source-nav{grid-column:1/-1;grid-row:2;justify-self:stretch;justify-content:flex-start;overflow-x:auto;scrollbar-width:thin;scrollbar-color:#567b9f #10243a}
  .source-nav a{min-height:34px;padding:7px 12px;font-size:11px}
  .settings-button{grid-column:2;grid-row:1;width:36px;height:36px}
  main{width:calc(100% - 24px);padding-top:14px}
  .page-heading{align-items:flex-start;flex-direction:column;gap:5px;padding-bottom:11px}
  .page-heading h1{font-size:18px}
  .page-status{max-width:100%;text-align:left;font-size:11px}
  .stats{grid-template-columns:repeat(2,minmax(0,1fr));border-top:1px solid var(--border);border-radius:8px}
  .stat{min-height:49px;padding:8px 10px;border-bottom:1px solid var(--rule)}
  .stat:nth-child(2){border-right:0}
  .stat:nth-child(3),.stat:nth-child(4){border-bottom:0}
  .stat strong{font-size:12px}
  .stat:first-child strong{font-size:16px}
  .controls{grid-template-columns:minmax(0,1fr) 128px;gap:7px}
  input,select{min-height:39px;padding:8px 9px;font-size:12px}
  .event-groups{gap:10px}
  .event-group-body{padding:6px}
  .event-group-head{min-height:44px;padding:9px 10px}
  .event-group-title{font-size:12px}
  .event-group-count{font-size:11px}
  .league-head{min-height:39px;padding:7px 9px}
  .match{grid-template-columns:minmax(0,1fr);gap:10px;padding:10px}
  .match-top{grid-template-columns:minmax(0,1fr) 48px minmax(0,1fr)}
  .team{font-size:12px}
  .markets{
    display:grid;
    grid-auto-flow:column;
    grid-auto-columns:minmax(250px,calc(100% - 10px));
    grid-template-columns:none;
    gap:7px;
    overflow-x:auto;
    padding-bottom:5px;
    scroll-snap-type:x proximity;
    scrollbar-width:thin;
    scrollbar-color:#567b9f #10243a;
  }
  .market{scroll-snap-align:start}
  .crown-market-grid{grid-template-columns:minmax(0,1fr)}
  .crown-market-row{grid-template-columns:minmax(0,1fr) auto}
  .modal-backdrop,.settings-modal,.pinnacle-modal{align-items:flex-end;padding:10px}
  .settings-panel,.pinnacle-panel,.login-panel{max-height:88vh;border-radius:12px}
}
@media(max-width:390px){
  .controls{grid-template-columns:minmax(0,1fr) 112px}
  .source-nav a{padding-inline:9px}
  .stats{grid-template-columns:1fr 1fr}
  .stat{gap:7px}
}
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{scroll-behavior:auto!important;transition-duration:.01ms!important;animation-duration:.01ms!important;animation-iteration-count:1!important}
}


.league-name::before{content:"";width:6px;height:6px;flex:none;border-radius:50%;background:#5b8dbd}
.moneyline-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}
.moneyline-cell{display:grid;min-width:0;gap:4px;padding:8px 7px;border-right:1px solid var(--rule);background:transparent;text-align:center}
.moneyline-cell:last-child{border-right:0}
.moneyline-cell span{color:var(--muted);font-size:11px;line-height:1.35}
.moneyline-cell .price{justify-self:center}
@media(min-width:761px) and (max-width:1100px){
  .markets{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
}
</style>
</head>
<body>
<header>
  <div class="nav-row">
    <div class="brand-lockup"><span class="brand-mark" aria-hidden="true"></span><span class="brand-copy"><strong>赛事数据终端</strong><small>足球 · 赔率数据</small></span></div>
    <nav class="source-nav" aria-label="数据源">
      <a class="active" href="/">平博联赛</a>
      <a href="/crown">皇冠联赛</a>
      <a href="/combined">同场对照</a>
    </nav>
    <button class="settings-button" id="settingsButton" type="button" aria-label="打开页面设置" title="页面设置">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6h10"/><path d="M18 6h2"/><path d="M4 12h2"/><path d="M10 12h10"/><path d="M4 18h10"/><path d="M18 18h2"/><circle cx="16" cy="6" r="2"/><circle cx="8" cy="12" r="2"/><circle cx="16" cy="18" r="2"/></svg>
    </button>
  </div>
</header>
<div class="settings-modal" id="settingsModal" role="dialog" aria-modal="true" aria-labelledby="settingsTitle">
 <section class="settings-panel">
  <div class="settings-panel-head"><span id="settingsTitle">页面设置</span><button class="settings-close" id="settingsClose" type="button" aria-label="关闭设置">×</button></div>
  <div class="settings-panel-body">
   <section class="settings-source-card"><div class="settings-source-head"><span>平博联赛</span><span>数据连接</span></div><div class="settings-source-copy">配置账号并打开平博浏览器会话，继续使用现有实时赔率采集。</div><div class="settings-source-actions"><button class="login-button" id="pinnacleExternalLoginButton" type="button"><span class="dot" aria-hidden="true"></span>登录平博</button><button class="pinnacle-config-button" id="pinnacleConfigButton" type="button">连接配置</button></div></section>
   <section class="settings-source-card"><div class="settings-source-head"><span>皇冠联赛</span><span>数据连接</span></div><div class="settings-source-copy">进入皇冠登录面板，管理账号、验证码和本地会话。</div><div class="settings-source-actions"><a href="/crown?openSettings=1">打开皇冠设置</a></div></section>
  </div>
 </section>
</div>
<div class="pinnacle-modal" id="pinnacleModal" role="dialog" aria-modal="true" aria-labelledby="pinnacleLoginTitle">
 <section class="pinnacle-panel">
  <div class="pinnacle-panel-head"><span id="pinnacleLoginTitle">平博连接配置</span><button class="pinnacle-close" id="pinnacleClose" type="button" aria-label="关闭">×</button></div>
  <form class="pinnacle-panel-body" id="pinnacleLoginForm">
   <label>账号<input id="pinnacleUsername" name="username" autocomplete="username" required></label>
   <label>密码<input id="pinnaclePassword" name="password" type="password" autocomplete="current-password" required></label>
   <button class="pinnacle-save-button" type="submit">保存账号密码</button>
  </form>
 </section>
</div>
<main>
  <section class="page-heading" aria-labelledby="pageTitle"><h1 id="pageTitle">平博赛事</h1><div class="page-status" id="status" aria-live="polite">正在连接赛事数据…</div></section>
  <div class="stats">
  <div class="stat"><small>滚球 + 今日</small><strong id="count">—</strong></div>
    <div class="stat"><small>联赛数量</small><strong id="leagueCount">—</strong></div>
    <div class="stat"><small>显示比赛</small><strong id="visibleCount">—</strong></div>
  <div class="stat"><small>赔率格式 · Chrome WSS</small><strong>港赔（HK）</strong></div>
  </div>
  <div class="controls">
    <input id="search" aria-label="搜索平博联赛、球队或比赛 ID" placeholder="搜索联赛、主队、客队或比赛 ID">
    <select id="filter" aria-label="筛选赛事范围">
      <option value="">滚球 + 今日</option>
      <option value="live">滚球比赛</option>
      <option value="today">今日比赛</option>
    </select>
  </div>
  <div id="events"><div class="notice">正在加载比赛…</div></div>
</main>
<script>
function installSourceRouter(){
 if(window.__sourceRouterInstalled)return;
 window.__sourceRouterInstalled=true;
 window.__sourceView=window.history.state?.sourceView||window.location.pathname+window.location.search;
 window.history.replaceState({...window.history.state,sourceView:window.__sourceView},"",window.location.href);
 window.__loadSourceView=async function(path,options={}){
  const target=new URL(path,window.location.origin);const sourcePath=target.pathname+target.search;
  if(window.__sourceLoading||sourcePath===window.__sourceView)return;
  window.__sourceLoading=true;
  try{
   const response=await fetch(sourcePath,{cache:"no-store"});if(!response.ok)throw new Error("HTTP "+response.status);
   const html=await response.text();const doc=new DOMParser().parseFromString(html,"text/html");
   const styles=[...doc.head.querySelectorAll("style")].map(style=>style.textContent||"");
   document.querySelectorAll("style[data-source-style]").forEach(style=>style.remove());
   styles.forEach(css=>{const style=document.createElement("style");style.dataset.sourceStyle="";style.textContent=css;document.head.appendChild(style);});
   document.title=doc.title||document.title;document.body.innerHTML=doc.body.innerHTML;
   window.__sourceView=sourcePath;
   const nextState={...window.history.state,sourceView:sourcePath};
   if(options.replace)window.history.replaceState(nextState,"",sourcePath);else window.history.pushState(nextState,"",sourcePath);
   const scripts=[...document.body.querySelectorAll("script")];
   for(const script of scripts){const code=script.textContent||"";script.remove();if(code)new Function(code)();}
  }catch(error){console.error("页面切换失败",error);}
  finally{window.__sourceLoading=false;}
 };
 document.addEventListener("click",event=>{
  const link=event.target.closest("a[href]");if(!link)return;
  const target=new URL(link.href,window.location.href);
  if(target.origin!==window.location.origin||!["/","/crown","/combined"].includes(target.pathname))return;
  event.preventDefault();window.__loadSourceView(target.pathname+target.search);
 });
 window.addEventListener("popstate",()=>window.__loadSourceView(window.history.state?.sourceView||window.location.pathname,{replace:true}));
}
installSourceRouter();
function installSportsDialogA11y(){
 if(window.__sportsDialogA11y)return;
 window.__sportsDialogA11y=true;
 window.__openSportsDialog=(dialog,opener)=>{
  dialog.__returnFocus=opener||document.activeElement;
  dialog.classList.add("open");
  const target=dialog.querySelector("[data-dialog-autofocus],button:not([disabled]),input:not([disabled]),select:not([disabled]),a[href]")||dialog;
  requestAnimationFrame(()=>target.focus({preventScroll:true}));
 };
 window.__closeSportsDialog=dialog=>{
  dialog.classList.remove("open");
  const target=dialog.__returnFocus;
  if(target&&target.isConnected)target.focus({preventScroll:true});
 };
 document.addEventListener("keydown",event=>{
  const dialog=[...document.querySelectorAll('[role="dialog"].open')].at(-1);
  if(!dialog)return;
  if(event.key==="Escape"){event.preventDefault();window.__closeSportsDialog(dialog);return;}
  if(event.key!=="Tab")return;
  const items=[...dialog.querySelectorAll('a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])')].filter(item=>item.getClientRects().length);
  if(!items.length){event.preventDefault();dialog.focus();return;}
  const first=items[0],last=items[items.length-1];
  if(event.shiftKey&&(document.activeElement===first||!dialog.contains(document.activeElement))){event.preventDefault();last.focus();}
  else if(!event.shiftKey&&(document.activeElement===last||!dialog.contains(document.activeElement))){event.preventDefault();first.focus();}
 });
}
installSportsDialogA11y();
const $=id=>document.getElementById(id);
function esc(s){
 return String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function isLive(e){
 if(typeof e.is_live==="boolean")return e.is_live;
 return !!(e.minute||["1H","2H","HT","ET","P","LIVE"].includes(String(e.period||"").toUpperCase()));
}
function isToday(e){
 const timestamp=Number(e.start_ms);
 if(!Number.isFinite(timestamp)||timestamp<=0)return false;
 const start=new Date(timestamp),now=new Date();
 return start.getFullYear()===now.getFullYear()&&start.getMonth()===now.getMonth()&&start.getDate()===now.getDate();
}
function isCurrentEvent(e){return isLive(e)||isToday(e);}
function formatMatchPhase(period,minute,fallback){
 const rawPeriod=String(period??"").trim();
 const rawMinute=String(minute??"").trim();
 const phaseLabels={
  "1H":"上半场","1ST":"上半场","1":"上半场",
  "2H":"下半场","2ND":"下半场","2":"下半场",
  "HT":"中场","HALF_TIME":"中场","HALFTIME":"中场",
  "FT":"已结束","FINISHED":"已结束","FINAL":"已结束",
  "ET":"加时赛","EXTRA_TIME":"加时赛","EXTRATIME":"加时赛",
  "P":"点球","PEN":"点球","PENALTIES":"点球","LIVE":"进行中"
 };
 const embedded=rawMinute.match(/^([A-Za-z0-9]+)\\s*\\^\\s*(.+)$/);
 if(embedded){
  const embeddedLabel=phaseLabels[String(embedded[1]).toUpperCase()];
  if(embeddedLabel)return `${embeddedLabel} ${embedded[2].trim()}${["FT","FINISHED","FINAL"].includes(rawPeriod.toUpperCase())?" · 已结束":""}`;
 }
 const label=phaseLabels[rawPeriod.toUpperCase()];
 if(label&&["上半场","下半场","加时赛","进行中"].includes(label)&&rawMinute)return `${label} ${rawMinute}`;
 if(label)return label;
 return rawMinute||fallback||"开赛时间未知";
}
function market(title,items,kind,event){
 if(!items||!items.length)
  return `<div class="market"><div class="market-title">${title}</div><div class="empty-price">暂无有效报价</div></div>`;
 const sideHead=kind==="handicap"?`<div class="handicap-side-head"><span>主</span><span>客</span></div>`:"";
 return `<div class="market"><div class="market-title">${title} · ${items.length} 组</div>${sideHead}${items.map((x,index)=>{
  const line=x.display_line||x.line||"盘口未知";
  const left=kind==="handicap"?(x.home_display_line||line):kind==="total"?`大 ${line}`:(x.left||line);
  const right=kind==="handicap"?(x.away_display_line||line):kind==="total"?`小 ${line}`:(x.right||"客队");
  const role=x.market_role==="alternate"||index>0?"附加盘":"主盘";
  const rolePrefix=kind==="handicap"?`${role} · `:"";
  return `<div class="quote-row"><div class="quote"><span class="line">${rolePrefix}${esc(left)}</span><span class="price">${esc(x.a||"—")}</span></div><div class="quote"><span class="line">${rolePrefix}${esc(right)}</span><span class="price">${esc(x.b||"—")}</span></div></div>`;
 }).join("")}</div>`;
}
function renderMatch(e){
 const m=e.display_markets||e.markets||{};
 const handicap=(m.handicap||[]);
 const total=(m.total||[]);
 const money=(m.moneyline||[]);
 const moneyHtml=money.length>=3
  ? `<div class="market moneyline"><div class="market-title">独赢 · 主 / 平 / 客</div><div class="moneyline-grid">${[["主队",money[0]],["平局",money[1]],["客队",money[2]]].map(([label,price])=>`<div class="moneyline-cell"><span>${label}</span><strong class="price">${esc(price||"—")}</strong></div>`).join("")}</div></div>`
  : `<div class="market"><div class="market-title">独赢</div><div class="empty-price">暂无有效报价</div></div>`;
 const startTime=e.start_ms?new Date(e.start_ms).toLocaleString("zh-CN",{month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit"}):"开赛时间未知";
 const time=formatMatchPhase(e.period,e.minute,startTime);
 return `<article class="match" data-event-id="${esc(e.id)}">
 <div class="match-top">
  <div class="team">${esc(e.home)}</div>
  <div class="score">${esc(e.score||"VS")}<span class="time">${esc(time)}</span></div>
  <div class="team away">${esc(e.away)}</div>
 </div>
 <div class="markets">
  ${market("亚洲让球",handicap,"handicap",e)}
  ${market("大小球",total,"total",e)}
  ${moneyHtml}
 </div>
 </article>`;
}
function countryFlag(name){
 const s=String(name||"").toLowerCase();
 const rules=[
  ["england","🏴"],["scotland","🏴"],["germany","🇩🇪"],["spain","🇪🇸"],
  ["italy","🇮🇹"],["france","🇫🇷"],["portugal","🇵🇹"],["netherlands","🇳🇱"],
  ["belgium","🇧🇪"],["japan","🇯🇵"],["china","🇨🇳"],["singapore","🇸🇬"],
  ["austria","🇦🇹"],["turkey","🇹🇷"],["brazil","🇧🇷"],["argentina","🇦🇷"],
  ["sweden","🇸🇪"],["norway","🇳🇴"],["denmark","🇩🇰"],["switzerland","🇨🇭"],
  ["poland","🇵🇱"],["croatia","🇭🇷"],["serbia","🇷🇸"],["ukraine","🇺🇦"],
  ["australia","🇦🇺"],["usa","🇺🇸"],["united states","🇺🇸"],["mexico","🇲🇽"],
  ["ireland","🇮🇪"],["wales","🏴"],["finland","🇫🇮"],["greece","🇬🇷"],
  ["romania","🇷🇴"],["bulgaria","🇧🇬"],["czech","🇨🇿"],["slovakia","🇸🇰"]
 ];
 const hit=rules.find(x=>s.includes(x[0]));
 return hit?hit[1]:"⚽";
}
const collapsedLeagues = new Set();
const collapsedGroups = new Set();
function render(d){
 const previousPrices=priceSnapshot();
 const currentEvents=(d.events||[]).filter(isCurrentEvent);
 $("count").textContent=currentEvents.length;
 $("leagueCount").textContent=new Set(currentEvents.map(e=>e.league).filter(Boolean)).size;
 const feedStatus=d.feed_status||"暂无成功数据";
 const feedTime=d.feed_updated_at||d.updated_at||"—";
 $("status").textContent=d.updated_at?`最后成功更新：${d.updated_at} · ${feedStatus} · 数据时间：${feedTime}`:`${feedStatus} · 等待首次成功请求…`;
 if(d.error)$("status").textContent+=" · 最近请求失败："+d.error;
 const q=$("search").value.trim().toLowerCase();
 const f=$("filter").value;
 const list=currentEvents.filter(e=>{
  if(q&&![e.league,e.home,e.away,e.id].join(" ").toLowerCase().includes(q))return false;
  if(f==="live"&&!isLive(e))return false;
  if(f==="today"&&!isToday(e))return false;
  return true;
 });
 $("visibleCount").textContent=list.length;
 const renderGroup=(key,title,items)=>{
  const groups=new Map();
  for(const e of items){
   if(!groups.has(e.league))groups.set(e.league,[]);
   groups.get(e.league).push(e);
  }
  const leagueHtml=[...groups.entries()].map(([name,events],leagueIndex)=>{
   const leagueCollapsed=collapsedLeagues.has(name);const leagueBodyId=`${key}-league-${leagueIndex}`;
   return `<section class="league${leagueCollapsed?" collapsed":""}" data-league="${esc(name)}">
     <button class="league-head" type="button" aria-expanded="${!leagueCollapsed}" aria-controls="${leagueBodyId}"><span class="league-name"><span>${esc(name)}</span></span><span class="league-count">${events.length} 场</span><span class="toggle" aria-hidden="true">⌄</span></button>
     <div class="league-matches" id="${leagueBodyId}">${events.map(renderMatch).join("")}</div>
    </section>`
  }).join("");
  const collapsed=collapsedGroups.has(key);
  return `<section class="event-group ${key}${collapsed?" collapsed":""}" data-group="${key}">
   <button class="event-group-head" type="button" data-group-toggle="${key}" aria-expanded="${!collapsed}" aria-controls="group-body-${key}"><span class="event-group-title">${title}</span><span class="event-group-count">${items.length} 场 · ${groups.size} 个联赛</span><span class="event-group-toggle" aria-hidden="true">⌄</span></button>
   <div class="event-group-body" id="group-body-${key}">${leagueHtml||'<div class="notice">当前没有符合条件的比赛</div>'}</div>
  </section>`;
 };
 const live=list.filter(isLive);
 const today=list.filter(e=>!isLive(e));
 $("events").innerHTML=`<div class="event-groups">${renderGroup("live","滚球比赛",live)}${renderGroup("prematch","今日比赛",today)}</div>`;
 highlightPriceChanges(previousPrices);
}
function priceSnapshot(){
 const values=new Map();
 document.querySelectorAll(".match[data-event-id]").forEach(match=>{
  match.querySelectorAll(".market").forEach((market,marketIndex)=>{
   const title=market.querySelector(".market-title")?.textContent.trim()||String(marketIndex);
   market.querySelectorAll(".quote-row").forEach((row,rowIndex)=>{
    row.querySelectorAll(".price").forEach((price,priceIndex)=>{
     const quote=price.closest(".quote");
     const label=quote?.querySelector(".line")?.textContent.trim()||quote?.textContent.replace(price.textContent,"").trim()||String(priceIndex);
     values.set([match.dataset.eventId,title,rowIndex,label,priceIndex].join("\u001f"),price.textContent.trim());
    });
   });
  });
 });
 return values;
}
function highlightPriceChanges(previous){
 if(!previous||!previous.size)return;
 document.querySelectorAll(".match[data-event-id]").forEach(match=>{
  match.querySelectorAll(".market").forEach((market,marketIndex)=>{
   const title=market.querySelector(".market-title")?.textContent.trim()||String(marketIndex);
   market.querySelectorAll(".quote-row").forEach((row,rowIndex)=>{
    row.querySelectorAll(".price").forEach((price,priceIndex)=>{
     const quote=price.closest(".quote");
     const label=quote?.querySelector(".line")?.textContent.trim()||quote?.textContent.replace(price.textContent,"").trim()||String(priceIndex);
     const key=[match.dataset.eventId,title,rowIndex,label,priceIndex].join("\u001f");
     const oldValue=previous.get(key);const newValue=price.textContent.trim();
     if(oldValue===undefined||oldValue===newValue)return;
     const oldNumber=/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(oldValue)?Number(oldValue):NaN;
     const newNumber=/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(newValue)?Number(newValue):NaN;
     price.classList.add("odds-changed",Number.isFinite(oldNumber)&&Number.isFinite(newNumber)&&newNumber>oldNumber?"odds-up":"odds-down");
     setTimeout(()=>price.classList.remove("odds-changed","odds-up","odds-down"),1300);
    });
   });
  });
 });
}
let renderedVersion="";
async function update(forceRender=false){
 try{
  const r=await fetch("/api/events",{cache:"no-store"});
  if(!r.ok)throw new Error("HTTP "+r.status);
  const data=await r.json();
  const version=[data.updated_at||"",data.error||"",data.events?.length||0].join("|");
  if(forceRender||version!==renderedVersion){renderedVersion=version;render(data);}
  const feedStatus=data.feed_status||"等待赔率数据";
  const browserStatus=data.browser_status||"等待 Chrome 启动";
  $("status").textContent=data.updated_at
   ? `最后成功更新：${data.updated_at} · ${feedStatus} · ${browserStatus}`
   : `${feedStatus} · ${browserStatus}`;
  if(data.error)$("status").textContent+=" · "+data.error;
 }catch(e){$("status").textContent="读取本地服务失败："+e.message;}
}
async function pinnacleCredentialsLoad(){
 try{
  const r=await fetch("/api/pinnacle/credentials",{cache:"no-store"});
  if(!r.ok)throw new Error("无法读取已保存账号");
  const data=await r.json();
  $("pinnacleUsername").value=data.username||"";
  $("pinnaclePassword").value="";
 }catch(e){window.alert(e.message||"读取已保存账号失败");}
}
function settingsOpen(){window.__openSportsDialog($("settingsModal"),$("settingsButton"));}
function settingsClose(){window.__closeSportsDialog($("settingsModal"));}
function pinnacleConfigOpen(){settingsClose();window.__openSportsDialog($("pinnacleModal"),$("settingsButton"));pinnacleCredentialsLoad();}
$("settingsButton").addEventListener("click",settingsOpen);
$("settingsClose").addEventListener("click",settingsClose);
$("settingsModal").addEventListener("click",e=>{if(e.target===$("settingsModal"))settingsClose();});
$("pinnacleConfigButton").addEventListener("click",pinnacleConfigOpen);
$("pinnacleClose").addEventListener("click",()=>window.__closeSportsDialog($("pinnacleModal")));
$("pinnacleModal").addEventListener("click",e=>{if(e.target===$("pinnacleModal"))window.__closeSportsDialog($("pinnacleModal"));});
$("pinnacleLoginForm").addEventListener("submit",async e=>{
 e.preventDefault();const b=e.currentTarget.querySelector("button[type=submit]"),label=b.textContent;b.disabled=true;
 try{const r=await fetch("/api/pinnacle/credentials",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:$("pinnacleUsername").value,password:$("pinnaclePassword").value})});const data=await r.json();if(!r.ok)throw new Error(data.error||"保存账号密码失败");$("pinnaclePassword").value="";b.textContent="已保存";setTimeout(()=>{b.textContent=label;},1400);}catch(err){window.alert(err.message||"保存账号密码失败");}finally{b.disabled=false;}
});
$("pinnacleExternalLoginButton").addEventListener("click",async()=>{
 const b=$("pinnacleExternalLoginButton");b.disabled=true;
 settingsClose();
 try{const r=await fetch("/api/pinnacle/browser/open",{method:"POST"});const data=await r.json();if(!r.ok){if(data.code==="credentials_missing"){pinnacleConfigOpen();return;}throw new Error(data.error||"无法启动 Chrome");}}
 catch(err){window.alert(err.message||"无法启动 Chrome");}
 finally{b.disabled=false;}
});
if(new URLSearchParams(window.location.search).get("openSettings")==="1")settingsOpen();
$("events").addEventListener("click",ev=>{
 const groupButton=ev.target.closest("[data-group-toggle]");
 if(groupButton){
  const key=groupButton.dataset.groupToggle;
  if(collapsedGroups.has(key))collapsedGroups.delete(key);
  else collapsedGroups.add(key);
  const collapsed=collapsedGroups.has(key);
  groupButton.closest(".event-group").classList.toggle("collapsed",collapsed);
  groupButton.setAttribute("aria-expanded",String(!collapsed));
  return;
 }
 const head=ev.target.closest(".league-head");
 if(!head)return;
 const section=head.closest(".league");
 if(!section)return;
 const name=section.dataset.league;
 if(collapsedLeagues.has(name))collapsedLeagues.delete(name);
 else collapsedLeagues.add(name);
 const collapsed=collapsedLeagues.has(name);
 section.classList.toggle("collapsed",collapsed);
 head.setAttribute("aria-expanded",String(!collapsed));
});
$("search").addEventListener("input",()=>update(true));
$("filter").addEventListener("change",()=>update(true));
update();
// 本地接口只读内存快照，2 秒轮询不会增加平博上游请求；新赔率快照到达后
// 页面可在下一次轮询立即显示，避免继续等待原来的 5 秒页面间隔。
setInterval(update,500);
</script>
</body>
</html>
"""

CROWN_HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>皇冠足球赛事</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Public+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style data-source-style>
:root{color-scheme:light;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
*{box-sizing:border-box}
body{margin:0;background:#eef0ef;color:#202124}
header{position:sticky;top:0;z-index:5;background:#eef0ef;padding:10px 18px;border-bottom:0}
.header-row{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}
h1{font-size:21px;margin:0 0 6px;font-weight:750}
.sub{font-size:12px;color:#737a80}
.page-status{display:none!important}
.header-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.login-button{display:inline-flex;align-items:center;gap:7px;min-height:34px;padding:7px 12px;border:1px solid #0f766e;border-radius:7px;background:#0f766e;color:#fff;font-size:13px;font-weight:700;cursor:pointer}
.login-button:hover{background:#115e59}
.login-button .dot{width:7px;height:7px;border-radius:50%;background:#a7f3d0}
.settings-modal{position:fixed;inset:0;z-index:30;display:none;align-items:flex-start;justify-content:center;padding:72px 16px 24px;background:rgba(9,18,32,.52);backdrop-filter:blur(8px)}
.settings-modal.open{display:flex}
.settings-panel{width:min(520px,100%);background:#edf2f7;border:1px solid #9ab4d3;border-radius:18px;box-shadow:0 22px 60px rgba(4,14,29,.38),inset 0 1px rgba(255,255,255,.72);overflow:hidden}
.settings-panel-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;background:linear-gradient(135deg,#dfeaf6,#f6f9fc);border-bottom:1px solid #c3d2e2;color:#172b45;font-weight:760}
.settings-panel-body{display:grid;gap:10px;padding:14px}
.settings-source-card{display:grid;gap:10px;padding:13px 14px;border:1px solid #c8d5e2;border-radius:12px;background:rgba(255,255,255,.72);box-shadow:inset 0 1px rgba(255,255,255,.8)}
.settings-source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;color:#1f3651;font-size:13px;font-weight:760}
.settings-source-copy{color:#6b7b8c;font-size:11px;line-height:1.5}
.settings-source-actions{display:flex;gap:8px;flex-wrap:wrap}
.settings-source-actions a,.settings-source-actions button{display:inline-flex;align-items:center;justify-content:center;min-height:32px;padding:6px 11px;border:1px solid #9cb5d1;border-radius:8px;background:#fff;color:#274666;text-decoration:none;font:inherit;font-size:12px;font-weight:700;cursor:pointer}
.settings-source-actions a:hover,.settings-source-actions button:hover{background:#eaf2fb;border-color:#6d9bd2}
.settings-close{width:30px;height:30px;border:1px solid #a9bfd8;border-radius:50%;background:#eaf1f8;color:#47617c;font-size:19px;line-height:1;cursor:pointer}
.nav-row{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:0}
.source-nav{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));width:min(100%,684px);padding:4px;gap:0;margin-top:0;border:1px solid rgba(116,157,204,.42);border-radius:999px;background:linear-gradient(135deg,rgba(17,39,68,.96),rgba(27,50,82,.9));box-shadow:inset 0 1px rgba(255,255,255,.1),inset 0 -8px 18px rgba(5,18,38,.24),0 8px 22px rgba(15,35,61,.12);backdrop-filter:blur(12px)}
.source-nav a{display:flex;align-items:center;justify-content:center;min-width:0;min-height:42px;padding:7px 16px;border:0;border-radius:999px;background:transparent;color:#b9c8da;text-decoration:none;font-size:13px;font-weight:650;white-space:nowrap;transition:background .24s ease,color .24s ease,box-shadow .24s ease,transform .24s ease}
.source-nav a:hover{color:#edf5ff;background:rgba(112,153,201,.16)}
.source-nav a.active{background:linear-gradient(135deg,#5aa0f6,#3985e8);color:#fff;box-shadow:inset 0 1px rgba(255,255,255,.36),0 0 16px rgba(61,139,240,.34);transform:translateY(-1px)}
.settings-button{display:inline-flex;align-items:center;justify-content:center;flex:0 0 44px;width:44px;height:44px;padding:0;border:1px solid rgba(126,164,207,.58);border-radius:50%;background:linear-gradient(145deg,rgba(27,52,84,.96),rgba(15,35,62,.95));color:#c3d3e5;box-shadow:inset 0 1px rgba(255,255,255,.14),0 7px 18px rgba(10,28,52,.18);cursor:pointer;transition:background .24s ease,color .24s ease,border-color .24s ease,box-shadow .24s ease,transform .24s ease}
.settings-button:hover{border-color:#8cb8ed;background:linear-gradient(145deg,#2b588e,#1d426f);color:#fff;box-shadow:inset 0 1px rgba(255,255,255,.22),0 0 15px rgba(78,148,235,.28);transform:translateY(-1px)}
.settings-button:focus-visible{outline:2px solid #8fc1ff;outline-offset:3px}
.settings-button svg{width:18px;height:18px}
main{max-width:1500px;margin:auto;padding:12px}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:12px}
.stat{background:#fff;border:1px solid #e1e4e3;border-radius:9px;padding:10px 13px}
.stat small{display:block;font-size:12px;color:#72787d}
.stat strong{display:block;font-size:20px;margin-top:3px}
.controls{display:grid;grid-template-columns:minmax(150px,1fr) 200px;gap:8px;margin-bottom:12px}
input,select{width:100%;min-width:0;border:1px solid #d2d8d6;background:#fff;border-radius:9px;padding:11px;font-size:14px;color:#202124}
.event-groups{display:grid;gap:14px}
.event-group{border:1px solid #dfe3e1;border-radius:9px;overflow:hidden;background:#f8faf9}
.event-group-head{display:flex;width:100%;min-height:48px;align-items:center;justify-content:space-between;gap:12px;padding:12px 14px;border:0;background:#fff;color:inherit;text-align:left;font-size:16px;font-weight:750}
.event-group-title{display:flex;align-items:center;gap:9px}
.event-group-title::before{content:"";width:8px;height:8px;border-radius:50%;background:#7a8580}
.event-group.live .event-group-title::before{background:#c83c2f}
.event-group-count{font-size:12px;color:#737a80;font-weight:500;white-space:nowrap}
.event-group-head{cursor:pointer}
.event-group-head:hover{background:#f5f7f6}
.event-group.collapsed .event-group-body{display:none}
.event-group-toggle{display:inline-flex;width:24px;height:24px;align-items:center;justify-content:center;border-radius:6px;background:#f0f2f1;color:#4d5552;transition:transform .18s}
.event-group.collapsed .event-group-toggle{transform:rotate(-90deg)}
.event-group-body{padding:10px}
.league{margin-bottom:10px;background:#fff;border:1px solid #dfe3e1;border-radius:9px;overflow:hidden}
.league:last-child{margin-bottom:0}
.league.collapsed .match{display:none}
.league-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px 14px;background:#fff;border-bottom:1px solid #e6e8e7;font-weight:750;cursor:pointer}
.league-head:hover{background:#f5f7f6}
.league-name{display:flex;align-items:center;gap:9px;min-width:0}
.flag{font-size:18px}
.league-count{font-size:12px;color:#737a80;font-weight:500;white-space:nowrap}
.toggle{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;background:#f0f2f1;color:#4d5552;font-size:18px;transition:transform .18s}
.league.collapsed .toggle{transform:rotate(-90deg)}
.match{padding:12px 14px;border-bottom:1px solid #e9ebea}
.match:last-child{border-bottom:0}
.match-top{display:grid;grid-template-columns:minmax(180px,1.2fr) minmax(100px,.55fr) minmax(180px,1.2fr);align-items:center;gap:10px;margin-bottom:10px}
.team{font-size:14px;font-weight:650;overflow-wrap:anywhere}.away{text-align:right}.score{text-align:center;font-size:13px;color:#5f666b;white-space:nowrap}.time{display:block;font-size:11px;color:#777f84;font-weight:400;margin-top:4px}
.markets{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px}.market{min-width:0;border:1px solid #e1e4e3;border-radius:5px;overflow:hidden}.market-title{background:#f7f8f8;color:#687078;text-align:center;font-size:12px;padding:7px 4px;border-bottom:1px solid #e6e8e7}.quote-row{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.quote{padding:7px 5px;text-align:center;font-size:12px;border-right:1px solid #eceeed;overflow-wrap:anywhere}.quote:last-child{border-right:0}.line{display:block;color:#62696f;font-size:11px;margin-bottom:3px}.price{font-size:13px;font-weight:750;color:#d52c1e}.empty-price{padding:12px 4px;text-align:center;color:#999;font-size:12px}
.crown-markets{display:grid;grid-template-columns:1fr;gap:8px}.crown-market-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.crown-market-card{border:1px solid #e1e4e3;border-radius:5px;overflow:hidden}.crown-market-card .market-title{display:flex;align-items:center;justify-content:space-between;gap:8px;text-align:left;padding-inline:9px}.crown-market-card .market-title small{font-size:10px;color:#7a8580;font-weight:500}.crown-market-row{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:7px 9px;border-bottom:1px solid #eceeed}.crown-market-row:last-of-type{border-bottom:0}.crown-market-label{min-width:0;overflow-wrap:anywhere;font-size:12px;color:#36413c}.crown-market-price{flex:none;font-size:14px;font-weight:800;color:#d52c1e}.crown-market-footer{padding:5px 9px;background:#fbfcfb;color:#7a8580;text-align:right;font-size:10px}.crown-moneyline{border:1px solid #e1e4e3;border-radius:5px;overflow:hidden}.crown-moneyline-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}.crown-moneyline-cell{display:grid;gap:3px;padding:8px 9px;text-align:center;border-right:1px solid #eceeed}.crown-moneyline-cell:last-child{border-right:0}.crown-moneyline-cell span{font-size:12px;color:#36413c;overflow-wrap:anywhere}.crown-moneyline-cell strong{font-size:14px;color:#d52c1e}.crown-market-empty{padding:12px 9px;color:#999;text-align:center;font-size:12px}
.notice{padding:28px 18px;background:#fff;border:1px solid #e1e4e3;border-radius:8px;color:#687078;text-align:center;line-height:1.7}
.notice strong{display:block;color:#202124;font-size:16px;margin-bottom:5px}
.modal-backdrop{position:fixed;inset:0;z-index:20;display:none;align-items:flex-start;justify-content:center;padding:72px 16px 24px;background:rgba(18,25,23,.34)}
.modal-backdrop.open{display:flex}
.login-panel{width:min(520px,100%);background:#fff;border:1px solid #d8dfdc;border-radius:10px;box-shadow:0 18px 50px rgba(20,35,29,.22);overflow:hidden}
.login-panel-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:15px 16px;border-bottom:1px solid #e2e7e4}
.login-panel-head strong{font-size:16px}
.close-button{width:30px;height:30px;border:0;border-radius:6px;background:#f1f4f2;color:#5d6661;font-size:20px;line-height:1;cursor:pointer}
.login-panel-body{padding:16px}
.login-status{display:flex;align-items:flex-start;gap:10px;padding:12px;border:1px solid #dce7e2;border-radius:8px;background:#f7fbf9;margin-bottom:14px}
.login-status-indicator{width:9px;height:9px;border-radius:50%;background:#9aa49f;margin-top:5px;flex:none}
.login-status-indicator.connected{background:#15803d}
.login-status-indicator.error{background:#c2410c}
.login-status-title{font-size:14px;font-weight:750}
.login-status-copy{font-size:12px;color:#68736d;line-height:1.6;margin-top:3px}
.login-actions{display:flex;gap:8px;flex-wrap:wrap}
.action-button{min-height:34px;padding:7px 11px;border:1px solid #cdd7d2;border-radius:7px;background:#fff;color:#39443f;font-size:13px;font-weight:650;cursor:pointer}
.action-button.primary{border-color:#2563eb;background:#2563eb;color:#fff}
.action-button:hover{background:#f3f6f4}
.action-button.primary:hover{background:#1d4ed8}
.account-form{display:grid;gap:9px;margin-top:14px;padding-top:14px;border-top:1px solid #e2e7e4}
.account-form label{display:grid;gap:5px;font-size:12px;color:#56615b;font-weight:650}
.account-form input{border-radius:7px;padding:9px 10px;font-size:13px}
.account-form-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.account-form-status{font-size:12px;color:#68736d;min-height:18px}
.account-list{display:grid;gap:8px;margin-top:14px}
.account-list-title{font-size:13px;font-weight:750;color:#39443f}
.account-row{display:grid;gap:8px;padding:10px;border:1px solid #e0e6e2;border-radius:8px;background:#fbfdfc}
.account-row-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.account-row-name{font-size:13px;font-weight:750;overflow-wrap:anywhere}
.account-row-meta{font-size:11px;color:#748079;line-height:1.55;overflow-wrap:anywhere}
.account-row-status{font-size:11px;color:#56615b;white-space:nowrap}
.account-row-actions{display:flex;gap:6px;flex-wrap:wrap}
.account-row-actions .action-button{min-height:30px;padding:5px 8px;font-size:11px}
.otp-form{display:flex;gap:6px;align-items:center}
.otp-form input{width:150px;padding:7px 8px;border:1px solid #ccd7d1;border-radius:6px}
.account-empty{padding:10px;border:1px dashed #ccd7d1;border-radius:8px;color:#748079;font-size:12px;text-align:center}
.login-help{margin:13px 0 0;color:#737d77;font-size:12px;line-height:1.65}
.login-help a{color:#2563eb;text-decoration:none}
@media(min-width:1100px){.match{display:grid;grid-template-columns:minmax(260px,.8fr) minmax(0,2fr);align-items:center;gap:18px}.match-top{margin:0;grid-template-columns:minmax(0,1fr) 65px minmax(0,1fr)}}
@media(max-width:600px){header{padding:12px}.header-row{align-items:stretch;flex-direction:column}h1{font-size:19px}.header-actions{justify-content:flex-start}.nav-row{gap:7px}.source-nav a{min-height:40px;padding-inline:8px;font-size:12px}.settings-button{flex-basis:40px;width:40px;height:40px}main{padding:7px}.stats{grid-template-columns:repeat(2,minmax(0,1fr))}.controls{grid-template-columns:1fr}.event-group-count{font-size:11px}.modal-backdrop,.settings-modal{padding:22px 9px}.league-head{padding:11px 10px}.match{padding:11px 9px}.match-top{grid-template-columns:minmax(0,1fr) 48px minmax(0,1fr);gap:5px}.team{font-size:13px}.markets{gap:4px}.crown-market-grid{grid-template-columns:1fr}.crown-moneyline-cell{padding-inline:5px}.market-title{font-size:11px;padding:7px 1px}.quote{padding:7px 2px;font-size:11px}.price{font-size:12px}.line{font-size:10px}}

/* Pinnodds-inspired gray operations theme */
:root{
  color-scheme:dark;
  --page-bg:#151a20;
  --surface:#1d242c;
  --surface-raised:#242d36;
  --surface-soft:#28323c;
  --surface-deep:#181e25;
  --line:#394651;
  --line-soft:#303b46;
  --text:#e8edf3;
  --text-strong:#f4f7fa;
  --muted:#9aa8b7;
  --muted-2:#788696;
  --accent:#4da3ff;
  --positive:#35d39a;
}
html{background:#151a20;min-height:100%;}
body{
  min-height:100vh;
  background:radial-gradient(circle at 50% -15%,rgba(64,92,122,.18),transparent 42%),linear-gradient(180deg,#151a20 0%,#171d24 46%,#12171d 100%);
  color:var(--text);
}
header{
  background:rgba(21,27,34,.92);
  border-bottom:1px solid rgba(111,133,154,.28);
  box-shadow:0 10px 26px rgba(0,0,0,.16);
}
main{background:transparent}
.stat,.league,.event-group,.event,.notice,.empty-state,.comparison-group{
  background:var(--surface);
  border-color:var(--line);
  box-shadow:0 8px 24px rgba(0,0,0,.14);
}
.stat small,.league-count,.event-group-count,.sub,.comparison-group-title small,.comparison-status,.comparison-footnote{
  color:var(--muted);
}
.stat strong,.event-group-title,.league-name,.team,.quote-name,.quote-line,.comparison-line-head,.comparison-group-title{
  color:var(--text-strong);
}
.stat{background:linear-gradient(145deg,#222b34,#1c232b)}
.league-head,.event-group-head{
  background:linear-gradient(180deg,#28323c,#232c35);
  color:var(--text-strong);
  border-bottom-color:var(--line);
}
.league-head:hover,.event-group-head:hover{background:#303b46}
.event-group-body{background:rgba(17,22,28,.48)}
.match,.event{background:#1b222a;border-bottom-color:var(--line-soft)}
.match-status,.time-debug,.evidence{color:var(--muted)}
.time-debug{background:#202831;border-bottom-color:var(--line-soft)}
.raw-detail{background:#161c22;border-top-color:var(--line-soft);color:#a9b7c5}
input,select,.tabs,.action{
  background:#222b34;
  color:var(--text);
  border-color:#43515e;
}
input::placeholder{color:#718091}
input:focus,select:focus{border-color:var(--accent);box-shadow:0 0 0 2px rgba(77,163,255,.18);outline:0}
.tabs{background:#202831}
.tabs button{color:#a9b6c3;border-right-color:#3a4652}
.tabs button[aria-pressed="true"]{background:#31506c;color:#fff}
.market,.crown-market-card,.crown-moneyline{
  background:var(--surface-deep);
  border-color:var(--line);
}
.market-title,.crown-market-card .market-title{
  background:#28323b;
  color:var(--text-strong);
  border-color:var(--line);
}
.quote,.crown-market-row,.crown-moneyline-cell{
  background:#202831;
  border-color:#34414c;
  color:var(--text);
}
.quote-side{background:transparent;border-bottom-color:#34414c}
.line,.quote-name,.quote-line,.crown-market-label,.crown-moneyline-cell span{color:#c0cbd5}
.market-extra,.crown-market-footer{background:#1a2128;border-color:#34414c;color:var(--muted)}
.empty-price,.crown-market-empty,.empty{color:var(--muted-2)}
.status-label{background:rgba(53,211,154,.16);color:#65e5b5}
.status-label.candidate{background:rgba(239,181,93,.16);color:#f2c77e}
.status-label.conflict{background:rgba(239,102,102,.16);color:#ff9f9f}
.status-label.unmatched{background:#2b343e;color:#aab7c4}
.action:not(.primary){background:#242e38;color:#d8e0e7;border-color:#43515e}
.action:not(.primary):hover{background:#2d3945}
.settings-panel,.pinnacle-panel,.login-panel{
  background:#202831;
  border-color:#43515e;
  color:var(--text);
}
.settings-panel-head{
  background:linear-gradient(135deg,#293746,#202a34);
  border-bottom-color:#43515e;
  color:var(--text-strong);
}
.settings-source-card{background:#252f39;border-color:#43515e;box-shadow:inset 0 1px rgba(255,255,255,.04)}
.settings-source-head{color:var(--text-strong)}
.settings-source-copy,.login-status-copy,.login-help{color:var(--muted)}
.settings-source-actions a,.settings-source-actions button,.pinnacle-config-button,.action-button{
  background:#27323c;
  color:#dbe5ed;
  border-color:#4a5967;
}
.settings-source-actions a:hover,.settings-source-actions button:hover,.pinnacle-config-button:hover,.action-button:hover{background:#31404d;border-color:#6795be}
.settings-close,.pinnacle-close,.close-button{
  background:#2a3540;
  color:#c4d0db;
  border-color:#4b5a68;
}
.pinnacle-panel-body input,.account-form input,.otp-form input{
  background:#1a222a;
  color:var(--text);
  border-color:#43515e;
}
.login-status{background:#252f39;border-color:#43515e}
.account-row{background:#242e38;border-color:#43515e}
.account-row-meta,.account-row-status,.account-list-title,.login-panel-body label,.account-form label{color:#c0cbd5}
.comparison-line{border-top-color:#34414c}
.comparison-group-title{background:#29343e}
.comparison-line-detail{color:#9aa8b7}
.source{background:#1b2229}
.source+.source{border-left-color:#394651}
.notice,.empty-state{color:#b6c2cd}
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{transition-duration:.01ms!important;animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important}
}

/* Taste Skill · data-terminal: restrained navy surfaces, rules and aligned market data */
:root{
  color-scheme:dark;
  --ground:#0c1c2d;
  --ground-raised:#10243a;
  --surface:#142b43;
  --surface-raised:#193550;
  --surface-quiet:#10243a;
  --border:#2b455f;
  --rule:#263e57;
  --ink:#eef4fb;
  --ink-soft:#d2deeb;
  --muted:#aabbd0;
  --muted-2:#91a5bb;
  --blue:#4f9df8;
  --blue-soft:#b8d9ff;
  --positive:#59c5a0;
  --negative:#ed7a86;
  --warning:#e3bb70;
  --font-ui:"Public Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  --font-data:"IBM Plex Mono","SFMono-Regular",Consolas,monospace;
}
html{min-height:100%;background:var(--ground)}
body{
  min-width:320px;
  min-height:100vh;
  margin:0;
  background:linear-gradient(155deg,#132b43 0%,#0c1c2d 38%,#102238 100%);
  color:var(--ink);
  font-family:var(--font-ui);
  font-size:14px;
  line-height:1.45;
}
header{
  position:sticky;
  top:0;
  z-index:10;
  padding:10px 22px;
  background:rgba(12,28,45,.97);
  border-bottom:1px solid var(--border);
  box-shadow:none;
}
.nav-row{
  display:grid;
  grid-template-columns:minmax(190px,1fr) auto minmax(44px,1fr);
  align-items:center;
  gap:20px;
  width:min(100%,1540px);
  margin:0 auto;
}
.brand-lockup{display:flex;align-items:center;gap:10px;min-width:0;color:var(--ink);text-decoration:none}
.brand-mark{position:relative;display:grid;width:30px;height:30px;flex:none;place-items:center;border:1px solid #528bc2;border-radius:50%}
.brand-mark::before{content:"";width:12px;height:12px;border:1px solid #83b8ec;border-radius:50%}
.brand-copy{display:grid;gap:1px;line-height:1.15}
.brand-copy strong{font-size:13px;font-weight:650;letter-spacing:.01em}
.brand-copy small{color:var(--muted-2);font:500 11px/1.3 var(--font-ui);letter-spacing:0}
.source-nav{
  display:flex;
  align-items:center;
  gap:4px;
  width:auto;
  max-width:100%;
  padding:4px;
  border:1px solid var(--border);
  border-radius:999px;
  background:#10243a;
  box-shadow:none;
  backdrop-filter:none;
}
.source-nav a{
  display:flex;
  align-items:center;
  justify-content:center;
  min-height:36px;
  padding:7px 15px;
  border:0;
  border-radius:999px;
  background:transparent;
  color:var(--muted);
  font-size:12px;
  font-weight:550;
  text-decoration:none;
  white-space:nowrap;
  transition:background-color .16s ease,color .16s ease;
}
.source-nav a:hover{background:#1a3551;color:var(--ink)}
.source-nav a.active{background:#244d75;color:#fff;box-shadow:none;transform:none}
.settings-button{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  justify-self:end;
  width:38px;
  height:38px;
  padding:0;
  border:1px solid var(--border);
  border-radius:999px;
  background:transparent;
  color:var(--ink-soft);
  box-shadow:none;
  cursor:pointer;
  transition:background-color .16s ease,border-color .16s ease,color .16s ease;
}
.settings-button:hover{border-color:#4d739b;background:#18334e;color:#fff;box-shadow:none;transform:none}
.settings-button svg{width:17px;height:17px}
main{width:min(100% - 40px,1480px);margin:0 auto;padding:20px 0 40px;background:transparent}
.page-heading{
  display:flex;
  align-items:flex-end;
  justify-content:space-between;
  gap:18px;
  min-width:0;
  padding:2px 0 14px;
  border-bottom:1px solid var(--rule);
}
.page-heading h1{margin:0;color:var(--ink);font-size:20px;font-weight:600;letter-spacing:-.02em;line-height:1.2}
.page-status{
  display:block!important;
  max-width:min(68%,780px);
  color:var(--muted);
  font-size:11px;
  line-height:1.5;
  text-align:right;
  overflow-wrap:anywhere;
}
.stats{
  display:grid;
  grid-template-columns:minmax(190px,1.25fr) repeat(2,minmax(125px,.72fr)) minmax(175px,1fr);
  gap:0;
  margin:0 0 14px;
  overflow:hidden;
  border:1px solid var(--border);
  border-top:0;
  border-radius:0 0 10px 10px;
  background:rgba(16,36,58,.72);
}
.stat{
  display:flex;
  min-width:0;
  min-height:58px;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:9px 15px;
  border:0;
  border-right:1px solid var(--rule);
  border-radius:0;
  background:transparent;
  box-shadow:none;
}
.stat:last-child{border-right:0}
.stat small{color:var(--muted);font-size:11px;line-height:1.35}
.stat strong{
  display:block;
  margin:0;
  color:var(--ink);
  font:500 15px/1.2 var(--font-data);
  font-variant-numeric:tabular-nums lining-nums;
  text-align:right;
  white-space:nowrap;
}
.stat:first-child strong{font-size:19px}
.controls{
  display:grid;
  grid-template-columns:minmax(0,1fr) 210px;
  gap:9px;
  margin:0 0 16px;
}
input,select{
  width:100%;
  min-width:0;
  min-height:40px;
  padding:9px 12px;
  border:1px solid var(--border);
  border-radius:8px;
  background:#10243a;
  color:var(--ink);
  font:400 13px/1.4 var(--font-ui);
}
input::placeholder{color:#91a5bb;opacity:1}
input:focus,select:focus{border-color:var(--blue);outline:2px solid rgba(79,157,248,.24);outline-offset:1px;box-shadow:none}
button:focus-visible,a:focus-visible{outline:2px solid #8ec4ff;outline-offset:2px}
.event-groups{display:grid;gap:14px}
.event-group{
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:10px;
  background:var(--surface-quiet);
  box-shadow:none;
}
.event-group-head{
  display:flex;
  width:100%;
  min-height:48px;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:10px 15px;
  border:0;
  border-bottom:1px solid var(--border);
  background:#152c44;
  color:var(--ink);
  text-align:left;
  cursor:pointer;
}
.event-group-head:hover{background:#1a3551}
.event-group-title{display:flex;align-items:center;gap:9px;color:var(--ink);font-size:14px;font-weight:600}
.event-group-title::before{content:"";width:7px;height:7px;flex:none;border-radius:50%;background:var(--blue)}
.event-group.live .event-group-title::before{background:var(--negative)}
.event-group-count{color:var(--muted);font:400 11px/1.4 var(--font-data);white-space:nowrap}
.event-group-toggle,.toggle{
  display:inline-flex;
  width:23px;
  height:23px;
  flex:none;
  align-items:center;
  justify-content:center;
  border:1px solid var(--border);
  border-radius:50%;
  background:transparent;
  color:var(--muted);
  font-size:16px;
  transition:transform .16s ease;
}
.event-group.collapsed .event-group-toggle,.league.collapsed .toggle{transform:rotate(-90deg)}
.event-group.collapsed .event-group-body{display:none}
.event-group-body{padding:9px;background:transparent}
.league{
  margin:0 0 8px;
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:8px;
  background:var(--surface);
  box-shadow:none;
}
.league:last-child{margin-bottom:0}
.league-head{
  display:flex;
  width:100%;
  min-height:42px;
  align-items:center;
  justify-content:space-between;
  gap:10px;
  padding:8px 12px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:#19334e;
  color:var(--ink);
  font:600 12px/1.4 var(--font-ui);
  text-align:left;
  cursor:pointer;
}
.league-head:hover{background:#1d3b59}
.league.collapsed .league-head{border-bottom-color:transparent}
.league.collapsed .league-matches{display:none}
.league-name{display:flex;min-width:0;align-items:center;gap:9px;color:var(--ink)}
.league-name>span:last-child{overflow-wrap:anywhere}
.flag{display:inline-flex;width:20px;justify-content:center;color:var(--blue-soft);font:500 11px/1 var(--font-data)}
.league-count{color:var(--muted);font:400 11px/1.4 var(--font-data);white-space:nowrap}
.match{
  display:grid;
  grid-template-columns:minmax(245px,.78fr) minmax(0,2fr);
  align-items:center;
  gap:16px;
  padding:12px 14px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:var(--surface);
}
.match:last-child{border-bottom:0}
.match-top{
  display:grid;
  grid-template-columns:minmax(0,1fr) 56px minmax(0,1fr);
  align-items:center;
  gap:9px;
  min-width:0;
  margin:0;
}
.team{min-width:0;color:var(--ink-soft);font-size:13px;font-weight:550;line-height:1.35;overflow-wrap:anywhere}
.away{text-align:right}
.score{color:var(--ink);font:500 14px/1.2 var(--font-data);text-align:center;white-space:nowrap}
.time{display:block;margin-top:4px;color:var(--muted);font:400 11px/1.35 var(--font-data);white-space:normal}
.markets{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;min-width:0}
.market,.crown-market-card,.crown-moneyline{
  min-width:0;
  overflow:hidden;
  border:1px solid var(--border);
  border-radius:7px;
  background:#10243a;
}
.market-title,.crown-market-card .market-title{
  display:flex;
  min-height:33px;
  align-items:center;
  justify-content:space-between;
  gap:8px;
  padding:7px 9px;
  border:0;
  border-bottom:1px solid var(--rule);
  background:#18324c;
  color:var(--ink-soft);
  font-size:11px;
  font-weight:600;
  text-align:left;
}
.quote-row{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);border-bottom:1px solid var(--rule)}
.quote-row:last-child{border-bottom:0}
.quote{
  display:grid;
  min-width:0;
  grid-template-columns:minmax(0,1fr) auto;
  align-items:center;
  gap:7px;
  padding:7px 9px;
  border:0;
  border-right:1px solid var(--rule);
  background:transparent;
  color:var(--ink-soft);
  text-align:left;
  overflow-wrap:anywhere;
}
.quote:last-child{border-right:0}
.line,.crown-market-label{
  min-width:0;
  margin:0;
  color:var(--muted);
  font-size:11px;
  line-height:1.4;
  overflow-wrap:anywhere;
}
.price,.crown-market-price,.crown-moneyline-cell strong{
  min-width:6ch;
  color:var(--ink);
  font:500 13px/1.2 var(--font-data);
  font-variant-numeric:tabular-nums lining-nums;
  text-align:right;
  white-space:nowrap;
}
.market-extra,.crown-market-footer{padding:5px 9px;border-top:1px solid var(--rule);background:transparent;color:var(--muted);font:400 11px/1.45 var(--font-data);text-align:left;overflow-wrap:anywhere}
.price.odds-changed{border-radius:3px;animation:price-change 1.2s ease-out}
.price.odds-up{--price-flash:rgba(89,197,160,.2);--price-flash-text:var(--positive)}
.price.odds-down{--price-flash:rgba(237,122,134,.2);--price-flash-text:var(--negative)}
@keyframes price-change{0%{background:var(--price-flash);color:var(--price-flash-text)}100%{background:transparent;color:var(--ink)}}
.crown-markets{display:grid;grid-template-columns:1fr;gap:7px;min-width:0}
.crown-market-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}
.crown-market-card .market-title small{color:var(--muted);font:400 11px/1.3 var(--font-data)}
.crown-market-row{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:9px;padding:7px 9px;border-bottom:1px solid var(--rule);background:transparent}
.crown-market-row:last-of-type{border-bottom:0}
.crown-market-label{font-size:11px}
.crown-market-price{flex:none}
.crown-moneyline-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}
.crown-moneyline-cell{display:grid;min-width:0;gap:4px;padding:8px 10px;border-right:1px solid var(--rule);background:transparent;text-align:center}
.crown-moneyline-cell:last-child{border-right:0}
.crown-moneyline-cell span{color:var(--muted);font-size:11px;line-height:1.35;overflow-wrap:anywhere}
.crown-moneyline-cell strong{justify-self:center}
.crown-market-empty,.empty-price{padding:11px 9px;color:var(--muted);font-size:11px;text-align:center}
.notice,.empty-state{
  padding:18px;
  border:1px dashed #395675;
  border-radius:8px;
  background:#10243a;
  color:var(--muted);
  text-align:center;
  line-height:1.6;
}
.settings-modal,.pinnacle-modal,.modal-backdrop{
  position:fixed;
  inset:0;
  z-index:30;
  display:none;
  align-items:center;
  justify-content:center;
  padding:20px;
  background:rgba(5,15,27,.78);
  backdrop-filter:none;
}
.settings-modal.open,.pinnacle-modal.open,.modal-backdrop.open{display:flex}
.settings-panel,.pinnacle-panel,.login-panel{
  width:min(560px,100%);
  max-height:min(88vh,900px);
  overflow:auto;
  border:1px solid #35516e;
  border-radius:12px;
  background:#142b43;
  color:var(--ink);
  box-shadow:0 18px 48px rgba(0,0,0,.3);
}
.pinnacle-panel{width:min(480px,100%)}
.login-panel{width:min(560px,100%)}
.settings-panel-head,.pinnacle-panel-head,.login-panel-head{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  padding:14px 16px;
  border-bottom:1px solid var(--border);
  background:#19334e;
  color:var(--ink);
  font-size:14px;
  font-weight:600;
}
.settings-panel-body{display:grid;gap:9px;padding:13px}
.settings-source-card{
  display:grid;
  gap:9px;
  padding:12px;
  border:1px solid var(--border);
  border-radius:8px;
  background:#10243a;
  box-shadow:none;
}
.settings-source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;color:var(--ink);font-size:12px;font-weight:600}
.settings-source-copy,.login-status-copy,.login-help{color:var(--muted);font-size:11px;line-height:1.6}
.settings-source-actions{display:flex;gap:8px;flex-wrap:wrap}
.settings-source-actions a,.settings-source-actions button,.pinnacle-config-button,.action-button,.login-button,.pinnacle-save-button{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-height:34px;
  padding:7px 11px;
  border:1px solid #3a5877;
  border-radius:999px;
  background:#19334e;
  color:var(--ink-soft);
  font:550 12px/1.3 var(--font-ui);
  text-decoration:none;
  cursor:pointer;
}
.settings-source-actions a:hover,.settings-source-actions button:hover,.pinnacle-config-button:hover,.action-button:hover{border-color:#5782af;background:#203e5c;color:#fff}
.login-button,.action-button.primary,.pinnacle-save-button{
  border-color:#397fca;
  background:#286fb3;
  color:#fff;
}
.login-button:hover,.action-button.primary:hover,.pinnacle-save-button:hover{border-color:#5a9fe8;background:#347fc4;color:#fff}
.login-button .dot{width:6px;height:6px;border-radius:50%;background:#a9d7ff}
.settings-close,.pinnacle-close,.close-button{
  display:inline-grid;
  width:30px;
  height:30px;
  place-items:center;
  border:1px solid var(--border);
  border-radius:50%;
  background:transparent;
  color:var(--ink-soft);
  font-size:19px;
  line-height:1;
  cursor:pointer;
}
.pinnacle-panel-body,.login-panel-body{display:grid;gap:10px;padding:15px}
.pinnacle-panel-body label,.account-form label{
  display:grid;
  gap:6px;
  color:var(--muted);
  font-size:11px;
  font-weight:550;
}
.pinnacle-panel-body input,.account-form input,.otp-form input{
  width:100%;
  min-height:38px;
  border:1px solid var(--border);
  border-radius:7px;
  background:#0f2237;
  color:var(--ink);
}
.pinnacle-save-button:disabled,.action-button:disabled{opacity:.6;cursor:wait}
.login-status{display:flex;align-items:flex-start;gap:10px;padding:11px;border:1px solid var(--border);border-radius:8px;background:#10243a}
.login-status-indicator{width:8px;height:8px;flex:none;margin-top:5px;border-radius:50%;background:var(--muted-2)}
.login-status-indicator.connected{background:var(--positive)}
.login-status-indicator.error{background:var(--warning)}
.login-status-title{color:var(--ink);font-size:12px;font-weight:600}
.login-actions{display:flex;gap:8px;flex-wrap:wrap}
.account-form{display:grid;gap:9px;margin-top:12px;padding-top:12px;border-top:1px solid var(--rule)}
.account-form-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.account-form-status{min-height:18px;color:var(--muted);font-size:11px}
.account-list{display:grid;gap:7px;margin-top:12px}
.account-list-title{color:var(--ink-soft);font-size:12px;font-weight:600}
.account-row{display:grid;gap:8px;padding:10px;border:1px solid var(--border);border-radius:8px;background:#10243a}
.account-row-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.account-row-name{color:var(--ink);font-size:12px;font-weight:600;overflow-wrap:anywhere}
.account-row-meta,.account-row-status{color:var(--muted);font-size:11px;line-height:1.5;overflow-wrap:anywhere}
.account-row-status{white-space:nowrap}
.account-row-actions{display:flex;gap:6px;flex-wrap:wrap}
.account-row-actions .action-button{min-height:30px;padding:5px 9px;font-size:11px}
.otp-form{display:flex;align-items:center;gap:7px;flex-wrap:wrap}
.otp-form input{width:min(180px,100%);padding:7px 9px}
.account-empty{padding:10px;border:1px dashed #395675;border-radius:7px;color:var(--muted);font-size:11px;text-align:center}
.login-help{margin:10px 0 0}
@media(max-width:760px){
  header{padding:9px 12px}
  .nav-row{grid-template-columns:minmax(0,1fr) 40px;gap:9px}
  .brand-lockup{grid-column:1;grid-row:1}
  .source-nav{grid-column:1/-1;grid-row:2;justify-self:stretch;justify-content:flex-start;overflow-x:auto;scrollbar-width:thin;scrollbar-color:#567b9f #10243a}
  .source-nav a{min-height:34px;padding:7px 12px;font-size:11px}
  .settings-button{grid-column:2;grid-row:1;width:36px;height:36px}
  main{width:calc(100% - 24px);padding-top:14px}
  .page-heading{align-items:flex-start;flex-direction:column;gap:5px;padding-bottom:11px}
  .page-heading h1{font-size:18px}
  .page-status{max-width:100%;text-align:left;font-size:11px}
  .stats{grid-template-columns:repeat(2,minmax(0,1fr));border-top:1px solid var(--border);border-radius:8px}
  .stat{min-height:49px;padding:8px 10px;border-bottom:1px solid var(--rule)}
  .stat:nth-child(2){border-right:0}
  .stat:nth-child(3),.stat:nth-child(4){border-bottom:0}
  .stat strong{font-size:12px}
  .stat:first-child strong{font-size:16px}
  .controls{grid-template-columns:minmax(0,1fr) 128px;gap:7px}
  input,select{min-height:39px;padding:8px 9px;font-size:12px}
  .event-groups{gap:10px}
  .event-group-body{padding:6px}
  .event-group-head{min-height:44px;padding:9px 10px}
  .event-group-title{font-size:12px}
  .event-group-count{font-size:11px}
  .league-head{min-height:39px;padding:7px 9px}
  .match{grid-template-columns:minmax(0,1fr);gap:10px;padding:10px}
  .match-top{grid-template-columns:minmax(0,1fr) 48px minmax(0,1fr)}
  .team{font-size:12px}
  .markets{
    display:grid;
    grid-auto-flow:column;
    grid-auto-columns:minmax(250px,calc(100% - 10px));
    grid-template-columns:none;
    gap:7px;
    overflow-x:auto;
    padding-bottom:5px;
    scroll-snap-type:x proximity;
    scrollbar-width:thin;
    scrollbar-color:#567b9f #10243a;
  }
  .market{scroll-snap-align:start}
  .crown-market-grid{grid-template-columns:minmax(0,1fr)}
  .crown-market-row{grid-template-columns:minmax(0,1fr) auto}
  .modal-backdrop,.settings-modal,.pinnacle-modal{align-items:flex-end;padding:10px}
  .settings-panel,.pinnacle-panel,.login-panel{max-height:88vh;border-radius:12px}
}
@media(max-width:390px){
  .controls{grid-template-columns:minmax(0,1fr) 112px}
  .source-nav a{padding-inline:9px}
  .stats{grid-template-columns:1fr 1fr}
  .stat{gap:7px}
}
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{scroll-behavior:auto!important;transition-duration:.01ms!important;animation-duration:.01ms!important;animation-iteration-count:1!important}
}


.league-name::before{content:"";width:6px;height:6px;flex:none;border-radius:50%;background:#5b8dbd}
.moneyline-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}
.moneyline-cell{display:grid;min-width:0;gap:4px;padding:8px 7px;border-right:1px solid var(--rule);background:transparent;text-align:center}
.moneyline-cell:last-child{border-right:0}
.moneyline-cell span{color:var(--muted);font-size:11px;line-height:1.35}
.moneyline-cell .price{justify-self:center}
</style>
</head>
<body>
<header>
  <div class="nav-row">
    <div class="brand-lockup"><span class="brand-mark" aria-hidden="true"></span><span class="brand-copy"><strong>赛事数据终端</strong><small>足球 · 赔率数据</small></span></div>
    <nav class="source-nav" aria-label="数据源">
      <a href="/">平博联赛</a>
      <a class="active" href="/crown">皇冠联赛</a>
      <a href="/combined">同场对照</a>
    </nav>
    <button class="settings-button" id="settingsButton" type="button" aria-label="打开页面设置" title="页面设置">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6h10"/><path d="M18 6h2"/><path d="M4 12h2"/><path d="M10 12h10"/><path d="M4 18h10"/><path d="M18 18h2"/><circle cx="16" cy="6" r="2"/><circle cx="8" cy="12" r="2"/><circle cx="16" cy="18" r="2"/></svg>
    </button>
  </div>
</header>
<main>
  <section class="page-heading" aria-labelledby="pageTitle"><h1 id="pageTitle">皇冠赛事</h1><div class="page-status" id="crownSubStatus" aria-live="polite">正在读取皇冠赛事数据…</div></section>
  <div class="stats">
    <div class="stat"><small>滚球 + 今日比赛</small><strong id="crownCount">—</strong></div>
    <div class="stat"><small>联赛数量</small><strong id="crownLeagueCount">—</strong></div>
    <div class="stat"><small>数据状态</small><strong id="crownStatus">检查中</strong></div>
    <div class="stat"><small>自动刷新</small><strong>1 秒</strong></div>
  </div>
  <div class="controls">
    <input id="crownSearch" aria-label="搜索皇冠联赛、球队或比赛 ID" placeholder="搜索皇冠联赛、主队、客队或比赛 ID">
    <select id="crownFilter" aria-label="筛选皇冠赛事范围"><option value="">滚球 + 今日</option><option value="live">滚球比赛</option><option value="prematch">今日比赛</option></select>
  </div>
  <div id="crownEvents"><div class="notice">正在读取皇冠只读会话…</div></div>
</main>
<div class="settings-modal" id="settingsModal" role="dialog" aria-modal="true" aria-labelledby="settingsTitle">
 <section class="settings-panel">
  <div class="settings-panel-head"><span id="settingsTitle">页面设置</span><button class="settings-close" id="settingsClose" type="button" aria-label="关闭设置">×</button></div>
  <div class="settings-panel-body">
   <section class="settings-source-card"><div class="settings-source-head"><span>平博联赛</span><span>数据连接</span></div><div class="settings-source-copy">进入平博页面配置账号并打开浏览器会话，继续使用实时赔率采集。</div><div class="settings-source-actions"><a href="/?openSettings=1">打开平博设置</a></div></section>
   <section class="settings-source-card"><div class="settings-source-head"><span>皇冠联赛</span><span>数据连接</span></div><div class="settings-source-copy">管理皇冠账号、验证码和本地会话。</div><div class="settings-source-actions"><button class="login-button" id="crownLoginButton" type="button"><span class="dot" aria-hidden="true"></span>皇冠登录</button></div></section>
  </div>
 </section>
</div>
<div class="modal-backdrop" id="loginModal" role="dialog" aria-modal="true" aria-labelledby="loginTitle">
  <section class="login-panel">
    <div class="login-panel-head"><strong id="loginTitle">皇冠登录</strong><button class="close-button" id="closeLogin" type="button" aria-label="关闭">×</button></div>
    <div class="login-panel-body">
      <div class="login-status"><span class="login-status-indicator" id="loginIndicator"></span><div><div class="login-status-title" id="loginStatusTitle">正在检查登录会话</div><div class="login-status-copy" id="loginStatusCopy">凭据和登录会话只在当前项目本地加密保存，不会展示在页面或接口响应中。</div></div></div>
      <div class="login-actions"><button class="action-button primary" id="checkCrownSession" type="button">检查全部会话</button></div>
      <form class="account-form" id="crownAccountForm">
        <label>域名或登录地址<input id="crownLoginUrl" name="login_url" placeholder="例如 https://example.com" required autocomplete="url"></label>
        <label>登录账号<input id="crownUsername" name="username" required autocomplete="username"></label>
        <label>登录密码<input id="crownPassword" name="password" type="password" required autocomplete="current-password"></label>
        <label>TOTP 密钥（可选）<input id="crownTotp" name="totp_secret" autocomplete="off"></label>
        <div class="account-form-actions"><button class="action-button primary" type="submit">添加账号并登录</button><span class="account-form-status" id="crownAccountFormStatus"></span></div>
      </form>
      <div class="account-list"><div class="account-list-title">当前项目保存的皇冠账号</div><div id="crownAccounts"><div class="account-empty">正在读取账号…</div></div></div>
      <p class="login-help">账号配置、加密凭据和浏览器会话全部保存在当前项目的 data 目录及本机 Keychain 中；不会读取另一个项目的账号库。</p>
    </div>
  </section>
</div>
<script>
function installSourceRouter(){
 if(window.__sourceRouterInstalled)return;
 window.__sourceRouterInstalled=true;
 window.__sourceView=window.history.state?.sourceView||window.location.pathname+window.location.search;
 window.history.replaceState({...window.history.state,sourceView:window.__sourceView},"",window.location.href);
 window.__loadSourceView=async function(path,options={}){
  const target=new URL(path,window.location.origin);const sourcePath=target.pathname+target.search;
  if(window.__sourceLoading||sourcePath===window.__sourceView)return;
  window.__sourceLoading=true;
  try{
   const response=await fetch(sourcePath,{cache:"no-store"});if(!response.ok)throw new Error("HTTP "+response.status);
   const html=await response.text();const doc=new DOMParser().parseFromString(html,"text/html");
   const styles=[...doc.head.querySelectorAll("style")].map(style=>style.textContent||"");
   document.querySelectorAll("style[data-source-style]").forEach(style=>style.remove());
   styles.forEach(css=>{const style=document.createElement("style");style.dataset.sourceStyle="";style.textContent=css;document.head.appendChild(style);});
   document.title=doc.title||document.title;document.body.innerHTML=doc.body.innerHTML;
   window.__sourceView=sourcePath;
   const nextState={...window.history.state,sourceView:sourcePath};
   if(options.replace)window.history.replaceState(nextState,"",sourcePath);else window.history.pushState(nextState,"",sourcePath);
   const scripts=[...document.body.querySelectorAll("script")];
   for(const script of scripts){const code=script.textContent||"";script.remove();if(code)new Function(code)();}
  }catch(error){console.error("页面切换失败",error);}
  finally{window.__sourceLoading=false;}
 };
 document.addEventListener("click",event=>{
  const link=event.target.closest("a[href]");if(!link)return;
  const target=new URL(link.href,window.location.href);
  if(target.origin!==window.location.origin||!["/","/crown","/combined"].includes(target.pathname))return;
  event.preventDefault();window.__loadSourceView(target.pathname+target.search);
 });
 window.addEventListener("popstate",()=>window.__loadSourceView(window.history.state?.sourceView||window.location.pathname,{replace:true}));
}
installSourceRouter();
function installSportsDialogA11y(){
 if(window.__sportsDialogA11y)return;
 window.__sportsDialogA11y=true;
 window.__openSportsDialog=(dialog,opener)=>{
  dialog.__returnFocus=opener||document.activeElement;
  dialog.classList.add("open");
  const target=dialog.querySelector("[data-dialog-autofocus],button:not([disabled]),input:not([disabled]),select:not([disabled]),a[href]")||dialog;
  requestAnimationFrame(()=>target.focus({preventScroll:true}));
 };
 window.__closeSportsDialog=dialog=>{
  dialog.classList.remove("open");
  const target=dialog.__returnFocus;
  if(target&&target.isConnected)target.focus({preventScroll:true});
 };
 document.addEventListener("keydown",event=>{
  const dialog=[...document.querySelectorAll('[role="dialog"].open')].at(-1);
  if(!dialog)return;
  if(event.key==="Escape"){event.preventDefault();window.__closeSportsDialog(dialog);return;}
  if(event.key!=="Tab")return;
  const items=[...dialog.querySelectorAll('a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])')].filter(item=>item.getClientRects().length);
  if(!items.length){event.preventDefault();dialog.focus();return;}
  const first=items[0],last=items[items.length-1];
  if(event.shiftKey&&(document.activeElement===first||!dialog.contains(document.activeElement))){event.preventDefault();last.focus();}
  else if(!event.shiftKey&&(document.activeElement===last||!dialog.contains(document.activeElement))){event.preventDefault();first.focus();}
 });
}
installSportsDialogA11y();
const crown$=id=>document.getElementById(id);
const crownCollapsedGroups=new Set();
const crownCollapsedLeagues=new Set();
function crownEsc(value){return String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
function crownLive(event){return !!event.is_live;}
function crownFormatMatchPhase(period,minute,fallback){
  const rawPeriod=String(period??"").trim();
  const rawMinute=String(minute??"").trim();
  const labels={"1H":"上半场","1ST":"上半场","1":"上半场","2H":"下半场","2ND":"下半场","2":"下半场","HT":"中场","HALF_TIME":"中场","HALFTIME":"中场","FT":"已结束","FINISHED":"已结束","FINAL":"已结束","ET":"加时赛","EXTRA_TIME":"加时赛","EXTRATIME":"加时赛","P":"点球","PEN":"点球","PENALTIES":"点球","LIVE":"进行中"};
  const embedded=rawMinute.match(/^([A-Za-z0-9]+)\\s*\\^\\s*(.+)$/);
  if(embedded){const embeddedLabel=labels[String(embedded[1]).toUpperCase()];if(embeddedLabel)return `${embeddedLabel} ${embedded[2].trim()}${["FT","FINISHED","FINAL"].includes(rawPeriod.toUpperCase())?" · 已结束":""}`;}
  const label=labels[rawPeriod.toUpperCase()];
  if(label&&["上半场","下半场","加时赛","进行中"].includes(label)&&rawMinute)return `${label} ${rawMinute}`;
  if(label)return label;
  return rawMinute||fallback||"开赛时间未知";
}
function crownNormalizeLine(value){return String(value??"").trim().replace(/\\s+/g,"").replace(/[–—−]/g,"-");}
function crownHandicapLine(line,side){
  const text=crownNormalizeLine(line);if(!text)return "盘口未知";
  if(text==="0"||text==="0.0")return "0";
  if(text.startsWith("-"))return side==="home"?text:`+${text.slice(1)}`;
  if(text.startsWith("+"))return side==="home"?text:`-${text.slice(1)}`;
  return side==="home"?`-${text}`:`+${text}`;
}
function crownMarketId(item){return item&& (item.gidm||item.market_id||item.gid||item.hgid) || "";}
function crownMarketCard(title,items,kind,event){
  const list=Array.isArray(items)?items:[];
  if(!list.length)return `<div class="crown-market-card"><div class="market-title"><span>${title}</span></div><div class="crown-market-empty">暂无有效报价</div></div>`;
  const rows=list.map((item,index)=>{
    const line=item.line||"盘口未知";
    const labels=kind==="handicap"
      ? [[`${event.home||"主队"} ${crownHandicapLine(line,"home")}`,item.a],[`${event.away||"客队"} ${crownHandicapLine(line,"away")}`,item.b]]
      : [[`大 ${crownNormalizeLine(line)}`,item.a],[`小 ${crownNormalizeLine(line)}`,item.b]];
    const id=crownMarketId(item);const idLabel=item.master==="Y"||!item.gidm?"主盘":"附加盘";
    return `${labels.map(([label,price],side)=>`<div class="crown-market-row" data-key="${kind}-${index}-${side}"><span class="crown-market-label">${crownEsc(label)}</span><strong class="crown-market-price">${crownEsc(price||"—")}</strong></div>`).join("")}${id?`<div class="crown-market-footer" data-key="${kind}-${index}-ids">${idLabel} · 皇冠 ID ${crownEsc(id)}</div>`:""}`;
  }).join("");
  return `<div class="crown-market-card"><div class="market-title"><span>${title}</span><small>${list.length} 组</small></div>${rows}</div>`;
}
function crownMoneyline(event){
  const values=Array.isArray(event.markets&&event.markets.moneyline)?event.markets.moneyline:[];
  if(values.length<3)return `<div class="crown-moneyline"><div class="market-title">全场胜平负</div><div class="crown-market-empty">暂无有效报价</div></div>`;
  const labels=[event.home||"主队","和局",event.away||"客队"];
  return `<div class="crown-moneyline"><div class="market-title">全场胜平负</div><div class="crown-moneyline-grid">${labels.map((label,index)=>`<div class="crown-moneyline-cell" data-key="moneyline-${index}"><span>${crownEsc(label)}</span><strong>${crownEsc(values[index]||"—")}</strong></div>`).join("")}</div></div>`;
}
function crownMatch(event){
  const markets=event.markets||{};
  const score=event.score||"VS";
  const detail=crownFormatMatchPhase(event.period,event.minute,event.start_label);
  return `<article class="match" data-key="event-${crownEsc(event.id)}"><div class="match-top"><div class="team">${crownEsc(event.home)}</div><div class="score">${crownEsc(score)}<span class="time">${crownEsc(detail)}</span></div><div class="team away">${crownEsc(event.away)}</div></div><div class="crown-markets">${crownMoneyline(event)}<div class="crown-market-grid">${crownMarketCard("让球",markets.handicap,"handicap",event)}${crownMarketCard("大小球",markets.total,"total",event)}</div></div></article>`;
}
function crownNodeKey(node){return node&&node.nodeType===1?node.getAttribute("data-key"):null;}
function crownPatchNode(current,next){
  if(current.nodeType===3||current.nodeType===8){if(current.nodeValue!==next.nodeValue)current.nodeValue=next.nodeValue;return;}
  if(current.nodeType!==1)return;
  [...current.attributes].forEach(attribute=>{if(!next.hasAttribute(attribute.name))current.removeAttribute(attribute.name);});
  [...next.attributes].forEach(attribute=>{if(current.getAttribute(attribute.name)!==attribute.value)current.setAttribute(attribute.name,attribute.value);});
  crownPatchChildren(current,next);
}
function crownPatchChildren(current,next){
  const incoming=[...next.childNodes];
  for(let index=0;index<incoming.length;index++){
    const desired=incoming[index];
    let existing=current.childNodes[index]||null;
    const desiredKey=crownNodeKey(desired);
    const existingKey=crownNodeKey(existing);
    if((desiredKey||existingKey)&&desiredKey!==existingKey){
      const matching=desiredKey?[...current.childNodes].slice(index+1).find(node=>crownNodeKey(node)===desiredKey):null;
      if(matching){current.insertBefore(matching,existing);existing=matching;}
      else{const inserted=desired.cloneNode(true);current.insertBefore(inserted,existing);existing=inserted;}
    }else if(!existing){existing=desired.cloneNode(true);current.append(existing);}
    const sameNode=existing.nodeType===desired.nodeType&&
      (existing.nodeType!==1||(existing.tagName===desired.tagName&&crownNodeKey(existing)===desiredKey));
    if(sameNode)crownPatchNode(existing,desired);
    else current.replaceChild(desired.cloneNode(true),existing);
  }
  while(current.childNodes.length>incoming.length)current.lastChild.remove();
}
function crownRender(state){
  const events=Array.isArray(state.events)?state.events:[];
  const query=crown$("crownSearch").value.trim().toLowerCase();
  const filter=crown$("crownFilter").value;
  const visible=events.filter(event=>{
    if(filter==="live"&&!crownLive(event))return false;
    if(filter==="prematch"&&crownLive(event))return false;
    if(query&&!([event.league,event.home,event.away,event.id].join(" ").toLowerCase().includes(query)))return false;
    return true;
  });
  crown$("crownCount").textContent=events.length;
  crown$("crownLeagueCount").textContent=(state.leagues||[]).length;
  crown$("crownStatus").textContent=state.status||"检查中";
  crown$("crownSubStatus").textContent=state.updated_at?`皇冠数据源页面 · 最后更新：${state.updated_at}`:`皇冠数据源页面 · ${state.status||"检查中"}`;
  const signature=JSON.stringify([events,state.leagues||[],query,filter,[...crownCollapsedGroups].sort(),[...crownCollapsedLeagues].sort()]);
  if(signature===crownRenderedEventsSignature)return;
  crownRenderedEventsSignature=signature;
  const group=(key,title,items)=>{
    const leagues=new Map();
    items.forEach(event=>{if(!leagues.has(event.league))leagues.set(event.league,[]);leagues.get(event.league).push(event);});
    const collapsed=crownCollapsedGroups.has(key);
    const body=[...leagues.entries()].map(([name,matches],leagueIndex)=>{const leagueCollapsed=crownCollapsedLeagues.has(name);const bodyId=`${key}-league-${leagueIndex}`;return `<section class="league${leagueCollapsed?" collapsed":""}" data-key="league-${crownEsc(name)}" data-league="${crownEsc(name)}"><button class="league-head" type="button" aria-expanded="${!leagueCollapsed}" aria-controls="${bodyId}"><span class="league-name"><span>${crownEsc(name)}</span></span><span class="league-count">${matches.length} 场</span><span class="toggle" aria-hidden="true">⌄</span></button><div class="league-matches" id="${bodyId}">${matches.map(crownMatch).join("")}</div></section>`;}).join("");
    return `<section class="event-group ${key}${collapsed?" collapsed":""}" data-key="group-${key}" data-group="${key}"><button class="event-group-head" type="button" data-group-toggle="${key}" aria-expanded="${!collapsed}" aria-controls="group-body-${key}"><span class="event-group-title">${title}</span><span class="event-group-count">${items.length} 场 · ${leagues.size} 个联赛</span><span class="event-group-toggle" aria-hidden="true">⌄</span></button><div class="event-group-body" id="group-body-${key}">${body||'<div class="notice">当前没有符合条件的比赛</div>'}</div></section>`;
  };
  const next=document.createElement("div");
  next.innerHTML=`<div class="event-groups">${group("live","滚球比赛",visible.filter(crownLive))}${group("prematch","今日比赛",visible.filter(event=>!crownLive(event)))}</div>`;
  crownPatchChildren(crown$("crownEvents"),next);
}
function crownSessionView(state){
  const connected=state.status==="已连接";
  crown$("loginIndicator").className="login-status-indicator "+(connected?"connected":state.error?"error":"");
  crown$("loginStatusTitle").textContent=connected?"皇冠会话已连接":"皇冠会话"+(state.status?" · "+state.status:"");
  crown$("loginStatusCopy").textContent=connected?`只读赛事接口正常，最近更新：${state.updated_at||"刚刚"}`:"请在下面添加账号并完成本项目自己的皇冠登录。";
}
function crownAccountStatus(account){
  const labels={idle:"未登录",logging_in:"登录中",checking:"检查中",awaiting_otp:"等待动态码",awaiting_manual:"等待浏览器操作",session_received:"已保存会话",authenticated:"已验证",expired:"会话已过期",disabled:"已停用"};
  return labels[account.status]||account.status||"未知";
}
function crownRenderAccounts(accounts){
  const root=crown$("crownAccounts");
  if(!accounts.length){root.innerHTML='<div class="account-empty">当前项目还没有添加皇冠账号。</div>';return;}
  root.innerHTML=accounts.map(account=>{
    const session=account.has_session?"已保存会话":"未保存会话";
    const checked=account.last_checked?` · 最后检查 ${crownEsc(account.last_checked)}`:"";
    const otp=account.status==="awaiting_otp"?`<form class="otp-form" data-otp-account="${crownEsc(account.id)}"><input name="code" inputmode="numeric" autocomplete="one-time-code" placeholder="动态验证码" required><button class="action-button primary" type="submit">提交验证码</button></form>`:"";
    return `<div class="account-row" data-account-id="${crownEsc(account.id)}"><div class="account-row-head"><div><div class="account-row-name">${crownEsc(account.username)}</div><div class="account-row-meta">${crownEsc(account.base_url)} · ${session}${checked}</div></div><div class="account-row-status">${crownEsc(crownAccountStatus(account))}</div></div>${otp}<div class="account-row-actions"><button class="action-button" data-account-action="login">登录</button><button class="action-button" data-account-action="check">检查会话</button><button class="action-button" data-account-action="relogin">重新登录</button><button class="action-button" data-account-action="cancel">取消</button><button class="action-button" data-account-action="clear">清除会话</button><button class="action-button" data-account-action="delete">删除</button></div></div>`;
  }).join("");
}
let crownRenderedEventsSignature="";
let crownRenderedAccountsSignature=null;
let crownUpdateInFlight=false;
async function crownUpdate(){
  if(crownUpdateInFlight)return;
  crownUpdateInFlight=true;
  try{
    const [eventResponse,accountResponse]=await Promise.all([fetch("/api/crown/events",{cache:"no-store"}),fetch("/api/crown/accounts",{cache:"no-store"})]);
    if(!eventResponse.ok||!accountResponse.ok)throw new Error("HTTP error");
    const data=await eventResponse.json();
    const accounts=await accountResponse.json();
    const accountList=Array.isArray(accounts.accounts)?accounts.accounts:[];
    crownRender(data);crownSessionView(data);
    const accountSignature=JSON.stringify(accountList);
    if(accountSignature!==crownRenderedAccountsSignature){crownRenderedAccountsSignature=accountSignature;crownRenderAccounts(accountList);}
  }catch(error){crown$("crownStatus").textContent="读取失败";}
  finally{crownUpdateInFlight=false;}
}
async function crownCheck(){
  const button=crown$("checkCrownSession");button.disabled=true;button.textContent="检查中…";
  try{await fetch("/api/crown/session/check",{method:"POST",headers:{"Content-Type":"application/json"}});}finally{button.disabled=false;button.textContent="检查全部会话";await crownUpdate();}
}
async function crownAccountAction(id,action){
  const row=document.querySelector(`[data-account-id="${CSS.escape(id)}"]`);
  const button=row&&row.querySelector(`[data-account-action="${action}"]`);
  if(button){button.disabled=true;button.textContent="处理中…";}
  try{
    const response=await fetch(`/api/crown/accounts/${encodeURIComponent(id)}/${action}`,{method:"POST",headers:{"Content-Type":"application/json"}});
    const payload=await response.json();
    if(!response.ok)throw new Error(payload.error||"操作失败");
  }catch(error){window.alert(error.message||"操作失败");}
  await crownUpdate();
}
crown$("crownAccountForm").addEventListener("submit",async event=>{
  event.preventDefault();
  const status=crown$("crownAccountFormStatus");
  const button=event.currentTarget.querySelector("button[type=submit]");
  button.disabled=true;status.textContent="正在保存并启动登录…";
  const payload={login_url:crown$("crownLoginUrl").value.trim(),username:crown$("crownUsername").value.trim(),password:crown$("crownPassword").value,totp_secret:crown$("crownTotp").value.trim()};
  try{
    const response=await fetch("/api/crown/accounts",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
    const result=await response.json();
    if(!response.ok)throw new Error(result.error||"添加账号失败");
    crown$("crownPassword").value="";crown$("crownTotp").value="";status.textContent="账号已保存，登录任务已启动。";
    await crownUpdate();
  }catch(error){status.textContent=error.message||"添加账号失败";}
  finally{button.disabled=false;}
});
function settingsOpen(){window.__openSportsDialog(crown$("settingsModal"),crown$("settingsButton"));}
function settingsClose(){window.__closeSportsDialog(crown$("settingsModal"));}
crown$("settingsButton").addEventListener("click",settingsOpen);
crown$("settingsClose").addEventListener("click",settingsClose);
crown$("settingsModal").addEventListener("click",event=>{if(event.target===crown$("settingsModal"))settingsClose();});
crown$("crownLoginButton").addEventListener("click",()=>{settingsClose();window.__openSportsDialog(crown$("loginModal"),crown$("settingsButton"));crownUpdate();});
crown$("closeLogin").addEventListener("click",()=>window.__closeSportsDialog(crown$("loginModal")));
crown$("loginModal").addEventListener("click",event=>{if(event.target===crown$("loginModal"))window.__closeSportsDialog(crown$("loginModal"));});
crown$("checkCrownSession").addEventListener("click",crownCheck);
crown$("crownAccounts").addEventListener("click",event=>{const button=event.target.closest("[data-account-action]");if(!button)return;const row=button.closest("[data-account-id]");if(!row)return;const action=button.dataset.accountAction;if(action==="delete"&&!window.confirm("确认删除这个账号及其本地会话吗？"))return;crownAccountAction(row.dataset.accountId,action);});
crown$("crownAccounts").addEventListener("submit",async event=>{
  const form=event.target.closest("[data-otp-account]");if(!form)return;event.preventDefault();
  const button=form.querySelector("button[type=submit]");button.disabled=true;
  try{const response=await fetch(`/api/crown/accounts/${encodeURIComponent(form.dataset.otpAccount)}/otp`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({code:new FormData(form).get("code")})});const result=await response.json();if(!response.ok)throw new Error(result.error||"验证码提交失败");await crownUpdate();}catch(error){window.alert(error.message||"验证码提交失败");button.disabled=false;}
});
crown$("crownSearch").addEventListener("input",crownUpdate);
crown$("crownFilter").addEventListener("change",crownUpdate);
crown$("crownEvents").addEventListener("click",event=>{const groupButton=event.target.closest("[data-group-toggle]");if(groupButton){const key=groupButton.dataset.groupToggle;if(crownCollapsedGroups.has(key))crownCollapsedGroups.delete(key);else crownCollapsedGroups.add(key);crownUpdate();return;}const head=event.target.closest(".league-head");if(!head)return;const name=head.closest(".league").dataset.league;if(crownCollapsedLeagues.has(name))crownCollapsedLeagues.delete(name);else crownCollapsedLeagues.add(name);crownUpdate();});
if(new URLSearchParams(window.location.search).get("openSettings")==="1")settingsOpen();
crownUpdate();setInterval(crownUpdate,1000);
</script>
</body>
</html>
"""

class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ValueError("请求格式不正确") from None
        if length < 0 or length > 128 * 1024:
            raise ValueError("请求体过大")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("请求格式不正确") from None
        if not isinstance(payload, dict):
            raise ValueError("请求必须是 JSON 对象")
        return payload

    def _crown_state(self):
        return _snapshot_crown()

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/events":
            with lock:
                payload = dict(state)
            payload["events"] = _pinnacle_display_events(
                _pinnacle_today_events(payload.get("events") or [])
            )
            payload["leagues"] = sorted({
                event["league"] for event in payload["events"] if event.get("league")
            })
            payload.update(_pinnacle_feed_status())
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/combined":
            try:
                self._send_json(_combined_payload())
            except Exception as exc:
                self._send_json({"error": f"赛事对照暂不可用：{type(exc).__name__}"}, status=500)
        elif path == "/api/crown/events":
            self._send_json(self._crown_state())
        elif path == "/api/crown/accounts":
            try:
                self._send_json({"accounts": public_accounts()})
            except Exception:
                self._send_json({"error": "账号存储暂不可用"}, status=500)
        elif path == "/api/crown/session":
            payload = self._crown_state()
            payload.pop("events", None)
            payload.pop("leagues", None)
            self._send_json(payload)
        elif path == "/api/pinnacle/session":
            payload = _pinnacle_feed_status()
            payload["browser_running"] = bool(_ps3838_browser and _ps3838_browser.is_running)
            self._send_json(payload)
        elif path == "/api/pinnacle/credentials":
            try:
                self._send_json(_public_pinnacle_credentials())
            except Exception:
                self._send_json({"error": "读取已保存账号失败"}, status=500)
        elif path == "/crown":
            body = CROWN_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/combined":
            body = Path(__file__).with_name("combined.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/":
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/matches/decision":
            try:
                payload = self._read_json()
                action = payload.get("action")
                crown_id = str(payload.get("crown_id") or "")
                pinnacle_id = str(payload.get("pinnacle_id") or "")
                decisions = list_mappings()
                if action == "unconfirm":
                    if not any(str(row.get("crown_id")) == crown_id for row in decisions["confirmed"]):
                        raise ValueError("没有找到这场比赛的人工确认映射")
                    unconfirm(crown_id)
                elif action in {"confirm", "reject"}:
                    current = _combined_payload()
                    eligible = any(str(row.get("crown", {}).get("id")) == crown_id
                                   and str(row.get("pinnacle", {}).get("id")) == pinnacle_id
                                   for row in current["candidates"])
                    if not eligible:
                        raise ValueError("候选已变化或不符合确认条件，请刷新后重试")
                    crown = _snapshot_crown()
                    with lock:
                        pin_events = list(state.get("events") or [])
                    crown_event = next((row for row in crown["events"] if str(row.get("id")) == crown_id), None)
                    pinnacle_event = next((row for row in pin_events if str(row.get("id")) == pinnacle_id), None)
                    if crown_event is None or pinnacle_event is None:
                        raise ValueError("赛事已从数据源消失，请刷新页面")
                    if action == "confirm":
                        confirm(crown_id, pinnacle_id, crown_event, pinnacle_event)
                    else:
                        reject(crown_id, pinnacle_id)
                else:
                    raise ValueError("不支持的映射操作")
                self._send_json(_combined_payload())
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=400)
            except Exception:
                self._send_json({"error": "保存比赛映射失败"}, status=500)
            return
        if path == "/api/crown/session/check":
            refresh_once()
            payload = self._crown_state()
            payload.pop("events", None)
            payload.pop("leagues", None)
            self._send_json(payload)
            return
        if path == "/api/pinnacle/credentials":
            try:
                result = _save_pinnacle_credentials(self._read_json())
                self._send_json({"ok": True, **result})
            except ValueError as exc:
                self._send_json({"error": str(exc), "code": "invalid_credentials"}, status=400)
            except Exception:
                self._send_json({"error": "保存账号密码失败"}, status=500)
            return
        if path == "/api/pinnacle/browser/open":
            try:
                self._send_json(_open_ps3838_browser())
            except ValueError as exc:
                self._send_json({"error": str(exc), "code": "credentials_missing"}, status=400)
            except Exception as exc:
                self._send_json({"error": f"启动 Chrome 失败：{type(exc).__name__}"}, status=500)
            return
        if path == "/api/pinnacle/browser/refresh":
            browser = _ps3838_browser
            if browser is None or not browser.is_running or not browser.refresh():
                self._send_json({"error": "平博 Chrome 会话尚未运行"}, status=409)
            else:
                self._send_json({"ok": True, "queued": True})
            return
        if path in {"/api/pinnacle/session/check", "/api/pinnacle/session/clear", "/api/pinnacle/login"}:
            self._send_json({
                "error": "平博登录和赔率连接现在由 Chrome 浏览器完成",
                "code": "browser_required",
            }, status=410)
            return
        if path == "/api/crown/accounts":
            try:
                payload = self._read_json()
                required = ("login_url", "username", "password")
                if any(not isinstance(payload.get(key), str) or not payload.get(key, "").strip() for key in required):
                    raise ValueError("登录地址、账号和密码不能为空")
                account = add_account(
                    payload["login_url"], payload["username"], payload["password"],
                    payload.get("totp_secret", "") if isinstance(payload.get("totp_secret", ""), str) else "",
                )
                # 保存后立即把登录任务交给皇冠运行时；浏览器和 Playwright
                # 只由运行时自己的 worker 线程操作，HTTP 线程不直接碰页面。
                account = account_action(account["id"], "login") or account
                self._send_json({"account": account}, status=201)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=400)
            except Exception:
                self._send_json({"error": "账号保存失败"}, status=500)
            return
        parts = [part for part in path.split("/") if part]
        if len(parts) == 5 and parts[:3] == ["api", "crown", "accounts"]:
            account_id, action = parts[3], parts[4]
            if action == "otp":
                try:
                    payload = self._read_json()
                    code = payload.get("code")
                    if not isinstance(code, str):
                        raise ValueError("动态验证码格式不正确")
                    account = submit_otp(account_id, code)
                    self._send_json({"account": account, "accounts": public_accounts()})
                except (KeyError, ValueError) as exc:
                    self._send_json({"error": str(exc)}, status=400)
                except Exception:
                    self._send_json({"error": "验证码提交失败"}, status=500)
                return
            if action not in {"login", "check", "relogin", "cancel", "clear", "delete"}:
                self._send_json({"error": "不支持的账号操作"}, status=404)
                return
            try:
                account = account_action(account_id, action)
                self._send_json({"account": account, "accounts": public_accounts()})
            except (KeyError, ValueError) as exc:
                self._send_json({"error": str(exc)}, status=400)
            except Exception:
                self._send_json({"error": "账号操作失败"}, status=500)
            return
        self.send_error(404)

    def do_DELETE(self):
        path = self.path.split("?", 1)[0]
        parts = [part for part in path.split("/") if part]
        if len(parts) == 4 and parts[:3] == ["api", "crown", "accounts"]:
            try:
                account = account_action(parts[3], "delete")
                self._send_json({"account": account, "accounts": public_accounts()})
            except (KeyError, ValueError) as exc:
                self._send_json({"error": str(exc)}, status=400)
            except Exception:
                self._send_json({"error": "删除账号失败"}, status=500)
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))

if __name__ == "__main__":
    initialize_crown()
    threading.Thread(target=crown_refresh_loop, daemon=True).start()
    print("平博足球网页已启动")
    print("本机访问：http://127.0.0.1:8887")
    print("停止服务：按 Control + C")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if _ps3838_browser is not None:
            _ps3838_browser.stop()
