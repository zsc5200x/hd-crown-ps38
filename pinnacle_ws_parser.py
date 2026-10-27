"""Adapters for complete and structurally verifiable PS3838 odds trees."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from decimal import Decimal, InvalidOperation
from typing import Any, Callable


class UnsupportedOddsUpdate(ValueError):
    """The frame is not a confirmed full compact/events snapshot."""


def parse_complete_odds_update(
    message: Mapping[str, Any],
    parse_response: Callable[[Mapping[str, Any]], Any],
) -> Any:
    """Parse refreshAll frames containing compact pre-match/live trees.

    The caller owns merging and cache replacement. Partial update arrays must
    never be interpreted as a complete event set.
    """
    if message.get("type") != "UPDATE_ODDS":
        raise UnsupportedOddsUpdate("不是 UPDATE_ODDS 消息")

    odds = message.get("odds")
    if not isinstance(odds, Mapping) or odds.get("refreshAll") is not True:
        raise UnsupportedOddsUpdate("增量 UPDATE_ODDS 尚未确认结构")

    prematch_tree = odds.get("n")
    live_tree = odds.get("l")
    if prematch_tree is None:
        prematch_tree = []
    if live_tree is None:
        live_tree = []
    if not isinstance(prematch_tree, list) or not isinstance(live_tree, list):
        raise UnsupportedOddsUpdate("完整赔率消息的 n/l 字段不是 compact 数组")
    if not prematch_tree and not live_tree:
        raise UnsupportedOddsUpdate("完整赔率消息缺少 compact n/l 数据")

    return parse_response({"n": prematch_tree, "l": live_tree})


def parse_incremental_odds_update(
    message: Mapping[str, Any],
    parse_response: Callable[[Mapping[str, Any]], Any],
    diagnostics: MutableMapping[str, Any] | None = None,
) -> Any:
    """Parse verified compact trees and full-time market rows from ``u``.

    Returned events are partial patches and must be merged by event ID. The
    market row's final field is the event ID; non-market status rows are ignored.
    """
    if message.get("type") != "UPDATE_ODDS":
        raise UnsupportedOddsUpdate("不是 UPDATE_ODDS 消息")

    odds = message.get("odds")
    if not isinstance(odds, Mapping) or odds.get("refreshAll") is not False:
        raise UnsupportedOddsUpdate("不是增量赔率消息")

    parsed_events: list[dict[str, Any]] = []
    parsed_leagues: set[str] = set()
    for section in ("n", "l"):
        tree = odds.get(section)
        if not isinstance(tree, list) or not _is_compact_soccer_tree(tree):
            continue

        payload = {"n": tree if section == "n" else [], "l": tree if section == "l" else []}
        events, leagues = parse_response(payload)
        for event in events:
            if not isinstance(event, Mapping):
                continue
            if bool(event.get("is_live")) != (section == "l"):
                continue
            if not event.get("id") or not event.get("home") or not event.get("away"):
                continue
            if not _event_has_valid_odds(event):
                continue
            parsed_events.append(dict(event))
        parsed_leagues.update(str(name) for name in leagues if name)

    u_diagnostics: dict[str, int] = {}
    parsed_events.extend(_parse_incremental_market_rows(odds.get("u"), u_diagnostics))
    if diagnostics is not None:
        diagnostics["incremental_u"] = u_diagnostics

    if not parsed_events:
        raise UnsupportedOddsUpdate("增量消息没有可验证的赛事赔率")
    parsed_events.sort(key=lambda event: (str(event.get("league", "")).lower(), str(event.get("home", "")).lower()))
    return parsed_events, sorted(parsed_leagues)


def _is_compact_soccer_tree(tree: list[Any]) -> bool:
    """Require the same nested sport/league/match layout as full snapshots."""
    found_soccer = False
    for sport in tree:
        if not isinstance(sport, list) or len(sport) < 3:
            return False
        if sport[0] != 29 and sport[1] not in ("Soccer", "足球"):
            continue
        found_soccer = True
        if not isinstance(sport[2], list):
            return False
        for league in sport[2]:
            if not isinstance(league, list) or len(league) < 3 or not isinstance(league[2], list):
                return False
            for match in league[2]:
                if (
                    not isinstance(match, list)
                    or len(match) <= 8
                    or isinstance(match[0], bool)
                    or not isinstance(match[0], (int, float))
                    or not isinstance(match[1], str)
                    or not match[1].strip()
                    or not isinstance(match[2], str)
                    or not match[2].strip()
                    or not isinstance(match[8], Mapping)
                ):
                    return False
    return found_soccer


def _event_has_valid_odds(event: Mapping[str, Any]) -> bool:
    markets = event.get("markets")
    if not isinstance(markets, Mapping):
        return False
    for market_type in ("handicap", "total"):
        rows = markets.get(market_type)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, Mapping) and (
                _valid_price(row.get("a"))
                or _valid_price(row.get("b"))
                or _valid_price(row.get("price"))
            ):
                return True
    moneyline = markets.get("moneyline")
    return isinstance(moneyline, list) and any(_valid_price(value) for value in moneyline)


def _valid_price(value: Any) -> bool:
    try:
        return value not in (None, "") and float(value) > 0
    except (TypeError, ValueError, OverflowError):
        return False


def _parse_incremental_market_rows(
    updates: Any,
    diagnostics: MutableMapping[str, int] | None = None,
) -> list[dict[str, Any]]:
    """Decode observed full-time market rows while rejecting unknown variants."""
    stats = {
        "groups": 0,
        "soccer_groups": 0,
        "non_soccer_groups": 0,
        "malformed_groups": 0,
        "rows_seen": 0,
        "short_rows": 0,
        "invalid_market_columns": 0,
        "non_full_period": 0,
        "invalid_event_ids": 0,
        "invalid_prices": 0,
        "unsupported_market_selection": 0,
        "invalid_lines": 0,
        "accepted_market_rows": 0,
        "events_emitted": 0,
    }
    if not isinstance(updates, list):
        if diagnostics is not None:
            diagnostics.update(stats)
        return []

    events_by_id: dict[str, dict[str, Any]] = {}
    for sport_update in updates:
        stats["groups"] += 1
        if not isinstance(sport_update, list) or len(sport_update) < 2:
            stats["malformed_groups"] += 1
            continue
        if sport_update[0] != 29:
            stats["non_soccer_groups"] += 1
            continue
        stats["soccer_groups"] += 1
        market_rows = sport_update[1]
        if not isinstance(market_rows, list):
            stats["malformed_groups"] += 1
            continue
        for row in market_rows:
            stats["rows_seen"] += 1
            if not isinstance(row, list) or len(row) < 13:
                stats["short_rows"] += 1
                continue
            period, market_type, selection = row[0], row[1], row[2]
            if isinstance(period, bool) or not isinstance(period, int):
                stats["invalid_market_columns"] += 1
                continue
            if period != 0:
                stats["non_full_period"] += 1
                continue
            if (
                isinstance(market_type, bool) or not isinstance(market_type, int)
                or isinstance(selection, bool) or not isinstance(selection, int)
            ):
                stats["invalid_market_columns"] += 1
                continue
            event_id_value = row[12]
            if isinstance(event_id_value, bool) or not isinstance(event_id_value, (int, float, str)):
                stats["invalid_event_ids"] += 1
                continue
            event_id = str(event_id_value).strip()
            if not event_id or not event_id.isdigit():
                stats["invalid_event_ids"] += 1
                continue

            raw_price = row[5]
            if not _valid_price(raw_price):
                stats["invalid_prices"] += 1
                continue
            price = str(raw_price)

            supported_market = (
                market_type == 1 and selection in (0, 1, 2)
                or market_type == 2 and selection in (0, 1)
                or market_type == 3 and selection in (3, 4)
            )
            if not supported_market:
                stats["unsupported_market_selection"] += 1
                continue

            line: str | None = None
            if market_type in (2, 3):
                line = _numeric_text(row[3])
                if line is None:
                    stats["invalid_lines"] += 1
                    continue

            event = events_by_id.setdefault(event_id, {
                "id": event_id,
                "markets": {"handicap": [], "total": [], "moneyline": [None, None, None]},
                "match_quotes": [],
            })
            markets = event["markets"]

            if market_type == 1 and selection in (0, 1, 2):
                # Observed order is draw, home, away; the local snapshot stores home, draw, away.
                selection_name, price_index = {0: ("draw", 1), 1: ("home", 0), 2: ("away", 2)}[selection]
                markets["moneyline"][price_index] = price
                event["match_quotes"].append({
                    "market_type": "moneyline", "period": "full", "selection": selection_name,
                    "line": None, "hk_odds": float(price), "raw_odds": price,
                })
            elif market_type == 2 and selection in (0, 1):
                side = "home" if selection == 0 else "away"
                markets["handicap"].append({"selection": side, "side_line": line, "price": price})
            elif market_type == 3 and selection in (3, 4):
                side = "over" if selection == 3 else "under"
                markets["total"].append({"line": line, "selection": side, "price": price})
            stats["accepted_market_rows"] += 1

    result = [event for event in events_by_id.values() if _event_has_valid_odds(event)]
    stats["events_emitted"] = len(result)
    if diagnostics is not None:
        diagnostics.update(stats)
    return result


def _numeric_text(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite():
        return None
    return format(number.normalize(), "f")
