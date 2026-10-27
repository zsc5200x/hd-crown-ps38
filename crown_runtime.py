"""当前项目独立的皇冠账号、登录会话和只读赛事运行时。"""
from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from crown_local.backend.crown_markets import crown_hk_pair, extract_quotes, normalize_line
from crown_local.backend.odds import to_hk_water
from event_filters import is_excluded_league


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

try:
    from crown_local.backend.auth import AuthService
    from crown_local.backend.crown_member import CrownMemberMonitor
    from crown_local.backend.crown_odds import CrownOddsClient
    from crown_local.backend.storage import Store
except ImportError as exc:  # pragma: no cover - shown as a safe UI error
    AuthService = None
    CrownMemberMonitor = None
    CrownOddsClient = None
    Store = None
    IMPORT_ERROR = str(exc)
else:
    IMPORT_ERROR = ""


lock = threading.RLock()
state = {
    "events": [],
    "leagues": [],
    "updated_at": None,
    "status": "未添加账号",
    "error": None,
    "source": "当前项目本地皇冠账号",
    "diagnostics": {
        "refresh_count": 0,
        "last_refresh_duration_ms": None,
        "last_list_duration_ms": None,
        "last_snapshot_interval_ms": None,
        "details_refreshing": False,
        "last_details_duration_ms": None,
    },
}
store = None
auth = None
monitor = None

# ``get_game_more`` is a second read-only request per fixture. Keep bounded
# per-fixture caches while the base today/live feed refreshes every five seconds.
_MORE_CACHE_TTL = 30.0
_MORE_LIVE_CACHE_TTL = 10.0
_MORE_MAX_WORKERS = 6
_more_cache_lock = threading.RLock()
_more_cache = {}
_more_refresh_lock = threading.Lock()
_last_snapshot_monotonic = None


def _configure_browser():
    # Use the user's installed browser when Playwright's bundled Chromium is
    # not present. This setting is local to this process.
    chrome = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    if chrome.exists():
        os.environ.setdefault("HD_REPORT_BROWSER_EXECUTABLE", str(chrome))


def initialize():
    global store, auth, monitor
    if store is not None or auth is not None or monitor is not None:
        return
    if Store is None or AuthService is None or CrownMemberMonitor is None:
        with lock:
            state["status"] = "运行环境未安装依赖"
            state["error"] = "请使用当前项目 .venv 启动"
        return
    _configure_browser()
    store = Store(DATA_DIR, initialize=True)
    auth = AuthService(store)
    monitor = CrownMemberMonitor(store, auth)
    monitor.start()


def public_accounts():
    initialize()
    if store is None:
        return []
    return [account for account in store.accounts() if account.get("kind") == "crown_odds"]


def add_account(login_url, username, password, totp_secret=""):
    initialize()
    if store is None:
        raise RuntimeError("当前项目运行环境未就绪")
    return store.save_account({
        "kind": "crown_odds",
        "base_url": login_url,
        "username": username,
        "password": password,
        "totp_secret": totp_secret,
        "enabled": True,
        "role": "primary",
        "login_layer": "",
        "interval": 30,
    })


def account_action(account_id, action):
    initialize()
    if auth is None or store is None:
        raise RuntimeError("当前项目运行环境未就绪")
    account = store.get_account(account_id)
    if account.get("kind") != "crown_odds":
        raise ValueError("只允许操作皇冠水位账号")
    if action == "login":
        auth.start(account_id)
    elif action == "check":
        auth.test(account_id, background=False)
    elif action == "relogin":
        try:
            auth.cancel(account_id)
        except Exception:
            pass
        store.clear_session(account_id)
        auth.start(account_id)
    elif action == "cancel":
        auth.cancel(account_id)
    elif action == "clear":
        try:
            auth.cancel(account_id)
        except Exception:
            pass
        store.clear_session(account_id)
    elif action == "delete":
        try:
            auth.cancel(account_id)
        except Exception:
            pass
        store.delete_account(account_id)
    else:
        raise ValueError("不支持的账号操作")
    return next((item for item in public_accounts() if item["id"] == account_id), None)


def submit_otp(account_id, code):
    initialize()
    if auth is None or store is None:
        raise RuntimeError("当前项目运行环境未就绪")
    account = store.get_account(account_id)
    if account.get("kind") != "crown_odds":
        raise ValueError("只允许操作皇冠水位账号")
    auth.submit_otp(account_id, code)
    return next((item for item in public_accounts() if item["id"] == account_id), None)


def _safe_reason(error):
    name = error.__class__.__name__
    if name == "MemberExpired":
        return "session_expired"
    if name == "MemberNeedsBrowser":
        return "session_unverified"
    if name in {"MemberUnavailable", "OddsUnavailable"}:
        return "upstream_unavailable"
    return "crown_unavailable"


def _format_line(value):
    normalized = normalize_line(value)
    return format(normalized.normalize(), "f") if normalized is not None else ""


def _more_pair(row, left_key, right_key, raw_format):
    left_raw = _price(row.get(left_key))
    right_raw = _price(row.get(right_key))
    if not left_raw or not right_raw:
        return None
    if raw_format == "CROWN_HK":
        pair = crown_hk_pair(left_raw, right_raw)
        if not pair:
            return None
        left, right = (_format_hk(float(pair[0])), _format_hk(float(pair[1])))
    else:
        left = _format_hk(to_hk_water(left_raw, raw_format))
        right = _format_hk(to_hk_water(right_raw, raw_format))
    if not left or not right:
        return None
    return {"a": left, "b": right, "raw_a": left_raw, "raw_b": right_raw}


def _more_ids(row):
    gid = str(row.get("GID") or row.get("ID") or "")
    gidm = str(row.get("GIDM") or "")
    hgid = str(row.get("HGID") or "")
    return {
        "gid": gid,
        "gidm": gidm,
        "hgid": hgid,
        "market_id": gidm or gid,
        "master": str(row.get("MASTER") or ""),
    }


def _normalize_more_markets(rows, raw_format):
    """Normalize every non-zero full-time line returned by ``game_more``.

    Crown uses a compact field catalogue rather than one XML node per market.
    The ``RATIO`` fields identify lines and the matching ``IOR`` fields carry
    the two water values. We preserve the provider keys and row IDs so the UI
    can show all available alternate lines without pretending that a shared
    ``GIDM`` is unique to one selection.
    """
    result = {"handicap": [], "total": [], "moneyline": [], "rows": []}
    seen = set()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ids = _more_ids(row)
        result["rows"].append({**ids, "ptype_id": str(row.get("PTYPE_ID") or "")})
        if str(row.get("MODE") or "FT").upper() != "FT" or str(row.get("PTYPE") or ""):
            continue
        token_values = {
            str(key)[6:]: value for key, value in row.items()
            if str(key).startswith("RATIO") and value not in (None, "", "0.0", "0.000")
        }
        # ``game_more`` contains many other RATIO_* fields for team totals,
        # corners and cards.  Only these ordinary full-time football markets
        # belong in the comparison table; accepting every suffix would leak
        # those side markets into the handicap/total columns.
        market_specs = {
            "": ("handicap", "IOR_RH", "IOR_RC", "R"),
            "R": ("handicap", "IOR_RH", "IOR_RC", "R"),
            "RE": ("handicap", "IOR_REH", "IOR_REC", "RE"),
            "O": ("total", "IOR_OUC", "IOR_OUH", "O"),
            "ROUO": ("total", "IOR_ROUC", "IOR_ROUH", "ROU"),
        }
        for token, raw_line in token_values.items():
            spec = market_specs.get(token)
            if spec is None:
                # ``U`` is the second name for the same O/U line.  All other
                # suffixes are non-standard side markets for this view.
                continue
            kind, left_key, right_key, market_key = spec
            line = _format_line(raw_line)
            if not line:
                continue
            pair = _more_pair(row, left_key, right_key, raw_format)
            if not pair:
                continue
            key = (ids["gid"], kind, market_key, line, pair["a"], pair["b"])
            if key in seen:
                continue
            seen.add(key)
            item = {
                **ids,
                **pair,
                "market_type": kind,
                "market_key": market_key,
                "line": line,
                "raw_line": str(raw_line),
                "line_source": f"RATIO_{token}" if token else "RATIO",
                "odds_source": {"a": left_key, "b": right_key},
                "odds_format": raw_format,
            }
            if kind == "total":
                item.update({"over": pair["a"], "under": pair["b"]})
            else:
                item.update({"home": pair["a"], "away": pair["b"]})
            result[kind].append(item)

        moneyline = {
            "home": _price(row.get("IOR_MH")),
            "draw": _price(row.get("IOR_MN")),
            "away": _price(row.get("IOR_MC")),
        }
        if any(moneyline.values()):
            result["moneyline"].append({**ids, **moneyline, "market_type": "moneyline", "odds_format": raw_format})
    return result


def _cached_more_markets(client, game, mode, raw_format):
    gid = str(game.get("GID") or game.get("ID") or "")
    if not gid or not callable(getattr(client, "game_more", None)):
        return {"status": "not_requested", "handicap": [], "total": [], "moneyline": [], "rows": []}
    now = time.monotonic()
    ttl = _MORE_LIVE_CACHE_TTL if mode == "live" else _MORE_CACHE_TTL
    cache_key = (gid, mode, raw_format)
    with _more_cache_lock:
        cached = _more_cache.get(cache_key)
        if cached and now - cached["created_at"] < ttl:
            return cached["value"]
    try:
        payload = client.game_more(game, mode)
        value = _normalize_more_markets(payload.get("games", []), raw_format)
        value["status"] = "loaded"
    except Exception:
        value = {"status": "unavailable", "handicap": [], "total": [], "moneyline": [], "rows": []}
    with _more_cache_lock:
        _more_cache[cache_key] = {"created_at": time.monotonic(), "value": value}
    return value


def _cached_more_snapshot(game, mode, raw_format):
    gid = str(game.get("GID") or game.get("ID") or "")
    if not gid:
        return None
    with _more_cache_lock:
        cached = _more_cache.get((gid, mode, raw_format))
        return cached["value"] if cached else None


def _more_markets_expired(game, mode, raw_format, now=None):
    gid = str(game.get("GID") or game.get("ID") or "")
    if not gid:
        return False
    current = time.monotonic() if now is None else now
    ttl = _MORE_LIVE_CACHE_TTL if mode == "live" else _MORE_CACHE_TTL
    with _more_cache_lock:
        cached = _more_cache.get((gid, mode, raw_format))
        return not cached or current - cached["created_at"] >= ttl


def _load_more_markets(client, games, raw_format):
    """Fetch detail rows in bounded parallelism while keeping base events safe."""
    jobs = {}
    for game, mode in games:
        gid = str(game.get("GID") or game.get("ID") or "")
        if gid and gid not in jobs:
            jobs[gid] = (game, mode)
    if not jobs or not callable(getattr(client, "game_more", None)):
        return {}
    result = {}
    worker_local = threading.local()

    def load_one(game, mode):
        worker_client = getattr(worker_local, "client", None)
        if worker_client is None:
            # urllib's opener and CookieJar are stateful.  Give each worker a
            # private client when the real Crown client exposes its session;
            # lightweight test doubles continue to use the supplied object.
            worker_client = client
            if CrownOddsClient is not None and hasattr(client, "account") and hasattr(client, "session"):
                try:
                    worker_client = CrownOddsClient(client.account, client.session, timeout=getattr(client, "timeout", 10))
                except Exception:
                    worker_client = client
            worker_local.client = worker_client
        return _cached_more_markets(worker_client, game, mode, raw_format)

    with ThreadPoolExecutor(max_workers=_MORE_MAX_WORKERS, thread_name_prefix="crown-more") as pool:
        futures = {
            pool.submit(load_one, game, mode): gid
            for gid, (game, mode) in jobs.items()
        }
        for future in as_completed(futures):
            gid = futures[future]
            try:
                result[gid] = future.result()
            except Exception:
                result[gid] = {"status": "unavailable", "handicap": [], "total": [], "moneyline": [], "rows": []}
    return result


def _start_more_markets_refresh(client, games, raw_format):
    if not games or not callable(getattr(client, "game_more", None)):
        return False
    now = time.monotonic()
    expired = [
        (game, mode) for game, mode in games
        if _more_markets_expired(game, mode, raw_format, now)
    ]
    if not expired or not _more_refresh_lock.acquire(blocking=False):
        return False

    with lock:
        diagnostics = dict(state.get("diagnostics") or {})
        diagnostics["details_refreshing"] = True
        state["diagnostics"] = diagnostics

    def refresh_details():
        started = time.monotonic()
        try:
            _load_more_markets(client, expired, raw_format)
        finally:
            completed = datetime.now().astimezone().isoformat(timespec="seconds")
            duration_ms = round((time.monotonic() - started) * 1000, 1)
            with lock:
                diagnostics = dict(state.get("diagnostics") or {})
                diagnostics.update({
                    "details_refreshing": False,
                    "last_details_duration_ms": duration_ms,
                    "last_details_completed_at": completed,
                })
                state["diagnostics"] = diagnostics
            _more_refresh_lock.release()

    try:
        threading.Thread(
            target=refresh_details,
            name="crown-more-refresh",
            daemon=True,
        ).start()
    except Exception:
        with lock:
            diagnostics = dict(state.get("diagnostics") or {})
            diagnostics["details_refreshing"] = False
            state["diagnostics"] = diagnostics
        _more_refresh_lock.release()
        return False
    return True


def refresh_once():
    global _last_snapshot_monotonic
    refresh_started = time.monotonic()
    initialize()
    if store is None or CrownOddsClient is None:
        with lock:
            state["events"] = []
            state["leagues"] = []
            state["status"] = "运行环境未安装依赖"
            state["error"] = "请使用当前项目 .venv 启动"
        return {"ok": False, "reason": "runtime_unavailable"}
    accounts = [
        account for account in store.accounts()
        if account.get("kind") == "crown_odds" and account.get("enabled")
    ]
    account = next((item for item in accounts if item.get("has_session")), None)
    if not account:
        with lock:
            state["events"] = []
            state["leagues"] = []
            state["status"] = "未登录" if accounts else "未添加账号"
            state["error"] = None
        return {"ok": False, "reason": "no_session"}
    try:
        session = store.load_session(account["id"])
        client = CrownOddsClient(account, session)
        display_format = client.odds_format
        raw_format = client.raw_odds_format
        list_started = time.monotonic()
        today = client.list_games("today")["games"]
        live = client.list_games("live")["games"]
        list_duration_ms = round((time.monotonic() - list_started) * 1000, 1)
        source_games = [(item, "today") for item in today] + [(item, "live") for item in live]
        source_games = [
            (item, mode) for item, mode in source_games
            if not is_excluded_league(item.get("LEAGUE"))
        ]
        more_by_gid = {
            str(item.get("GID") or item.get("ID") or ""): cached
            for item, mode in source_games
            if (cached := _cached_more_snapshot(item, mode, raw_format)) is not None
        }
        events = [
            _normalize_event(
                item, mode, display_format, raw_format,
                more_markets=more_by_gid.get(str(item.get("GID") or item.get("ID") or "")),
            )
            for item, mode in source_games
        ]
        events = list({item["id"]: item for item in events if item["id"]}.values())
        events.sort(key=lambda item: (item["league"].lower(), item["home"].lower()))
        leagues = sorted({item["league"] for item in events})
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        committed = time.monotonic()
        previous_snapshot = _last_snapshot_monotonic
        _last_snapshot_monotonic = committed
        with lock:
            diagnostics = dict(state.get("diagnostics") or {})
            diagnostics.update({
                "refresh_count": int(diagnostics.get("refresh_count") or 0) + 1,
                "last_refresh_duration_ms": round((committed - refresh_started) * 1000, 1),
                "last_list_duration_ms": list_duration_ms,
                "last_snapshot_interval_ms": (
                    round((committed - previous_snapshot) * 1000, 1)
                    if previous_snapshot is not None else None
                ),
            })
            state.update({
                "events": events,
                "leagues": leagues,
                "updated_at": timestamp,
                "status": "已连接",
                "error": None,
                "diagnostics": diagnostics,
            })
        _start_more_markets_refresh(client, source_games, raw_format)
        return {"ok": True, "events": events, "leagues": leagues}
    except Exception as exc:
        reason = _safe_reason(exc)
        with lock:
            state["status"] = "需要重新登录" if reason == "session_expired" else "暂不可用"
            state["error"] = reason
        return {"ok": False, "reason": reason}


def _price(value):
    text = str(value or "").strip()
    return text if text and text not in {"0", "0.000"} else ""


def _market(line, left, right, raw_format="CROWN_HK"):
    raw_left, raw_right = _price(left), _price(right)
    if not raw_left and not raw_right:
        return []
    if raw_format == "CROWN_HK":
        pair = crown_hk_pair(raw_left, raw_right) if raw_left and raw_right else None
        left, right = (_format_hk(float(pair[0])), _format_hk(float(pair[1]))) if pair else ("", "")
    else:
        left = _format_hk(to_hk_water(raw_left, raw_format))
        right = _format_hk(to_hk_water(raw_right, raw_format))
    return [{"line": str(line or ""), "a": left, "b": right,
             "a_hk": left, "b_hk": right,
             "raw_a": raw_left, "raw_b": raw_right,
             "odds_format": raw_format}]


def _format_hk(value):
    return "" if value is None else f"{value:.2f}"


def _match_quotes(game, *, inplay, odds_format):
    """Return direction-aware Crown quotes for cross-provider matching.

    The legacy ``markets`` payload is kept for the existing display.  Matching
    uses the parser's explicit selection and normalized line instead, so an
    unknown handicap direction or a blocked market cannot be compared by
    accident.
    """
    try:
        quotes = extract_quotes(game, inplay=inplay, odds_format=odds_format)
    except (TypeError, ValueError):
        return []
    result = []
    for quote in quotes:
        if quote.get("status") != "open" or quote.get("hk_odds") is None:
            continue
        if quote.get("market_type") not in {"handicap", "total"}:
            # The match engine can compare 1X2 quotes too, but keep the
            # selection names explicit when they are present.
            if quote.get("market_type") != "moneyline":
                continue
        result.append({
            "market_type": str(quote.get("market_type") or quote.get("type") or ""),
            "period": str(quote.get("period") or "full"),
            "selection": str(quote.get("selection") or ""),
            "line": quote.get("line"),
            "hk_odds": float(quote["hk_odds"]),
            "raw_odds": quote.get("raw_odds"),
            "source": quote.get("source") or {},
        })
    return result


def _normalize_event(game, mode, display_format="HK", raw_format="CROWN_HK", more_markets=None):
    live = mode == "live"
    if live:
        handicap = _market(game.get("RATIO_RE"), game.get("IOR_REH"), game.get("IOR_REC"), raw_format)
        # Crown's ordinary total market exposes under on IOR_*H and over on
        # IOR_*C. Keep the public pair in the same over/under order as
        # Pinnacle so cross-provider matching compares the same selections.
        total = _market(game.get("RATIO_ROUO"), game.get("IOR_ROUC"), game.get("IOR_ROUH"), raw_format)
    else:
        handicap = _market(game.get("RATIO_R"), game.get("IOR_RH"), game.get("IOR_RC"), raw_format)
        total = _market(game.get("RATIO_OUO"), game.get("IOR_OUC"), game.get("IOR_OUH"), raw_format)
    # The account's verified odd_f_type is HK. Three-way prices are individual
    # display values, unlike the paired handicap/total IOR fields.
    moneyline = [_price(game.get("IOR_MH")), _price(game.get("IOR_MN")), _price(game.get("IOR_MC"))]
    if not any(moneyline):
        moneyline = []
    score = " - ".join(item for item in (str(game.get("SCORE_H") or ""), str(game.get("SCORE_C") or "")) if item)
    more_markets = more_markets or {
        "status": "not_requested", "handicap": [], "total": [], "moneyline": [], "rows": [],
    }

    # Attach the provider row identifiers to the ordinary list markets.  The
    # list endpoint has the cleanest display values, while ``game_more`` is
    # where Crown exposes the row/group IDs for the master line.  Matching by
    # normalized line keeps quarter-goal strings such as ``1 / 1.5`` aligned
    # with the more response's decimal ``1.25`` representation.
    def attach_ids(items, extras):
        masters = [item for item in extras if isinstance(item, dict) and item.get("master") == "Y"]
        for item in items:
            if not isinstance(item, dict):
                continue
            item_line = normalize_line(item.get("line"))
            for master in masters:
                master_line = normalize_line(master.get("line"))
                if item_line is None or master_line is None or item_line != master_line:
                    continue
                for key in ("gid", "gidm", "hgid", "market_id", "master"):
                    value = master.get(key)
                    if value and not item.get(key):
                        item[key] = value
                break

    attach_ids(handicap, more_markets.get("handicap") or [])
    attach_ids(total, more_markets.get("total") or [])
    event_id = str(game.get("GID") or game.get("ID") or "")
    for item in handicap + total:
        if not isinstance(item, dict) or not event_id:
            continue
        item.setdefault("gid", event_id)
        item.setdefault("market_id", event_id)
    return {
        "id": str(game.get("GID") or game.get("ID") or ""),
        "league_id": str(game.get("LID") or ""),
        "league": str(game.get("LEAGUE") or "未命名联赛"),
        "is_live": live or str(game.get("IS_RB") or "").upper() == "Y",
        "home": str(game.get("TEAM_H") or ""),
        "away": str(game.get("TEAM_C") or ""),
        "start_label": str(game.get("DATETIME") or ""),
        "score": score,
        "minute": str(game.get("RETIMESET") or ""),
        "period": str(game.get("NOW_MODEL") or "") if live else "",
        "markets": {"handicap": handicap, "total": total, "moneyline": moneyline},
        "extra_markets": {
            "status": more_markets.get("status", "not_requested"),
            "handicap": list(more_markets.get("handicap") or []),
            "total": list(more_markets.get("total") or []),
            "moneyline": list(more_markets.get("moneyline") or []),
            "rows": list(more_markets.get("rows") or []),
        },
        "match_quotes": _match_quotes(game, inplay=live, odds_format=raw_format),
        "odds_format": display_format,
        "odds_format_label": "皇冠港赔（HK）" if display_format == "HK" else f"皇冠赔率（{display_format}）",
    }


def close():
    global store, auth, monitor
    if monitor is not None:
        monitor.close()
    if auth is not None:
        auth.close()
    if store is not None and hasattr(store, "close"):
        store.close()
    auth = None
    monitor = None
    store = None
