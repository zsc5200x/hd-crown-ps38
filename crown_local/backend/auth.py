"""Account-isolated browser login with encrypted persistence supplied by Store.

All Playwright objects live on one worker thread. Passwords are typed into the
site's own form so site scripts create their own device and version parameters.
No captured credentials or fingerprint headers are replayed.
"""
from dataclasses import dataclass, field
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlsplit

from .providers import KINDS, MAX_RESPONSE_BYTES, parse_login_response, parse_member_login_domain
from .crown_live import MAX_RESPONSE_BYTES as CROWN_ADMIN_RESPONSE_BYTES
from .storage import SessionChangedError
from .totp import TOTP_INTERVAL, generate_code


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _origin(url):
    parts = urlsplit(url)
    return parts.scheme.lower(), (parts.hostname or "").lower(), parts.port or (443 if parts.scheme == "https" else 80)


# These values are the member site's own login handoff context, retained only
# inside the encrypted account session. Never expose them in account responses.
_MEMBER_RESUME_KEYS = frozenset({
    "cu", "cuipv6", "ipv6", "uid", "pay_type", "username", "passwd_safe", "mid", "ltype",
    "currency", "odd_f", "domain", "blackBoxStatus", "odd_f_type", "timetype",
    "sw_2fa", "editdate", "four_pwd", "abox4pwd_notshow", "msg", "langx", "iovationCnt",
})
_MEMBER_QUERY_MAX_LENGTH = 16384
_SESSION_BASELINE_UNSET = object()
_CROWN_ADMIN_PROBE_ATTEMPTS = 4


class _SessionSaveError(RuntimeError):
    """A fixed internal category; original storage exceptions stay private."""


class _SessionChanged(RuntimeError):
    """A newer saved browser session won the persistence race."""


@dataclass
class _Job:
    account_id: str
    account: dict
    context: object
    page: object
    deadline: float
    status: str = "logging_in"
    uid: str = field(default="", repr=False)
    evidence: object = None
    next_screenshot: float = 0
    login_attempted: bool = False
    restore: bool = False
    form_pending: bool = False
    member_origin: str = ""
    member_authenticated: bool = False
    session_storage: dict = field(default_factory=dict, repr=False)
    resume_url: str = ""
    background: bool = False
    session_saved: bool = False
    session_baseline: object = field(default_factory=lambda: _SESSION_BASELINE_UNSET, repr=False)
    session_conflict: bool = False
    login_saved: bool = False
    login_recorded: bool = False
    member_protocol: dict = field(default_factory=dict, repr=False)
    member_resume_query: dict = field(default_factory=dict, repr=False)
    member_ui_verified: bool = False
    ui_verified_at: float = 0
    admin_completion_pending: bool = False
    admin_ui_verified_at: float = 0
    admin_ui_url: str = ""
    admin_ui_storage_state: dict = field(default_factory=dict, repr=False)
    totp_submitted_counter: int = -1
    totp_attempts: int = 0
    admin_protocol: dict = field(default_factory=dict, repr=False)
    admin_protocol_headers: dict = field(default_factory=dict, repr=False)
    admin_login_layer: str = ""
    admin_report_attempts: int = 0
    admin_report_retry_at: float = 0
    transient_failures: int = 0
    retry_at: float = 0
    stage: str = "open_page"
    navigation_url: str = field(default="", repr=False)
    keep_open: bool = False
    pinnacle_verified: bool = False
    pinnacle_scope: dict = field(default_factory=dict, repr=False)
    next_session_sync: float = 0


class AuthService:
    def __init__(self, store):
        self.store = store
        self._commands = queue.Queue(maxsize=64)
        self._screenshots = {}
        self._lock = threading.Lock()
        self._jobs = {}
        self._stop = threading.Event()
        self._browser = None
        self._background_browser = None
        self._pinnacle_browser = None
        self._playwright = None
        self._destinations = {}
        self._thread = threading.Thread(target=self._run, name="account-login", daemon=True)
        self._thread.start()

    def _account(self, account_id):
        account = self.store.get_account(account_id)
        if not account:
            raise ValueError("账号不存在。")
        if account.get("kind") not in KINDS:
            raise ValueError("不支持的账号类型。")
        return account

    def _enqueue(self, action, account_id, payload=None):
        with self._lock:
            account = self._account(account_id)
            if action in {"start", "test", "otp"} and not account.get("enabled", True):
                raise ValueError("账号已停用，请先启用。")
            if self._stop.is_set():
                raise ValueError("登录服务已关闭。")
            if action in {"start", "test"}:
                live = self._jobs.get(account_id)
                if live and live.keep_open and self._is_current(live):
                    # All page operations still run on the login worker.
                    try:
                        self._commands.put_nowait(("reuse", account_id, {"check": action == "test"}))
                    except queue.Full:
                        raise ValueError("登录任务繁忙，请稍后重试。") from None
                    return account
                background = action == "test" and bool((payload or {}).get("background"))
                if (account_id in self._jobs or account.get("status") in {"logging_in", "checking", "awaiting_otp", "awaiting_manual"}
                        or account_id in getattr(self, "_pending_checks", set())):
                    raise ValueError("该账号已有登录任务，请先完成或停止。")
                expected = {"expected_revision": account["revision"]} if account.get("revision") is not None else {}
                if background:
                    if not hasattr(self, "_pending_checks"):
                        self._pending_checks = set()
                    self._pending_checks.add(account_id)
                else:
                    self.store.set_state(account_id, "checking" if action == "test" else "logging_in",
                                         "已排队检查会话。" if action == "test" else "登录任务已排队。", **expected)
            try:
                self._commands.put_nowait((action, account_id, payload))
            except queue.Full:
                if action in {"start", "test"}:
                    getattr(self, "_pending_checks", set()).discard(account_id)
                    self._set(account_id, account["status"], account.get("last_error", ""))
                raise ValueError("登录任务繁忙，请稍后重试。") from None
        return self.store.get_account(account_id)

    def start(self, account_id):
        self._account(account_id)
        self._enqueue("start", account_id)
        return self.store.get_account(account_id)

    def submit_otp(self, account_id, code):
        code = str(code).strip()
        if not code.isascii() or not code.isdigit() or not 4 <= len(code) <= 10:
            raise ValueError("请输入 4 至 10 位数字动态码。")
        account = self._account(account_id)
        if account.get("status") != "awaiting_otp":
            raise ValueError("该账号当前没有等待动态验证码的登录任务。")
        return self._enqueue("otp", account_id, code)

    def test(self, account_id, background=False):
        return self._enqueue("test", account_id, {"background": bool(background)})

    def cancel(self, account_id):
        return self._enqueue("cancel", account_id)

    def screenshot(self, account_id):
        self._account(account_id)
        job = self._jobs.get(account_id)
        if job and not self._is_current(job):
            return None
        with self._lock:
            return self._screenshots.get(account_id)

    def browser_open(self, account_id):
        # HTTP handlers only read worker metadata, never Playwright objects.
        job = self._jobs.get(account_id)
        return bool(job and job.keep_open and self._is_current(job))

    def close(self):
        self._stop.set()
        # Browser navigation is bounded; never operate Playwright on this thread.
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=5)

    def _set(self, account_id, status, message, **fields):
        job = self._jobs.get(account_id)
        if job:
            if not self._is_current(job):
                return
            if job.account.get("revision") is not None:
                fields["expected_revision"] = job.account["revision"]
        self.store.set_state(account_id, status, message, **fields)

    def _is_current(self, job):
        try:
            account = self.store.get_account(job.account_id)
        except (KeyError, ValueError):
            return False
        if not account.get("enabled", True):
            return False
        if account.get("revision") != job.account.get("revision"):
            return False
        return all(account.get(key) == job.account.get(key) for key in
                   ("kind", "base_url", "login_path", "username", "login_layer", "source_id"))

    def _session_baseline_current(self, job):
        baseline = getattr(job, "session_baseline", _SESSION_BASELINE_UNSET)
        if baseline is _SESSION_BASELINE_UNSET:
            return True
        try:
            current = self.store.load_session(job.account_id)
        except Exception:
            return False
        if baseline is None or current is None:
            return baseline is current
        expected = dict(baseline)
        expected.setdefault("base_url", job.account.get("base_url", ""))
        current = dict(current)
        current.setdefault("base_url", job.account.get("base_url", ""))
        return current == expected

    def _end_stale_session_job(self, job):
        """Stop stale checks while preserving persistent Pinnacle windows."""
        if job.keep_open and job.account.get("kind") == "pinnacle" and self._is_current(job):
            # A newer saved session must win without destroying the user's
            # visible page. Only an explicit check may adopt its baseline.
            job.session_conflict = True
            job.pinnacle_verified = False
            job.pinnacle_scope = {}
            job.evidence = None
            job.form_pending = False
            job.retry_at = 0
            job.deadline = float("inf")
            job.status = "awaiting_manual"
            self._set(job.account_id, job.status,
                      "保存会话已被另一个任务更新；当前登录窗口保持打开，自动检查与同步已暂停。请点击验证会话重新核验。",
                      last_checked=_now())
            return
        try:
            account = self.store.get_account(job.account_id)
            if (account.get("status") in {"checking", "logging_in"}
                    and self.store.load_session(job.account_id)):
                self._set(job.account_id, "session_received",
                          "保存会话已被另一个任务更新；本次旧页面检查已停止。", last_checked=_now())
        except Exception:
            pass
        self._dispose(job.account_id)

    def _log(self, account_id, action, status, message):
        try:
            account = self.store.get_account(account_id)
            self.store.add_log(account.get("kind", ""), account_id, action, status, message)
        except Exception:
            pass  # A deleted account must not terminate other browser sessions.

    def _launch(self, background=False, headed=False):
        browser = (getattr(self, "_pinnacle_browser", None) if headed else
                   getattr(self, "_background_browser", None) if background else self._browser)
        if browser and browser.is_connected():
            return browser
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError("缺少 Playwright，请安装项目依赖和 Chromium 后重试。") from None
        if self._playwright is None:
            self._playwright = sync_playwright().start()
        headless_setting = os.environ.get("HD_REPORT_HEADLESS", "").strip().lower()
        headless = headless_setting not in {"0", "false", "no"} if headless_setting else sys.platform != "darwin"
        options = {"headless": False if headed else True if background else headless, "timeout": 30000}
        executable = os.environ.get("HD_REPORT_BROWSER_EXECUTABLE", "")
        if executable:
            options["executable_path"] = executable
        elif not Path(self._playwright.chromium.executable_path).exists() and Path("/usr/bin/chromium").exists():
            options["executable_path"] = "/usr/bin/chromium"
        try:
            browser = self._playwright.chromium.launch(**options)
            if headed:
                self._pinnacle_browser = browser
            elif background:
                self._background_browser = browser
            else:
                self._browser = browser
            return browser
        except Exception:
            raise RuntimeError("浏览器启动失败，请运行 python -m playwright install chromium，或检查浏览器配置。") from None

    def _run(self):
        try:
            while not self._stop.is_set():
                try:
                    action, account_id, payload = self._commands.get(timeout=0.08)
                except queue.Empty:
                    action = None
                if action:
                    try:
                        if action == "cancel":
                            self._set(account_id, "idle", "已停止本次登录；已保存的会话保持不变。")
                            self._dispose(account_id)
                        elif action == "otp":
                            self._otp(account_id, payload)
                        elif action == "reuse":
                            self._reuse_pinnacle(account_id, check=bool((payload or {}).get("check")))
                        else:
                            self._begin(account_id, restore=action == "test",
                                        background=bool((payload or {}).get("background")))
                    except Exception as exc:
                        # Never log exception text: browser errors may embed URLs,
                        # post bodies, inline DOM content, or sensitive identifiers.
                        self._diagnostic(account_id, "command", exc)
                        self._fail(account_id, "登录操作未完成，请检查浏览器、域名和页面提示。")
                    finally:
                        getattr(self, "_pending_checks", set()).discard(account_id)
                        payload = None
                self._tick()
        finally:
            for account_id in list(self._jobs):
                self._dispose(account_id)
            for browser in (self._browser, self._background_browser, getattr(self, "_pinnacle_browser", None)):
                if not browser:
                    continue
                try:
                    browser.close()
                except Exception:
                    pass
            if self._playwright:
                try:
                    self._playwright.stop()
                except Exception:
                    pass

    def _begin(self, account_id, restore=False, background=False):
        account = self._account(account_id)
        keep_open = account["kind"] == "pinnacle"
        if keep_open:
            background = False
        if not account.get("enabled", True):
            return
        if len(self._jobs) >= 8 and account_id not in self._jobs:
            raise RuntimeError("同时打开的登录浏览器已达 8 个，请先停止其他任务。")
        self._dispose(account_id)
        if not background:
            self._set(account_id, "checking" if restore else "logging_in", "正在检查会话。" if restore else "正在打开正常登录页面。")
        existing_session = self.store.load_session(account_id)
        stored = existing_session if restore else None
        if restore and not stored:
            self._set(account_id, "idle", "尚无已保存会话，请先登录。")
            return
        base_url = account["base_url"].rstrip("/") + "/"
        # A restored Pinnacle session enters the known report page directly;
        # its login route may keep showing a form even with valid cookies.
        entry_path = ("/partner-ui-v2/report-winloss-detail-v2" if keep_open and restore
                      else account.get("login_path") or "/")
        login_url = urljoin(base_url, entry_path)
        if _origin(base_url) != _origin(login_url):
            raise RuntimeError("登录路径必须属于账号配置的同一来源。")
        if stored and stored.get("auth_origin") != list(_origin(base_url)):
            self._set(account_id, "expired", "域名已变化，旧会话不能移交新来源，请重新登录。")
            return
        check_account = {**account, "status": "checking"} if not background else account
        if stored and account["kind"] == "crown_odds" and self._check_saved_member(check_account, stored):
            return
        browser = self._launch(background=background, headed=True) if keep_open else self._launch(background=background)
        context_args = {"viewport": {"width": 1100, "height": 800}, "ignore_https_errors": False}
        if stored:
            context_args["storage_state"] = stored.get("storage_state", {"cookies": [], "origins": []})
        context = browser.new_context(**context_args)
        context.set_default_timeout(2500)
        context.set_default_navigation_timeout(8000 if background else 30000)
        context.route("**/*", lambda route: self._guard_destination(route, self._jobs.get(account_id)))
        page = context.new_page()
        job = _Job(account_id, account, context, page, time.monotonic() + (35 if background else 300),
                   status="checking" if restore else "logging_in", uid=(stored or {}).get("uid", ""), restore=restore,
                   background=background, session_saved=bool(existing_session), session_baseline=existing_session,
                   keep_open=keep_open)
        self._jobs[account_id] = job
        if keep_open:
            page.bring_to_front()
        if stored:
            self._restore_member(job, stored)
            if (account["kind"] == "crown_odds"
                    and ((stored.get("member_origin") and not job.member_origin)
                         or not job.resume_url or not self._member_query_origin(job, job.resume_url)
                         or not self._member_query(job, job.member_resume_query))):
                self._set(account_id, "session_received", "旧会话缺少完整恢复信息，已保留原会话；请重新登录一次以补齐。", last_checked=_now())
                self._dispose(account_id)
                return
            login_url = self._member_resume_url(job) or login_url
        job.navigation_url = login_url
        # Login responses may be issued by frames or popups, so observe context.
        page.on("framenavigated", lambda frame: self._observe_member_navigation(job, frame))
        context.on("request", lambda request: self._observe_member_request(job, request))
        context.on("response", lambda response: self._response(job, response))
        for attempt in range(3):
            try:
                page.goto(login_url, wait_until="domcontentloaded")
                page.wait_for_timeout(400)
                break
            except Exception as exc:
                self._diagnostic(account_id, "open_page", exc)
                if not self._transient_exception(exc) or attempt == 2:
                    raise
                page.wait_for_timeout(150 * (attempt + 1))
        if not self._trusted_page(job, page.url):
            if background:
                self._fail(account_id, "后台会话检查尚未确认页面来源；已保留会话，稍后重试。")
                return
            self._set(account_id, "awaiting_manual", "页面跳转到了其他来源；未填入凭据，请确认并修改账号域名。")
            job.status = "awaiting_manual"
            return
        if restore:
            job.stage = "check_page"
            try:
                self._check_page(job)
            except Exception as exc:
                if not self._defer_transient(job, exc):
                    raise
            return
        job.stage = "login_form"
        try:
            self._fill_login(job)
        except Exception as exc:
            if not self._defer_transient(job, exc):
                raise

    def _check_saved_member(self, account, stored):
        """Verify a complete member snapshot without replaying its login handoff."""
        from .crown_member import CrownMemberClient, MemberNeedsBrowser, MemberExpired, MemberUnavailable
        try:
            client = CrownMemberClient(account, stored)
            client.heartbeat()
            updates = client.session_updates()
            status, message = "authenticated", "水位会员在线检查通过，会话已复用；盘口权限需单独验证。"
        except MemberNeedsBrowser:
            return False
        except MemberExpired:
            updates = None
            status, message = "expired", "会员在线接口确认当前会话失效；原会话保留，请重新登录。"
        except MemberUnavailable:
            updates = None
            status, message = "session_received", "水位在线检查暂时失败，已保留会话，稍后重试。"
        # The helper and main service use separate Store instances/processes.
        # Compare the snapshot and apply its result in one database transaction.
        self.store.apply_session_check(
            account["id"], stored, status, message, updates=updates,
            expected_revision=account.get("revision"), expected_status=account.get("status"),
            last_checked=_now())
        return True

    @staticmethod
    def _trusted_page(job, url):
        return (_origin(url) == _origin(job.account["base_url"])
                or (job.account["kind"] == "crown_odds" and job.member_origin
                    and _origin(url) == _origin(job.member_origin)))

    @staticmethod
    def _member_query(job, values, uid=None):
        """Accept one bounded, account-bound bootstrap query from a saved page."""
        uid = job.uid if uid is None else uid
        if not uid or not isinstance(values, dict) or len(values) > len(_MEMBER_RESUME_KEYS):
            return {}
        if values.get("uid") != uid:
            return {}
        accepted = {}
        for key, value in values.items():
            if (key not in _MEMBER_RESUME_KEYS or not isinstance(value, str)
                    or len(value) > 2048 or any(ord(char) < 32 or ord(char) == 127 for char in value)):
                return {}
            if key == "passwd_safe" and value != job.account.get("username"):
                return {}
            accepted[key] = value
        if len(urlencode(accepted)) > _MEMBER_QUERY_MAX_LENGTH:
            return {}
        return accepted

    def _capture_member_resume(self, job, url):
        """Retain the normal login bootstrap before its scripts clear the URL."""
        evidence_uid = (getattr(job.evidence, "uid", "")
                        if getattr(job.evidence, "status", "") == "session_received" else "")
        uid = job.uid or evidence_uid
        if (not self._member_query_origin(job, url)
                or not (job.member_authenticated or evidence_uid) or not uid):
            return
        try:
            parts = urlsplit(url)
            if len(parts.query) > _MEMBER_QUERY_MAX_LENGTH:
                return
            pairs = parse_qsl(parts.query, keep_blank_values=True, max_num_fields=64)
            query = {}
            for key, value in pairs:
                if key not in _MEMBER_RESUME_KEYS:
                    continue
                # This observed parameter is an account identifier only when it
                # exactly matches the configured username. Never retain a secret.
                if key == "passwd_safe" and value != job.account.get("username"):
                    continue
                if key in query:
                    return
                query[key] = value
            accepted = self._member_query(job, query, uid=uid)
            if accepted:
                job.member_resume_query = accepted
                job.resume_url = parts._replace(query="", fragment="").geturl()
            elif query.get("uid") and query["uid"] != uid:
                # Never apply one session's bootstrap to a subsequently changed UID.
                job.member_resume_query = {}
        except (TypeError, ValueError):
            return

    def _observe_member_navigation(self, job, frame):
        try:
            if (self._jobs.get(job.account_id) is job and self._is_current(job)
                    and job.account["kind"] == "crown_odds" and frame == job.page.main_frame):
                self._capture_member_resume(job, frame.url)
        except Exception:
            # A replaced frame or malformed location cannot change saved state.
            return

    @staticmethod
    def _member_query_origin(job, url):
        """Only the account's verified member origin may receive bootstrap state."""
        try:
            parts = urlsplit(url)
            return (bool(parts.scheme in {"http", "https"} and parts.hostname)
                    and not parts.username and not parts.password
                    and _origin(url) == _origin(job.member_origin or job.account["base_url"]))
        except (TypeError, ValueError):
            return False

    def _member_resume_url(self, job):
        """Construct a private navigation target; the persisted URL stays clean."""
        resume = job.resume_url
        if not resume or not self._trusted_page(job, resume):
            return ""
        parts = urlsplit(resume)
        if parts.username or parts.password:
            return ""
        query = self._member_query(job, job.member_resume_query)
        if query and self._member_query_origin(job, resume):
            return parts._replace(query=urlencode(query), fragment="").geturl()
        return parts._replace(query="", fragment="").geturl()

    def _restore_member(self, job, stored):
        if job.account["kind"] != "crown_odds":
            return
        job.member_resume_query = {}
        job.member_origin = ""
        job.resume_url = ""
        # Only records saved by this account at its unchanged entry origin may
        # supply a member destination. Recheck DNS before opening that page.
        from .storage import normalize_base_url, validate_target_url
        candidate = stored.get("member_origin", "")
        if candidate:
            try:
                candidate = normalize_base_url(candidate)
            except ValueError:
                candidate = ""
            if candidate and validate_target_url(candidate):
                job.member_origin = candidate
        job.member_authenticated = bool(job.uid and stored.get("member_authenticated", True))
        protocol = stored.get("member_protocol", {})
        job.member_protocol = {}
        if isinstance(protocol, dict):
            origin = protocol.get("origin", "")
            ver = self._protocol_value(protocol.get("ver"), r"[A-Za-z0-9._-]+", 96)
            langx = self._protocol_value(protocol.get("langx"), r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", 16)
            try:
                parts = urlsplit(origin)
                canonical = f"{parts.scheme}://{parts.netloc}" if not parts.path and not parts.query and not parts.fragment else ""
                if (canonical and not parts.username and not parts.password
                        and self._trusted_page(job, canonical) and ver and langx):
                    job.member_protocol = {"origin": canonical, "ver": ver, "langx": langx}
            except Exception:
                job.member_protocol = {}
        job.member_ui_verified = bool(stored.get("member_ui_verified", False))
        resume = stored.get("resume_url", "")
        if resume and self._trusted_page(job, resume):
            parts = urlsplit(resume)
            if not parts.username and not parts.password:
                job.resume_url = parts._replace(query="", fragment="").geturl()
                if self._member_query_origin(job, job.resume_url):
                    job.member_resume_query = self._member_query(job, stored.get("member_resume_query", {}))
        sessions = stored.get("session_storage", {})
        if isinstance(sessions, dict):
            job.session_storage = {origin: deepcopy(values) for origin, values in sessions.items()
                                   if self._trusted_page(job, origin) and isinstance(values, dict)}
        if job.session_storage:
            data = json.dumps(job.session_storage, ensure_ascii=True)
            job.context.add_init_script("""(() => {
                const states = """ + data + """;
                const values = states[location.origin];
                if (values) for (const [key, value] of Object.entries(values)) {
                    if (typeof value === 'string' && sessionStorage.getItem(key) === null) {
                        sessionStorage.setItem(key, value);
                    }
                }
            })();""")

    def _guard_destination(self, route, job=None):
        """Do not let configured sites reach localhost/private network services."""
        try:
            from .storage import validate_target_url
            target = _origin(route.request.url)
            cached = self._destinations.get(target)
            if cached is None or time.monotonic() - cached[0] > 30:
                allowed = validate_target_url(route.request.url)
                self._destinations[target] = (time.monotonic(), allowed)
            else:
                allowed = cached[1]
            if allowed:
                if not job or not self._capture_member_request(job, route):
                    route.continue_()
            else:
                route.abort("blockedbyclient")
        except Exception:
            route.abort("blockedbyclient")

    def _capture_member_request(self, job, route):
        """Buffer the site's own member auth response before its JS navigates.

        A response event can arrive too late to read Chromium's response body
        after a cross-origin navigation. Send the intercepted request once,
        observe its result, then deliver that same result to the page.
        """
        request = route.request
        data = request.post_data or ""
        if (job.account["kind"] != "crown_odds" or not self._is_current(job)
                or _origin(request.url) != _origin(job.account["base_url"])
                or request.method != "POST" or urlsplit(request.url).path != "/transform_nl.php"
                or parse_qs(data, keep_blank_values=True).get("p") not in
                (["chk_login"], ["check_login_domain"])):
            return False
        upstream = route.fetch(max_redirects=0, timeout=30000)
        try:
            body = upstream.body()
            if len(body) <= MAX_RESPONSE_BYTES:
                self._accept_member_response(job, request.url, upstream.status,
                                             body.decode("utf-8", errors="replace"), data)
            # APIRequestContext decodes compression. Remove transport headers
            # when delivering those bytes so the browser does not decode twice.
            headers = {key: value for key, value in upstream.headers.items()
                       if key.lower() not in {"content-encoding", "content-length", "transfer-encoding"}}
            route.fulfill(status=upstream.status, headers=headers, body=body)
        finally:
            upstream.dispose()
        return True

    @staticmethod
    def _protocol_value(value, pattern, limit):
        value = str(value or "").strip()
        return value if len(value) <= limit and re.fullmatch(pattern, value, flags=re.ASCII) else ""

    def _observe_member_request(self, job, request):
        """Remember only the site's ordinary authenticated mem_online request."""
        try:
            if job.account.get("kind") == "crown_admin":
                self._observe_crown_admin_request(job, request)
                return
            if (job.account.get("kind") != "crown_odds" or not self._is_current(job)
                    or request.method != "POST" or urlsplit(request.url).path != "/transform.php"):
                return
            source = _origin(request.url)
            if not (source == _origin(job.account["base_url"])
                    or (job.member_origin and source == _origin(job.member_origin))):
                return
            body = parse_qs(request.post_data or "", keep_blank_values=True, strict_parsing=False)
            if body.get("p") != ["mem_online"]:
                return
            uid_values = body.get("uid", []) + body.get("UID", [])
            if len(uid_values) != 1:
                return
            uid = uid_values[0]
            evidence_uid = (getattr(job.evidence, "uid", "")
                            if getattr(job.evidence, "status", "") == "session_received" else "")
            if not uid or uid not in {job.uid, evidence_uid}:
                return
            query = parse_qs(urlsplit(request.url).query, keep_blank_values=True)
            ver_candidates = query.get("ver", []) + body.get("ver", [])
            lang_candidates = query.get("langx", []) + body.get("langx", [])
            if (not ver_candidates or not lang_candidates
                    or any(not self._protocol_value(value, r"[A-Za-z0-9._-]+", 96) for value in ver_candidates)
                    or any(not self._protocol_value(value, r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", 16) for value in lang_candidates)):
                return
            ver_values = {self._protocol_value(value, r"[A-Za-z0-9._-]+", 96) for value in ver_candidates}
            lang_values = {self._protocol_value(value, r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", 16) for value in lang_candidates}
            ver_values.discard("")
            lang_values.discard("")
            if len(ver_values) != 1 or len(lang_values) != 1:
                return
            hostname = f"[{source[1]}]" if ":" in source[1] else source[1]
            default_port = 443 if source[0] == "https" else 80
            job.member_protocol = {"origin": f"{source[0]}://{hostname}" +
                                   (f":{source[2]}" if source[2] != default_port else ""),
                                   "ver": next(iter(ver_values)), "langx": next(iter(lang_values))}
        except Exception as exc:
            self._diagnostic(job.account_id, "member_request", exc)

    def _observe_crown_admin_request(self, job, request):
        """Capture protocol fields from the site's own authenticated wager poll."""
        if (not self._is_current(job) or request.method != "POST"
                or urlsplit(request.url).path != "/transform.php"
                or _origin(request.url) != _origin(job.account["base_url"])):
            return
        query = parse_qs(urlsplit(request.url).query, keep_blank_values=True)
        body = parse_qs(request.post_data or "", keep_blank_values=True, strict_parsing=False)
        def one_value(key):
            values = query.get(key, []) + body.get(key, [])
            valid = {value.strip() for value in values if isinstance(value, str) and value.strip()}
            return next(iter(valid)) if len(valid) == 1 else ""

        if body.get("p") == ["login_chk"]:
            values = {value for value in (one_value("login_layer"), one_value("layer")) if value}
            if len(values) == 1 and next(iter(values)) in ("co", "su", "ag"):
                job.admin_login_layer = next(iter(values))
            return
        if (body.get("p") != ["get_wmc_list_bet"]
                or not (job.admin_completion_pending or job.restore)):
            return

        uid = one_value("uid")
        ver = one_value("ver")
        langx = one_value("langx")
        layer = one_value("login_layer")
        if (not uid or len(uid) > 2048
                or any(ord(char) < 32 or ord(char) == 127 for char in uid)):
            return
        ver = self._protocol_value(ver, r"[A-Za-z0-9._-]+", 96)
        langx = self._protocol_value(langx, r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", 16)
        layer = layer or job.admin_login_layer or job.account.get("login_layer")
        if not ver or not langx or layer not in ("co", "su", "ag"):
            return
        job.uid = uid
        source = _origin(request.url)
        hostname = f"[{source[1]}]" if ":" in source[1] else source[1]
        default_port = 443 if source[0] == "https" else 80
        job.admin_protocol = {
            "origin": f"{source[0]}://{hostname}" + (f":{source[2]}" if source[2] != default_port else ""),
            "ver": ver,
            "langx": langx,
            "login_layer": layer,
        }
        try:
            headers = request.all_headers()
        except Exception:
            headers = {}
        allowed_headers = {
            "user-agent", "accept", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
            "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "x-requested-with",
        }
        job.admin_protocol_headers = {
            key.lower(): value[:512]
            for key, value in headers.items()
            if isinstance(key, str) and key.lower() in allowed_headers
            and isinstance(value, str) and value and len(value) <= 512
            and not any(ord(char) < 32 or ord(char) == 127 for char in value)
        }
        observed_user_agent = job.admin_protocol_headers.get("user-agent")
        if observed_user_agent:
            job.admin_protocol["user_agent"] = observed_user_agent

    def _capture_crown_admin_protocol(self, job):
        if job.admin_protocol.get("ver") and job.admin_protocol.get("login_layer") in ("co", "su", "ag"):
            return
        for frame in job.page.frames:
            if not self._trusted_page(job, frame.url):
                continue
            try:
                values = frame.evaluate("""() => {
                    const root = window.top;
                    return {
                        ver: root.ver || '',
                        langx: root.langx || document.documentElement.lang || '',
                        uid: root.uid || root.UID || '',
                        loginLayer: root.login_layer || root.layer || '',
                        userAgent: navigator.userAgent || '',
                    };
                }""")
            except Exception:
                continue
            if not isinstance(values, dict):
                continue
            uid = values.get("uid")
            if (isinstance(uid, str) and uid and len(uid) <= 2048
                    and not any(ord(char) < 32 or ord(char) == 127 for char in uid)):
                job.uid = uid
            ver = self._protocol_value(values.get("ver"), r"[A-Za-z0-9._-]+", 96)
            langx = self._protocol_value(values.get("langx"), r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?", 16)
            page_layer = values.get("loginLayer")
            layer = (job.admin_login_layer
                     or (page_layer if page_layer in ("co", "su", "ag") else "")
                     or job.account.get("login_layer"))
            if not ver or not langx or layer not in ("co", "su", "ag"):
                continue
            source = _origin(frame.url)
            hostname = f"[{source[1]}]" if ":" in source[1] else source[1]
            default_port = 443 if source[0] == "https" else 80
            job.admin_protocol = {
                "origin": f"{source[0]}://{hostname}" + (f":{source[2]}" if source[2] != default_port else ""),
                "ver": ver,
                "langx": langx,
                "login_layer": layer,
                "user_agent": str(values.get("userAgent", ""))[:512],
            }
            user_agent = job.admin_protocol["user_agent"]
            if user_agent and not any(ord(char) < 32 or ord(char) == 127 for char in user_agent):
                job.admin_protocol_headers = {"user-agent": user_agent}
            return

    def _probe_crown_admin_report(self, job):
        """Verify read-only wager access inside the browser context that logged in."""
        protocol = job.admin_protocol
        if (not job.uid or not protocol.get("ver")
                or protocol.get("login_layer") not in ("co", "su", "ag")
                or _origin(protocol.get("origin", "")) != _origin(job.account["base_url"])):
            return False, ""
        try:
            result = job.page.evaluate("""async ({ver, uid, layer, langx, maxBytes}) => {
                const query = new URLSearchParams({ver});
                const form = new URLSearchParams({
                    p: 'get_wmc_list_bet', uid, login_layer: layer,
                    langx, ver, totalBets: 'wmc', gtype: 'ALL', sel_maxid: '0'
                });
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), 8000);
                try {
                    const response = await fetch('/transform.php?' + query.toString(), {
                        method: 'POST', credentials: 'same-origin', signal: controller.signal,
                        headers: {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
                        body: form.toString()
                    });
                    const reader = response.body.getReader();
                    const chunks = [];
                    let size = 0;
                    while (true) {
                        const {done, value} = await reader.read();
                        if (done) break;
                        size += value.byteLength;
                        if (size > maxBytes) {
                            await reader.cancel();
                            return {httpOk: response.ok, oversized: true};
                        }
                        chunks.push(value);
                    }
                    const bytes = new Uint8Array(size);
                    let offset = 0;
                    for (const chunk of chunks) {
                        bytes.set(chunk, offset);
                        offset += chunk.byteLength;
                    }
                    const text = new TextDecoder().decode(bytes);
                    const body = JSON.parse(text);
                    if (!body || typeof body !== 'object' || Array.isArray(body))
                        return {httpOk: response.ok};
                    return {
                        httpOk: response.ok,
                        code: ['4X014', '4X013', 'm_rep'].includes(String(body.code || ''))
                            ? String(body.code) : '',
                        rejected: body.status === false || ['error', 'fail', 'failed', 'false']
                            .includes(String(body.status || '').trim().toLowerCase()),
                        hasMaxId: Object.prototype.hasOwnProperty.call(body, 'maxid'),
                        hasWagers: Array.isArray(body.wagers)
                    };
                } catch (_) {
                    return {httpOk: false};
                } finally {
                    clearTimeout(timer);
                }
            }""", {
                "ver": protocol["ver"], "uid": job.uid,
                "layer": protocol["login_layer"],
                "langx": protocol.get("langx") or "zh-cn",
                "maxBytes": CROWN_ADMIN_RESPONSE_BYTES,
            })
        except Exception as exc:
            self._diagnostic(job.account_id, "admin_report_probe", exc)
            return False, ""
        if not isinstance(result, dict):
            return False, ""
        code = str(result.get("code", ""))
        ok = (result.get("httpOk") is True and not result.get("oversized") and not result.get("rejected")
              and code not in {"4X014", "4X013", "m_rep"}
              and result.get("hasMaxId") is True and result.get("hasWagers") is True)
        return ok, code

    def _accept_member_response(self, job, url, status, body, data):
        if self._jobs.get(job.account_id) is not job or not self._is_current(job):
            return
        if parse_qs(data, keep_blank_values=True).get("p") == ["check_login_domain"]:
            candidate = parse_member_login_domain(url, status, body, data)
            from .storage import validate_target_url
            if candidate and validate_target_url(candidate):
                job.member_origin = candidate
        else:
            evidence = parse_login_response("crown_odds", url, status, body, data)
            if evidence:
                job.evidence = evidence

    @staticmethod
    def _visible_locator(frame, selectors):
        for selector in selectors:
            try:
                locator = frame.locator(selector)
                for index in range(min(locator.count(), 6)):
                    item = locator.nth(index)
                    if item.is_visible() and item.is_enabled():
                        return item
            except Exception:
                continue
        return None

    def _fill_login(self, job):
        if not self._is_current(job):
            return
        job.form_pending = False
        credentials = self.store.get_credentials(job.account_id)
        password = credentials.get("password", "")
        if not password:
            raise RuntimeError("尚未保存登录密码，请先编辑账号。")
        username_selectors = [
            'input[name="username"]', '#username', 'input[name="user"]', '#user',
            'input[name="usr"]', '#usr',
            'input[name="account"]', '#account', 'input[autocomplete="username"]',
            'input[name="loginName"]', '#loginName', 'input[name="loginname"]',
        ]
        if job.account["kind"] == "pinnacle":
            username_selectors.extend([
                'input[placeholder*="用户名"]', 'input[placeholder*="用戶名"]',
                'input[placeholder*="账号"]', 'input[placeholder*="Username" i]',
                'input[aria-label*="用户名"]', 'input[aria-label*="Username" i]',
            ])
        password_selectors = [
            'input[name="password"]', 'input[name="pwd"]', '#password', '#pwd',
            'input[type="password"]:not([name="safe"]):not([name="pwd_safe"])',
        ]
        for frame in job.page.frames:
            if _origin(frame.url) != _origin(job.account["base_url"]):
                continue
            # Empty legacy/API selections follow the normal page. Explicit
            # crown choices map 登入1/2/3 to the site's A/B/C controls.
            value = job.account.get("login_layer", "")
            if job.account["kind"] == "crown_admin" and value:
                layer = self._visible_locator(frame, ['select[name="login_layer"]', 'select[name="selLayer"]', '#selLayer'])
                layer_name = {"co": "A", "su": "B", "ag": "C"}[value]
                if layer is None:
                    layer_button = self._visible_locator(frame, [f'#btn_{layer_name}', f'[name="btn_{layer_name}"]'])
                    if layer_button is None:
                        if self._visible_locator(frame, username_selectors) is not None:
                            self._manual(job, "未识别皇冠后台登入方式，请在本机浏览器确认后完成登录。")
                            return
                        continue
                    layer_button.click()
                else:
                    selected = False
                    for option in (value, layer_name):
                        try:
                            layer.select_option(value=option, timeout=700)
                            selected = True
                            break
                        except Exception:
                            continue
                    if not selected:
                        self._manual(job, "页面登录层级与配置不匹配，请在浏览器选择后完成登录。")
                        return
            # Layer selection may replace the form or reveal another panel.
            # Resolve the currently visible fields after the site's own switch.
            username = self._visible_locator(frame, username_selectors)
            if username is None and job.account["kind"] == "crown_odds":
                for label in ("登入账号", "登录账号", "用户名", "账号"):
                    try:
                        labeled = frame.get_by_label(label, exact=False)
                        for index in range(min(labeled.count(), 3)):
                            candidate = labeled.nth(index)
                            if candidate.is_visible() and candidate.is_enabled():
                                username = candidate
                                break
                    except Exception:
                        continue
                    if username is not None:
                        break
            if username is None and job.account["kind"] == "crown_odds":
                username = self._visible_locator(frame, [
                    'input[placeholder*="账号"]', 'input[placeholder*="用户名"]',
                    'input[aria-label*="账号"]', 'input[aria-label*="用户名"]',
                ])
                if username is None:
                    candidates = []
                    inputs = frame.locator(
                        'input:visible:not([type="hidden"]):not([type="password"]):not([type="submit"]):not([type="button"]):not([type="checkbox"]):not([type="radio"])'
                    )
                    for index in range(inputs.count()):
                        item = inputs.nth(index)
                        if not item.is_enabled():
                            continue
                        identity = " ".join((
                            item.get_attribute("name") or "",
                            item.get_attribute("id") or "",
                            item.get_attribute("placeholder") or "",
                            item.get_attribute("aria-label") or "",
                        )).lower()
                        if any(token in identity for token in ("captcha", "verify", "otp", "code", "safe")):
                            continue
                        candidates.append(item)
                    if len(candidates) == 1:
                        username = candidates[0]
            password_input = self._visible_locator(frame, password_selectors)
            if username is None or password_input is None:
                continue
            username.fill(job.account["username"])
            password_input.fill(password)
            # 登入3 has no safety field, including after editing an account
            # which still has an encrypted safety password from 登入1/2.
            needs_safe = not (job.account["kind"] == "crown_admin" and value == "ag")
            if needs_safe and credentials.get("safe_password"):
                safe_input = self._visible_locator(frame, ['input[name="safe"]', '#safe', '#pwd_safe', 'input[name="pwd_safe"]', 'input[name="safe_password"]'])
                if safe_input is None:
                    self._manual(job, "已配置安全密码，但页面未找到对应输入框，请在本机浏览器确认。")
                    return
                safe_input.fill(credentials["safe_password"])
            # Do not submit over image/CAPTCHA challenges or hidden iframes. OTP
            # follows account authentication and is handled by its own form.
            captcha = self._visible_locator(frame, [
                'input[name*="captcha" i]', 'input[id*="captcha" i]',
                'iframe[src*="recaptcha"]', 'iframe[src*="hcaptcha"]',
                'input[name="checkCode"]', 'input[name="verifyCode"]',
            ])
            if captcha is not None:
                self._manual(job, "页面需要验证码，请在本机打开的浏览器完成；云端可查看截图。")
                return
            submit = self._visible_locator(frame, [
                'button[type="submit"]', 'input[type="submit"]', '#login_btn', '#btn_login', '#login', '#btnLogin',
                '[name="login"]', '#loginBtn', 'button:has-text("登录")',
                'button:has-text("登入")', 'button:has-text("Login")',
                '[role="button"]:text-is("登入")', '[role="button"]:text-is("登录")',
                '[role="button"]:text-is("Login")', ':text-is("登入")',
                'a:has-text("登录")', 'a:has-text("登入")',
            ])
            job.login_attempted = True
            if submit is not None:
                submit.click()
            else:
                password_input.press("Enter")
            self._log(job.account_id, "login", "logging_in", "已通过站点正常页面提交登录表单。")
            return
        job.form_pending = True
        job.status = "logging_in"
        self._set(job.account_id, "logging_in", "正在识别登录表单，暂未检测到验证步骤。")

    def _manual(self, job, message):
        job.status = "awaiting_manual"
        self._set(job.account_id, job.status, message)

    def _response(self, job, response):
        try:
            if self._jobs.get(job.account_id) is not job or not self._is_current(job) or _origin(response.url) != _origin(job.account["base_url"]):
                return
            request = response.request
            from .providers import is_login_response
            request_data = request.post_data or ""
            if request.method != "POST":
                return
            length = response.header_value("content-length")
            if length and length.isdigit() and int(length) > MAX_RESPONSE_BYTES:
                return
            if job.account["kind"] == "crown_odds":
                if (urlsplit(response.url).path == "/transform_nl.php"
                        and parse_qs(request_data).get("p") == ["check_login_domain"]):
                    self._accept_member_response(job, response.url, response.status,
                                                 response.text(), request_data)
                    return
            if not is_login_response(job.account["kind"], response.url, request_data):
                return
            evidence = parse_login_response(job.account["kind"], response.url,
                                            response.status, response.text(), request_data)
            if evidence:
                if job.account["kind"] == "crown_admin" and evidence.status == "authenticated":
                    self._observe_crown_admin_request(job, request)
                job.evidence = evidence
        except Exception:
            # Network failures are shown as timeouts/manual checks, never expose
            # response bodies or request values in exception/log output.
            return

    def _probe_pinnacle_session(self, job):
        """Confirm identity and read-only order access with this page's cookies."""
        job.pinnacle_verified = False
        job.pinnacle_scope = {}
        if not self._is_current(job) or not self._trusted_page(job, job.page.url):
            return False
        from .storage import validate_target_url
        base = job.account["base_url"].rstrip("/")
        headers = {"Accept": "application/json, text/plain, */*", "Origin": base,
                   "Referer": base + "/partner-ui-v2/report-winloss-detail-v2"}

        def read_body(response):
            try:
                if not 200 <= response.status < 300:
                    return None
                if len(response.body()) > MAX_RESPONSE_BYTES:
                    return None
                value = response.json()
                if not isinstance(value, dict):
                    return None
                from scripts.poll_pinnacle_orders import _session_failure_marker
                if _session_failure_marker(value):
                    return None
                code = value.get("statusCodeValue", 200)
                if isinstance(code, bool) or not 200 <= int(code) < 300:
                    return None
                value = value.get("body", value)
                return value if isinstance(value, dict) else None
            finally:
                response.dispose()

        try:
            profile_url = base + "/partner-account/ddd/user-info/login-user/profile"
            orders_url = base + "/partner-reporting/ddd/common/bet-list/v2"
            if not validate_target_url(profile_url) or not validate_target_url(orders_url):
                return False
            body = read_body(job.context.request.get(
                profile_url, headers=headers, timeout=8000, max_redirects=0))
            profile = (body or {}).get("profile")
            if not isinstance(profile, dict):
                return False
            user_id, user_code = profile.get("userId"), profile.get("userCode")
            # loginId identifies the configured sign-in; userCode is the
            # business scope used by the order API and may be different.
            login_id = profile.get("loginId", user_code)
            if (isinstance(user_id, bool) or not str(user_id).isdigit() or int(user_id) <= 0
                    or not isinstance(user_code, str) or not user_code.strip()
                    or not isinstance(login_id, str) or not login_id.strip()
                    or login_id.strip().casefold() != job.account["username"].strip().casefold()):
                return False
            user_code, login_id = user_code.strip(), login_id.strip()
            payload = {"betListType": "DETAIL", "ownerUserId": str(user_id),
                       "playerUserId": int(user_id), "selectedUserCode": user_code,
                       "product": "SB", "status": "OPEN",
                       "paging": {"pageNumber": 1, "rowsPerPage": 1}}
            orders = read_body(job.context.request.post(
                orders_url, headers=headers, data=payload, timeout=8000, max_redirects=0))
            if (not isinstance(orders, dict) or not isinstance(orders.get("data"), list)
                    or not isinstance(orders.get("paging"), dict)):
                return False
            total = orders["paging"].get("totalItems")
            if isinstance(total, bool) or not str(total).isdigit():
                return False
            job.pinnacle_scope = {"user_id": int(user_id), "user_code": user_code,
                                  "login_id": login_id}
            job.pinnacle_verified = True
            return True
        except Exception as exc:
            self._diagnostic(job.account_id, "check_page", exc)
            return False

    def _check_pinnacle_page(self, job):
        if job.session_conflict:
            return
        if not self._probe_pinnacle_session(job):
            job.status = "awaiting_manual"
            self._set(job.account_id, job.status,
                      "登录浏览器保持打开；请完成登录或验证。尚未确认同一会话可以读取订单。", last_checked=_now())
            return
        job.status = "authenticated"
        job.form_pending = False
        job.login_saved = not job.restore
        if not self._persist_verified(job):
            return
        job.next_session_sync = time.monotonic() + 5
        self._set(job.account_id, "authenticated",
                  "有头浏览器保持打开；已用该会话通过身份和只读订单接口验证，Cookie 已加密保存。", last_checked=_now())
        self._log(job.account_id, "test" if job.restore else "login", "authenticated",
                  "平博浏览器会话已通过身份和只读订单接口验证，登录窗口保持打开。")

    def _sync_pinnacle_session(self, job):
        """Only this worker writes browser snapshots; collectors read them."""
        if job.session_conflict:
            return
        if time.monotonic() < job.next_session_sync:
            return
        job.next_session_sync = time.monotonic() + 5
        if not self._session_baseline_current(job):
            self._end_stale_session_job(job)
            return
        if not self._trusted_page(job, job.page.url):
            job.pinnacle_verified = False
            self._manual(job, "登录窗口已离开配置站点；请返回平博后台后验证会话。")
            return
        for frame in job.page.frames:
            if self._trusted_page(job, frame.url) and self._otp_challenge(frame):
                job.pinnacle_verified = False
                self._check_page(job)
                return
            if self._trusted_page(job, frame.url) and self._visible_locator(frame, ['input[type="password"]']) is not None:
                job.pinnacle_verified = False
                job.status = "awaiting_manual"
                self._set(job.account_id, "expired", "平博窗口已返回登录页，请在保留的浏览器中重新登录。", last_checked=_now())
                return
        state = job.context.storage_state()
        baseline = job.session_baseline if isinstance(job.session_baseline, dict) else {}
        if state != baseline.get("storage_state"):
            self._persist_verified(job)

    def _reuse_pinnacle(self, account_id, check=False):
        job = self._jobs.get(account_id)
        if not job or not job.keep_open or job.page.is_closed():
            self._begin(account_id, restore=bool(self.store.load_session(account_id)))
            return
        job.page.bring_to_front()
        if job.session_conflict:
            if not check or not self._is_current(job):
                return
            try:
                baseline = deepcopy(self.store.load_session(account_id))
            except Exception:
                self._set(account_id, "awaiting_manual",
                          "当前登录窗口保持打开；读取最新保存会话失败，请稍后再次验证。", last_checked=_now())
                return
            job.session_baseline = baseline
            job.session_conflict = False
            job.pinnacle_verified = False
            job.pinnacle_scope = {}
            job.evidence = None
            job.status = "checking"
        if not self._trusted_page(job, job.page.url):
            login_url = urljoin(job.account["base_url"].rstrip("/") + "/", job.account.get("login_path") or "/")
            job.page.goto(login_url, wait_until="domcontentloaded")
        if check or job.status not in {"logging_in", "awaiting_otp", "authenticated"}:
            self._check_page(job)
            if not check and job.status == "awaiting_manual" and self._trusted_page(job, job.page.url):
                self._fill_login(job)

    def _save(self, job):
        if not self._is_current(job):
            return
        if job.session_conflict or not self._session_baseline_current(job):
            raise _SessionChanged("saved session changed")
        if job.account["kind"] == "crown_odds":
            for frame in job.page.frames:
                if not self._trusted_page(job, frame.url):
                    continue
                try:
                    job.session_storage[frame.evaluate("location.origin")] = frame.evaluate(
                        "() => Object.fromEntries(Object.entries(sessionStorage))")
                except Exception:
                    pass  # A frame can be replaced while the site redirects.
            if self._trusted_page(job, job.page.url):
                if job.member_resume_query.get("uid") != job.uid:
                    job.member_resume_query = {}
                self._capture_member_resume(job, job.page.url)
                if not job.member_resume_query:
                    job.resume_url = urlsplit(job.page.url)._replace(query="", fragment="").geturl()
        elif job.account["kind"] == "crown_admin" and not job.admin_protocol.get("ver"):
            self._capture_crown_admin_protocol(job)
        expected = {"expected_revision": job.account["revision"]} if job.account.get("revision") is not None else {}
        try:
            storage_state = job.context.storage_state()
        except Exception:
            raise _SessionSaveError("browser snapshot failed") from None
        session = {
            "storage_state": storage_state,
            "uid": job.uid,
            "auth_origin": list(_origin(job.account["base_url"])),
            "member_origin": job.member_origin,
            "member_authenticated": job.member_authenticated,
            "resume_url": job.resume_url,
            "session_storage": job.session_storage,
            "member_protocol": job.member_protocol,
            "member_resume_query": job.member_resume_query,
            "admin_protocol": job.admin_protocol if job.account["kind"] == "crown_admin" else {},
            "admin_protocol_headers": (job.admin_protocol_headers
                                        if job.account["kind"] == "crown_admin" else {}),
            "member_ui_verified": job.member_ui_verified,
            "saved_at": _now(),
            "business_verified": bool(job.account["kind"] == "pinnacle" and job.pinnacle_verified),
        }
        if job.account["kind"] == "pinnacle":
            session["pinnacle_scope"] = dict(job.pinnacle_scope)
        try:
            update_last_login = job.login_saved and not job.login_recorded
            save_options = {"update_last_login": update_last_login, **expected}
            baseline = getattr(job, "session_baseline", _SESSION_BASELINE_UNSET)
            if baseline is not _SESSION_BASELINE_UNSET:
                save_options["expected_session"] = baseline
            self.store.save_session(job.account_id, session, **save_options)
        except SessionChangedError as exc:
            raise _SessionChanged("saved session changed") from exc
        except Exception:
            raise _SessionSaveError("session save failed") from None
        # CAS must compare an immutable snapshot with the persisted record.
        # The job continues collecting sessionStorage/protocol fields while
        # the login page settles; sharing their dicts mutates our own baseline.
        job.session_baseline = deepcopy({**session, "base_url": job.account["base_url"]})
        job.session_saved = True
        if update_last_login:
            job.login_recorded = True

    def _persist_verified(self, job):
        if job.session_conflict:
            return False
        try:
            self._save(job)
            return True
        except _SessionChanged as exc:
            self._diagnostic(job.account_id, "save_session", exc)
            self._end_stale_session_job(job)
            return False
        except _SessionSaveError as exc:
            self._diagnostic(job.account_id, "save_session", exc)
            self._set(job.account_id, "error", "登录界面已验证，但会话保存失败；现有会话未覆盖。", last_checked=_now())
            if job.keep_open:
                job.pinnacle_verified = False
                job.status = "awaiting_manual"
            else:
                self._dispose(job.account_id)
            return False

    def _otp(self, account_id, code):
        job = self._jobs.get(account_id)
        if not job or not self._is_current(job) or job.status != "awaiting_otp":
            raise RuntimeError("验证码会话已结束，请重新开始登录。")
        job.totp_attempts = 0
        job.totp_submitted_counter = int(time.time() // TOTP_INTERVAL)
        self._submit_otp_code(job, code)

    def _submit_otp_code(self, job, code, automatic=False):
        for frame in job.page.frames:
            if not self._trusted_page(job, frame.url):
                continue
            field = self._visible_locator(frame, [
                'input[name="otpCode"]', '#otpCode', 'input[autocomplete="one-time-code"]',
                'input[name="otp"]', '#otp', 'input[name="code"]', 'input[inputmode="numeric"]',
                'input[name*="authenticator" i]', 'input[id*="authenticator" i]',
                'input[name*="verifycode" i]', 'input[id*="verifycode" i]',
                'input[type="tel"]', 'input[type="number"]', 'input[maxlength="6"]',
            ])
            if field is None:
                continue
            field.fill(code)
            button = self._visible_locator(frame, [
                'button[type="submit"]', 'input[type="submit"]',
                'button:has-text("提交")', 'button:has-text("验证")', 'button:has-text("Submit")',
            ])
            if button is not None:
                button.click()
            else:
                field.press("Enter")
            job.deadline = time.monotonic() + 180
            message = ("已自动提交二次验证动态码，正在等待站点确认。" if automatic
                       else "动态码已提交，正在等待站点验证。")
            self._set(job.account_id, "awaiting_otp", message)
            return
        self._manual(job, "未找到动态码输入框，请在本机浏览器完成验证，或重新登录。")

    def _auto_submit_totp(self, job):
        """Submit at most one code per time step and stop after three attempts."""
        secret = self.store.get_credentials(job.account_id).get("totp_secret", "")
        if not secret:
            return False
        try:
            code, counter, remaining = generate_code(secret)
        except ValueError:
            self._set(job.account_id, "awaiting_otp",
                      "已保存的二次验证密钥无法使用，请编辑账号更新密钥，或手动输入验证码。")
            return True
        if counter == job.totp_submitted_counter:
            return True
        if remaining <= 5:
            self._set(job.account_id, "awaiting_otp", "正在等待下一个有效动态码周期。")
            return True
        if job.totp_attempts >= 3:
            self._set(job.account_id, "awaiting_otp",
                      "自动二次验证已尝试三次，请检查密钥或手动输入验证码。")
            return True
        job.totp_submitted_counter = counter
        job.totp_attempts += 1
        self._submit_otp_code(job, code, automatic=True)
        return True

    def _otp_challenge(self, frame):
        """Recognize an explicit authenticator prompt without mistaking CAPTCHA for OTP."""
        field = self._visible_locator(frame, [
            'input[autocomplete="one-time-code"]', 'input[name*="otp" i]', 'input[id*="otp" i]',
            'input[name*="authenticator" i]', 'input[id*="authenticator" i]',
            'input[name*="verifycode" i]', 'input[id*="verifycode" i]',
            'input[name="code"]', '#code', 'input[inputmode="numeric"]',
            'input[type="tel"]', 'input[type="number"]', 'input[maxlength="6"]',
        ])
        if field is None:
            return False
        try:
            text = frame.locator("body").inner_text(timeout=700)
        except Exception:
            return False
        return bool(re.search(
            r"身份验证器|身分驗證器|动态验证码|動態驗證碼|动态码|動態碼|"
            r"谷歌验证|谷歌驗證|Google\s*Authenticator|authenticator|"
            r"一次性密码|一次性密碼|one[- ]time(?: password| code)?|TOTP|"
            r"双重验证|雙重驗證|双因素验证|雙因素驗證|两步验证|兩步驗證|二次验证|二次驗證|2FA",
            text, re.IGNORECASE,
        ))

    def _check_page(self, job):
        """Confirm the signed-in UI on the entry or its verified member domain."""
        if not self._is_current(job):
            return
        if job.session_conflict:
            return
        if not self._session_baseline_current(job):
            self._end_stale_session_job(job)
            return
        if job.page.is_closed():
            return
        if not self._trusted_page(job, job.page.url):
            if job.background:
                self._fail(job.account_id, "后台检查无法确认当前页面来源；原会话保留。")
                return
            self._manual(job, "页面位于其他来源，尚未确认会话；请核对配置域名。")
            return
        trusted_frames = [frame for frame in job.page.frames if self._trusted_page(job, frame.url)]
        if any(self._otp_challenge(frame) for frame in trusted_frames):
            if job.account["kind"] == "pinnacle":
                job.pinnacle_verified = False
            if job.status != "awaiting_otp":
                job.deadline = time.monotonic() + 600
            if job.account["kind"] == "crown_admin" and not job.admin_completion_pending:
                job.admin_completion_pending = True
                job.admin_ui_verified_at = 0
                job.admin_ui_url = ""
                job.admin_ui_storage_state = {}
            job.status = "awaiting_otp"
            if not self._auto_submit_totp(job):
                self._set(job.account_id, job.status, "网站要求输入身份验证器动态码，请填写本次有效验证码。")
            return

        otp_was_pending = job.status == "awaiting_otp"
        if otp_was_pending:
            job.totp_attempts = 0
            job.totp_submitted_counter = -1
            job.status = "logging_in"
            self._set(job.account_id, job.status, "已离开动态码验证页，正在确认登录结果。")

        if job.account["kind"] == "pinnacle":
            for frame in trusted_frames:
                if self._visible_locator(frame, ['input[type="password"]']) is not None:
                    self._manual(job, "平博有头浏览器保持打开，请在该窗口完成登录；成功后自动检查并保存会话。")
                    return
            self._check_pinnacle_page(job)
            return

        for frame in trusted_frames:
            if (job.account["kind"] == "crown_odds"
                    and self._dismiss_member_quick_password_prompt(frame)):
                return
            password_field = self._visible_locator(frame, ['input[type="password"]'])
            if password_field is not None:
                if job.admin_completion_pending:
                    job.admin_ui_verified_at = 0
                    job.admin_ui_url = ""
                    job.admin_ui_storage_state = {}
                if job.background:
                    job.restore = False
                    job.status = "expired"
                    self._set(job.account_id, "expired", "后台检查确认页面已返回登录表单；会话已失效。", last_checked=_now())
                    self._dispose(job.account_id)
                    return
                if job.restore and job.account["kind"] == "crown_admin":
                    # A session check must never turn into a credential login.
                    # Keep the old snapshot for inspection and let an explicit
                    # login action start a fresh authentication flow.
                    job.restore = False
                    job.status = "expired"
                    self._set(job.account_id, "expired", "保存会话已返回登录表单；原会话已保留，请显式重新登录。", last_checked=_now())
                    self._dispose(job.account_id)
                    return
                if otp_was_pending and not job.restore:
                    job.admin_completion_pending = False
                    self._manual(job, "动态码页面已离开，但仍返回登录表单，尚未确认成功；请查看浏览器提示。")
                    return
                if job.restore and _origin(frame.url) == _origin(job.account["base_url"]):
                    job.restore = False
                    job.uid = ""
                    job.member_authenticated = False
                    job.member_ui_verified = False
                    job.member_protocol = {}
                    job.member_resume_query = {}
                    job.status = "logging_in"
                    self._set(job.account_id, "logging_in", "旧会话已回到登录页，正在尝试一次重新登录；验证码仍需你处理。", last_checked=_now())
                    self._fill_login(job)
                elif job.restore:
                    job.restore = False
                    job.uid = ""
                    job.member_authenticated = False
                    job.member_ui_verified = False
                    job.member_protocol = {}
                    job.member_resume_query = {}
                    job.session_storage = {}
                    job.status = "logging_in"
                    self._set(job.account_id, "logging_in", "会员会话已失效，正在返回登录入口重新登录。", last_checked=_now())
                    entry = urljoin(job.account["base_url"].rstrip("/") + "/", job.account.get("login_path") or "/")
                    job.page.goto(entry, wait_until="domcontentloaded")
                    if _origin(job.page.url) == _origin(job.account["base_url"]):
                        self._fill_login(job)
                return
            logout = self._visible_locator(frame, [
                'a:has-text("退出登录")', 'button:has-text("退出登录")',
                'a:has-text("登出")', 'button:has-text("登出")',
                'a:has-text("Logout")', 'button:has-text("Logout")',
                '[aria-label="Logout"]', '[title="登出"]', '[title="退出登录"]',
            ])
            member_signed_in = (job.account["kind"] == "crown_odds"
                                and job.member_authenticated and job.uid
                                and self._member_signed_in(frame))
            entry_page = _origin(frame.url) == _origin(job.account["base_url"])
            confirmed_logout = logout is not None and (entry_page or (job.member_authenticated and job.uid))
            admin_dashboard = (job.account["kind"] == "crown_admin" and entry_page
                               and self._crown_admin_dashboard(frame))
            if job.account["kind"] == "crown_admin":
                if admin_dashboard:
                    job.admin_completion_pending = True
                    self._finish_crown_admin_login(job)
                    return
                if job.admin_completion_pending:
                    return
            if (confirmed_logout and job.account["kind"] != "crown_admin") or member_signed_in:
                job.status = "authenticated"
                job.member_ui_verified = bool(job.account["kind"] == "crown_odds")
                if not job.ui_verified_at:
                    job.ui_verified_at = time.monotonic()
                job.login_saved = not job.restore and not job.background
                if (job.background and job.account["kind"] == "crown_odds" and not job.member_protocol
                        and time.monotonic() - job.ui_verified_at < 10
                        and time.monotonic() < job.deadline):
                    # Let the normal member page emit its own metadata-bearing request.
                    return
                if not self._persist_verified(job):
                    return
                self._set(job.account_id, "authenticated", "站点显示已登录界面，会话已保存；订单/盘口读取权限尚未验证。", last_checked=_now())
                self._dispose(job.account_id)
                return
        if job.restore:
            job.member_ui_verified = False
            job.ui_verified_at = 0
            job.status = "session_received"
            if not job.background:
                self._set(job.account_id, "session_received", "已载入保存会话，但页面不足以确认仍然有效；稍后可重新检查。", last_checked=_now())

    def _finish_crown_admin_login(self, job):
        """Save only after the same-origin signed-in page and browser state settle."""
        if time.monotonic() < job.admin_report_retry_at:
            return
        self._capture_crown_admin_protocol(job)
        try:
            if job.page.evaluate("() => document.readyState") != "complete":
                job.admin_ui_verified_at = 0
                job.admin_ui_url = ""
                job.admin_ui_storage_state = {}
                return
            state = job.context.storage_state()
        except Exception:
            job.admin_ui_verified_at = 0
            job.admin_ui_url = ""
            job.admin_ui_storage_state = {}
            return

        now = time.monotonic()
        current_url = job.page.url
        if (not job.admin_ui_verified_at or current_url != job.admin_ui_url
                or state != job.admin_ui_storage_state):
            job.admin_ui_verified_at = now
            job.admin_ui_url = current_url
            job.admin_ui_storage_state = state
            return
        if now - job.admin_ui_verified_at < 0.5:
            return

        job.admin_report_attempts += 1
        probe_ok, probe_code = self._probe_crown_admin_report(job)
        if not probe_ok:
            if (probe_code not in {"4X014", "4X013"}
                    and job.admin_report_attempts < _CROWN_ADMIN_PROBE_ATTEMPTS
                    and now < job.deadline):
                job.admin_report_retry_at = time.monotonic() + 0.5 * (2 ** (job.admin_report_attempts - 1))
                job.status = "logging_in"
                self._set(job.account_id, "logging_in",
                          "皇冠页面已登录，正在等待订单接口就绪并自动重试；原会话保留。")
                return
            message = ("皇冠页面显示已登录，但只读订单接口拒绝当前会话；原会话未覆盖。请检查登录层级和报表权限。"
                       if probe_code in {"4X014", "4X013"}
                       else "皇冠页面显示已登录，但只读订单接口尚未确认；原会话未覆盖，请稍后重试。")
            self._set(job.account_id, "session_received", message, last_checked=_now())
            self._log(job.account_id, "report_probe", "error", message)
            self._dispose(job.account_id)
            return

        job.status = "authenticated"
        job.login_saved = not job.restore and not job.background
        if not self._persist_verified(job):
            return
        self._set(job.account_id, "authenticated", "皇冠登录成功，订单读取权限已验证，会话已保存。", last_checked=_now())
        self._dispose(job.account_id)

    def _dismiss_member_quick_password_prompt(self, frame):
        try:
            prompt = frame.get_by_text(re.compile(r"[你您]要设置一个简易密码以便快速登入吗"), exact=False)
            if not prompt.is_visible():
                return False
            decline = self._visible_locator(frame, [
                'button:text-is("否")', 'input[type="button"][value="否"]',
                'a:text-is("否")', '[role="button"]:text-is("否")', 'div:text-is("否")',
            ])
            if decline is None:
                return False
            decline.click()
            return True
        except Exception:
            return False

    def _crown_admin_dashboard(self, frame):
        """Require several private dashboard sections before confirming Crown admin UI."""
        try:
            text = frame.locator("body").inner_text(timeout=700)
        except Exception:
            return False
        markers = (
            r"(?:\bPerformance Overview\b|绩效概况)",
            r"(?:\bAccount Overview\b|账号概况|帐号概况)",
            r"(?:\bAvailable Balance\b|可用额度|剩余额度)",
            r"(?:\bCredit\b|信用额度)",
        )
        return all(re.search(marker, text, re.IGNORECASE) for marker in markers)

    def _member_signed_in(self, frame):
        """The observed member header hides logout inside the account menu."""
        account = self._visible_locator(frame, [
            '#myAcc_page', '.btn_acc', '#user_info', '[aria-label="账户"]',
        ])
        history = self._visible_locator(frame, [
            '#header_todaywagers', '#header_historydata',
            'button:has-text("投注记录")', 'a:has-text("投注记录")',
            'button:has-text("投注紀錄")', 'a:has-text("投注紀錄")',
        ])
        if account is None or history is None:
            return False
        try:
            text = frame.locator("body").inner_text(timeout=700)
            return bool(re.search(r"(?:RMB|CNY|USD|HKD|余额|餘額)\s*[:：]?\s*[\d,]+(?:\.\d+)?", text))
        except Exception:
            return False  # A redirect may replace the header during detection.

    def _tick(self):
        for account_id, job in list(self._jobs.items()):
            try:
                if not self._is_current(job):
                    self._dispose(account_id)
                    continue
                if job.retry_at:
                    if time.monotonic() < job.retry_at:
                        continue
                    job.retry_at = 0
                    self._retry_stage(job)
                    if self._jobs.get(account_id) is not job:
                        continue
                if job.page.is_closed():
                    if job.background and job.session_saved:
                        self._set(account_id, "session_received", "后台检查页面已关闭；已保留原会话，稍后可重试。", last_checked=_now())
                    elif job.status not in {"authenticated", "session_received"}:
                        self._set(account_id, "idle", "浏览器已关闭，请重新开始登录。")
                    self._dispose(account_id)
                    continue
                # Pump Playwright's event loop; every browser object stays here.
                job.page.wait_for_timeout(30)
                if job.session_conflict:
                    continue
                if job.keep_open and job.status == "authenticated" and job.pinnacle_verified and not job.evidence:
                    self._sync_pinnacle_session(job)
                    continue
                if job.background and job.member_ui_verified and job.status == "authenticated":
                    if job.member_protocol or time.monotonic() - job.ui_verified_at >= 10:
                        # The page may have returned to login while metadata was pending.
                        job.stage = "check_page"
                        self._check_page(job)
                        if self._jobs.get(account_id) is not job:
                            continue
                if job.evidence:
                    evidence, job.evidence = job.evidence, None
                    job.status = evidence.status
                    if job.keep_open:
                        job.pinnacle_verified = False
                    if job.keep_open and evidence.status in {"authenticated", "session_received"}:
                        # OTP success can precede the redirect and final cookies.
                        # Keep the page and verify its real order access first.
                        job.status = "checking"
                        job.form_pending = False
                        job.next_screenshot = 0
                        self._set(account_id, "checking", "认证响应已通过，正在等待页面完成并验证订单会话。")
                        continue
                    if evidence.uid:
                        job.uid = evidence.uid
                        if job.account["kind"] == "crown_odds" and evidence.status == "session_received":
                            job.member_authenticated = True
                    if evidence.status in {"authenticated", "session_received"}:
                        if job.account["kind"] == "crown_odds" and evidence.status == "session_received":
                            # The login response precedes the member-domain
                            # handoff and final browser state. Keep the account
                            # busy and the old snapshot until the signed-in UI
                            # is confirmed, so monitoring cannot race the login.
                            job.status = "logging_in"
                            job.form_pending = False
                            job.next_screenshot = 0
                            self._set(account_id, "logging_in", "会员登录响应已成功，正在等待会员页面完成并保存完整会话。")
                            self._log(account_id, "login", "logging_in", "会员登录响应已成功，等待页面完成会话。")
                            continue
                        if job.account["kind"] == "crown_admin" and evidence.status == "authenticated":
                            # login_chk confirms credentials, while the site's
                            # normal success callback may still be setting cookies.
                            job.admin_completion_pending = True
                            job.admin_ui_verified_at = 0
                            job.admin_ui_url = ""
                            job.admin_ui_storage_state = {}
                            job.admin_report_attempts = 0
                            job.admin_report_retry_at = 0
                            job.status = "logging_in"
                            self._set(account_id, "logging_in", "登录响应已成功，正在等待皇冠页面完成登录并稳定会话。")
                            self._log(account_id, "login", "logging_in", "登录响应已成功，等待页面完成会话。")
                            continue
                        job.login_saved = not job.restore and not job.background
                        if not self._persist_verified(job):
                            continue
                        self._set(account_id, evidence.status, evidence.message, last_checked=_now())
                        self._log(account_id, "login", evidence.status, evidence.message)
                        if evidence.status == "authenticated":
                            self._dispose(account_id)
                            continue
                    else:
                        self._set(account_id, evidence.status, evidence.message)
                if job.admin_completion_pending and job.status != "awaiting_otp":
                    job.stage = "check_page"
                    self._check_page(job)
                    if self._jobs.get(account_id) is not job:
                        continue
                if time.monotonic() >= job.next_screenshot:
                    job.next_screenshot = time.monotonic() + 3
                    if job.status in {"logging_in", "awaiting_manual"} and job.form_pending:
                        self._fill_login(job)
                    if job.status in {"logging_in", "awaiting_manual", "awaiting_otp", "session_received", "checking"}:
                        job.stage = "check_page"
                        self._check_page(job)
                    if self._jobs.get(account_id) is not job:
                        continue
                    # Mask passwords, OTP and safety fields even if the site
                    # renders them as plain text inputs.
                    masks = []
                    for frame in job.page.frames:
                        masks.append(frame.locator('input[type="password"], input[autocomplete="one-time-code"], input[name="otpCode"], input[name="otp"], input[id*="otp" i], input[name*="authenticator" i], input[id*="authenticator" i], input[inputmode="numeric"], input[type="tel"], input[type="number"], input[maxlength="6"], input[name="code"], input[name="safe"], input[name="pwd_safe"], #pwd_safe'))
                    try:
                        png = job.page.screenshot(type="png", mask=masks, timeout=2500)
                        with self._lock:
                            self._screenshots[account_id] = png
                    except Exception:
                        pass  # A preview failure must not destroy a valid login.
                if time.monotonic() >= job.deadline:
                    if job.keep_open:
                        job.deadline = float("inf")
                        self._manual(job, "登录浏览器保持打开，请继续完成登录或验证；完成后会自动保存可用会话。")
                    elif job.background:
                        self._fail(account_id, "后台检查超过时限；已保留原会话，稍后重试。")
                    elif job.status in {"authenticated", "session_received"}:
                        self._persist_verified(job)
                        self._dispose(account_id)
                    else:
                        self._fail(account_id, "登录等待超过 5 分钟，已停止任务；可重新登录。")
            except Exception as exc:
                if self._defer_transient(job, exc):
                    continue
                self._diagnostic(account_id, job.stage, exc)
                self._fail(account_id, "登录操作未完成；已有会话保留，必要时可重新检查或登录。")

    @staticmethod
    def _transient_exception(exc):
        name = type(exc).__name__.lower()
        message = str(exc).lower()
        return ("timeout" in name or "timeout" in message
                or "navigation interrupted" in message or "net::err_" in message
                or "frame was detached" in message or "execution context was destroyed" in message)

    def _defer_transient(self, job, exc):
        if (not self._transient_exception(exc) or job.transient_failures >= 3
                or time.monotonic() >= job.deadline):
            return False
        job.transient_failures += 1
        job.retry_at = time.monotonic() + 0.2 * job.transient_failures
        self._diagnostic(job.account_id, job.stage, exc, retry=job.transient_failures)
        return True

    def _diagnostic(self, account_id, stage, exc, retry=0):
        name = type(exc).__name__.lower()
        category = ("storage" if isinstance(exc, _SessionSaveError) else
                    "timeout" if "timeout" in name or "timeout" in str(exc).lower() else
                    "navigation_or_page")
        stage = stage if stage in {"open_page", "login_form", "check_page", "member_request", "save_session", "command"} else "browser"
        message = f"内部诊断：{stage}/{category}" + (f"，短暂重试第 {retry} 次。" if retry else "，已安全结束本次操作。")
        self._log(account_id, "auth_diagnostic", "retry" if retry else "warning", message)

    def _retry_stage(self, job):
        if not self._is_current(job) or job.page.is_closed():
            return
        if job.stage == "open_page":
            job.page.goto(job.navigation_url, wait_until="domcontentloaded", timeout=8000 if job.background else 30000)
        elif job.stage == "login_form" and not job.login_attempted:
            self._fill_login(job)
        else:
            self._check_page(job)

    def _fail(self, account_id, message):
        live = self._jobs.get(account_id)
        if live and live.keep_open and self._is_current(live):
            try:
                if not live.page.is_closed():
                    live.form_pending = False
                    self._manual(live, "登录暂未完成，浏览器保持打开；请查看页面提示后继续，或重新验证会话。")
                    self._log(account_id, "login", "awaiting_manual", "平博登录暂未完成，已保留有头窗口供继续操作。")
                    return
            except Exception:
                pass
        try:
            job = self._jobs.get(account_id)
            if job and not self._is_current(job):
                return
            saved = bool(job and job.session_saved)
            if not job:
                try:
                    saved = bool(self.store.load_session(account_id))
                except Exception:
                    saved = False
            if saved and (not job or job.status != "expired"):
                status = "session_received"
                safe_message = "本次检查未能确认当前登录；已保留原会话，稍后重试。"
            else:
                status, safe_message = "error", message
            self._set(account_id, status, safe_message, last_checked=_now())
            self._log(account_id, "login", status, safe_message)
        except Exception:
            pass
        finally:
            self._dispose(account_id)

    def _dispose(self, account_id):
        job = self._jobs.pop(account_id, None)
        with self._lock:
            self._screenshots.pop(account_id, None)
        if job:
            try:
                job.context.close()
            except Exception:
                pass
