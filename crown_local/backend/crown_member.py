"""Bounded session heartbeat for saved Crown member sessions.

This checks only the member session endpoint observed in the normal site. It
does not collect odds or claim that odds permissions are available.
"""
from __future__ import annotations

from datetime import datetime, timezone
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from http.client import HTTPException
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, HTTPRedirectHandler, ProxyHandler, Request, build_opener
import xml.etree.ElementTree as ET


HEARTBEAT_INTERVAL = 60
MAX_RESPONSE_BYTES = 64 * 1024
_VERSION = re.compile(r"[A-Za-z0-9._-]{1,96}\Z")
_LANG = re.compile(r"[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?\Z")
_TIMESTAMP = re.compile(r"\d{9,12}\Z")


class MemberError(RuntimeError):
    pass


class MemberNeedsBrowser(MemberError):
    pass


class MemberUnavailable(MemberError):
    pass


class MemberExpired(MemberError):
    pass


class _SameOriginCookies(DefaultCookiePolicy):
    def __init__(self, host):
        super().__init__()
        self.host = host

    def _allowed(self, cookie, request):
        parsed = urlsplit(request.full_url)
        if parsed.scheme != "https" or parsed.hostname != self.host or parsed.port not in (None, 443):
            return False
        domain = cookie.domain.lower()
        return self.host == domain or (domain.startswith(".") and
                                      (self.host == domain[1:] or self.host.endswith(domain)))

    def set_ok(self, cookie, request):
        return self._allowed(cookie, request) and super().set_ok(cookie, request)

    def return_ok(self, cookie, request):
        return self._allowed(cookie, request) and super().return_ok(cookie, request)


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def _origin(value):
    try:
        parts = urlsplit(str(value or ""))
        if (parts.scheme != "https" or not parts.hostname or parts.username or parts.password
                or parts.port not in (None, 443) or parts.path not in ("", "/")
                or parts.query or parts.fragment):
            return ""
    except ValueError:
        return ""
    return "https://" + parts.hostname.lower()


def _now():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


class CrownMemberClient:
    """Issue the observed, read-only `mem_online` POST on its member origin."""

    def __init__(self, account, session, timeout=10, transport=None):
        self.account = account
        self.session = session or {}
        self.timeout = min(15, max(1, timeout))
        self.transport = transport
        self.origin, self.uid, self.protocol = self._session_contract()
        self._cookies = CookieJar(policy=_SameOriginCookies(urlsplit(self.origin).hostname))
        self._restore_cookies()
        self._opener = build_opener(ProxyHandler({}), _NoRedirects(), HTTPCookieProcessor(self._cookies))

    def _session_contract(self):
        session = self.session
        member_origin = session.get("member_origin")
        origin = _origin(member_origin)
        protocol = session.get("member_protocol")
        uid = session.get("uid")
        # Direct member entry pages do not always perform a domain handoff.
        # Reuse their verified entry origin only when all saved provenance
        # agrees, and still apply the public HTTPS destination check below.
        if (member_origin in (None, "") and session.get("member_ui_verified")
                and isinstance(protocol, dict)):
            candidate = _origin(self.account.get("base_url"))
            auth_origin = session.get("auth_origin")
            if (candidate and isinstance(auth_origin, (list, tuple))
                    and tuple(auth_origin) == ("https", urlsplit(candidate).hostname, 443)
                    and _origin(protocol.get("origin")) == candidate):
                origin = candidate
        if (not session.get("member_ui_verified") or not origin
                or not isinstance(protocol, dict)
                or _origin(protocol.get("origin")) != origin
                or not isinstance(protocol.get("ver"), str) or not _VERSION.fullmatch(protocol["ver"])
                or not isinstance(protocol.get("langx"), str) or not _LANG.fullmatch(protocol["langx"])
                or not isinstance(uid, str) or not 1 <= len(uid) <= 2048
                or any(char in uid for char in "\r\n\x00")):
            raise MemberNeedsBrowser("会员会话信息不足，等待安全页面复核")
        from .storage import validate_target_url
        if not validate_target_url(origin):
            raise MemberUnavailable("会员站点暂时无法安全连接，稍后自动重试")
        return origin, uid, protocol

    def _restore_cookies(self):
        cookies = self.session.get("storage_state", {}).get("cookies", [])
        host = urlsplit(self.origin).hostname
        for cookie in cookies if isinstance(cookies, list) else []:
            if not isinstance(cookie, dict):
                continue
            domain = str(cookie.get("domain", "")).lower()
            if not (host == domain or (domain.startswith(".") and
                                       (host == domain[1:] or host.endswith(domain)))):
                continue
            path = str(cookie.get("path") or "/")
            if not "/transform.php".startswith(path):
                continue
            expires = cookie.get("expires", -1)
            if isinstance(expires, (int, float)) and 0 < expires <= time.time():
                continue
            name, value = cookie.get("name"), cookie.get("value")
            if (not isinstance(name, str) or not re.fullmatch(r"[!#$%&'()*+.^_`|~0-9A-Za-z-]+", name)
                    or not isinstance(value, str) or any(char in value for char in "\r\n;")):
                continue
            rest = {"SameSite": cookie.get("sameSite", "Lax")}
            if cookie.get("httpOnly"):
                rest["HttpOnly"] = None
            self._cookies.set_cookie(Cookie(
                version=0, name=name, value=value, port=None, port_specified=False,
                domain=domain, domain_specified=domain.startswith("."),
                domain_initial_dot=domain.startswith("."), path=path, path_specified=True,
                secure=bool(cookie.get("secure", True)),
                expires=int(expires) if isinstance(expires, (int, float)) and expires > 0 else None,
                discard=not isinstance(expires, (int, float)) or expires <= 0,
                comment=None, comment_url=None,
                rest=rest, rfc2109=False,
            ))

    def _storage_cookies(self):
        result = []
        for cookie in self._cookies:
            result.append({
                "name": cookie.name, "value": cookie.value, "domain": cookie.domain,
                "path": cookie.path, "expires": cookie.expires or -1,
                "httpOnly": any(key.lower() == "httponly" for key in cookie._rest),
                "secure": bool(cookie.secure), "sameSite": cookie._rest.get("SameSite", "Lax"),
            })
        return result

    def session_updates(self):
        original = self.session.get("storage_state", {})
        origins = original.get("origins", []) if isinstance(original, dict) else []
        cookies = original.get("cookies", []) if isinstance(original, dict) else []
        host = urlsplit(self.origin).hostname
        untouched = []
        for cookie in cookies if isinstance(cookies, list) else []:
            if not isinstance(cookie, dict):
                continue
            domain = str(cookie.get("domain", "")).lower()
            applies = host == domain or (domain.startswith(".") and
                                        (host == domain[1:] or host.endswith(domain)))
            if not applies or not "/transform.php".startswith(str(cookie.get("path") or "/")):
                untouched.append(cookie)
        return {"storage_state": {"cookies": untouched + self._storage_cookies(),
                                  "origins": origins if isinstance(origins, list) else []}}

    def heartbeat(self):
        fields = {"p": "mem_online", "uid": self.uid,
                  "ver": self.protocol["ver"], "langx": self.protocol["langx"]}
        if self.transport is not None:
            try:
                status, body = self.transport(self.origin + "/transform.php?" +
                                              urlencode({"ver": self.protocol["ver"]}), fields,
                                              self._headers(include_cookie=True), self.timeout)
            except MemberExpired:
                raise
            except Exception:
                raise MemberUnavailable("会员在线检查暂时失败，稍后自动重试") from None
        else:
            url = self.origin + "/transform.php?" + urlencode({"ver": self.protocol["ver"]})
            request = Request(url, data=urlencode(fields).encode("utf-8"), method="POST", headers=self._headers())
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    status, body = response.status, response.read(MAX_RESPONSE_BYTES + 1)
            except HTTPError as error:
                if error.code in (401, 440):
                    raise MemberExpired("会员站点明确拒绝当前会话") from None
                raise MemberUnavailable("会员在线检查暂时失败，稍后自动重试") from None
            except (URLError, OSError, HTTPException):
                raise MemberUnavailable("会员在线检查暂时失败，稍后自动重试") from None
        if status in (401, 440):
            raise MemberExpired("会员站点明确拒绝当前会话")
        if status != 200 or not isinstance(body, (bytes, bytearray)) or len(body) > MAX_RESPONSE_BYTES:
            raise MemberUnavailable("会员在线检查暂时失败，稍后自动重试")
        self._parse_response(body)
        return True

    def _cookie_header(self):
        request = Request(self.origin + "/transform.php")
        self._cookies.add_cookie_header(request)
        return request.get_header("Cookie", "")

    def _headers(self, include_cookie=False):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "text/xml,application/xml", "Origin": self.origin,
            "Referer": self.origin + "/", "User-Agent": "Mozilla/5.0",
        }
        cookie = self._cookie_header()
        if cookie and include_cookie:
            headers["Cookie"] = cookie
        return headers

    @staticmethod
    def _parse_response(body):
        try:
            text = bytes(body).decode("utf-8", errors="strict")
        except UnicodeError:
            raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试") from None
        if "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
            raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试")
        try:
            root = ET.fromstring(text)
        except (ET.ParseError, UnicodeError):
            raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试") from None
        if root.tag != "serverresponse":
            raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试")
        values = {}
        for child in root:
            if child.tag not in ("msg", "systemtime", "systimestamp") or child.tag in values or list(child):
                raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试")
            values[child.tag] = child.text or ""
        if (values.get("msg", "") != "" or
                not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", values.get("systemtime", "")) or
                not _TIMESTAMP.fullmatch(values.get("systimestamp", ""))):
            raise MemberUnavailable("会员在线检查未确认会话，稍后由登录页面复核")
        try:
            datetime.strptime(values["systemtime"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise MemberUnavailable("会员在线检查返回格式异常，稍后自动重试") from None


class CrownMemberMonitor:
    """One serialized monitor with per-account backoff and bounded relogin."""

    def __init__(self, store, auth, *, client_factory=CrownMemberClient,
                 monotonic_fn=None, now_fn=None):
        self.store, self.auth = store, auth
        self.client_factory = client_factory
        self._monotonic = monotonic_fn or time.monotonic
        self._now = now_fn or _now
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        self._next_due = {}
        self._failures = {}
        self._relogin = {}
        self._session_keys = {}
        self._clients = {}
        self._last_browser = {}
        self._metadata_state = {}

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="crown-member-heartbeat", daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        self._wake.set()
        if self._thread and threading.current_thread() is not self._thread:
            self._thread.join(timeout=20)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.check_once()
            except Exception:
                pass
            self._wake.wait(5)
            self._wake.clear()

    def _accounts(self):
        return [account for account in self.store.accounts()
                if account.get("kind") == "crown_odds" and account.get("enabled") and account.get("has_session")]

    def _is_busy(self, account):
        return account.get("status") in ("logging_in", "checking", "awaiting_otp", "awaiting_manual")

    def _identity(self, account, session):
        return (account.get("revision"), session.get("uid"), session.get("saved_at"))

    def _verified(self, account, session):
        current = self.store.get_account(account["id"])
        latest = self.store.load_session(account["id"])
        return bool(current.get("enabled") and current.get("revision") == account.get("revision")
                    and latest and self._identity(current, latest) == self._identity(account, session))

    def _request_browser_check(self, account, session=None, delay=300):
        self._next_due[account["id"]] = self._monotonic() + delay
        if (self.auth is None or self._stop.is_set()
                or (session is not None and not self._verified(account, session))
                or self._is_busy(self.store.get_account(account["id"]))):
            return
        try:
            self.auth.test(account["id"], background=True)
            self._last_browser[account["id"]] = self._monotonic()
        except (ValueError, RuntimeError, KeyError):
            pass

    def _start_relogin(self, account, session=None):
        identity = account["id"]
        record = self._relogin.get(identity, {"next": 0, "attempts": 0})
        now = self._monotonic()
        if (self._stop.is_set() or (session is not None and not self._verified(account, session))
                or self._is_busy(self.store.get_account(identity))):
            return
        if self.auth is None or now < record["next"] or record["attempts"] >= 3:
            self._next_due[identity] = max(record["next"], now + 300)
            return
        try:
            self.auth.start(identity)
        except (ValueError, RuntimeError, KeyError):
            self._next_due[identity] = now + 30
            return
        attempts = record["attempts"] + 1
        self._relogin[identity] = {"attempts": attempts,
                                   "next": now + min(1800, 120 * (2 ** (attempts - 1)))}
        self._next_due[identity] = self._relogin[identity]["next"]

    def _update_state(self, account, session, status, message, updates=None, checked=True):
        """Apply only while both configuration and session remain unchanged."""
        if self._stop.is_set() or self._is_busy(account):
            return False
        return self.store.apply_session_check(
            account["id"], session, status, message, updates=updates,
            expected_revision=account.get("revision"), expected_status=account.get("status"),
            last_checked=self._now() if checked else None)

    def check_once(self, force=False):
        if not self._lock.acquire(blocking=False):
            return False
        try:
            now = self._monotonic()
            for account in self._accounts():
                if self._stop.is_set():
                    break
                identity = account["id"]
                if self._is_busy(account):
                    continue
                session = self.store.load_session(identity)
                if not session:
                    continue
                session_key = self._identity(account, session)
                previous = self._session_keys.get(identity)
                has_metadata = bool(session.get("member_ui_verified") and isinstance(session.get("member_protocol"), dict))
                if previous != session_key:
                    self._session_keys[identity] = session_key
                    self._failures[identity] = 0
                    if not previous or previous[:2] != session_key[:2]:
                        self._relogin.pop(identity, None)
                        self._next_due.pop(identity, None)
                    elif has_metadata and (not self._metadata_state.get(identity)
                                           or account.get("status") == "authenticated"):
                        self._next_due.pop(identity, None)
                    self._clients.pop(identity, None)
                    self._last_browser[identity] = now
                self._metadata_state[identity] = has_metadata
                explicitly_expired = account.get("status") == "expired"
                if self._next_due.get(identity, 0) > now and not force and not explicitly_expired:
                    continue
                if account.get("status") == "expired":
                    self._start_relogin(account, session)
                    continue
                if previous is None and account.get("status") != "authenticated":
                    self._request_browser_check(account, session)
                    continue
                if not session.get("member_ui_verified") or not isinstance(session.get("member_protocol"), dict):
                    self._request_browser_check(account, session)
                    continue
                if now - self._last_browser.get(identity, now) >= 900:
                    self._request_browser_check(account, session, delay=60)
                    continue
                try:
                    client = self._clients.get(identity)
                    if client is None or self._identity(client.account, client.session) != session_key:
                        client = self.client_factory(account, session)
                        self._clients[identity] = client
                    client.heartbeat()
                    updates = client.session_updates() if hasattr(client, "session_updates") else {}
                    if not self._update_state(account, session, "authenticated",
                                              "水位会员在线检查通过；盘口权限需单独验证", updates):
                        self._clients.pop(identity, None)
                        continue
                    self._failures[identity] = 0
                    self._relogin.pop(identity, None)
                    self._next_due[identity] = now + HEARTBEAT_INTERVAL
                except MemberNeedsBrowser:
                    self._clients.pop(identity, None)
                    self._request_browser_check(account, session)
                except MemberExpired:
                    self._clients.pop(identity, None)
                    if self._update_state(account, session, "expired", "会员站点确认会话失效，正在尝试重新登录"):
                        self._next_due[identity] = now
                        self._start_relogin(account, session)
                except MemberUnavailable:
                    self._clients.pop(identity, None)
                    count = self._failures.get(identity, 0) + 1
                    self._failures[identity] = count
                    delay = min(300, 15 * (2 ** min(count - 1, 5)))
                    self._next_due[identity] = now + delay
                    current = self.store.get_account(identity)
                    status = "authenticated" if count < 3 and current.get("status") == "authenticated" else "session_received"
                    message = "水位在线检查暂时失败，保留已保存会话并稍后重试"
                    if not self._update_state(account, session, status, message):
                        continue
                    if count >= 3:
                        self._request_browser_check(account, session)
                except (ValueError, KeyError):
                    self._clients.pop(identity, None)
                    self._next_due[identity] = now + 300
                except Exception:
                    self._clients.pop(identity, None)
                    self._next_due[identity] = now + 300
                    try:
                        current = self.store.get_account(identity)
                        status = current.get("status")
                        if status != "expired":
                            self._update_state(account, session, status, "水位在线检查暂时失败，保留已保存会话并稍后重试")
                    except Exception:
                        pass
            return True
        finally:
            self._lock.release()
