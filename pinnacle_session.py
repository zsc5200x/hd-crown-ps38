"""持久化的 PS3838/平博只读会话管理。

这个模块只负责会话生命周期，不负责解析赛事。认证协议的动态字段由调用方
通过环境变量 ``PINNACLE_AUTH_EXTRA_JSON`` 提供；不会猜测 tokens 或把整个
认证响应当作 Cookie。若服务器返回 OTP/验证码等挑战，模块会报告等待人工处理。
"""
from __future__ import annotations

import http.cookiejar
import json
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:  # cryptography is already a project dependency
    from cryptography.fernet import Fernet, InvalidToken
except Exception:  # pragma: no cover - only used in an incomplete environment
    Fernet = None
    InvalidToken = Exception


AUTH_PATH = "/member-auth/v2/auth"
DEFAULT_BASE_URL = "https://www.ps3838.com"
DEFAULT_ODDS_URL = (
    "/sports-service/sv/compact/events?btg=1&c=&cl=5&d=&ec=&ev=&g=QQ%3D%3D"
    "&hle=false&ic=false&ice=false&inl=false&l=5&lang=&lg=&lv=0&me=0&me01="
    "&mk=1&more=false&o=1&ot=2&pa=0&pimo=0%2C1%2C8%2C39%2C2%2C3%2C6%2C7%2C4%2C5"
    "&pn=-1&pv=1&ru=&sp=29&tm=0&v=0&locale=zh_CN"
)


class PinnacleSessionError(RuntimeError):
    """会话或认证失败。``code`` 用于页面显示稳定的错误类别。"""

    def __init__(self, message: str, code: str = "session_error"):
        super().__init__(message)
        self.code = code


class AuthenticationRequired(PinnacleSessionError):
    def __init__(self, message: str = "未配置平博账号或密码"):
        super().__init__(message, "credentials_missing")


class AuthenticationChallenge(PinnacleSessionError):
    def __init__(self, message: str = "需要人工完成验证码或二次验证"):
        super().__init__(message, "awaiting_challenge")


class AuthenticationFailed(PinnacleSessionError):
    def __init__(self, message: str = "平博认证失败"):
        super().__init__(message, "authentication_failed")


class SessionExpired(PinnacleSessionError):
    def __init__(self, message: str = "平博会话已过期"):
        super().__init__(message, "session_expired")


class RateLimited(PinnacleSessionError):
    def __init__(self, message: str = "平博请求受到限流"):
        super().__init__(message, "rate_limited")


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str
    base_url: str = DEFAULT_BASE_URL
    auth_url: str | None = None
    odds_url: str | None = None
    extra: Mapping[str, Any] | None = None

    @property
    def resolved_auth_url(self) -> str:
        return self.auth_url or urllib.parse.urljoin(self.base_url.rstrip("/") + "/", AUTH_PATH.lstrip("/"))

    @property
    def resolved_odds_url(self) -> str:
        return self.odds_url or urllib.parse.urljoin(self.base_url.rstrip("/") + "/", DEFAULT_ODDS_URL.lstrip("/"))


def load_credentials_from_env(env: Mapping[str, str] | None = None) -> Credentials | None:
    """从环境变量加载凭据；不打印、不回传密码。

    必填 ``PINNACLE_USERNAME`` 和 ``PINNACLE_PASSWORD``。动态认证字段可以放在
    ``PINNACLE_AUTH_EXTRA_JSON``，例如浏览器 Network 已确认的 fingerprint 字段。
    """
    env = env or os.environ
    username = str(env.get("PINNACLE_USERNAME", "")).strip()
    password = str(env.get("PINNACLE_PASSWORD", ""))
    if not username or not password:
        return None
    extra: dict[str, Any] = {}
    raw_extra = str(env.get("PINNACLE_AUTH_EXTRA_JSON", "")).strip()
    if raw_extra:
        try:
            decoded = json.loads(raw_extra)
        except json.JSONDecodeError as exc:
            raise ValueError("PINNACLE_AUTH_EXTRA_JSON 不是有效 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("PINNACLE_AUTH_EXTRA_JSON 必须是 JSON 对象")
        extra = decoded
    return Credentials(
        username=username,
        password=password,
        base_url=str(env.get("PINNACLE_BASE_URL", DEFAULT_BASE_URL)).strip() or DEFAULT_BASE_URL,
        auth_url=str(env.get("PINNACLE_AUTH_URL", "")).strip() or None,
        odds_url=str(env.get("PINNACLE_ODDS_URL", "")).strip() or None,
        extra=extra,
    )


@dataclass
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))


class UrllibTransport:
    """带 CookieJar 的标准库 HTTP 传输层，可被测试替换。"""

    def __init__(self, cookie_jar: http.cookiejar.CookieJar | None = None):
        self.cookie_jar = cookie_jar or http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))

    def request(self, method: str, url: str, *, headers: Mapping[str, str] | None = None,
                data: bytes | None = None, timeout: float = 20) -> HttpResponse:
        req = urllib.request.Request(url, data=data, headers=dict(headers or {}), method=method.upper())
        try:
            with self.opener.open(req, timeout=timeout) as response:
                return HttpResponse(response.status, dict(response.headers.items()), response.read())
        except urllib.error.HTTPError as exc:
            return HttpResponse(exc.code, dict(exc.headers.items()), exc.read())


def _safe_json_body(response: Any) -> Any:
    try:
        if hasattr(response, "json"):
            return response.json()
        body = response.body if hasattr(response, "body") else b""
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def _response_status(response: Any) -> int:
    return int(getattr(response, "status", getattr(response, "status_code", 0)) or 0)


def _response_text(response: Any) -> str:
    body = getattr(response, "body", b"")
    if isinstance(body, bytes):
        return body.decode("utf-8", "replace")
    return str(body or "")


def _same_odds_endpoint(target: str, expected: str) -> bool:
    """Compare compact/events endpoints while ignoring browser cache markers.

    The browser appends ``withCredentials=true`` and a changing ``_`` query
    parameter on every poll.  Those values must not disable the response-shape
    validation that protects the session manager from treating a login page as
    an authorized odds response.
    """
    try:
        target_url = urllib.parse.urlsplit(str(target))
        expected_url = urllib.parse.urlsplit(str(expected))
    except Exception:
        return False
    if (target_url.scheme, target_url.netloc, target_url.path) != (
        expected_url.scheme,
        expected_url.netloc,
        expected_url.path,
    ):
        return False

    def normalized_query(url: urllib.parse.SplitResult) -> list[tuple[str, str]]:
        pairs = urllib.parse.parse_qsl(url.query, keep_blank_values=True)
        return sorted((key, value) for key, value in pairs if key not in {"_", "withCredentials"})

    return normalized_query(target_url) == normalized_query(expected_url)


def parse_auth_result(response: Any) -> dict[str, Any]:
    """解析认证结果，必须有明确成功信号才返回 authenticated=True。"""
    status = _response_status(response)
    body = _safe_json_body(response)
    text = _response_text(response).strip()
    result = {"authenticated": False, "challenge": False, "status": status, "code": None, "state": None}
    if status in {401, 403}:
        result["code"] = "authentication_failed"
        return result
    if not isinstance(body, (dict, list)):
        result["raw_kind"] = "scalar"
        result["raw_value"] = text[:32] if text else None
        return result
    obj = body if isinstance(body, dict) else {}
    result["code"] = obj.get("code") or obj.get("statusCode")
    result["state"] = obj.get("state") or obj.get("authState")
    state = str(result["state"] or "").lower()
    code = str(result["code"] or "").lower()
    status_value = str(obj.get("status") or "").lower()
    challenge_words = ("otp", "captcha", "challenge", "verify", "verification", "mfa", "2fa")
    result["challenge"] = any(word in " ".join((state, code, status_value)) for word in challenge_words)
    if result["challenge"]:
        result["code"] = "awaiting_challenge"
        return result
    success = (
        obj.get("authenticated") is True
        or obj.get("success") is True
        or status_value in {"ok", "success", "authenticated"}
        or state in {"authenticated", "success", "logged_in", "loggedin"}
        or code in {"ok", "success", "0"} and bool(obj.get("tokens") or obj.get("token") or obj.get("session"))
    )
    result["authenticated"] = bool(success)
    if not success:
        result["code"] = result["code"] or "authentication_failed"
    return result


class SessionStore:
    """加密保存 Cookie 和经认证协议确认的非敏感元数据。"""

    def __init__(self, path: str | os.PathLike[str], key_path: str | os.PathLike[str] | None = None):
        self.path = Path(path)
        self.key_path = Path(key_path) if key_path else self.path.with_name(self.path.stem + ".key")
        self.lock = threading.RLock()

    def _key(self) -> bytes:
        if Fernet is None:
            raise PinnacleSessionError("当前环境缺少 cryptography，无法安全保存会话", "storage_unavailable")
        self.key_path.parent.mkdir(parents=True, exist_ok=True)
        if self.key_path.exists():
            key = self.key_path.read_bytes()
        else:
            key = Fernet.generate_key()
            self.key_path.write_bytes(key)
            os.chmod(self.key_path, 0o600)
        return key

    def save(self, payload: Mapping[str, Any]) -> None:
        raw = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        encrypted = Fernet(self._key()).encrypt(raw)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.parent.mkdir(parents=True, exist_ok=True)
        temp.write_bytes(encrypted)
        os.chmod(temp, 0o600)
        os.replace(temp, self.path)

    def load(self) -> dict[str, Any] | None:
        with self.lock:
            if not self.path.exists():
                return None
            try:
                raw = Fernet(self._key()).decrypt(self.path.read_bytes())
                value = json.loads(raw.decode("utf-8"))
                return value if isinstance(value, dict) else None
            except (OSError, ValueError, InvalidToken, json.JSONDecodeError):
                return None

    def clear(self) -> None:
        with self.lock:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass


def _cookie_to_dict(cookie: http.cookiejar.Cookie) -> dict[str, str]:
    return {"name": cookie.name, "value": cookie.value, "domain": cookie.domain, "path": cookie.path}


class PinnacleSessionManager:
    """认证、会话恢复和有限重试的线程安全管理器。"""

    def __init__(self, credentials: Credentials | None = None, *, transport: Any | None = None,
                 session_path: str | os.PathLike[str] = "data/pinnacle_session.enc",
                 timeout: float = 20, max_retries: int = 2, sleep_fn=time.sleep,
                 clock=time.monotonic):
        self.credentials = credentials or load_credentials_from_env()
        self.transport = transport or UrllibTransport()
        self.store = SessionStore(session_path)
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.sleep_fn = sleep_fn
        self.clock = clock
        self.auth_lock = threading.RLock()
        self.state_lock = threading.RLock()
        self._restored = False
        self._needs_restore_verification = False
        self._authenticated = False
        self._status = "未配置" if self.credentials is None else "未认证"
        self._last_error: str | None = None
        self._last_auth_at: str | None = None
        self._last_response_at: str | None = None
        self._restore_once()

    def _restore_once(self) -> None:
        if self._restored:
            return
        self._restored = True
        payload = self.store.load()
        if not payload or not isinstance(self.transport, UrllibTransport):
            return
        for item in payload.get("cookies", []):
            if not isinstance(item, dict) or not item.get("name"):
                continue
            try:
                cookie = http.cookiejar.Cookie(
                    version=0, name=str(item["name"]), value=str(item.get("value", "")),
                    port=None, port_specified=False, domain=str(item.get("domain", "")),
                    domain_specified=bool(item.get("domain")), domain_initial_dot=False,
                    path=str(item.get("path", "/")), path_specified=True,
                    secure=False, expires=None, discard=True, comment=None, comment_url=None,
                    rest={"HttpOnly": None}, rfc2109=False,
                )
                self.transport.cookie_jar.set_cookie(cookie)
            except Exception:
                continue
        with self.state_lock:
            self._status = "待验证"
        self._needs_restore_verification = True

    def _persist(self) -> None:
        if not isinstance(self.transport, UrllibTransport):
            return
        cookies = [_cookie_to_dict(c) for c in self.transport.cookie_jar]
        self.store.save({"cookies": cookies, "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})

    def status(self) -> dict[str, Any]:
        with self.state_lock:
            if self._authenticated:
                session_mode = "authenticated_session"
            elif self._needs_restore_verification and self.store.path.exists():
                session_mode = "restored_session_pending"
            elif self.credentials is not None:
                session_mode = "credentials_configured"
            else:
                session_mode = "anonymous_feed"
            return {
                "status": self._status,
                "authenticated": bool(self._authenticated),
                "session_mode": session_mode,
                "last_auth_at": self._last_auth_at,
                "last_response_at": self._last_response_at,
                "error": self._last_error,
                "username_configured": bool(self.credentials and self.credentials.username),
                "session_persisted": self.store.path.exists(),
            }

    def _set_error(self, status: str, error: str | None = None) -> None:
        with self.state_lock:
            self._status, self._last_error = status, error

    def _has_session_cookie(self) -> bool:
        """Return whether the transport received a non-empty session cookie."""
        cookie_jar = getattr(self.transport, "cookie_jar", None)
        if cookie_jar is None:
            return False
        try:
            return any(bool(getattr(cookie, "value", "")) for cookie in cookie_jar)
        except TypeError:
            return False

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": self.credentials.base_url if self.credentials else DEFAULT_BASE_URL,
            "Referer": (self.credentials.base_url if self.credentials else DEFAULT_BASE_URL) + "/",
        }

    def authenticate(self, *, force: bool = False) -> dict[str, Any]:
        with self.auth_lock:
            # 即使多个请求同时发现旧会话失效，先完成认证的线程会让后续线程
            # 直接复用结果，避免 force 重试造成重复登录。需要彻底重新登录时
            # 先调用 clear_session() 或通过 configure() 创建新的管理器。
            if self._authenticated:
                return self.status()
            if self.credentials is None:
                self._set_error("未配置", "请配置 PINNACLE_USERNAME 和 PINNACLE_PASSWORD")
                raise AuthenticationRequired()
            self._set_error("登录中", None)
            payload = {"username": self.credentials.username, "password": self.credentials.password}
            if self.credentials.extra:
                payload.update(dict(self.credentials.extra))
            headers = self._headers()
            headers["Content-Type"] = "application/json"
            try:
                response = self.transport.request("POST", self.credentials.resolved_auth_url,
                                                  headers=headers, data=json.dumps(payload).encode(), timeout=self.timeout)
            except (TimeoutError, urllib.error.URLError, OSError) as exc:
                self._set_error("上游不可用", type(exc).__name__)
                raise PinnacleSessionError("平博认证请求超时或网络不可用", "upstream_error") from exc
            parsed = parse_auth_result(response)
            if parsed.get("challenge"):
                self._set_error("等待验证", "需要人工完成验证码或二次验证")
                raise AuthenticationChallenge()
            if not parsed.get("authenticated"):
                # PS3838 当前认证接口成功时可能只返回数字 0；真正的成功信号
                # 在同一响应下发的会话 Cookie，以及随后可读取的赔率快照中。
                scalar_zero = parsed.get("raw_kind") == "scalar" and parsed.get("raw_value") == "0"
                if scalar_zero and self._has_session_cookie() and self.verify_session():
                    self._last_auth_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    self._set_error("已认证", None)
                    return self.status()
                self._set_error("认证失败", "平博认证响应未返回明确成功状态")
                raise AuthenticationFailed()
            self._authenticated = True
            self._last_auth_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            self._set_error("已认证", None)
            self._persist()
            return self.status()

    def verify_session(self) -> bool:
        # A saved CookieJar can be checked after a restart even when the
        # password is intentionally not available in the environment.  If the
        # cookie is invalid, a later request will raise AuthenticationRequired
        # and the operator can provide credentials through the login form.
        if self.credentials is None and not self._needs_restore_verification and not self.store.path.exists():
            return False
        try:
            odds_url = (self.credentials.resolved_odds_url if self.credentials
                        else urllib.parse.urljoin(DEFAULT_BASE_URL.rstrip("/") + "/",
                                                  DEFAULT_ODDS_URL.lstrip("/")))
            response = self.transport.request("GET", odds_url,
                                              headers=self._headers(), timeout=self.timeout)
            status = _response_status(response)
            if status in {401, 403} or self._looks_unauthorized(response):
                self._authenticated = False
                self._set_error("会话过期", "赔率接口未授权")
                return False
            if 200 <= status < 300 and self._looks_like_odds_payload(response):
                self._authenticated = True
                if self._last_auth_at is None:
                    # 进程重启后没有把认证时间写入明文状态；首次用持久化
                    # Cookie 验证成功时记录本次有效会话的确认时间。
                    self._last_auth_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self._last_response_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self._set_error("已认证", None)
                self._persist()
                return True
            return False
        except Exception as exc:
            self._set_error("上游不可用", type(exc).__name__)
            return False

    @staticmethod
    def _looks_unauthorized(response: Any) -> bool:
        text = _response_text(response).lower()
        return any(marker in text for marker in (
            "unauthorized", "not authorized", "login required", "session expired",
            "login-form", "<html", "sign in",
        ))

    @staticmethod
    def _looks_like_odds_payload(response: Any) -> bool:
        """确认 2xx 响应确实是赔率快照，而不是登录页或 HTML。"""
        body = _safe_json_body(response)
        if not isinstance(body, dict):
            return False
        # 当前 compact/events 接口返回 n(未开始) 与 l(滚球) 两个数组。
        if isinstance(body.get("n"), list) or isinstance(body.get("l"), list):
            return True
        # 允许已经由代理层包装成赛事列表的只读响应，但不接受任意 JSON。
        events = body.get("events")
        return isinstance(events, list)

    def request(self, method: str, url: str | None = None, *, headers: Mapping[str, str] | None = None,
                data: bytes | str | Mapping[str, Any] | None = None, json_body: Any = None,
                timeout: float | None = None) -> HttpResponse:
        if self.credentials is None:
            if not self._needs_restore_verification and not self._authenticated:
                raise AuthenticationRequired()
        target = url or (self.credentials.resolved_odds_url if self.credentials
                         else urllib.parse.urljoin(DEFAULT_BASE_URL.rstrip("/") + "/",
                                                   DEFAULT_ODDS_URL.lstrip("/")))
        merged = self._headers()
        merged.update(dict(headers or {}))
        payload = data
        if json_body is not None:
            payload = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            merged.setdefault("Content-Type", "application/json")
        elif isinstance(payload, str):
            payload = payload.encode("utf-8")
        auth_attempted = False
        transient_attempts = 0
        while True:
            if not self._authenticated:
                if self._needs_restore_verification:
                    # 重启后先用保存的 Cookie 调只读赔率接口验证，只有失效时才重新认证。
                    self._needs_restore_verification = False
                    if not self.verify_session():
                        self.authenticate()
                else:
                    self.authenticate()
            try:
                response = self.transport.request(method, target, headers=merged, data=payload,
                                                  timeout=timeout or self.timeout)
            except (TimeoutError, urllib.error.URLError, OSError) as exc:
                if transient_attempts >= self.max_retries:
                    self._set_error("上游不可用", type(exc).__name__)
                    raise PinnacleSessionError("平博请求超时或网络不可用", "upstream_error") from exc
                delay = min(8.0, 0.5 * (2 ** transient_attempts) + random.random() * 0.1)
                transient_attempts += 1
                self.sleep_fn(delay)
                continue
            status = _response_status(response)
            self._last_response_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            if status in {401, 403} or self._looks_unauthorized(response):
                self._authenticated = False
                if auth_attempted:
                    self._set_error("会话过期", "重新认证后仍未获得赔率权限")
                    raise SessionExpired()
                auth_attempted = True
                self.authenticate(force=True)
                continue
            if status == 429 or status >= 500:
                if transient_attempts >= self.max_retries:
                    self._set_error("上游不可用", f"HTTP {status}")
                    raise RateLimited() if status == 429 else PinnacleSessionError(f"平博上游 HTTP {status}", "upstream_error")
                delay = min(8.0, 0.5 * (2 ** transient_attempts) + random.random() * 0.1)
                transient_attempts += 1
                self.sleep_fn(delay)
                continue
            if 200 <= status < 300:
                expected_odds_url = (self.credentials.resolved_odds_url if self.credentials
                                     else urllib.parse.urljoin(DEFAULT_BASE_URL.rstrip("/") + "/",
                                                               DEFAULT_ODDS_URL.lstrip("/")))
                if _same_odds_endpoint(target, expected_odds_url) and not self._looks_like_odds_payload(response):
                    self._set_error("赔率权限未验证", "赔率接口返回结构未识别")
                    raise PinnacleSessionError("平博赔率响应结构未识别，未确认赔率权限", "odds_unverified")
                self._authenticated = True
                self._set_error("已认证", None)
                self._persist()
            return response

    def get(self, url: str | None = None, **kwargs: Any) -> HttpResponse:
        return self.request("GET", url, **kwargs)

    def post_json(self, url: str | None = None, payload: Any = None, **kwargs: Any) -> HttpResponse:
        return self.request("POST", url, json_body=payload, **kwargs)

    def clear_session(self) -> None:
        with self.auth_lock:
            self._authenticated = False
            self.store.clear()
            if isinstance(self.transport, UrllibTransport):
                self.transport.cookie_jar.clear()
            self._set_error("未认证", None)


# 轻量进程级入口，供 app.py 的页面按钮和后台刷新复用同一个会话管理器。
_default_lock = threading.RLock()
_default_manager: PinnacleSessionManager | None = None


def get_manager() -> PinnacleSessionManager:
    global _default_manager
    with _default_lock:
        if _default_manager is None:
            _default_manager = PinnacleSessionManager()
        return _default_manager


def configure(*, username: str, password: str, base_url: str = DEFAULT_BASE_URL,
              auth_url: str | None = None, odds_url: str | None = None,
              extra: Mapping[str, Any] | None = None,
              session_path: str | os.PathLike[str] = "data/pinnacle_session.enc",
              transport: Any | None = None) -> PinnacleSessionManager:
    """配置当前项目自己的账号会话；密码只保存在进程内，不进入 public_state。"""
    global _default_manager
    if not str(username).strip() or not str(password):
        raise AuthenticationRequired("账号和密码不能为空")
    manager = PinnacleSessionManager(
        Credentials(str(username).strip(), str(password), base_url=base_url,
                    auth_url=auth_url, odds_url=odds_url, extra=extra),
        transport=transport, session_path=session_path,
    )
    with _default_lock:
        _default_manager = manager
    return manager


def login(*, force: bool = False) -> dict[str, Any]:
    return get_manager().authenticate(force=force)


def check() -> bool:
    return get_manager().verify_session()


def request(method: str, url: str | None = None, **kwargs: Any) -> HttpResponse:
    return get_manager().request(method, url, **kwargs)


def public_state() -> dict[str, Any]:
    return get_manager().status()


def logout() -> None:
    get_manager().clear_session()
