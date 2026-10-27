"""Read-only HG report collection, durable orders and bounded background polling.

The official account statement is the authority for settlement: WIN_GOLD is
stored and added directly.  Odds and match scores never determine the money.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import html
from http.client import HTTPException, IncompleteRead
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import ssl
import sqlite3
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, HTTPRedirectHandler, ProxyHandler, Request, build_opener


LOCAL_TZ = timezone(timedelta(hours=8))
UPSTREAM_TZ = timezone(timedelta(hours=-4))
_PROTOCOL_VALUE = re.compile(r'^[A-Za-z0-9._-]{1,96}$', re.ASCII)
_LANG_VALUE = re.compile(r'^[A-Za-z]{2,3}(?:[-_][A-Za-z]{2,4})?$', re.ASCII)
_LEGACY_VERSION = 'version-10-15'
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_REPORT_REQUESTS = 2000
_ORIGIN_THROTTLE_LOCK = threading.Lock()
_ORIGIN_NEXT_REQUEST = {}
_ACTION_KEYS = {'sid', 'su_user', 'aid', 'ag_user', 'mid', 'mem_user', 'report_date', 'report_daily', 'pay_type'}
_TAG = re.compile(r'<[^>]*>')
_SAFE_ROW_FIELDS = (
    'WAGERS_ID', 'DATETIME', 'LEAGUE', 'TEAM_H', 'TEAM_C', 'ORDER_TYPE',
    'WAGERS_TYPE', 'ORDER_CON', 'IORATIO', 'GOLD', 'WIN_GOLD', 'RESULT_WL', 'SETTLED',
    'CANCEL_MSG', 'ODDF_TYPE', 'GTYPE', 'WTYPE', 'RESULT', 'SCORE_H',
    'SCORE_C', 'SCORE', 'DIF_SCORE', 'G_TIME', 'GID', 'MEMBER', 'USERNAME', 'CURRENCY', 'DATE',
    'SHOWTEXT_TEAM', 'TNAME', 'WAGERSTYPE', 'MARKET',
)


def _now():
    return datetime.now(LOCAL_TZ)


def _iso(value=None):
    return (value or _now()).astimezone(LOCAL_TZ).isoformat(timespec='seconds')


def week_range(current=None):
    """Last Monday through this Sunday, in HG's UTC-4 business calendar.

    The report server uses its own natural weeks. In particular, Monday morning
    in Beijing is still Sunday in the backend and belongs to the earlier week.
    A bare date is treated as an already-selected backend business date.
    """
    current = current or _now()
    if isinstance(current, datetime):
        if current.tzinfo is None:
            current = current.replace(tzinfo=LOCAL_TZ)
        current = current.astimezone(UPSTREAM_TZ).date()
    monday = current - timedelta(days=current.weekday())
    return monday - timedelta(days=7), monday + timedelta(days=6)


def _money(value):
    try:
        # HG formats some official amounts as HTML. Use the same cleaned text
        # that is retained in raw fields, then require a complete number.
        text = _text(value).replace(',', '').strip()
        amount = Decimal(text)
        return amount if amount.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _official_push_marker(value):
    """Require HG's explicit ticket-level push wording, not a tied score."""
    text = _text(value, 80)
    return bool(re.match(r'^\s*注单(?:平局|和局|走水)(?=\s*(?:$|[（(：:]))', text))


def _official_text_marker(*values):
    """Return a bounded Chinese settlement marker supplied by HG itself."""
    for value in values:
        text = re.split(r'[（(]', _text(value, 80), maxsplit=1)[0].strip()
        if re.fullmatch(r'[\u3400-\u9fff ·、，：:]{2,40}', text):
            return text
    return ''


def _number(value):
    return float(value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)) if value is not None else None


def _text(value, limit=4096):
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        return ''
    return html.unescape(_TAG.sub(' ', str(value))).strip()[:limit]


def _safe_order_raw(raw):
    # These two bounded markup fields are evidence, never rendered as HTML.
    # Keep the blue score node so a saved wager can be parsed after restart.
    return {key: (raw[key][:4096] if key in ('SHOWTEXT_TEAM', 'TNAME')
                  and isinstance(raw[key], str) else _text(raw[key]))
            for key in _SAFE_ROW_FIELDS if key in raw}


class _BlueBetScore(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.active = False
        self.parts = []
        self.scores = []

    def handle_starttag(self, tag, attrs):
        if tag == 'tt':
            classes = dict(attrs).get('class') or ''
            self.active = 'word_blue' in classes.split()
            self.parts = []

    def handle_data(self, data):
        if self.active:
            self.parts.append(data)

    def handle_endtag(self, tag):
        if tag == 'tt' and self.active:
            match = re.fullmatch(r'\s*[(（]\s*(\d{1,3})\s*[-:：–—]\s*(\d{1,3})\s*[)）]\s*', ''.join(self.parts))
            if match:
                self.scores.append((int(match.group(1)), int(match.group(2))))
            self.active = False


def _html_bet_score(raw):
    for key in ('SHOWTEXT_TEAM', 'TNAME'):
        value = raw.get(key)
        if not isinstance(value, str):
            continue
        parser = _BlueBetScore()
        parser.feed(value[:4096])
        scores = set(parser.scores)
        if len(scores) == 1:
            return next(iter(scores))
    return None, None


def _official_day(value):
    """Parse a date from the two observed Crown report timestamp formats."""
    text = _text(value, 128)
    for pattern, fmt in (
        (r'^\d{1,2}-\d{1,2}-20\d{2}(?:\s|$)', '%m-%d-%Y'),
        (r'^20\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:\s|$)', '%Y-%m-%d'),
    ):
        if re.match(pattern, text):
            date_text = text.split()[0].replace('/', '-')
            try:
                return datetime.strptime(date_text, fmt).date()
            except ValueError:
                return None
    return None


def _score_pair(value):
    """Return a numeric score pair only when the source is an explicit pair."""
    text = _text(value, 64)
    match = re.fullmatch(r'\s*(\d{1,3})\s*[-:：–—]\s*(\d{1,3})\s*', text)
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)


def _score_component(value):
    text = _text(value, 16)
    return int(text) if re.fullmatch(r'\d{1,3}', text) else None


def _match_phase(raw, bet_date):
    """Use Crown's explicit wager label first, then compare its game date."""
    source = ' '.join(_text(raw.get(key), 256) for key in ('WAGERS_TYPE', 'WAGERSTYPE', 'WTYPE', 'GTYPE'))
    if (_text(raw.get('MARKET'), 32).lower() == 'rb'
            or re.search(r'滚球|在赛|in[\s_-]*play|\blive\b', source, re.I)):
        return 'live', '滚球'
    if re.search(r'今日|today', source, re.I):
        return 'today', '今日'
    if re.search(r'赛前|prematch|pre[\s_-]*match', source, re.I):
        return 'prematch', '赛前'
    game_day = _official_day(raw.get('G_TIME'))
    try:
        wager_day = date.fromisoformat(bet_date)
    except (ValueError, TypeError):
        wager_day = None
    if game_day and wager_day:
        if game_day == wager_day:
            return 'today', '今日'
        if game_day > wager_day:
            return 'prematch', '赛前'
    return '', ''


def _match_display_fields(raw, bet_date, *, allow_final_score):
    phase, label = _match_phase(raw, bet_date)
    score_home = _score_component(raw.get('SCORE_H'))
    score_away = _score_component(raw.get('SCORE_C'))
    if score_home is None or score_away is None:
        score_home, score_away = _score_pair(raw.get('SCORE'))
    if phase == 'live' and (score_home is None or score_away is None):
        score_home, score_away = _html_bet_score(raw)
    final_home = final_away = None
    if allow_final_score:
        result = _text(raw.get('RESULT_WL'), 80)
        match = re.search(r'(?:全场|完场|FT)\s*[:：]?\s*(\d{1,3})\s*[-:：–—]\s*(\d{1,3})', result, re.I)
        if match:
            final_home, final_away = int(match.group(1)), int(match.group(2))
    return {
        'matchPhase': phase, 'matchPhaseLabel': label,
        'isInPlay': phase == 'live', 'isToday': phase == 'today',
        'gameTime': _text(raw.get('G_TIME'), 128),
        'betScoreHome': score_home if phase == 'live' else None,
        'betScoreAway': score_away if phase == 'live' else None,
        'finalHome': final_home, 'finalAway': final_away,
    }


def _query_value(query, key):
    value = query.get(key, '')
    if isinstance(value, list):
        value = value[0] if value else ''
    return str(value).strip()


def _report_weeks(values):
    """Validate persisted official membership without changing bet timestamps."""
    weeks = set()
    if not isinstance(values, (list, tuple, set)):
        return weeks
    for value in values:
        try:
            day = date.fromisoformat(str(value))
        except (ValueError, TypeError):
            continue
        if day.weekday() == 0:
            weeks.add(day.isoformat())
    return weeks


def _official_filter_weeks(from_date, to_date, business_date=None):
    """Only whole official weeks or the current incomplete week use membership.

    Arbitrary day ranges retain the explicitly different bet-date semantics.
    """
    if not from_date or not to_date:
        return None
    try:
        start, end = date.fromisoformat(from_date), date.fromisoformat(to_date)
    except (ValueError, TypeError):
        return None
    current = business_date or _now().astimezone(UPSTREAM_TZ).date()
    if start > end or start.weekday() != 0 or (end.weekday() != 6 and end != current):
        return None
    weeks = set()
    cursor = start
    while cursor <= end:
        weeks.add(cursor.isoformat())
        cursor += timedelta(days=7)
    return weeks


def _matches_date_range(row, from_date, to_date, official_weeks=None):
    membership = _report_weeks(row.get('reportWeeks'))
    if official_weeks is not None and membership:
        return bool(membership & official_weeks)
    return ((not from_date or row['date'] >= from_date)
            and (not to_date or row['date'] <= to_date))


class CrownError(RuntimeError):
    """Only fixed, public-safe messages are exposed to API clients."""


class SessionExpired(CrownError):
    pass


class IncompleteReport(CrownError):
    pass


class TransientReadFailed(CrownError):
    """Bounded request retries failed; a cooperative reader may resume here."""
    def __init__(self, kind):
        self.kind = kind if kind in {
            'upstream_busy', 'tls_eof', 'tls_error', 'timeout', 'incomplete_response',
            'http_transport', 'network', 'http_transient', 'non_json_response',
            'invalid_response',
        } else 'network'
        super().__init__('皇冠报表暂时繁忙，稍后自动重试' if self.kind == 'upstream_busy'
                         else '皇冠报表暂时无法读取，稍后自动重试')


class _RetryableRead(CrownError):
    def __init__(self, message, *, kind='upstream_busy', http_status=None, exception=None):
        super().__init__(message)
        self.kind = kind
        self.http_status = http_status
        self.exception_name = exception


class _RejectedRead(CrownError):
    def __init__(self, message, *, kind='upstream_rejected', http_status=None, exception=None):
        super().__init__(message)
        self.kind = kind
        self.http_status = http_status
        self.exception_name = exception


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        # A report endpoint does not need redirects. Never forward UID/cookies.
        return None


class _SameOriginCookies(DefaultCookiePolicy):
    def __init__(self, origin):
        super().__init__()
        self.origin = origin

    def _allowed(self, cookie, request):
        parsed = urlsplit(request.full_url)
        if parsed.scheme != 'https' or 'https://' + (parsed.hostname or '') != self.origin or parsed.port not in (None, 443):
            return False
        host, domain = parsed.hostname, cookie.domain.lower()
        return host == domain or (domain.startswith('.') and (host == domain[1:] or host.endswith(domain)))

    def set_ok(self, cookie, request):
        return self._allowed(cookie, request) and super().set_ok(cookie, request)

    def return_ok(self, cookie, request):
        return self._allowed(cookie, request) and super().return_ok(cookie, request)


def _cookie_path_matches(request_path, cookie_path):
    """Apply RFC 6265 path-match for the report endpoint."""
    request_path = request_path or '/'
    cookie_path = cookie_path or '/'
    if not cookie_path.startswith('/'):
        return False
    return (request_path == cookie_path
            or (request_path.startswith(cookie_path)
                and (cookie_path.endswith('/') or request_path[len(cookie_path):].startswith('/'))))


class CrownClient:
    """Small authenticated report client; all operations are read-only POSTs."""

    supports_future_pending = True

    def __init__(self, account, session, timeout=20, transport=None, sleep_fn=None, diagnostic_observer=None):
        self.account = account
        self.session = session or {}
        self.timeout = min(30, max(1, timeout))
        self.transport = transport
        self._sleep = sleep_fn or time.sleep
        self._last_request_at = None
        self.diagnostics = []
        self.last_diagnostic = None
        self._diagnostic_observer = diagnostic_observer
        self._last_http_status = None
        self.recent_wagers = []
        origin = urlsplit(account.get('base_url', ''))
        if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password or origin.port not in (None, 443):
            raise CrownError('皇冠账号站点配置无效')
        self.origin = 'https://' + origin.hostname
        if self.session.get('base_url', self.origin).rstrip('/') != self.origin:
            raise CrownError('皇冠保存会话与站点不一致，请重新登录')
        self.uid = self.session.get('uid')
        if not isinstance(self.uid, str) or not self.uid or len(self.uid) > 2048:
            raise CrownError('皇冠没有可用会话，请先登录')
        protocol = self.session.get('admin_protocol')
        self.legacy_protocol = not isinstance(protocol, dict)
        if self.legacy_protocol:
            # Keep existing encrypted sessions usable until the next browser check
            # captures the live page values; never treat their rejection as logout.
            self.ver, self.langx = _LEGACY_VERSION, 'zh-cn'
            self.layer = account.get('login_layer') or 'su'
        else:
            if protocol.get('origin') != self.origin:
                raise CrownError('皇冠会话协议来源与当前站点不一致，请在“我的”重新验证后同步')
            self.ver = protocol.get('ver')
            self.langx = protocol.get('langx')
            if (not isinstance(self.ver, str) or not _PROTOCOL_VALUE.fullmatch(self.ver)
                    or not isinstance(self.langx, str) or not _LANG_VALUE.fullmatch(self.langx)):
                raise CrownError('皇冠会话缺少有效的当前页面协议版本，请在“我的”重新验证后同步')
            self.layer = protocol.get('login_layer') or account.get('login_layer') or 'su'
        allowed_headers = {
            'user-agent', 'accept', 'sec-ch-ua', 'sec-ch-ua-mobile', 'sec-ch-ua-platform',
            'sec-fetch-site', 'sec-fetch-mode', 'sec-fetch-dest', 'x-requested-with',
        }
        protocol_headers = self.session.get('admin_protocol_headers', {})
        self.protocol_headers = {}
        if isinstance(protocol_headers, dict):
            for key, value in protocol_headers.items():
                key = str(key).lower()
                if (key in allowed_headers and isinstance(value, str) and value and len(value) <= 512
                        and not any(ord(char) < 32 or ord(char) == 127 for char in value)):
                    self.protocol_headers[key] = value
        if (not self.legacy_protocol and not self.protocol_headers.get('user-agent')
                and isinstance(protocol.get('user_agent'), str)
                and 0 < len(protocol['user_agent']) <= 512
                and not any(ord(char) < 32 or ord(char) == 127 for char in protocol['user_agent'])):
            self.protocol_headers['user-agent'] = protocol['user_agent']
        if self.layer not in ('su', 'ag', 'co'):
            raise CrownError('皇冠账号登录层级无效')
        self._requests = 0
        self._cookies = CookieJar(policy=_SameOriginCookies(self.origin))
        self._restore_cookies()
        self._opener = build_opener(ProxyHandler({}), _NoRedirects(), HTTPCookieProcessor(self._cookies))

    def _restore_cookies(self):
        host = urlsplit(self.origin).hostname
        cookies = self.session.get('storage_state', {}).get('cookies', [])
        for cookie in cookies if isinstance(cookies, list) else []:
            if not isinstance(cookie, dict):
                continue
            if cookie.get('partitionKey'):
                continue
            domain = str(cookie.get('domain', '')).lower()
            # Host-only cookies cannot be broadened to sibling subdomains.
            matches = host == domain or (domain.startswith('.') and (host == domain[1:] or host.endswith(domain)))
            path = str(cookie.get('path') or '/')
            if not matches or not _cookie_path_matches('/transform.php', path):
                continue
            expires = cookie.get('expires', -1)
            if isinstance(expires, (int, float)) and 0 < expires <= time.time():
                continue
            name, value = cookie.get('name'), cookie.get('value')
            if not isinstance(name, str) or not isinstance(value, str):
                continue
            if not re.fullmatch(r'[!#$%&\x27*+.^_`|~0-9A-Za-z-]+', name) or any(x in value for x in ('\r', '\n', ';')):
                continue
            rest = {}
            if cookie.get('httpOnly') is True:
                rest['HttpOnly'] = None
            same_site = str(cookie.get('sameSite', '')).capitalize()
            if same_site in ('Lax', 'Strict', 'None'):
                rest['SameSite'] = same_site
            self._cookies.set_cookie(Cookie(
                version=0, name=name, value=value, port=None, port_specified=False,
                domain=domain, domain_specified=domain.startswith('.'), domain_initial_dot=domain.startswith('.'),
                path=path, path_specified=True,
                secure=bool(cookie.get('secure', True)),
                expires=expires if isinstance(expires, (int, float)) and expires > 0 else None,
                discard=not isinstance(expires, (int, float)) or expires <= 0,
                comment=None, comment_url=None, rest=rest, rfc2109=False,
            ))

    def _cookie_header(self):
        request = Request(self.origin + '/transform.php')
        self._cookies.add_cookie_header(request)
        return request.get_header('Cookie', '')

    def session_updates(self):
        """Return rotated report cookies while preserving unrelated browser state."""
        storage_state = self.session.get('storage_state')
        if not isinstance(storage_state, dict):
            return {}
        host = urlsplit(self.origin).hostname

        def report_cookie(cookie):
            if not isinstance(cookie, dict):
                return False
            if cookie.get('partitionKey'):
                return False
            domain = str(cookie.get('domain', '')).lower()
            path = str(cookie.get('path') or '/')
            matches = host == domain or (domain.startswith('.') and (host == domain[1:] or host.endswith(domain)))
            return matches and _cookie_path_matches('/transform.php', path)

        cookies = [dict(cookie) for cookie in storage_state.get('cookies', [])
                   if isinstance(cookie, dict) and not report_cookie(cookie)]
        endpoint_path = '/transform.php'
        for cookie in self._cookies:
            if cookie.is_expired() or not _cookie_path_matches(endpoint_path, cookie.path or '/'):
                continue
            domain = str(cookie.domain or '').lower()
            if not (host == domain or (domain.startswith('.') and (host == domain[1:] or host.endswith(domain)))):
                continue
            item = {
                'name': cookie.name,
                'value': cookie.value,
                'domain': cookie.domain,
                'path': cookie.path or '/',
                'expires': cookie.expires if cookie.expires else -1,
                'httpOnly': cookie.has_nonstandard_attr('HttpOnly'),
                'secure': bool(cookie.secure),
            }
            same_site = str(cookie._rest.get('SameSite', '')).capitalize()
            if same_site in ('Lax', 'Strict', 'None'):
                item['sameSite'] = same_site
            cookies.append(item)

        old_cookies = [dict(cookie) for cookie in storage_state.get('cookies', [])
                       if isinstance(cookie, dict)]
        order = lambda item: (item.get('domain', ''), item.get('path', ''), item.get('name', ''))
        if sorted(cookies, key=order) == sorted(old_cookies, key=order):
            return {}
        return {'storage_state': {**storage_state, 'cookies': cookies}}

    def begin_cycle(self):
        self._requests = 0

    def request(self, operation, **values):
        for attempt in range(3):
            # The report server rejects bursts as m_rep. Keep neighboring reads
            # apart and retry only bounded, read-only transport/busy failures.
            if self.transport is None:
                # Sleep while holding the reservation: a delayed worker must
                # not burst alongside a worker with a later reservation.
                with _ORIGIN_THROTTLE_LOCK:
                    clock = time.monotonic()
                    pause = _ORIGIN_NEXT_REQUEST.get(self.origin, clock) - clock
                    if pause > 0:
                        self._sleep(pause)
                    _ORIGIN_NEXT_REQUEST[self.origin] = time.monotonic() + 0.7
            self._last_request_at = time.monotonic()
            self._last_http_status = None
            try:
                result = self._request_once(operation, **values)
            except _RetryableRead as exc:
                self._record_diagnostic(operation, attempt + 1, exc.kind,
                                        http_status=exc.http_status, exception=exc.exception_name)
                if attempt == 2:
                    raise TransientReadFailed(exc.kind) from None
                self._sleep(2 ** attempt)
            except _RejectedRead as exc:
                self._record_diagnostic(operation, attempt + 1, exc.kind,
                                        http_status=exc.http_status, exception=exc.exception_name)
                raise
            except SessionExpired:
                self._record_diagnostic(operation, attempt + 1, 'session_rejected')
                raise
            except CrownError:
                self._record_diagnostic(operation, attempt + 1, 'invalid_report')
                raise
            else:
                self._record_diagnostic(operation, attempt + 1, 'success')
                return result

    def _record_diagnostic(self, operation, attempt, kind, *, http_status=None, exception=None):
        # Never retain request fields, exception text, headers or response data.
        # An observer may publish these bounded facts without exposing a UID or
        # an upstream error body that embeds credentials/order information.
        diagnostic = {
            'operation': operation if operation in (
                'get_report_su', 'get_report_ag', 'get_report_mem',
                'get_report_list_bet', 'get_wmc_list_bet') else 'unsupported',
            'kind': kind,
            'attempt': attempt,
            'seconds': round(max(0, time.monotonic() - self._last_request_at), 3),
        }
        status = http_status if http_status is not None else self._last_http_status
        if isinstance(status, int):
            diagnostic['http_status'] = status
        if exception:
            diagnostic['exception'] = exception
        self.last_diagnostic = diagnostic
        self.diagnostics.append(diagnostic)
        del self.diagnostics[:-50]
        if self._diagnostic_observer is not None:
            try:
                self._diagnostic_observer(dict(diagnostic))
            except Exception:
                # Recording diagnostics cannot interrupt a successful read.
                pass

    def _request_once(self, operation, **values):
        if operation not in ('get_report_su', 'get_report_ag', 'get_report_mem', 'get_report_list_bet', 'get_wmc_list_bet'):
            raise CrownError('不支持的皇冠只读查询')
        self._requests += 1
        if self._requests > MAX_REPORT_REQUESTS:
            raise IncompleteReport('皇冠报表层级过多，本次同步尚未全部完成')
        fields = {'p': operation, 'uid': self.uid, 'login_layer': self.layer, 'langx': self.langx, 'ver': self.ver, **values}
        if self.transport is not None:
            result = self.transport(operation, fields)
        else:
            body = urlencode(fields).encode('utf-8')
            headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Accept': 'application/json',
                       'Origin': self.origin, 'Referer': self.origin + '/', 'User-Agent': 'Mozilla/5.0 HDReport/1.0'}
            headers.update({key.title(): value for key, value in self.protocol_headers.items()})
            request = Request(self.origin + '/transform.php?' + urlencode({'ver': self.ver}), data=body, headers=headers, method='POST')
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    self._last_http_status = getattr(response, 'status', None)
                    content = response.read(MAX_RESPONSE_BYTES + 1)
            except HTTPError as exc:
                try:
                    status = int(exc.code)
                except (TypeError, ValueError):
                    status = None
                # An HTTPError owns a response stream as well; release the
                # failed connection before retrying and never read its body.
                exc.close()
                retryable = status in (408, 429) or (status is not None and 500 <= status <= 599)
                error_type = _RetryableRead if retryable else _RejectedRead
                raise error_type(
                    '皇冠报表暂时无法读取，稍后自动重试' if retryable
                    else '皇冠报表请求被拒绝，请检查账号权限或站点状态',
                    kind='http_transient' if retryable else 'http_rejected',
                    http_status=status, exception=type(exc).__name__,
                ) from None
            except (URLError, OSError, HTTPException) as exc:
                reason = exc.reason if isinstance(exc, URLError) and isinstance(exc.reason, BaseException) else exc
                if isinstance(reason, ssl.SSLCertVerificationError):
                    raise _RejectedRead('皇冠站点安全证书验证失败，请检查站点配置',
                                        kind='tls_certificate', exception=type(reason).__name__) from None
                if isinstance(reason, ssl.SSLEOFError):
                    kind = 'tls_eof'
                elif isinstance(reason, ssl.SSLError):
                    kind = 'tls_error'
                elif isinstance(reason, TimeoutError):
                    kind = 'timeout'
                elif isinstance(reason, IncompleteRead):
                    kind = 'incomplete_response'
                elif isinstance(reason, HTTPException):
                    kind = 'http_transport'
                else:
                    kind = 'network'
                raise _RetryableRead('皇冠报表暂时无法读取，稍后自动重试',
                                     kind=kind, exception=type(reason).__name__) from None
            if len(content) > MAX_RESPONSE_BYTES:
                raise IncompleteReport('皇冠报表响应过大，本次同步尚未全部完成')
            try:
                result = json.loads(content)
            except (ValueError, UnicodeError):
                raise _RetryableRead('皇冠报表返回格式异常，稍后自动重试', kind='non_json_response') from None
        if not isinstance(result, dict):
            raise _RetryableRead('皇冠报表返回格式异常，稍后自动重试', kind='invalid_response')
        if str(result.get('code', '')) == '4X014':
            if self.legacy_protocol:
                raise CrownError('皇冠接口拒绝旧版请求上下文；保存的浏览器会话已保留，请在“我的”重新验证后同步')
            raise SessionExpired('皇冠接口已拒绝保存会话；原会话已保留，请在“我的”显式重新登录')
        if str(result.get('code', '')) == 'm_rep':
            raise _RetryableRead('皇冠报表暂时繁忙，稍后自动重试', kind='upstream_busy')
        status = str(result.get('status', '')).lower()
        if status in ('error', 'fail', 'failed', 'false') or result.get('status') is False:
            raise _RejectedRead('皇冠报表查询未成功，请检查账号权限或重新登录')
        return result

    @staticmethod
    def report_rows(document):
        data = document.get('data')
        if not isinstance(data, dict):
            raise IncompleteReport('皇冠报表数据不完整，本次同步尚未全部完成')
        array = data.get('array')
        if not isinstance(array, dict):
            raise IncompleteReport('皇冠报表层级不完整，本次同步尚未全部完成')
        if 'row0' not in array:
            display = array.get('display')
            if isinstance(display, dict) and display.get('LIST_NODATA') == '' and display.get('LIST_CLOSEDATA', 'none') == 'none':
                rows = []
            else:
                raise IncompleteReport('皇冠报表层级不完整，本次同步尚未全部完成')
        else:
            rows = array['row0']
        if rows is None:
            rows = []
        if isinstance(rows, dict):
            rows = [rows] if rows else []
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise IncompleteReport('皇冠报表行格式不完整，本次同步尚未全部完成')
        # An unobserved pagination mechanism must never silently drop the tail.
        total = array.get('total', data.get('total', document.get('total')))
        if isinstance(total, dict):
            if 'WCOUNT' in total:
                # Detail endpoints return all rows; the site's pagination is
                # entirely local. Validate their declared official order count.
                expected = _money(total['WCOUNT'])
                if expected is not None and expected != len(rows):
                    raise IncompleteReport('皇冠注单总数尚未对齐，本次同步尚未全部完成')
            elif 'WCOUNT0' in total:
                expected = _money(total['WCOUNT0'])
                actual = sum((_money(row.get('WCOUNT0')) or Decimal('0') for row in rows), Decimal('0'))
                if expected is not None and expected != actual:
                    raise IncompleteReport('皇冠报表层级总数尚未对齐，本次同步尚未全部完成')
        if isinstance(total, (int, float, str)):
            try:
                count = int(total)
            except (ValueError, TypeError):
                count = 0
            if count > len(rows):
                raise IncompleteReport('皇冠报表还有分页未读取，本次同步尚未全部完成')
        return rows

    def _history_read(self, operation, values, cooperative):
        while True:
            try:
                return self.request(operation, **values)
            except TransientReadFailed as exc:
                if not cooperative:
                    raise
                # The suspended frame owns the original read-only parameters.
                # Yield only an enum; the service backs off this same request
                # without closing the hierarchy or retaining secrets in markers.
                yield [], {'_read_retry': exc.kind}

    def history(self, start, end, result_types=('Y', 'N'), report_kind='A', *, cooperative=False,
                include_future_pending=False):
        """Yield completed leaf batches, preserving good data on later failure."""
        start = start.isoformat() if isinstance(start, date) else str(start)
        end = end.isoformat() if isinstance(end, date) else str(end)
        # The official UI calls check_report before changing filters/pages, then
        # reads these same endpoints. Observed successful collection of all four
        # categories confirms check_report is not a required preparation step;
        # avoid adding an extra request to each already-working report read.
        for result_type in result_types:
            query_end = end
            if include_future_pending and report_kind == 'A' and result_type == 'N':
                # HG's pending report excludes future fixtures when date_end is
                # today, even if their wagers were placed today or earlier.
                # Read the complete official week; keep the original wager
                # timestamps for the page's bet-date filters.
                try:
                    first, last = date.fromisoformat(start), date.fromisoformat(end)
                except (ValueError, TypeError):
                    raise IncompleteReport('皇冠报表查询日期不完整') from None
                sunday = first + timedelta(days=6 - first.weekday())
                if last < first or last > sunday:
                    raise IncompleteReport('皇冠未结算报表需要按官方周分段读取')
                query_end = sunday.isoformat()
            common = {'report_kind': report_kind, 'result_type': result_type, 'date_start': start, 'date_end': query_end,
                      'gtype': '', 'wtype': ''}
            # The hierarchy below deliberately mirrors the official reports.
            # Direct aggregate leaf queries are not assumed to enumerate users.
            supers = self.report_rows((yield from self._history_read('get_report_su', common, cooperative)))
            if cooperative:
                yield [], {'_read_step': True}
            expected_count = sum((_money(row.get('WCOUNT0')) or Decimal('0') for row in supers), Decimal('0'))
            received_tickets = set()
            for superior in supers:
                sid = superior.get('SID0', superior.get('ID0', ''))
                su_user = superior.get('NAME0', '')
                if not sid or not su_user:
                    raise IncompleteReport('皇冠报表账号层级不完整，本次同步尚未全部完成')
                su = {**common, 'sid': sid, 'su_user': su_user, 'report_date': start, 'report_daily': '', **self.action_fields(superior)}
                agents = self.report_rows((yield from self._history_read('get_report_ag', su, cooperative)))
                if cooperative:
                    yield [], {'_read_step': True}
                for agent in agents:
                    aid = agent.get('AID0', agent.get('ID0', ''))
                    ag_user = agent.get('NAME0', '')
                    if not aid or not ag_user:
                        raise IncompleteReport('皇冠报表代理层级不完整，本次同步尚未全部完成')
                    ag = {**su, 'aid': aid, 'ag_user': ag_user, 'pay_type': '0', **self.action_fields(agent)}
                    members = self.report_rows((yield from self._history_read('get_report_mem', ag, cooperative)))
                    if cooperative:
                        yield [], {'_read_step': True}
                    for member in members:
                        mid, mem_user = member.get('ID0'), member.get('NAME0', '')
                        if not mid or not mem_user:
                            raise IncompleteReport('皇冠报表会员层级不完整，本次同步尚未全部完成')
                        leaf = {**ag, 'mid': mid, 'mem_user': mem_user, **self.action_fields(member)}
                        orders = self.report_rows((yield from self._history_read('get_report_list_bet', leaf, cooperative)))
                        received_tickets.update(str(row.get('WAGERS_ID', '')) for row in orders if row.get('WAGERS_ID'))
                        yield orders, {'member': str(mem_user), 'agent': str(ag_user), 'result_type': result_type,
                                       'report_kind': report_kind, 'report_from': start, 'report_to': query_end}
            if expected_count != len(received_tickets):
                raise IncompleteReport('皇冠分层注单总数尚未对齐，本次同步尚未全部完成')

    @staticmethod
    def action_fields(row):
        """Accept only hierarchy parameters; session/p/host come from this client."""
        action = row.get('ACTION0', '')
        if not isinstance(action, str):
            return {}
        action = html.unescape(action)
        query = urlsplit(action).query if '?' in action else action.lstrip('&')
        parsed = parse_qs(query, keep_blank_values=True)
        return {key: values[0] for key, values in parsed.items() if key in _ACTION_KEYS and values and len(values[0]) <= 512}

    def poll(self, checkpoint='0'):
        previous = str(checkpoint or '0')
        result = self.request('get_wmc_list_bet', totalBets='wmc', gtype='ALL', sel_maxid=str(checkpoint or '0'))
        wagers = result.get('wagers')
        if not isinstance(wagers, list):
            raise CrownError('皇冠实时订单返回格式异常，稍后自动重试')
        # Retain bounded score evidence until the matching report row arrives.
        recent = {str(row.get('TID') or row.get('WAGERS_ID') or ''): row
                  for row in self.recent_wagers}
        for wager in wagers:
            if not isinstance(wager, dict):
                continue
            ticket = _text(wager.get('TID') or wager.get('WAGERS_ID'), 128)
            if ticket:
                recent[ticket] = {'TID': ticket, **_safe_order_raw(wager)}
        self.recent_wagers = list(recent.values())[-1000:]
        # maxid is a notification checkpoint only, not proof of a complete ledger.
        checkpoint = result.get('maxid', checkpoint)
        checkpoint = str(checkpoint or '0')[:128]
        return checkpoint, bool(wagers) or checkpoint != previous


def normalize_order(raw, account, context=None, fallback_date=''):
    context = context or {}
    report_kind = context.get('report_kind') or 'A'
    if report_kind not in ('A', 'D', 'D4'):
        report_kind = 'A'
    report_weeks = set()
    if context.get('report_from') and context.get('report_to'):
        try:
            report_start = date.fromisoformat(context['report_from'])
            report_end = date.fromisoformat(context['report_to'])
            report_monday = report_start - timedelta(days=report_start.weekday())
            if report_start > report_end or report_end > report_monday + timedelta(days=6):
                raise ValueError('cross-week report')
            report_weeks.add(report_monday.isoformat())
        except (ValueError, TypeError):
            raise IncompleteReport('皇冠报表归属范围不完整，本次同步尚未全部完成') from None
    ticket = _text(raw.get('WAGERS_ID'), 128)
    if not ticket:
        raise IncompleteReport('皇冠注单缺少单号，本次同步尚未全部完成')
    bet_time = _text(raw.get('DATETIME'), 128)
    day_match = re.search(r'(20\d{2})[-/](\d{1,2})[-/](\d{1,2})', bet_time)
    bet_date = ''
    if day_match:
        try:
            bet_date = date(*(int(x) for x in day_match.groups())).isoformat()
        except ValueError:
            pass
    if not bet_date:
        # Daily official report queries supply the upstream business date.
        bet_date = fallback_date
    if not bet_date:
        raise IncompleteReport('皇冠注单时间不完整，本次同步尚未全部完成')
    stake = _money(raw.get('GOLD'))
    profit = _money(raw.get('WIN_GOLD'))
    settled_value = str(raw.get('SETTLED', '')).strip().upper()
    explicit_pending = settled_value in ('N', '0', 'FALSE', 'NO')
    result_value = _text(raw.get('RESULT_WL'), 80)
    reported_cancel = _text(raw.get('CANCEL_MSG'), 256)
    if reported_cancel.upper() in ('0', '-', '--', 'N', 'NONE', 'NULL', 'FALSE'):
        reported_cancel = ''
    settled = settled_value in ('Y', '1', 'TRUE', 'YES')
    if explicit_pending:
        settled = False
    elif not settled_value:
        settled = context.get('result_type') == 'Y' and profit is not None
    # Keep HG's original message separate from the generated D-report fallback.
    # A live wager can contain “注单平局 取消” while SETTLED is still N; it is
    # not a settlement and must not become a push or a cancellation label.
    official_marker = _official_text_marker(reported_cancel, raw.get('WIN_GOLD'), result_value)
    official_push = (settled and report_kind == 'A' and not reported_cancel
                     and _official_push_marker(raw.get('WIN_GOLD'))
                     and _official_push_marker(result_value))
    if profit is None and official_push:
        profit = Decimal('0')
    cancel = reported_cancel
    if report_kind == 'D':
        cancel = cancel or '皇冠官方取消注单'
    if report_kind == 'D4':
        # An abnormal report category is distinct from ordinary pending bets.
        # Its category does not itself establish a monetary settlement.
        result, label = 'abnormal', '非正常投注单'
    elif explicit_pending and report_kind == 'A':
        result, label = 'pending', '未结算'
    elif cancel:
        result, label = 'cancelled', '已取消'
        settled = True
    elif not settled:
        result, label = 'pending', '未结算'
    elif profit is None:
        result, label = 'unknown', official_marker or '已结算'
    elif profit > 0:
        result, label = ('win_half', '赢半') if '半赢' in result_value or '赢半' in result_value else ('win', '赢')
    elif profit < 0:
        result, label = ('lose_half', '输半') if '半输' in result_value or '输半' in result_value else ('lose', '输')
    else:
        result, label = 'push', '走水'
    if official_marker and not explicit_pending and (settled or not settled_value):
        label = official_marker
    if report_kind == 'D':
        settlement_reason = reported_cancel or '皇冠官方取消注单'
    elif explicit_pending and report_kind == 'A':
        settlement_reason = '等待皇冠官方结算'
    elif reported_cancel:
        settlement_reason = reported_cancel
    elif official_marker and not settled:
        settlement_reason = '皇冠官方状态：' + official_marker
    elif official_marker and settled and profit is None:
        settlement_reason = '皇冠官方明确标记为“' + official_marker + '”，金额字段未提供数值'
    elif official_push and settled:
        settlement_reason = '皇冠官方明确标记注单平局，净盈亏按0计'
    elif settled:
        settlement_reason = '官方净输赢，包含站点结算口径'
    else:
        settlement_reason = '等待皇冠官方结算'
    # WAGERS_TYPE is the market (e.g. 大 / 小 / 让球). ORDER_TYPE is
    # the actual selection (大 / 小 / team name), not a market category.
    official_market = _text(raw.get('WAGERS_TYPE'), 256)
    selection = _text(raw.get('ORDER_TYPE'), 512)
    play = official_market or selection
    market = play or '其他'
    compact_market = re.sub(r'\s+', '', play).replace('／', '/')
    if '大/小' in compact_market or '大小球' in compact_market:
        market = '大小球'
    for candidate in ('让球', '让分', '独赢', '角球', '波胆', '单双', '串关', '过关'):
        if candidate in play:
            market = candidate
            break
    pick = _text(raw.get('ORDER_CON'))
    odds = _text(raw.get('IORATIO'), 128)
    if official_market:
        if selection and not pick.startswith(selection):
            pick = ' '.join(part for part in (selection, pick) if part)
        if odds and '@' not in pick:
            pick += (' ' if pick else '') + '@' + odds
    home, away = _text(raw.get('TEAM_H'), 512), _text(raw.get('TEAM_C'), 512)
    final_score_settled = (
        report_kind == 'A' and not explicit_pending and (
            settled_value in ('Y', '1', 'TRUE', 'YES')
            or (not settled_value and context.get('result_type') == 'Y' and profit is not None)
        )
    )
    match_display = _match_display_fields(raw, bet_date, allow_final_score=final_score_settled)
    return {
        'ticket': ticket, 'accountId': account['id'], 'backend': account.get('name') or account.get('username') or '皇冠',
        'reportKind': report_kind,
        'reportWeeks': sorted(report_weeks),
        'member': _text(context.get('member'), 128), 'agent': _text(context.get('agent'), 128),
        'matchPhase': match_display['matchPhase'], 'matchPhaseLabel': match_display['matchPhaseLabel'],
        'isInPlay': match_display['isInPlay'], 'isToday': match_display['isToday'],
        'gameTime': match_display['gameTime'],
        'status': label, 'play': play, 'market': market, 'period': '',
        'pick': pick, 'league': _text(raw.get('LEAGUE'), 512),
        'home': home, 'away': away, 'match': ' v '.join(x for x in (home, away) if x),
        'scoreHome': raw.get('SCORE_H'), 'scoreAway': raw.get('SCORE_C'),
        'betScoreHome': match_display['betScoreHome'], 'betScoreAway': match_display['betScoreAway'],
        'odds': odds, 'oddsFormat': _text(raw.get('ODDF_TYPE'), 64),
        'betTime': bet_time, 'date': bet_date, 'sortTime': bet_time or bet_date,
        'settled': settled, 'stake': str(stake) if stake is not None else None,
        'profit': str(profit) if settled and profit is not None else None,
        'settlement': {'result': result, 'label': label, 'source': '皇冠官方账单',
                       'reason': settlement_reason,
                       'odds': _text(raw.get('IORATIO'), 128), 'officialResult': result_value,
                       'finalHome': match_display['finalHome'], 'finalAway': match_display['finalAway']},
        'raw': _safe_order_raw(raw),
    }


class CrownOrderStore:
    """Own tables only; never reinitialize account/vault storage."""

    def __init__(self, directory):
        self.path = Path(directory) / 'crown.sqlite3'
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as connection:
            connection.executescript('''
                CREATE TABLE IF NOT EXISTS crown_live_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL, ticket TEXT NOT NULL,
                    bet_date TEXT NOT NULL, bet_time TEXT NOT NULL,
                    content TEXT NOT NULL, first_seen TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(account_id,ticket)
                );
                CREATE INDEX IF NOT EXISTS crown_live_orders_date ON crown_live_orders(bet_date,bet_time);
                CREATE TABLE IF NOT EXISTS crown_live_sync (
                    account_id TEXT PRIMARY KEY, checkpoint TEXT NOT NULL DEFAULT '0',
                    range_from TEXT NOT NULL DEFAULT '', range_to TEXT NOT NULL DEFAULT '',
                    last_synced TEXT NOT NULL DEFAULT '', last_attempt TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'idle', last_error TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 0
                );
            ''')
            if 'coverage' not in {row['name'] for row in connection.execute('PRAGMA table_info(crown_live_sync)')}:
                connection.execute("ALTER TABLE crown_live_sync ADD COLUMN coverage TEXT NOT NULL DEFAULT '{}'")
            self._migrate_official_amounts(connection)
            self._migrate_official_settlement_text(connection)
            self._migrate_match_display_fields(connection)

    @staticmethod
    def _migrate_official_amounts(connection):
        """Recover missing stakes only from numeric persisted official GOLD."""
        for stored in connection.execute('SELECT id,content FROM crown_live_orders').fetchall():
            row = json.loads(stored['content'])
            raw = row.get('raw') or {}
            value = _money(raw.get('GOLD'))
            if row.get('stake') is None and value is not None:
                row['stake'] = str(value)
                content = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                connection.execute('UPDATE crown_live_orders SET content=?,updated_at=? WHERE id=?',
                                   (content, _iso(), stored['id']))

    @staticmethod
    def _migrate_official_settlement_text(connection):
        """Use HG's stored status text and correct only explicit push values."""
        rows = connection.execute('SELECT id,content FROM crown_live_orders').fetchall()
        for stored in rows:
            row = json.loads(stored['content'])
            raw = row.get('raw') or {}
            reported_cancel = _text(raw.get('CANCEL_MSG'), 256)
            if reported_cancel.upper() in ('0', '-', '--', 'N', 'NONE', 'NULL', 'FALSE'):
                reported_cancel = ''
            official_marker = _official_text_marker(reported_cancel, raw.get('WIN_GOLD'), raw.get('RESULT_WL'))
            if not official_marker and row.get('status') == '已结算·金额待确认':
                official_marker = '已结算'
            report_kind = row.get('reportKind', 'A')
            settled_value = str(raw.get('SETTLED', '')).strip().upper()
            # A live A-category row can carry transient text such as
            # “注单平局 取消” while the upstream settlement flag is N.
            # Repair any label previously inferred from that text, but keep
            # the raw fields intact for audit and the next official sync.
            if report_kind == 'A' and settled_value in ('N', '0', 'FALSE', 'NO'):
                row['settled'] = False
                row['profit'] = None
                row['status'] = '未结算'
                settlement = row.setdefault('settlement', {})
                settlement.update(result='pending', label='未结算', reason='等待皇冠官方结算')
                content = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                if content != json.dumps(json.loads(stored['content']), ensure_ascii=False,
                                         sort_keys=True, separators=(',', ':')):
                    connection.execute('UPDATE crown_live_orders SET content=?,updated_at=? WHERE id=?',
                                       (content, _iso(), stored['id']))
                continue
            if not official_marker:
                continue
            official_push = (row.get('settled') and report_kind == 'A' and not reported_cancel
                             and _official_push_marker(raw.get('WIN_GOLD'))
                             and _official_push_marker(raw.get('RESULT_WL')))
            before = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            if official_push:
                row['profit'] = '0'
                row['status'] = official_marker
                result = 'push'
                reason = '皇冠官方明确标记注单平局，净盈亏按0计'
            else:
                row['status'] = official_marker
                result = row.get('settlement', {}).get('result') or ('unknown' if row.get('settled') else 'pending')
                previous_reason = row.get('settlement', {}).get('reason') or ''
                if report_kind == 'D':
                    reason = reported_cancel or '皇冠官方取消注单'
                elif row.get('settled') and row.get('profit') is None and official_marker != '已结算':
                    reason = '皇冠官方明确标记为“' + official_marker + '”，金额字段未提供数值'
                elif row.get('settled') and row.get('profit') is None:
                    reason = '皇冠官方已结算，未提供盈亏数值'
                else:
                    reason = previous_reason or '皇冠官方状态：' + official_marker
            settlement = row.setdefault('settlement', {})
            settlement.update(result=result, label=official_marker, reason=reason)
            content = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            if before != content:
                connection.execute('UPDATE crown_live_orders SET content=?,updated_at=? WHERE id=?',
                                   (content, _iso(), stored['id']))

    @staticmethod
    def _migrate_match_display_fields(connection):
        """Backfill safe phase and score fields on orders already in the ledger."""
        rows = connection.execute('SELECT id,content FROM crown_live_orders').fetchall()
        for stored in rows:
            row = json.loads(stored['content'])
            raw = row.get('raw') or {}
            raw_settled = str(raw.get('SETTLED', '')).strip().upper()
            allow_final = (bool(row.get('settled')) and row.get('reportKind', 'A') == 'A'
                           and raw_settled not in ('N', '0', 'FALSE', 'NO'))
            display = _match_display_fields(raw, row.get('date', ''), allow_final_score=allow_final)
            before = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            if display['matchPhase']:
                row.update({key: display[key] for key in ('matchPhase', 'matchPhaseLabel', 'isInPlay', 'isToday')})
            elif not row.get('matchPhase'):
                row.update(matchPhase='', matchPhaseLabel='', isInPlay=False, isToday=False)
            if display['gameTime']:
                row['gameTime'] = display['gameTime']
            if display['betScoreHome'] is not None and display['betScoreAway'] is not None:
                row['betScoreHome'] = display['betScoreHome']
                row['betScoreAway'] = display['betScoreAway']
            elif not row.get('settled'):
                row['betScoreHome'] = display['betScoreHome']
                row['betScoreAway'] = display['betScoreAway']
            settlement = row.setdefault('settlement', {})
            if display['finalHome'] is not None and display['finalAway'] is not None:
                settlement['finalHome'] = display['finalHome']
                settlement['finalAway'] = display['finalAway']
            else:
                settlement['finalHome'] = settlement['finalAway'] = None
            after = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            if before != after:
                connection.execute('UPDATE crown_live_orders SET content=?,updated_at=? WHERE id=?',
                                   (after, _iso(), stored['id']))

    @contextmanager
    def _db(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA busy_timeout=10000')
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def upsert(self, account, raw_rows, context=None, fallback_date=''):
        inserted, updated = 0, 0
        rows = [normalize_order(row, account, context, fallback_date) for row in raw_rows]
        with self._db() as connection:
            for row in rows:
                old = connection.execute('SELECT content FROM crown_live_orders WHERE account_id=? AND ticket=?', (account['id'], row['ticket'])).fetchone()
                if old is not None:
                    previous = json.loads(old['content'])
                    old_weeks = _report_weeks(previous.get('reportWeeks'))
                    row['reportWeeks'] = sorted(old_weeks | _report_weeks(row.get('reportWeeks')))
                    self._preserve_bet_score(row, previous)
                content = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                if old is None:
                    connection.execute('INSERT INTO crown_live_orders(account_id,ticket,bet_date,bet_time,content,first_seen,updated_at) VALUES(?,?,?,?,?,?,?)',
                                       (account['id'], row['ticket'], row['date'], row['betTime'], content, _iso(), _iso()))
                    inserted += 1
                elif old['content'] != content:
                    connection.execute('UPDATE crown_live_orders SET bet_date=?,bet_time=?,content=?,updated_at=? WHERE account_id=? AND ticket=?',
                                       (row['date'], row['betTime'], content, _iso(), account['id'], row['ticket']))
                    updated += 1
        return inserted, updated

    @staticmethod
    def _preserve_bet_score(row, previous):
        """A later report may omit the score carried on the original wager."""
        if row.get('betScoreHome') is not None and row.get('betScoreAway') is not None:
            return
        if previous.get('betScoreHome') is None or previous.get('betScoreAway') is None:
            return
        row['betScoreHome'], row['betScoreAway'] = previous['betScoreHome'], previous['betScoreAway']
        row.update(matchPhase='live', matchPhaseLabel='滚球', isInPlay=True, isToday=False)
        raw = row.setdefault('raw', {})
        evidence = previous.get('raw') or {}
        for key in ('SHOWTEXT_TEAM', 'TNAME', 'WAGERSTYPE', 'MARKET', 'SCORE_H', 'SCORE_C', 'SCORE'):
            if key in evidence:
                raw[key] = evidence[key]

    def enrich_wagers(self, account, wagers):
        """Merge score evidence into account-scoped report rows, never money/state."""
        updated = 0
        with self._db() as connection:
            for wager in wagers:
                if not isinstance(wager, dict):
                    continue
                ticket = _text(wager.get('TID') or wager.get('WAGERS_ID'), 128)
                if not ticket:
                    continue
                display = _match_display_fields(wager, '', allow_final_score=False)
                if display['betScoreHome'] is None or display['betScoreAway'] is None:
                    continue
                stored = connection.execute(
                    'SELECT id,content FROM crown_live_orders WHERE account_id=? AND ticket=?',
                    (account['id'], ticket),
                ).fetchone()
                if stored is None:
                    continue
                row = json.loads(stored['content'])
                if row.get('betScoreHome') is not None and row.get('betScoreAway') is not None:
                    continue
                row.update({key: display[key] for key in ('matchPhase', 'matchPhaseLabel', 'isInPlay', 'isToday', 'betScoreHome', 'betScoreAway')})
                raw = row.setdefault('raw', {})
                evidence = _safe_order_raw(wager)
                for key in ('SHOWTEXT_TEAM', 'TNAME', 'WAGERSTYPE', 'MARKET', 'SCORE_H', 'SCORE_C', 'SCORE'):
                    if key in evidence:
                        raw[key] = evidence[key]
                content = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                connection.execute('UPDATE crown_live_orders SET content=?,updated_at=? WHERE id=?',
                                   (content, _iso(), stored['id']))
                updated += 1
        return updated

    def state(self, account_id):
        with self._db() as connection:
            row = connection.execute('SELECT * FROM crown_live_sync WHERE account_id=?', (account_id,)).fetchone()
        return dict(row) if row else {}

    def set_state(self, account_id, **values):
        allowed = {'checkpoint', 'range_from', 'range_to', 'last_synced', 'last_attempt', 'status', 'last_error', 'revision', 'coverage'}
        if set(values) - allowed:
            raise ValueError('无效的皇冠同步字段')
        with self._db() as connection:
            connection.execute('INSERT OR IGNORE INTO crown_live_sync(account_id) VALUES(?)', (account_id,))
            if values:
                connection.execute('UPDATE crown_live_sync SET ' + ','.join(key + '=?' for key in values) + ' WHERE account_id=?', [*values.values(), account_id])

    def rows(self, account_ids=None):
        with self._db() as connection:
            if account_ids is not None:
                if not account_ids:
                    return []
                rows = connection.execute('SELECT id,content,first_seen,updated_at FROM crown_live_orders WHERE account_id IN (' + ','.join('?' for _ in account_ids) + ') ORDER BY bet_date DESC,bet_time DESC,id DESC', list(account_ids)).fetchall()
            else:
                rows = connection.execute('SELECT id,content,first_seen,updated_at FROM crown_live_orders ORDER BY bet_date DESC,bet_time DESC,id DESC').fetchall()
        return [{**json.loads(row['content']), 'id': row['id'], 'firstSeenAt': row['first_seen'], 'updatedAt': row['updated_at']} for row in rows]


def query_orders(all_rows, query, business_date=None):
    """Match the Crown page's existing query and response vocabulary."""
    keyword = _query_value(query, 'keyword').casefold()
    filters = {key: _query_value(query, key) for key in ('from', 'to', 'league', 'play', 'backend', 'group', 'follow')}
    # A single Monday can also describe the current partial report week. Daily
    # buttons explicitly select bet-date semantics instead of that membership.
    official_weeks = (None if _query_value(query, 'date_basis') == 'bet_date'
                      else _official_filter_weeks(filters['from'], filters['to'], business_date))
    rows = []
    for row in all_rows:
        if not _matches_date_range(row, filters['from'], filters['to'], official_weeks):
            continue
        if filters['league'] and row['league'] != filters['league'] or filters['play'] and row['market'] != filters['play'] or filters['backend'] and row['backend'] != filters['backend']:
            continue
        if filters['group'] == 'settled' and not row['settled'] or filters['group'] == 'pending' and row['settled']:
            continue
        if filters['follow'] and filters['follow'] != 'not_applicable':
            continue
        if keyword and keyword not in ' '.join(str(row.get(key, '')) for key in ('league', 'home', 'away', 'match', 'play', 'pick', 'ticket', 'backend', 'member', 'agent')).casefold():
            continue
        rows.append(row)
    known_stake_values = [_money(row.get('stake')) for row in rows]
    unknown_stake_count = sum(value is None for value in known_stake_values)
    total_stake = sum((value for value in known_stake_values if value is not None), Decimal('0'))
    settled = [row for row in rows if row['settled']]
    total_profit = sum((_money(row['profit']) or Decimal('0') for row in settled), Decimal('0'))
    unknown_profit_count = sum(1 for row in settled if _money(row['profit']) is None)
    def unpriced_status(row):
        raw = row.get('raw') or {}
        return (_money(row.get('profit')) is None
                and row.get('settlement', {}).get('result') in ('cancelled', 'abnormal')
                and bool(_official_text_marker(raw.get('CANCEL_MSG'), raw.get('WIN_GOLD'))))
    unpriced_status_count = sum(unpriced_status(row) for row in settled)
    settled_stake_values = [_money(row.get('stake')) for row in settled]
    unknown_settled_stake_count = sum(value is None for value in settled_stake_values)
    settled_stake = sum((value for value in settled_stake_values if value is not None), Decimal('0'))
    days, markets = {}, {}
    for row in settled:
        day, market = row['date'], row['market']
        if not day:
            continue
        profit, stake = _money(row['profit']), _money(row.get('stake'))
        unknown = profit is None
        known_profit = profit if profit is not None else Decimal('0')
        bucket = days.setdefault(day, {'profit': Decimal('0'), 'stake': Decimal('0'), 'count': 0,
                                      'unknownProfitCount': 0, 'unknownStakeCount': 0, 'unpricedStatusCount': 0})
        bucket['profit'] += known_profit
        bucket['stake'] += stake if stake is not None else Decimal('0')
        bucket['count'] += 1
        bucket['unknownProfitCount'] += int(unknown)
        bucket['unknownStakeCount'] += int(stake is None)
        bucket['unpricedStatusCount'] += int(unpriced_status(row))
        bucket = markets.setdefault(market, {'profit': Decimal('0'), 'stake': Decimal('0'), 'count': 0,
                                             'unknownProfitCount': 0, 'unknownStakeCount': 0, 'unpricedStatusCount': 0,
                                             'series': defaultdict(Decimal), 'unknownDays': defaultdict(int)})
        bucket['profit'] += known_profit
        bucket['stake'] += stake if stake is not None else Decimal('0')
        bucket['count'] += 1
        bucket['unknownProfitCount'] += int(unknown)
        bucket['unknownStakeCount'] += int(stake is None)
        bucket['unpricedStatusCount'] += int(unpriced_status(row))
        bucket['series'][day] += known_profit
        bucket['unknownDays'][day] += int(unknown)
    trend = [{'date': day, 'followProfit': None, 'originalProfit': None if bucket['unknownProfitCount'] else _number(bucket['profit']),
              'knownProfit': _number(bucket['profit']), 'unknownProfitCount': bucket['unknownProfitCount'],
              'unpricedStatusCount': bucket['unpricedStatusCount'],
              'profitUnknown': bool(bucket['unknownProfitCount']),
              'followAmount': None, 'originalAmount': None if bucket['unknownStakeCount'] else _number(bucket['stake']),
              'knownStake': _number(bucket['stake']), 'unknownStakeCount': bucket['unknownStakeCount'],
              'count': bucket['count']}
             for day, bucket in sorted(days.items())]
    directions = [{'name': name, 'count': bucket['count'], 'share': round(bucket['count'] / (len(settled) or 1) * 100, 2),
                   'followProfit': None, 'originalProfit': None if bucket['unknownProfitCount'] else _number(bucket['profit']),
                   'knownProfit': _number(bucket['profit']), 'unknownProfitCount': bucket['unknownProfitCount'],
                   'unpricedStatusCount': bucket['unpricedStatusCount'],
                   'profitUnknown': bool(bucket['unknownProfitCount']),
                   'followAmount': None, 'originalAmount': None if bucket['unknownStakeCount'] else _number(bucket['stake']),
                   'knownStake': _number(bucket['stake']), 'unknownStakeCount': bucket['unknownStakeCount'],
                   'series': [None if bucket['unknownDays'][entry['date']] else _number(bucket['series'][entry['date']]) for entry in trend]}
                  for name, bucket in sorted(markets.items(), key=lambda pair: pair[1]['count'], reverse=True)]
    try:
        page = max(1, int(_query_value(query, 'page') or '1'))
        page_size = min(100, max(10, int(_query_value(query, 'page_size') or '20')))
    except ValueError:
        page, page_size = 1, 20
    total_pages = max(1, (len(rows) + page_size - 1) // page_size)
    page = min(page, total_pages)
    orders = [public_order(row) for row in rows[(page - 1) * page_size:page * page_size]]
    date_basis = 'official_report_week' if official_weeks is not None or not (filters['from'] or filters['to']) else 'bet_date'
    currencies = sorted({currency for row in rows
                         if (currency := _text(row.get('raw', {}).get('CURRENCY'), 32))})
    return {'orders': orders, 'items': orders, 'total': len(rows), 'page': page, 'pageSize': page_size, 'totalPages': total_pages,
            'dateBasis': date_basis,
            'currencyStatus': 'not_provided' if not currencies else ('provided' if len(currencies) == 1 else 'mixed'),
            'currencies': currencies,
            'trendDateBasis': 'bet_date',
            'dateMin': min((row['date'] for row in all_rows if row['date']), default=''),
            'dateMax': max((row['date'] for row in all_rows if row['date']), default=''),
            'leagues': sorted({row['league'] for row in all_rows if row['league']}),
            'plays': sorted({row['market'] for row in all_rows if row['market']}),
            'backends': sorted({row['backend'] for row in all_rows if row['backend']}),
            'trend': trend, 'directions': directions, 'messageCount': 0,
            'kpis': {'followProfit': None, 'originalProfit': None if unknown_profit_count else _number(total_profit),
                     'knownProfit': _number(total_profit), 'unknownProfitCount': unknown_profit_count,
                     'unpricedStatusCount': unpriced_status_count, 'orderCount': len(rows),
                     'messageCount': 0, 'uniqueTickets': len(rows), 'followedCount': 0, 'notFollowedCount': 0,
                     'processingCount': 0, 'settledCount': len(settled), 'pendingCount': len(rows) - len(settled),
                     'settledRate': round(len(settled) / len(rows) * 100, 2) if rows else None,
                     'backendCount': len({row['backend'] for row in rows}), 'followAmount': None,
                     'originalAmount': None if unknown_stake_count else _number(total_stake),
                     'knownStake': _number(total_stake), 'unknownStakeCount': unknown_stake_count,
                     'plannedFollowAmount': None, 'settledFollowAmount': None,
                     'settledOriginalAmount': None if unknown_settled_stake_count else _number(settled_stake),
                     'settledKnownStake': _number(settled_stake),
                     'unknownSettledStakeCount': unknown_settled_stake_count}}


def public_order(row, detail=False):
    result = {key: value for key, value in row.items() if key not in ('raw', 'profit', 'stake')}
    stake, profit = _money(row.get('stake')), _money(row.get('profit'))
    result.update(originalStake=_number(stake), plannedFollow=None, followStake=None,
                  originalProfit=_number(profit), followProfit=None, followState='not_applicable',
                  notificationCount=0, chatTitle='皇冠官方账单', messageTime=row.get('betTime', ''),
                  messageId=None, messageLink='', sourceType='hg', officialProfit=_number(profit))
    if detail:
        result['officialFields'] = row.get('raw', {})
    return result


class CrownLiveService:
    def __init__(self, store, auth=None, *, client_factory=CrownClient, now_fn=None, monotonic_fn=None,
                 orders=None, slice_seconds=8):
        self.store, self.auth = store, auth
        self.orders = orders if orders is not None else CrownOrderStore(store.directory)
        self.client_factory = client_factory
        self._now = now_fn or _now
        self._monotonic = monotonic_fn or time.monotonic
        self._lock = threading.RLock()
        self._sync_lock = threading.Lock()
        self._wake, self._stop = threading.Event(), threading.Event()
        self._thread = None
        self._status = 'idle'
        self._error = ''
        self._new_count = 0
        self._next_poll = ''
        self._revision = 0
        self._last_reconcile = {}
        self._last_current_reconcile = {}
        # Preserve server-issued cookie rotations in memory across polls, until
        # the browser saves a different session or account configuration changes.
        self._clients = {}
        self._runtime = {}
        self._next_due = {}
        self._account_locks = {}
        self._write_lock = threading.RLock()
        self._work = {}
        self._next_poll_due = {}
        self._last_polled = {}
        self._poll_errors = {}
        self._report_progress_at = {}
        self._report_progress = {}
        self._slice_seconds = max(0.01, float(slice_seconds))
        self._scheduler_started = False
        self._scheduler_error = ''
        self._scheduler_error_kind = ''
        self._scheduler_last_error_at = ''
        self._scheduler_last_tick = ''
        self._scheduler_failures = 0
        self._scheduler_retry_at = None

    def _accounts(self):
        accounts = [account for account in self.store.accounts() if account.get('kind') == 'crown_admin' and account.get('enabled')]
        accounts.sort(key=lambda account: (account.get('role') != 'primary', account.get('status') != 'authenticated', account.get('created_at', ''), account['id']))
        return accounts

    def _account(self):
        accounts = self._accounts()
        return accounts[0] if accounts else None

    def _set_runtime(self, account, status, error='', new_count=0, changed=0):
        with self._lock:
            self._runtime[account['id']] = {'status': status, 'lastError': error, 'newOrderCount': new_count}
            self._revision += changed

    def start(self):
        with self._lock:
            if self._stop.is_set():
                raise CrownError('皇冠同步服务已关闭，请创建新的同步服务实例')
            if self._thread and self._thread.is_alive():
                return
            self._scheduler_started = True
            self._thread = threading.Thread(target=self._run, name='crown-live-reports', daemon=True)
            self._thread.start()

    def close(self):
        # A read already in flight may finish later.  Serialize stopping with
        # every write so that it cannot save rows or session updates afterwards.
        with self._write_lock:
            self._stop.set()
        self._wake.set()
        if self._thread and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2)

    def request_sync(self):
        if self._stop.is_set():
            return {'ok': False, 'sync': self.sync_state()}
        # New instances can recover an unexpectedly stopped scheduler without
        # reviving a collector that the application explicitly closed.
        self.start()
        self._wake.set()
        return {'ok': True, 'sync': self.sync_state()}

    def scheduler_health(self):
        # Return process-local liveness without querying a possibly busy DB.
        with self._lock:
            alive = bool(self._thread and self._thread.is_alive()) if self._scheduler_started else None
            if not self._scheduler_started:
                status, message = 'not_started', ''
            elif self._stop.is_set():
                status, message = 'stopped', '皇冠订单监听已停止'
            elif not alive:
                status, message = 'failed', '皇冠订单监听线程已停止，请重新启动监听'
            elif self._scheduler_error:
                status, message = 'retrying', self._scheduler_error
            else:
                status, message = 'running', ''
            remaining = (max(0, self._scheduler_retry_at - self._monotonic())
                         if self._scheduler_retry_at is not None else None)
            return {'schedulerAlive': alive, 'schedulerStatus': status, 'schedulerError': message,
                    'schedulerErrorKind': self._scheduler_error_kind,
                    'schedulerLastErrorAt': self._scheduler_last_error_at,
                    'schedulerLastTickAt': self._scheduler_last_tick,
                    'schedulerConsecutiveFailures': self._scheduler_failures,
                    'schedulerRetryInSeconds': remaining}

    def _scheduler_failed(self, exc, failures, delay):
        if isinstance(exc, sqlite3.OperationalError):
            code = getattr(exc, 'sqlite_errorcode', None)
            busy = code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or any(
                marker in str(exc).casefold() for marker in ('database is locked', 'database table is locked', 'database is busy'))
            kind = 'database_busy' if busy else 'database_error'
            message = ('皇冠账号数据库暂忙，订单监听正在自动重试' if busy else
                       '皇冠账号数据库读取暂时失败，订单监听正在自动重试')
        else:
            kind = 'unexpected_error'
            message = '皇冠订单监听调度暂时中断，正在自动重试'
        with self._lock:
            self._scheduler_error = message
            self._scheduler_error_kind = kind
            self._scheduler_last_error_at = _iso(self._now())
            self._scheduler_failures = failures
            self._scheduler_retry_at = self._monotonic() + delay

    def _interval(self, account=None):
        try:
            return min(3600, max(15, int((account or {}).get('interval') or self.store.settings().get('sync_interval', 60))))
        except (TypeError, ValueError, AttributeError):
            return 60

    def _run(self):
        # The executor and active bookkeeping survive transient scheduler/DB
        # failures. A busy accounts read must not silently kill order polling.
        pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='crown-account')
        pending_force, active = set(), {}
        failures = 0
        try:
            while not self._stop.is_set():
                try:
                    accounts = self._accounts()
                    if self._stop.is_set():
                        break
                    identities = {account['id'] for account in accounts}
                    if self._wake.is_set():
                        pending_force.update(identities)
                        self._wake.clear()
                    pending_force.intersection_update(identities)
                    for identity, future in list(active.items()):
                        if future.done():
                            try:
                                future.result()
                            except Exception:
                                account = next((item for item in accounts if item['id'] == identity), None)
                                if account:
                                    self._set_runtime(account, 'error', '皇冠同步暂时中断，稍后自动重试')
                            del active[identity]
                    clock = self._monotonic()
                    ready = [account for account in accounts if account['id'] not in active
                             and (account['id'] in pending_force or clock >= self._next_due.get(account['id'], 0))]
                    ready.sort(key=lambda account: (account['id'] not in pending_force, self._next_due.get(account['id'], 0)))
                    for account in ready[:max(0, 2 - len(active))]:
                        identity = account['id']
                        force = identity in pending_force
                        active[identity] = pool.submit(self._sync_account, account, force, self._slice_seconds)
                        pending_force.discard(identity)
                    for identity in set(self._clients) - identities - set(active):
                        self._prune_account(identity)
                    with self._lock:
                        self._scheduler_error = ''
                        self._scheduler_error_kind = ''
                        self._scheduler_failures = 0
                        self._scheduler_retry_at = None
                        self._scheduler_last_tick = _iso(self._now())
                    failures = 0
                except Exception as exc:
                    failures += 1
                    delay = min(30, 2 ** min(failures - 1, 5))
                    self._scheduler_failed(exc, failures, delay)
                    # Unlike _wake.wait(), an already-pending manual sync cannot
                    # collapse this delay into a busy loop. close() interrupts it.
                    self._stop.wait(delay)
                    continue
                # A short interruptible scheduler wait also detects account adds,
                # login completion and disable/delete operations promptly.
                self._wake.wait(1)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _login_expired(self, account, session):
        """Stop polling a rejected snapshot until the user explicitly logs in."""
        identity = account['id']
        message = '皇冠接口已拒绝保存会话；原会话已保留，请在“我的”显式重新登录'
        expire = getattr(self.store, 'expire_session_if_current', None)
        if not callable(expire) or not expire(identity, account.get('revision'), session,
                                              message, _iso(self._now())):
            return False
        self._set_runtime(account, 'waiting_login', message)
        return True

    def _same_session(self, account, session):
        current = self.store.get_account(account['id'])
        if not current.get('enabled') or current.get('revision') != account.get('revision'):
            return False
        latest = self.store.load_session(account['id'])
        if not isinstance(latest, dict) or not isinstance(session, dict):
            return False
        expected = {**session, 'base_url': session.get('base_url', account.get('base_url'))}
        actual = {**latest, 'base_url': latest.get('base_url', account.get('base_url'))}
        return actual == expected

    def _assert_session(self, account, session):
        if not self._same_session(account, session):
            raise IncompleteReport('皇冠会话或配置已更新，已取得订单已保存，稍后继续同步')

    def _verified(self, account, session=None, client=None):
        with self._write_lock:
            self._assert_running()
            return self._verified_unlocked(account, session, client)

    def _verified_unlocked(self, account, session=None, client=None):
        """Record real business access without rewriting session/last_login."""
        setter = getattr(self.store, 'set_state', None)
        if setter is None:
            return
        if session is not None:
            self._assert_session(account, session)
            updates = getattr(client, 'session_updates', lambda: {})() if client is not None else {}
            refresh = getattr(self.store, 'refresh_session', None)
            if updates and callable(refresh):
                refreshed = refresh(account['id'], session, updates,
                                    expected_revision=account.get('revision'))
                if not refreshed:
                    self._assert_session(account, session)
                else:
                    current_session = self.store.load_session(account['id'])
                    if current_session and isinstance(session, dict):
                        session.clear()
                        session.update(current_session)
                        if hasattr(client, 'session'):
                            client.session = current_session
        current = self.store.get_account(account['id'])
        if not current.get('enabled') or current.get('revision') != account.get('revision'):
            raise CrownError('皇冠账号配置已变化，等待使用新配置重新同步')
        if current.get('status') in ('logging_in', 'checking', 'awaiting_otp', 'awaiting_manual'):
            return
        expected = {'expected_revision': account['revision']} if account.get('revision') is not None else {}
        setter(account['id'], 'authenticated', '订单读取权限已通过实际只读接口验证', last_checked=_iso(self._now()), **expected)

    def sync_once(self, force=False, respect_due=False):
        """Visit every enabled admin account, with no more than two workers.

        One failing account cannot prevent other accounts from being collected.
        Direct calls are deterministic passes; the background loop honors each
        account's independent due time.
        """
        if not self._sync_lock.acquire(blocking=False):
            return False
        try:
            accounts = self._accounts()
            enabled_ids = {account['id'] for account in accounts}
            for identity in set(self._clients) - enabled_ids:
                self._prune_account(identity)
            due = [account for account in accounts if force or not respect_due or self._monotonic() >= self._next_due.get(account['id'], 0)]
            if not accounts:
                with self._lock:
                    self._status, self._error = 'waiting_login', '请先在“我的”添加皇冠订单后台'
                return False
            if not due:
                return True
            results = []
            if len(due) == 1:
                results.append(self._sync_account(due[0], force))
            else:
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix='crown-account') as pool:
                    futures = {pool.submit(self._sync_account, account, force): account for account in due}
                    for future in as_completed(futures):
                        account = futures[future]
                        try:
                            results.append(future.result())
                        except Exception:
                            self._set_runtime(account, 'error', '皇冠同步暂时中断，稍后自动重试')
                            results.append(False)
            return bool(results) and all(results)
        finally:
            self._sync_lock.release()

    def _sync_account(self, account, force=False, budget=None):
        with self._lock:
            guard = self._account_locks.setdefault(account['id'], threading.Lock())
        if not guard.acquire(blocking=False):
            return False
        try:
            return self._collect_account(account, force, budget)
        finally:
            guard.release()

    def _assert_running(self):
        if self._stop.is_set():
            raise IncompleteReport('皇冠同步已暂停，部分数据已保存')

    def _save_state(self, account, session=None, **values):
        with self._write_lock:
            self._assert_running()
            if session is not None:
                self._assert_session(account, session)
            self.orders.set_state(account['id'], **values)

    def _prune_account(self, identity):
        with self._lock:
            guard = self._account_locks.setdefault(identity, threading.Lock())
        if not guard.acquire(blocking=False):
            return
        try:
            self._clients.pop(identity, None)
            self._discard_work(identity)
        finally:
            guard.release()

    def _discard_work(self, identity):
        work = self._work.pop(identity, None)
        if work:
            for task in work['tasks']:
                iterator = task.get('iterator')
                if iterator is not None:
                    iterator.close() if hasattr(iterator, 'close') else None
        self._report_progress.pop(identity, None)

    def _new_work(self, account, session, client_key, start, last_day, full, budget):
        monday = start + timedelta(days=7)
        spans = [(start, min(start + timedelta(days=6), last_day)), (monday, last_day)] if full else [(monday, last_day)]
        spans = [(left, right) for left, right in spans if left <= right]
        if budget is not None:
            spans.reverse()  # Live collection reaches this week's ledger first.
        tasks = []
        for kind in (('A', 'D', 'D4') if full else ('A',)):
            for left, right in spans:
                tasks.append({'kind': kind, 'start': left, 'end': right, 'done': False,
                              'iterator': None, 'attempts': 0, 'read_attempts': 0, 'retry_at': 0, 'error': ''})
        work = {'session': session, 'key': (client_key, start, last_day), 'tasks': tasks,
                'full': full, 'refresh_current': False, 'started': _iso(self._now())}
        self._work[account['id']] = work
        return work

    @staticmethod
    def _work_coverage(work, previous):
        coverage = dict(previous)
        for kind, keys in (('A', ('settled', 'pending')), ('D', ('cancelled',)), ('D4', ('abnormal',))):
            tasks = [task for task in work['tasks'] if task['kind'] == kind]
            if tasks:
                for key in keys:
                    # A routine current-week refresh does not erase a prior
                    # complete reconciliation of this same saved date range.
                    # Its in-progress/failure state is reported separately.
                    coverage[key] = bool((not work['full'] and previous.get(key)) or all(task['done'] for task in tasks))
        return coverage

    def _collect_account(self, account, force=False, budget=None):
        """Advance one account without letting a large history starve live reads.

        A background slice yields after an upstream request/leaf, preserving the
        generator and completed categories. Direct sync_once calls drain every
        ready task once and remain deterministic for reconciliation callers.
        """
        identity = account['id']
        session, new_count, changed = None, 0, 0
        clock_start = self._monotonic()
        state = {}
        yielded = False
        retry_due = None
        try:
            self._assert_running()
            account = self.store.get_account(identity)
            if not account.get('enabled') or account.get('kind') != 'crown_admin':
                self._discard_work(identity)
                return False
            if account.get('status') == 'expired':
                self._discard_work(identity)
                message = '皇冠保存会话已失效；原会话已保留，请在“我的”显式重新登录'
                self._save_state(account, status='waiting_login', last_error=message)
                self._set_runtime(account, 'waiting_login', message)
                return False
            if account.get('status') in ('logging_in', 'checking', 'awaiting_otp', 'awaiting_manual'):
                self._discard_work(identity)
                status = 'waiting_verification' if account.get('status') in ('awaiting_otp', 'awaiting_manual') else 'reconnecting'
                message = '皇冠正在等待登录验证，请在“我的”完成验证' if status == 'waiting_verification' else '皇冠正在重新登录，成功后继续同步'
                self._set_runtime(account, status, message)
                return False
            if not account.get('has_session'):
                self._discard_work(identity)
                self._set_runtime(account, 'waiting_login', '请先在“我的”登录此皇冠订单后台')
                return False
            current = self._now()
            start, end = week_range(current)
            last_day = min(end, current.astimezone(UPSTREAM_TZ).date())
            state = self.orders.state(identity)
            try:
                coverage = json.loads(state.get('coverage') or '{}')
            except (ValueError, TypeError):
                coverage = {}
            range_matches = state.get('range_from') == start.isoformat() and state.get('range_to') == last_day.isoformat()
            if not range_matches:
                coverage = {}
            complete_range = range_matches and bool(state.get('last_synced')) and all(coverage.get(key) for key in ('settled', 'pending', 'cancelled', 'abnormal'))
            session = self.store.load_session(identity)
            client_key = (identity, account.get('revision'), account.get('base_url'),
                          (session or {}).get('uid'), (session or {}).get('saved_at'))
            cached = self._clients.get(identity)
            work = self._work.get(identity)
            # Session equality also detects browser cookie replacements that
            # happen to retain the same UID and saved_at metadata.
            if work and (work['key'] != (client_key, start, last_day) or work['session'] != session):
                self._discard_work(identity)
                work = None
                cached = None
            if cached is None or cached[0] != client_key or (len(cached) > 2 and cached[2] != session):
                cached = (client_key, self.client_factory(account, session), session)
                self._clients[identity] = cached
                self._last_reconcile.pop(identity, None)
                self._last_current_reconcile.pop(identity, None)
                self._next_poll_due.pop(identity, None)
                self._poll_errors.pop(identity, None)
            client = cached[1]
            if work is not None:
                session = work['session']
            else:
                session = cached[2]
            if work is None and hasattr(client, 'begin_cycle'):
                client.begin_cycle()
            self._set_runtime(account, 'syncing')
            self._save_state(account, session, status='syncing', last_attempt=_iso(current), last_error='')
            reconcile_age = self._monotonic() - self._last_reconcile.get(identity, 0)
            full = force or (not complete_range and work is None) or reconcile_age >= 300 or identity not in self._last_reconcile
            checkpoint, has_new = state.get('checkpoint', '0'), False
            # A forced/full history must never suppress background new-order
            # polling. Legacy direct callers may provide a history-only client.
            should_poll = (budget is not None and self._monotonic() >= self._next_poll_due.get(identity, 0)) or (budget is None and complete_range and not full)
            if should_poll and callable(getattr(client, 'poll', None)):
                try:
                    previous_checkpoint = checkpoint
                    checkpoint, has_new = client.poll(checkpoint)
                    has_new = has_new or str(checkpoint) != str(previous_checkpoint)
                    self._verified(account, session, client)
                    self._last_polled[identity] = _iso(self._now())
                    self._poll_errors.pop(identity, None)
                    self._next_poll_due[identity] = self._monotonic() + self._interval(account)
                    self._save_state(account, session, checkpoint=checkpoint)
                except SessionExpired:
                    raise
                except CrownError as exc:
                    self._assert_running()
                    self._assert_session(account, session)
                    self._poll_errors[identity] = str(exc)
                    self._next_poll_due[identity] = self._monotonic() + min(15, self._interval(account))
                    # A temporary live-read failure cannot suppress a still
                    # readable official ledger in the same account's plan.
            current_due = self._monotonic() - self._last_current_reconcile.get(identity, 0) >= 60
            if work is None and (full or has_new or current_due):
                work = self._new_work(account, session, client_key, start, last_day, full, budget)
            elif work is not None:
                if full and not work['full']:
                    # A manual/full-due request arriving during a current-week
                    # iterator expands its plan, preserving the active iterator.
                    previous = {(task['kind'], task['start'], task['end']): task for task in work['tasks']}
                    refresh_current = work['refresh_current']
                    work = self._new_work(account, session, client_key, start, last_day, True, budget)
                    work['tasks'] = [previous.get((task['kind'], task['start'], task['end']), task) for task in work['tasks']]
                    work['refresh_current'] = refresh_current
                    if hasattr(client, 'begin_cycle'):
                        client.begin_cycle()
                normal_tasks = [task for task in work['tasks'] if task['kind'] == 'A']
                current_task = max(normal_tasks, key=lambda task: task['start'])
                if current_task['done'] and (has_new or current_due):
                    current_task.update(done=False, iterator=None, attempts=0, read_attempts=0, retry_at=0, error='')
                elif has_new and not current_task['done']:
                    work['refresh_current'] = True
            failed = False
            if work is not None:
                progressed = False
                for task in work['tasks']:
                    if task['done']:
                        continue
                    if budget is not None and task['retry_at'] > self._monotonic():
                        retry_due = task['retry_at'] if retry_due is None else min(retry_due, task['retry_at'])
                        continue
                    try:
                        if task['iterator'] is None:
                            if task['attempts'] and hasattr(client, 'begin_cycle'):
                                client.begin_cycle()
                            options = {} if task['kind'] == 'A' else {'result_types': ('N',), 'report_kind': task['kind']}
                            if isinstance(client, CrownClient):
                                options['cooperative'] = budget is not None
                                options['include_future_pending'] = True
                                if task['kind'] == 'A' and budget is not None:
                                    options['result_types'] = ('N', 'Y')
                            task['iterator'] = iter(client.history(task['start'], task['end'], **options))
                        while True:
                            self._assert_running()
                            self._assert_session(account, session)
                            try:
                                raw_rows, context = next(task['iterator'])
                            except StopIteration:
                                self._verified(account, session, client)
                                task.update(done=True, iterator=None, error='', retry_at=0, attempts=0, read_attempts=0)
                                if task['kind'] == 'A' and task['start'] == max(item['start'] for item in work['tasks'] if item['kind'] == 'A'):
                                    self._last_current_reconcile[identity] = self._monotonic()
                                    if work['refresh_current'] and any(not item['done'] for item in work['tasks'] if item is not task):
                                        # A signal observed while the iterator
                                        # was suspended may arrive after a leaf
                                        # already read. Re-read the current week
                                        # independently of failing D/D4 tasks.
                                        task.update(done=False, attempts=0, read_attempts=0)
                                        work['refresh_current'] = False
                                        yielded = True
                                progressed = True
                                break
                            if context.get('_read_retry'):
                                self._assert_running()
                                self._assert_session(account, session)
                                task['read_attempts'] += 1
                                task['error'] = str(TransientReadFailed(context['_read_retry']))
                                task['retry_at'] = self._monotonic() + min(300, 10 * 2 ** min(task['read_attempts'] - 1, 5))
                                retry_due = task['retry_at'] if retry_due is None else min(retry_due, task['retry_at'])
                                # Keep this iterator suspended at the failed
                                # operation. Neither a business verification nor
                                # an order write has succeeded at this marker.
                                failed, yielded = True, True
                                break
                            with self._write_lock:
                                self._assert_running()
                                self._assert_session(account, session)
                                if not context.get('_read_step'):
                                    context = {**context, 'report_from': task['start'].isoformat(),
                                               'report_to': context.get('report_to') or task['end'].isoformat()}
                                    added, updated = self.orders.upsert(account, raw_rows, context)
                                    new_count += added
                                    changed += added + updated
                                # Persist cookie rotations after every successful
                                # step, including hierarchy reads without rows.
                                self._verified_unlocked(account, session, client)
                            task.update(error='', retry_at=0, read_attempts=0)
                            progressed = True
                            self._report_progress_at[identity] = _iso(self._now())
                            if budget is not None and self._monotonic() - clock_start >= budget:
                                yielded = True
                                break
                        if yielded:
                            break
                    except SessionExpired:
                        raise
                    except CrownError as exc:
                        self._assert_running()
                        self._assert_session(account, session)
                        task['iterator'] = None  # Python closes a failed generator.
                        task['attempts'] += 1
                        task['read_attempts'] = 0
                        task['error'] = str(exc)
                        task['retry_at'] = self._monotonic() + min(300, 10 * 2 ** min(task['attempts'] - 1, 5))
                        retry_due = task['retry_at'] if retry_due is None else min(retry_due, task['retry_at'])
                        failed = True
                        if budget is not None and self._monotonic() - clock_start >= budget:
                            yielded = True
                            break
                if yielded and task in work['tasks']:
                    # Rotate a suspended task behind the other report tasks;
                    # steady live signals cannot starve previous weeks or D/D4.
                    work['tasks'].remove(task)
                    work['tasks'].append(task)
                coverage = self._work_coverage(work, coverage)
                pending = [task for task in work['tasks'] if not task['done']]
                self._report_progress[identity] = {'completed': len(work['tasks']) - len(pending), 'total': len(work['tasks']),
                                                   'retrying': sum(bool(task['error']) for task in pending)}
                self._save_state(account, session, range_from=start.isoformat(), range_to=last_day.isoformat(), coverage=json.dumps(coverage))
                if not pending:
                    completed_at = _iso(self._now())
                    if work['full']:
                        self._last_reconcile[identity] = self._monotonic()
                        # Only all four categories over the complete retained
                        # range constitute a new complete reconciliation time.
                        self._save_state(account, session, last_synced=completed_at)
                    refresh_current = work['refresh_current']
                    self._discard_work(identity)
                    work = None
                    if refresh_current:
                        self._last_current_reconcile.pop(identity, None)
                        yielded = True
                elif not yielded and not failed:
                    # Every remaining task is waiting for its independent retry.
                    retry_due = min(task['retry_at'] for task in pending)
            with self._write_lock:
                self._assert_running()
                self._assert_session(account, session)
                changed += self.orders.enrich_wagers(account, getattr(client, 'recent_wagers', []))
            errors = [task['error'] for task in work['tasks'] if task['error'] and not task['done']] if work else []
            if self._poll_errors.get(identity):
                errors.insert(0, self._poll_errors[identity])
            normal_complete = bool(coverage.get('settled') and coverage.get('pending'))
            is_partial = not all(coverage.get(key) for key in ('settled', 'pending', 'cancelled', 'abnormal'))
            status = ('partial' if errors and (normal_complete or new_count or changed) else 'error') if errors else ('syncing' if work else ('partial' if is_partial else 'online'))
            message = errors[0] if errors else ('皇冠报表分批同步中，已取得订单已保存' if work else ('正常注单已同步；取消或异常注单覆盖尚未完整，稍后自动重试' if is_partial else ''))
            self._save_state(account, session, checkpoint=checkpoint, status=status, last_error=message, revision=int(state.get('revision') or 0) + changed)
            self._set_runtime(account, status, message, new_count, changed)
            # Direct callers historically accept readable normal ledgers with
            # disclosed unavailable cancellation/abnormal coverage.
            return not errors or normal_complete
        except SessionExpired as exc:
            self._discard_work(identity)
            with self._write_lock:
                self._assert_running()
                expired = self._login_expired(account, session)
                if not expired:
                    exc = SessionExpired('皇冠会话已更新，稍后使用新会话继续同步')
                status = 'waiting_login' if expired else 'reconnecting'
                self.orders.set_state(identity, status=status, last_error=str(exc))
            self._set_runtime(account, status, str(exc), new_count, changed)
            return False
        except CrownError as exc:
            if session is not None and not self._same_session(account, session):
                self._discard_work(identity)
                self._clients.pop(identity, None)
            if self._stop.is_set():
                return False
            status = 'partial' if new_count or changed else 'error'
            self._save_state(account, status=status, last_error=str(exc))
            self._set_runtime(account, status, str(exc), new_count, changed)
            return False
        except Exception:
            if self._stop.is_set():
                return False
            message = '皇冠同步暂时中断，稍后自动重试'
            self._set_runtime(account, 'error', message, new_count, changed)
            self._save_state(account, status='error', last_error=message)
            return False
        finally:
            with self._lock:
                waiting = self._runtime.get(identity, {}).get('status') in ('waiting_login', 'waiting_verification', 'reconnecting')
                interval = min(15, self._interval(account)) if waiting else self._interval(account)
                due = self._monotonic() + interval
                if budget is not None and identity in self._work:
                    tasks = [task for task in self._work[identity]['tasks'] if not task['done']]
                    if any(task['retry_at'] <= self._monotonic() for task in tasks):
                        due = self._monotonic()
                    elif tasks:
                        due = min(due, min(task['retry_at'] for task in tasks))
                if yielded and budget is not None:
                    due = self._monotonic()
                if budget is not None and identity in self._next_poll_due:
                    due = min(due, self._next_poll_due[identity])
                self._next_due[identity] = due

    def _account_state(self, account, counts, start, end, target_end):
        state = self.orders.state(account['id'])
        try:
            coverage = json.loads(state.get('coverage') or '{}')
        except (ValueError, TypeError):
            coverage = {}
        range_current = (state.get('range_from') == start.isoformat()
                         and state.get('range_to') == target_end.isoformat())
        if not range_current:
            coverage = {}
        with self._lock:
            runtime = dict(self._runtime.get(account['id'], {}))
            due = self._next_due.get(account['id'])
        # Saved coverage remains evidence about its range, but a new process
        # still has to check this account before presenting it as currently live.
        status = runtime.get('status') or ('syncing' if account.get('has_session') else 'waiting_login')
        error = runtime.get('lastError', state.get('last_error', ''))
        auth_status = account.get('status')
        if auth_status in ('awaiting_otp', 'awaiting_manual'):
            status, error = 'waiting_verification', '皇冠正在等待登录验证，请在“我的”完成验证'
        elif auth_status in ('logging_in', 'checking'):
            status, error = 'reconnecting', '皇冠正在重新登录，成功后继续同步'
        elif auth_status == 'expired':
            status, error = 'waiting_login', '皇冠保存会话已失效；原会话已保留，请在“我的”显式重新登录'
        elif not account.get('has_session'):
            status, error = 'waiting_login', '请先在“我的”登录此皇冠订单后台'
        if status == 'online' and not range_current:
            status = 'syncing'
        remaining = max(0, due - self._monotonic()) if due is not None else 0
        return {'accountId': account['id'], 'accountName': account.get('name') or account.get('username') or '皇冠',
                'status': status, 'lastSyncedAt': state.get('last_synced', ''), 'lastError': error,
                'lastPollAt': self._last_polled.get(account['id'], ''),
                'lastReportProgressAt': self._report_progress_at.get(account['id'], ''),
                'reportProgress': dict(self._report_progress.get(account['id'], {})),
                'orderCount': counts.get(account['id'], 0), 'newOrderCount': runtime.get('newOrderCount', 0),
                'pollInterval': self._interval(account),
                'coverageComplete': bool(range_current and state.get('last_synced') and all(coverage.get(key) for key in ('settled', 'pending', 'cancelled', 'abnormal'))),
                'historyCoverage': coverage, 'cancelCoverage': bool(coverage.get('cancelled')),
                'rangeFrom': start.isoformat(), 'rangeTo': target_end.isoformat(), 'weekRangeTo': end.isoformat(),
                'nextPollAt': _iso(self._now() + timedelta(seconds=remaining)) if due is not None else '',
                'revision': int(state.get('revision') or 0)}

    def sync_state(self):
        accounts = self._accounts()
        start, end = week_range(self._now())
        target_end = min(end, self._now().astimezone(UPSTREAM_TZ).date())
        counts = defaultdict(int)
        official_weeks = _official_filter_weeks(start.isoformat(), end.isoformat())
        for row in self.orders.rows([account['id'] for account in accounts]):
            if _matches_date_range(row, start.isoformat(), end.isoformat(), official_weeks):
                counts[row['accountId']] += 1
        details = [self._account_state(account, counts, start, end, target_end) for account in accounts]
        health = self.scheduler_health()
        if health['schedulerError']:
            for detail in details:
                if detail['status'] in ('online', 'partial', 'syncing', 'error'):
                    detail.update(status='error', lastError=health['schedulerError'])
        complete = sum(bool(item['coverageComplete']) for item in details)
        online = sum(item['status'] in ('online', 'partial') for item in details)
        if not details:
            status, error = 'waiting_login', '请先在“我的”添加皇冠订单后台'
        elif len(details) == 1:
            status, error = details[0]['status'], details[0]['lastError']
        elif any(item['status'] == 'syncing' for item in details):
            status, error = 'syncing', ''
        elif all(item['status'] == 'online' for item in details):
            status, error = 'online', ''
        elif online:
            status, error = 'partial', '部分账号本轮同步未成功或报表尚未完整核对，请查看各账号状态'
        elif all(item['status'] == 'waiting_login' for item in details):
            status, error = 'waiting_login', '请在“我的”登录皇冠订单后台'
        elif any(item['status'] == 'waiting_verification' for item in details):
            status, error = 'waiting_verification', '部分皇冠账号等待人工验证，其余账号继续同步'
        elif any(item['status'] == 'reconnecting' for item in details):
            status, error = 'reconnecting', '部分皇冠账号正在重新登录，其余账号继续同步'
        else:
            status, error = 'error', '皇冠同步暂时中断，稍后自动重试'
        if health['schedulerError']:
            status, error = 'error', health['schedulerError']
        coverage = ({key: all(bool(item['historyCoverage'].get(key)) for item in details)
                     for key in ('settled', 'pending', 'cancelled', 'abnormal')}
                    if details and all(item['historyCoverage'] for item in details) else {})
        synced = [item['lastSyncedAt'] for item in details if item['lastSyncedAt']]
        due = [item['nextPollAt'] for item in details if item['nextPollAt']]
        with self._lock:
            return {**health, 'status': status, 'lastSyncedAt': min(synced) if synced else '', 'lastError': error,
                    'pollInterval': min((item['pollInterval'] for item in details), default=60),
                    'orderCount': sum(counts.values()), 'newOrderCount': sum(item['newOrderCount'] for item in details),
                    'revision': max(self._revision, sum(item['revision'] for item in details)),
                    'rangeFrom': start.isoformat(), 'rangeTo': target_end.isoformat(), 'weekRangeTo': end.isoformat(),
                    'accountName': details[0]['accountName'] if len(details) == 1 else f'{len(details)} 个皇冠后台',
                    'nextPollAt': min(due) if due else '', 'readOnly': True, 'upstreamTimezone': 'UTC-4',
                    'historyCoverage': coverage, 'cancelCoverage': bool(coverage.get('cancelled')),
                    'coverageComplete': bool(details and complete == len(details)), 'accounts': details,
                    'accountCount': len(details), 'onlineAccountCount': online, 'completeAccountCount': complete,
                    'waitingAccountCount': sum(item['status'] in ('waiting_login', 'waiting_verification', 'reconnecting') for item in details),
                    'failedAccountCount': sum(item['status'] == 'error' for item in details)}

    def query(self, query):
        accounts = self._accounts()
        all_rows = self.orders.rows([account['id'] for account in accounts])
        database_dates = [row['date'] for row in all_rows if row.get('date')]
        rows = all_rows
        current = self._now()
        business_date = current.astimezone(UPSTREAM_TZ).date()
        history = _query_value(query, 'range') == 'history'
        has_dates = bool(_query_value(query, 'from') or _query_value(query, 'to'))
        # Keep the API's default two-week snapshot, while allowing a selected
        # day or saved history to reach records outside that rolling window.
        if not history and not has_dates:
            start, end = week_range(current)
            official_weeks = _official_filter_weeks(start.isoformat(), end.isoformat(), business_date)
            rows = [row for row in rows if _matches_date_range(row, start.isoformat(), end.isoformat(), official_weeks)]
        result = query_orders(rows, query, business_date)
        # Keep the date picker bounded by all saved orders, even when the
        # current page query is limited to its default reporting week.
        result['dateMin'] = min(database_dates, default='')
        result['dateMax'] = max(database_dates, default='')
        if history and not has_dates:
            result['dateBasis'] = 'all_saved'
        result['businessDate'] = business_date.isoformat()
        state = self.sync_state()
        result.update(sync=state, source={'type': 'hg', 'name': '皇冠后台真实订单', 'latestMessageAt': state['lastSyncedAt'], 'error': state['lastError'], 'timezone': 'UTC-4', 'timeLabel': '皇冠后台时间（UTC-4）', 'dateBasis': result['dateBasis']})
        return result

    def detail(self, row_id):
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            return None
        rows = self.orders.rows([account['id'] for account in self._accounts()])
        row = next((row for row in rows if row['id'] == row_id), None)
        return public_order(row, detail=True) if row else None
