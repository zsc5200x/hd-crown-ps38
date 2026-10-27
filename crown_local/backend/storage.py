"""Local two-database account storage; public projections never contain credentials."""
from __future__ import annotations

import ipaddress
import json
import os
from collections import OrderedDict
from pathlib import Path
import re
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from http.client import HTTPException
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import uuid

from cryptography.fernet import Fernet, InvalidToken
from .totp import normalize_secret as normalize_totp_secret

KINDS = {'pinnacle': 'pinnacle.sqlite3', 'crown_admin': 'crown.sqlite3', 'crown_odds': 'crown.sqlite3'}
DEFAULT_PATHS = {'pinnacle': '/partner/zh-cn/login', 'crown_admin': '/', 'crown_odds': '/'}
AUTHENTICATED = {'authenticated'}
BUSY = {'logging_in', 'awaiting_otp', 'awaiting_manual', 'checking'}
_FAKE_IP_NETWORK = ipaddress.ip_network('198.18.0.0/15')
_DOH_ENDPOINT = 'https://dns.google/resolve'
_DOH_TIMEOUT = 4
_DOH_MAX_BYTES = 65536
_DOH_CACHE_TTL = 60
_DOH_CACHE_LIMIT = 256
_doh_cache = OrderedDict()
_doh_cache_lock = threading.Lock()
_SESSION_UNCHECKED = object()


class _NoDohRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        # The resolver endpoint is fixed; never follow an arbitrary destination.
        return None


class SessionChangedError(ValueError):
    """Raised when a session write was based on a stale saved snapshot."""


def _dns_address(value):
    address = ipaddress.ip_address(value)
    if isinstance(address, ipaddress.IPv6Address):
        # RFC 6145 IPv4-translated addresses are not covered by ipv4_mapped and
        # Python may call them global even when the embedded IPv4 is private.
        if int(address) >> 32 == 0xffff0000:
            return ipaddress.IPv4Address(int(address) & 0xffffffff)
        if address.ipv4_mapped is not None:
            return address.ipv4_mapped
    return address


def _doh_answers(host, record_type):
    query = urlencode({'name': host, 'type': record_type})
    request = Request(_DOH_ENDPOINT + '?' + query, headers={'Accept': 'application/dns-json'})
    opener = build_opener(ProxyHandler({}), _NoDohRedirects())
    with opener.open(request, timeout=_DOH_TIMEOUT) as response:
        if response.getcode() != 200:
            raise ValueError('DNS resolver rejected the query')
        body = response.read(_DOH_MAX_BYTES + 1)
    if len(body) > _DOH_MAX_BYTES:
        raise ValueError('DNS response exceeds size limit')
    document = json.loads(body)
    if not isinstance(document, dict) or document.get('Status') != 0:
        raise ValueError('DNS resolver returned an error')
    answers = document.get('Answer', [])
    if not isinstance(answers, list):
        raise ValueError('Invalid DNS answer list')
    addresses = []
    for answer in answers:
        if not isinstance(answer, dict):
            raise ValueError('Invalid DNS answer')
        answer_type = answer.get('type')
        if answer_type not in (1, 28):
            continue
        address = ipaddress.ip_address(answer.get('data', ''))
        if address.version != (4 if answer_type == 1 else 6):
            raise ValueError('DNS answer family mismatch')
        addresses.append(_dns_address(str(address)))
    return addresses


def _public_dns_confirmed(host):
    """Confirm proxy FakeIP destinations using a bounded trusted DoH lookup."""
    current_time = time.monotonic()
    with _doh_cache_lock:
        cached = _doh_cache.get(host)
        if cached and cached[0] > current_time:
            _doh_cache.move_to_end(host)
            return cached[1]
        _doh_cache.pop(host, None)
    try:
        addresses = _doh_answers(host, 'A') + _doh_answers(host, 'AAAA')
        allowed = bool(addresses) and all(address.is_global for address in addresses)
    except (ValueError, TypeError, OSError, HTTPException):
        allowed = False
    with _doh_cache_lock:
        _doh_cache[host] = (time.monotonic() + _DOH_CACHE_TTL, allowed)
        _doh_cache.move_to_end(host)
        while len(_doh_cache) > _DOH_CACHE_LIMIT:
            _doh_cache.popitem(last=False)
    return allowed


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def normalize_base_url(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('请填写采集源域名')
    raw = value.strip()
    if '://' not in raw:
        raw = 'https://' + raw
    try:
        p = urlsplit(raw)
        host = (p.hostname or '').encode('idna').decode('ascii').lower()
        port = p.port
    except (ValueError, UnicodeError):
        raise ValueError('域名格式不正确') from None
    if p.scheme != 'https' or not host or p.username is not None or p.password is not None:
        raise ValueError('采集源必须是无用户名密码的 HTTPS 域名')
    if p.path not in ('', '/') or p.query or p.fragment or port not in (None, 443):
        raise ValueError('域名不要包含路径、参数或非 HTTPS 默认端口；登录路径请单独填写')
    if host == 'localhost' or '.' not in host or host.endswith(('.localhost', '.local', '.internal')):
        raise ValueError('不支持本机或内网采集地址')
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', host) or any(not label or len(label) > 63 or label.startswith('-') or label.endswith('-') for label in host.split('.')):
            raise ValueError('域名格式不正确')
    else:
        if not address.is_global:
            raise ValueError('不支持本机或内网采集地址')
    return 'https://' + host


def normalize_login_url(value):
    """Accept a domain or login-page URL while keeping its origin separate."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError('请填写登录站点域名')
    raw = value.strip()
    if any(ord(character) < 32 for character in raw):
        raise ValueError('登录地址格式不正确')
    if '://' not in raw:
        raw = 'https://' + raw
    try:
        parsed = urlsplit(raw)
    except ValueError:
        raise ValueError('登录地址格式不正确') from None
    if parsed.query or parsed.fragment:
        raise ValueError('登录地址不要包含查询参数或片段，请填写域名或登录页路径')
    base = normalize_base_url(parsed._replace(path='', query='', fragment='').geturl())
    return base, parsed.path if parsed.path not in ('', '/') else None


def validate_target_url(value):
    """Validate HTTPS browser destinations, including redirects and subresources."""
    try:
        p = urlsplit(value)
        if p.scheme != 'https' or p.username is not None or p.password is not None or p.port not in (None, 443):
            return False
        base = normalize_base_url('https://' + (p.hostname or ''))
        host = urlsplit(base).hostname
        addresses = [_dns_address(item[4][0]) for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)]
        if not addresses:
            return False
        fake_ip = False
        for address in addresses:
            if isinstance(address, ipaddress.IPv4Address) and address in _FAKE_IP_NETWORK:
                fake_ip = True
            elif not address.is_global:
                return False
        return _public_dns_confirmed(host) if fake_ip else True
    except (ValueError, OSError):
        return False


class Vault:
    def __init__(self, directory):
        self.backend = 'environment'
        key = os.environ.get('REPORT_MASTER_KEY')
        if key:
            try:
                self._fernet = Fernet(key.encode())
            except (ValueError, TypeError):
                raise RuntimeError('REPORT_MASTER_KEY 格式不正确；需要 Fernet 密钥') from None
            return
        if sys.platform == 'darwin':
            import keyring
            identity = str(Path(directory).resolve())
            # Use a project-specific Keychain namespace so this account store
            # cannot reuse credentials or encryption keys from another app.
            key = keyring.get_password('pingbo-request', identity)
            if key is None:
                key = Fernet.generate_key().decode()
                keyring.set_password('pingbo-request', identity, key)
            self.backend = 'macOS Keychain'
        else:
            # Development fallback. Never silently use a public/world-readable key.
            private = Path(directory) / '.keys'
            private.mkdir(mode=0o700, parents=True, exist_ok=True)
            if private.stat().st_mode & 0o077:
                raise RuntimeError('密钥目录权限必须为 0700')
            path = private / 'master.key'
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                with os.fdopen(fd, 'wb') as f:
                    f.write(Fernet.generate_key())
            if path.is_symlink() or path.stat().st_mode & 0o077:
                raise RuntimeError('密钥文件权限必须为 0600，且不能是符号链接')
            key = path.read_text().strip()
            self.backend = 'local key file (development)'
        try:
            self._fernet = Fernet(key.encode())
        except (ValueError, TypeError):
            raise RuntimeError('无法载入加密密钥；请恢复密钥或重新配置账号') from None

    def encrypt(self, value):
        return self._fernet.encrypt(json.dumps(value, ensure_ascii=False).encode()).decode()

    def decrypt(self, value):
        if not value:
            return None
        try:
            return json.loads(self._fernet.decrypt(value.encode()).decode())
        except (InvalidToken, ValueError, UnicodeError):
            raise RuntimeError('无法解密账号资料；请恢复原密钥，或删除该账号后重新配置') from None


class Store:
    def __init__(self, directory, *, initialize=True):
        self.directory = Path(directory).expanduser().resolve()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.vault = Vault(self.directory)
        self._lock = threading.RLock()
        # CLI consumers of an already-running server must not reset its login
        # states or rerun startup migrations when reading encrypted sessions.
        if initialize:
            self._initialize()

    def close(self):
        """关闭存储对象；数据库连接按操作短连接，无需额外释放。"""
        return None

    @contextmanager
    def _db(self, kind):
        connection = sqlite3.connect(self.directory / KINDS[kind], timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA busy_timeout=10000')
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self):
        for kind in ('pinnacle', 'crown_admin'):
            with self._db(kind) as c:
                c.executescript('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL,
                    source_id TEXT NOT NULL, base_url TEXT NOT NULL, login_path TEXT NOT NULL,
                    username TEXT NOT NULL, role TEXT NOT NULL, login_layer TEXT NOT NULL,
                    enabled INTEGER NOT NULL, interval INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'idle', last_login TEXT, last_checked TEXT,
                    last_error TEXT NOT NULL DEFAULT '', credentials TEXT NOT NULL,
                    session TEXT, revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
                    kind TEXT NOT NULL, account_id TEXT, action TEXT NOT NULL,
                    status TEXT NOT NULL, message TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                PRAGMA user_version=1;
                ''')
                if 'revision' not in {r['name'] for r in c.execute('PRAGMA table_info(accounts)')}:
                    c.execute('ALTER TABLE accounts ADD COLUMN revision INTEGER NOT NULL DEFAULT 1')
                c.execute("UPDATE accounts SET status=CASE WHEN enabled=0 THEN 'disabled' WHEN session IS NOT NULL THEN 'session_received' ELSE 'idle' END, last_error='' WHERE status IN ('logging_in','awaiting_otp','awaiting_manual','checking','authenticated')")
            os.chmod(self.directory / KINDS[kind], 0o600)

    def _find(self, account_id):
        for kind in ('pinnacle', 'crown_admin'):
            with self._db(kind) as c:
                row = c.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
            if row:
                return dict(row)
        raise KeyError('账号不存在')

    def _public(self, row):
        credentials = self.vault.decrypt(row['credentials']) if row['credentials'] else {}
        return {**{k: row[k] for k in ('id','kind','name','source_id','base_url','login_path','username','role','login_layer','interval','status','last_login','last_checked','last_error','created_at','updated_at','revision')},
                'enabled': bool(row['enabled']), 'has_password': bool(row['credentials']),
                'has_totp_secret': bool(credentials and credentials.get('totp_secret')),
                'has_session': bool(row['session'])}

    def get_account(self, account_id):
        return self._public(self._find(account_id))

    def accounts(self):
        rows = []
        for kind in ('pinnacle', 'crown_admin'):
            with self._db(kind) as c:
                rows.extend(self._public(dict(r)) for r in c.execute('SELECT * FROM accounts ORDER BY created_at,id'))
        return sorted(rows, key=lambda x: (x['created_at'], x['id']))

    def save_account(self, payload, account_id=None):
        with self._lock:
            old = self._find(account_id) if account_id else None
            allowed = {'kind','name','source_id','base_url','login_path','username','password','safe_password','totp_secret','clear_totp_secret','role','login_layer','enabled','interval'}
            if set(payload) - allowed:
                raise ValueError('存在不支持的账号字段')
            if old and old['status'] in BUSY:
                raise ValueError('请先取消正在执行的登录或验证，再修改账号')
            identity = account_id or uuid.uuid4().hex
            default_interval = self.settings()['sync_interval'] if not old else 60
            values = {k: payload.get(k, old[k] if old else default) for k, default in
                      [('kind',''),('name',''),('source_id',''),('base_url',''),('login_path',''),('username',''),('role','primary'),('login_layer',''),('enabled',True),('interval',default_interval)]}
            if values['kind'] not in KINDS or (old and values['kind'] != old['kind']):
                raise ValueError('请选择有效账号用途；已有账号不能更换用途')
            if 'name' not in payload:
                values['name'] = values['username']
            if not old and 'source_id' not in payload:
                values['source_id'] = values['kind'] + '-' + identity
            for k in ('name','source_id','username'):
                if not isinstance(values[k], str) or not values[k].strip() or len(values[k]) > 128:
                    raise ValueError('账号名称、来源标识和用户名为必填，最长128字')
                values[k] = values[k].strip()
            if not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', values['source_id']):
                raise ValueError('来源标识请使用1至64位字母、数字、点、横线或下划线')
            values['base_url'], supplied_path = normalize_login_url(values['base_url'])
            if supplied_path and ('login_path' not in payload or values['login_path'] == ''):
                values['login_path'] = supplied_path
            if not isinstance(values['login_path'], str):
                raise ValueError('登录路径必须是站内路径，不包含参数')
            path = values['login_path'] or DEFAULT_PATHS[values['kind']]
            if not isinstance(path, str) or len(path) > 256 or not path.startswith('/') or path.startswith('//') or '\\' in path or any(ord(x) < 32 for x in path) or '?' in path or '#' in path or urlsplit(path).netloc:
                raise ValueError('登录路径必须是站内路径，不包含参数')
            values['login_path'] = path
            if old and 'enabled' not in payload:
                values['enabled'] = bool(values['enabled'])
            if values['role'] not in ('primary','backup') or values['login_layer'] not in ('','co','su','ag'):
                raise ValueError('主备角色或皇冠登录层级不正确')
            if type(values['enabled']) is not bool or type(values['interval']) is not int or not 15 <= values['interval'] <= 3600:
                raise ValueError('启用状态必须为布尔值，间隔必须为15至3600秒')
            password = payload.get('password', '')
            safe = payload.get('safe_password', None)
            totp_secret = payload.get('totp_secret', '')
            clear_totp_secret = payload.get('clear_totp_secret', False)
            if (not isinstance(password, str) or len(password) > 1024
                    or (safe is not None and (not isinstance(safe, str) or len(safe) > 1024))
                    or not isinstance(totp_secret, str) or len(totp_secret) > 128
                    or type(clear_totp_secret) is not bool
                    or (clear_totp_secret and totp_secret.strip())):
                raise ValueError('凭据格式或长度不正确')
            normalized_totp = normalize_totp_secret(totp_secret) if totp_secret.strip() else ''
            credentials = self.vault.decrypt(old['credentials']) if old else {'password':'','safe_password':''}
            if password:
                credentials['password'] = password
            if safe is not None and (safe or not old):
                credentials['safe_password'] = safe
            if normalized_totp:
                credentials['totp_secret'] = normalized_totp
            if clear_totp_secret:
                credentials['totp_secret'] = ''
            if not credentials.get('password'):
                raise ValueError('新增账号需要填写密码')
            if len(self.accounts()) >= 100 and not old:
                raise ValueError('本机版本最多保存100个账号')
            invalidates = not old or any(values[k] != old[k] for k in ('base_url','login_path','username','source_id','login_layer')) or bool(password or safe)
            values.update(id=identity, enabled=int(values['enabled']), credentials=self.vault.encrypt(credentials),
                          session=None if invalidates else old['session'], status='disabled' if not values['enabled'] else ('idle' if invalidates else old['status']),
                          last_login=old['last_login'] if old else None, last_checked=old['last_checked'] if old else None,
                          last_error='', revision=old['revision']+1 if old else 1, created_at=old['created_at'] if old else now(), updated_at=now())
            if old and not old['enabled'] and values['enabled']:
                values['status'] = 'session_received' if values['session'] else 'idle'
            with self._db(values['kind']) as c:
                keys=list(values)
                c.execute('INSERT OR REPLACE INTO accounts ('+','.join(keys)+') VALUES ('+','.join('?' for _ in keys)+')', [values[k] for k in keys])
            self.add_log(values['kind'], values['id'], 'account_saved', 'success', '账号配置已加密保存' if not old else '账号配置已更新')
            return self.get_account(values['id'])

    def delete_account(self, account_id):
        with self._lock:
            row = self._find(account_id)
            with self._db(row['kind']) as c:
                c.execute('DELETE FROM accounts WHERE id=?', (account_id,))
            self.add_log(row['kind'], account_id, 'account_deleted', 'success', '账号配置和凭据已删除')

    def get_credentials(self, account_id):
        return self.vault.decrypt(self._find(account_id)['credentials'])

    def load_session(self, account_id):
        row=self._find(account_id)
        session=self.vault.decrypt(row['session'])
        if session and session.get('base_url',row['base_url']) != row['base_url']:
            return None
        return session

    @staticmethod
    def _normalized_session_snapshot(session, base_url):
        if session is None:
            return None
        if not isinstance(session, dict):
            return session
        return {**session, 'base_url': session.get('base_url', base_url)}

    def _save_session_in_transaction(self, c, row, session, *, update_last_login):
        stored_session = self.vault.encrypt({**session, 'base_url': row['base_url']})
        timestamp = now()
        if update_last_login:
            assignments = 'session=?,last_login=?,updated_at=?'
            values = [stored_session, timestamp, timestamp]
        else:
            assignments = 'session=?,updated_at=?'
            values = [stored_session, timestamp]
        if row['session'] is None:
            session_condition = 'session IS NULL'
            session_values = []
        else:
            session_condition = 'session=?'
            session_values = [row['session']]
        cursor = c.execute(
            f'UPDATE accounts SET {assignments} WHERE id=? AND revision=? AND {session_condition}',
            values + [row['id'], row['revision']] + session_values,
        )
        if cursor.rowcount != 1:
            raise SessionChangedError('保存的会话已变化，请基于最新会话重试')

    def save_session(self, account_id, session, expected_revision=None, *,
                     update_last_login=True, expected_session=_SESSION_UNCHECKED):
        with self._lock:
            kind = self._find(account_id)['kind']
            with self._db(kind) as c:
                c.execute('BEGIN IMMEDIATE')
                row = c.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
                if not row:
                    raise KeyError('账号不存在')
                row = dict(row)
                if expected_revision is not None and row['revision'] != expected_revision:
                    raise ValueError('账号配置已变化，旧登录任务已失效')
                if expected_session is not _SESSION_UNCHECKED:
                    expected = self._normalized_session_snapshot(expected_session, row['base_url'])
                    current = self.vault.decrypt(row['session'])
                    if expected_session is None:
                        matches = row['session'] is None
                    else:
                        matches = current == expected
                    if not matches:
                        raise SessionChangedError('保存的会话已变化，请基于最新会话重试')
                self._save_session_in_transaction(
                    c, row, session, update_last_login=update_last_login,
                )

    def refresh_session(self, account_id, original, updates, expected_revision=None):
        """Refresh cookies only if the exact account/session is still current."""
        with self._lock:
            kind = self._find(account_id)['kind']
            with self._db(kind) as c:
                c.execute('BEGIN IMMEDIATE')
                row = c.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
                if not row:
                    return False
                row = dict(row)
                if (not row['enabled']
                        or (expected_revision is not None and row['revision'] != expected_revision)):
                    return False
                current = self.vault.decrypt(row['session'])
                expected = self._normalized_session_snapshot(original, row['base_url'])
                if (not current or not isinstance(original, dict) or not original.get('uid')
                        or not original.get('saved_at') or current != expected):
                    return False
                try:
                    self._save_session_in_transaction(
                        c, row, {**current, **updates}, update_last_login=False,
                    )
                except SessionChangedError:
                    return False
                return True

    def apply_session_check(self, account_id, original, status, message='', *,
                            updates=None, expected_revision=None, expected_status=None,
                            last_checked=None):
        """Apply one check result and cookie refresh in a single database transaction."""
        if updates is not None and not isinstance(updates, dict):
            raise ValueError('会话更新必须是对象')
        with self._lock:
            try:
                kind = self._find(account_id)['kind']
            except KeyError:
                return False
            with self._db(kind) as c:
                # Separate services have separate Python locks. The write lock
                # must cover snapshot comparison, cookies, and account status.
                c.execute('BEGIN IMMEDIATE')
                row = c.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
                if not row:
                    return False
                row = dict(row)
                if (not row['enabled']
                        or (expected_revision is not None and row['revision'] != expected_revision)
                        or (expected_status is not None and row['status'] != expected_status)):
                    return False
                current = self.vault.decrypt(row['session'])
                expected = self._normalized_session_snapshot(original, row['base_url'])
                if not isinstance(original, dict) or not isinstance(current, dict) or current != expected:
                    return False
                timestamp = now()
                assignments = ['status=?', 'last_error=?', 'updated_at=?']
                values = [status, str(message)[:300], timestamp]
                if last_checked is not None:
                    assignments.append('last_checked=?')
                    values.append(last_checked)
                if updates is not None:
                    assignments.append('session=?')
                    values.append(self.vault.encrypt({**current, **updates, 'base_url': row['base_url']}))
                cursor = c.execute(
                    'UPDATE accounts SET ' + ','.join(assignments)
                    + ' WHERE id=? AND revision=? AND session=? AND status=?',
                    values + [account_id, row['revision'], row['session'], row['status']],
                )
                return cursor.rowcount == 1

    def clear_session(self, account_id):
        with self._lock:
            row=self._find(account_id)
            with self._db(row['kind']) as c:
                c.execute("UPDATE accounts SET session=NULL,revision=revision+1,status=CASE WHEN enabled=1 THEN 'idle' ELSE 'disabled' END,last_error='',updated_at=? WHERE id=?",(now(),account_id))
            self.add_log(row['kind'],account_id,'session_cleared','success','已清除保存的登录会话')

    def set_state(self, account_id, status, message='', expected_revision=None, **fields):
        allowed = {'last_login','last_checked'}
        if set(fields)-allowed:
            raise ValueError('不支持的状态字段')
        with self._lock:
            row=self._find(account_id)
            if expected_revision is not None and row['revision'] != expected_revision:
                raise ValueError('账号配置已变化，旧登录任务已失效')
            with self._db(row['kind']) as c:
                updates={'status':status,'last_error':str(message)[:300],'updated_at':now(),**fields}
                c.execute('UPDATE accounts SET '+','.join(k+'=?' for k in updates)+' WHERE id=?',list(updates.values())+[account_id])
            return self.get_account(account_id)

    def expire_session_if_current(self, account_id, expected_revision, expected_session,
                                  message, last_checked):
        """Mark a rejected session expired only while its exact snapshot is current."""
        with self._lock:
            kind = self._find(account_id)['kind']
            with self._db(kind) as c:
                c.execute('BEGIN IMMEDIATE')
                row = c.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
                if not row:
                    return False
                row = dict(row)
                if (not row['enabled']
                        or (expected_revision is not None and row['revision'] != expected_revision)
                        or row['status'] in BUSY):
                    return False
                current = self.vault.decrypt(row['session'])
                expected = self._normalized_session_snapshot(expected_session, row['base_url'])
                if not isinstance(current, dict) or current != expected:
                    return False
                cursor = c.execute(
                    "UPDATE accounts SET status='expired',last_error=?,last_checked=?,updated_at=? "
                    "WHERE id=? AND revision=? AND session=? AND status NOT IN "
                    "('logging_in','checking','awaiting_otp','awaiting_manual')",
                    (str(message)[:300], last_checked, now(), account_id, row['revision'], row['session']),
                )
                return cursor.rowcount == 1

    def add_log(self, kind, account_id, action, status, message):
        if kind not in KINDS:
            kind='pinnacle'
        with self._db(kind) as c:
            c.execute('INSERT INTO logs(created_at,kind,account_id,action,status,message) VALUES(?,?,?,?,?,?)',(now(),kind,account_id,str(action)[:60],str(status)[:40],str(message)[:300]))
            c.execute('DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 1000)')

    def logs(self):
        logs=[]
        for kind in ('pinnacle','crown_admin'):
            with self._db(kind) as c:
                logs.extend(dict(r) for r in c.execute('SELECT created_at,kind,account_id,action,status,message FROM logs ORDER BY id DESC LIMIT 100'))
        return sorted(logs,key=lambda x:x['created_at'],reverse=True)[:100]

    def settings(self):
        defaults={'quote_retry_seconds':60,'sync_interval':60,'page_refresh_seconds':15}
        with self._db('pinnacle') as c:
            for r in c.execute('SELECT * FROM settings'):
                if r['key'] in defaults:
                    defaults[r['key']]=json.loads(r['value'])
        return defaults

    def update_settings(self, payload):
        limits={'quote_retry_seconds':(5,300),'sync_interval':(15,3600),'page_refresh_seconds':(5,300)}
        if set(payload)-set(limits):
            raise ValueError('存在不支持的设置字段')
        for k,v in payload.items():
            low,high=limits[k]
            if type(v) is not int or not low<=v<=high:
                raise ValueError('设置应为允许范围内的整数秒')
        with self._db('pinnacle') as c:
            for k,v in payload.items():
                c.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',(k,json.dumps(v)))
        self.add_log('pinnacle',None,'settings_saved','success','运行参数已保存；订单采集尚未接入')
        return self.settings()
