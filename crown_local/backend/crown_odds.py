"""Read-only football odds requests using a verified Crown member session."""
from __future__ import annotations

import time
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from .crown_member import CrownMemberClient, MemberExpired, MemberNeedsBrowser, MemberUnavailable
from .crown_markets import parse_games_xml

MAX_ODDS_BYTES = 8 * 1024 * 1024


class OddsUnavailable(MemberUnavailable):
    """Safe diagnostic enum; never retain upstream bodies, URLs or messages."""
    REASONS = frozenset({'upstream_timeout', 'upstream_connection_failed',
                         'upstream_http_error', 'upstream_response_invalid',
                         'catalog_incomplete', 'catalog_changed'})

    def __init__(self, reason, *, request_attempts=1):
        self.reason = reason if reason in self.REASONS else 'upstream_response_invalid'
        self.request_attempts = max(1, min(3, int(request_attempts)))
        super().__init__('皇冠只读行情暂不可用')


def safe_failure(error):
    """Return only allowlisted diagnostics, irrespective of exception text."""
    if isinstance(error, OddsUnavailable):
        result = {'reason': error.reason, 'error_type': 'MemberUnavailable'}
        if error.request_attempts > 1:
            result['request_attempts'] = error.request_attempts
        return result
    if isinstance(error, MemberExpired):
        return {'reason': 'session_expired', 'error_type': 'MemberExpired'}
    if isinstance(error, MemberNeedsBrowser):
        return {'reason': 'session_unverified', 'error_type': 'MemberNeedsBrowser'}
    if isinstance(error, MemberUnavailable):
        return {'reason': 'upstream_unavailable', 'error_type': 'MemberUnavailable'}
    if isinstance(error, TimeoutError):
        return {'reason': 'upstream_timeout', 'error_type': 'TimeoutError'}
    if isinstance(error, (URLError, OSError)):
        return {'reason': 'upstream_connection_failed', 'error_type': 'ConnectionError'}
    return {'reason': 'unexpected_error', 'error_type': 'UnexpectedError'}


class CrownOddsClient(CrownMemberClient):
    """Read-only Crown market client with a small in-request reconnect budget.

    The quote worker already retries jobs over a longer window.  This shorter
    retry catches connection resets and incomplete HTTP reads immediately so a
    new order does not have to wait for the next queue attempt.  The operations
    below are provider reads and are therefore safe to repeat; bet submission
    operations remain rejected before any request is sent.
    """

    _RETRYABLE_HTTP = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __init__(self, *args, max_retries=2, retry_delay=0.15, sleep_fn=time.sleep, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_retries = max(0, min(2, int(max_retries)))
        self.retry_delay = max(0.0, min(1.0, float(retry_delay)))
        self.sleep_fn = sleep_fn

    def setting(self, key):
        value = self.session.get('member_resume_query', {}).get(key)
        if isinstance(value, list):
            value = value[0] if value else None
        return str(value) if isinstance(value, (str, int)) else None

    @property
    def odds_format(self):
        value = self.setting('odd_f_type')
        formats = {'H':'HK', 'M':'MY', 'E':'EU', 'I':'ID'}
        if value not in formats:
            raise MemberNeedsBrowser('会员赔率格式尚未确认')
        return formats[value]

    @property
    def raw_odds_format(self):
        # Crown's two-sided internal IOR values are not displayed HK odds.
        if self.setting('odd_f_type') != 'H':
            raise MemberNeedsBrowser('当前取价仅支持已验证的港赔显示格式')
        return 'CROWN_HK'

    def request_odds(self, fields):
        if fields.get('p') not in {'get_game_list', 'get_game_more'}:
            raise ValueError('仅允许已验证的赛事和盘口只读操作')
        payload = {'uid':self.uid, 'ver':self.protocol['ver'], 'langx':self.protocol['langx'], **fields}
        url = self.origin + '/transform.php?' + urlencode({'ver':self.protocol['ver']})
        for attempt in range(self.max_retries + 1):
            reason = None
            retryable = True
            try:
                if self.transport is not None:
                    status, body = self.transport(url, payload, self._headers(include_cookie=True), self.timeout)
                else:
                    request = Request(url, data=urlencode(payload).encode(), method='POST', headers=self._headers())
                    with self._opener.open(request, timeout=self.timeout) as response:
                        status, body = response.status, response.read(MAX_ODDS_BYTES + 1)
            except HTTPError as error:
                status = error.code
                error.close()
                if status in (401, 403, 440):
                    raise MemberExpired('皇冠拒绝当前盘口读取会话') from None
                reason = 'upstream_http_error'
                retryable = status in self._RETRYABLE_HTTP
            except TimeoutError:
                reason = 'upstream_timeout'
            except URLError as error:
                reason = ('upstream_timeout' if isinstance(error.reason, TimeoutError)
                          else 'upstream_connection_failed')
            except (OSError, HTTPException):
                # Includes connection reset, remote disconnect, incomplete
                # response and bad status line from the member endpoint.
                reason = 'upstream_connection_failed'
            else:
                if status in (401, 403, 440):
                    raise MemberExpired('皇冠拒绝当前盘口读取会话')
                if status != 200:
                    reason = 'upstream_http_error'
                    retryable = status in self._RETRYABLE_HTTP
                elif not isinstance(body, (bytes, bytearray)) or len(body) > MAX_ODDS_BYTES:
                    reason = 'upstream_response_invalid'
                else:
                    try:
                        parsed = parse_games_xml(body)
                    except (ValueError, TypeError):
                        reason = 'upstream_response_invalid'
                    else:
                        codes = {key.lower(): value for key, value in parsed['metadata'].items()}
                        if codes.get('status') in {'401', '403', '440'}:
                            raise MemberExpired('皇冠盘口会话已失效')
                        parsed['_request_attempts'] = attempt + 1
                        return parsed
            if not retryable or attempt >= self.max_retries:
                raise OddsUnavailable(reason, request_attempts=attempt + 1) from None
            self.sleep_fn(self.retry_delay * (2 ** attempt))

    def list_page(self, showtype, **page_fields):
        if showtype not in {'today','live'}:
            raise ValueError('仅支持足球今日和滚球')
        ltype = self.setting('ltype')
        if not ltype or not ltype.isdigit():
            raise MemberNeedsBrowser('会员盘口级别尚未确认')
        stamp = str(int(time.time()*1000))
        return self.request_odds({
            'p':'get_game_list','p3type':'','date':'','gtype':'ft','showtype':showtype,
            'rtype':'r' if showtype=='today' else 'rb', 'ltype':ltype,
            'filter':'FT' if showtype=='today' else '', 'cupFantasy':'N','sorttype':'L',
            'specialClick':'','isFantasy':'N','ts':stamp,'chgSortTS':stamp,**page_fields,
        })

    def list_games(self, showtype):
        # PAGE_SW=false omits page_no on the verified member page. In that mode
        # pageCount can still exceed one even though all games are returned.
        first=self.list_page(showtype)
        metadata={key.lower():value for key,value in first['metadata'].items()}
        try:
            pages=int(metadata['pagecount'])
        except (KeyError,TypeError,ValueError):
            raise OddsUnavailable('catalog_incomplete') from None
        if not 1<=pages<=30:
            raise OddsUnavailable('catalog_incomplete')
        games=list(first['games'])
        total=metadata.get('totaldatacount')
        count=metadata.get('datacount')
        if total is not None:
            try:
                total=int(total)
            except (TypeError,ValueError):
                raise OddsUnavailable('catalog_incomplete') from None
            if total<0:
                raise OddsUnavailable('catalog_incomplete')
        if total is not None and count is not None:
            try:
                complete=total==int(count)==len(games)
            except (TypeError,ValueError):
                raise OddsUnavailable('catalog_incomplete') from None
            if complete:
                pages=1
        if pages>1:
            first=self.list_page(showtype,page_no='1')
            page_info={key.lower():value for key,value in first['metadata'].items()}
            if str(page_info.get('pagecount'))!=str(pages):
                raise OddsUnavailable('catalog_changed')
            games=list(first['games'])
        if pages>1 and not games:
            raise OddsUnavailable('catalog_incomplete')
        for page in range(2,pages+1):
            following=self.list_page(showtype,page_no=str(page))
            info={key.lower():value for key,value in following['metadata'].items()}
            if str(info.get('pagecount'))!=str(pages):
                raise OddsUnavailable('catalog_changed')
            if not following['games']:
                raise OddsUnavailable('catalog_incomplete')
            games.extend(following['games'])
        ids=[game.get('GID') for game in games]
        if any(not gid for gid in ids) or len(set(ids))!=len(ids):
            raise OddsUnavailable('catalog_incomplete')
        if total is not None and len(games)!=total:
            raise OddsUnavailable('catalog_incomplete')
        return {'games':games,'metadata':first['metadata']}

    def game_more(self, game, showtype):
        if showtype not in {'today','live'} or not str(game.get('ECID','')).isdigit() or not str(game.get('LID','')).isdigit():
            raise ValueError('需要有效皇冠赛事标识')
        return self.request_odds({
            'p':'get_game_more','gtype':'ft','showtype':showtype,'ltype':self.setting('ltype'),
            'isRB':'Y' if showtype=='live' else 'N','lid':game['LID'],'ecid':game['ECID'],
            'specialClick':'','mode':'NORMAL','from':'game_more','filter':'Main',
            'ts':str(int(time.time()*1000)),
        })
