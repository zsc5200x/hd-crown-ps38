from __future__ import annotations

import getpass
import json
import sys
import time
import uuid
from datetime import datetime
from urllib.parse import urlsplit

import websocket

from app import parse_response
from pinnacle_ws_parser import UnsupportedOddsUpdate, parse_complete_odds_update


ORIGIN = "https://www.ps3838.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/156.0.0.0 Safari/537.36"
)
ODDS_SUBSCRIPTION = {
    "sp": 29,
    "lg": "",
    "ev": "",
    "mk": 1,
    "btg": "1",
    "ot": 2,
    "d": "",
    "o": 1,
    "l": 5,
    "me": 0,
    "more": False,
    "lang": "",
    "tm": 0,
    "pa": 0,
    "c": "",
    "g": "QQ==",
    "pn": -1,
    "ec": "",
    "cl": 5,
    "hle": False,
    "pimo": "0,1,8,39,2,3,6,7,4,5",
    "inl": False,
    "pv": 1,
    "ic": False,
    "ice": False,
    "me01": "",
    "ru": "",
    "locale": "zh_CN",
}


def log(message: str) -> None:
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{timestamp}] {message}", flush=True)


def read_credentials() -> tuple[str, str]:
    url = getpass.getpass("粘贴当前有效的 WSS Request URL（输入不回显）: ").strip()
    cookie = getpass.getpass("粘贴 Cookie 值，不含 Cookie: 前缀（输入不回显）: ").strip()
    if cookie.lower().startswith("cookie:"):
        cookie = cookie.split(":", 1)[1].strip()

    parsed = urlsplit(url)
    if (
        parsed.scheme != "wss"
        or parsed.hostname != "www.ps3838.com"
        or parsed.path != "/sports-websocket/ws"
        or not parsed.query
    ):
        raise ValueError("地址必须是当前会话的 www.ps3838.com WSS Request URL")
    if not cookie:
        raise ValueError("Cookie 不能为空")
    if not cookie_value(cookie, "dpPZl"):
        raise ValueError("Cookie 缺少当前连接需要的 dpPZl 字段")
    return url, cookie


def cookie_value(cookie: str, name: str) -> str | None:
    for part in cookie.split(";"):
        key, separator, value = part.strip().partition("=")
        if separator and key.strip() == name:
            return value.strip()
    return None


def make_odds_subscription(dp_pzl: str) -> str:
    body = dict(ODDS_SUBSCRIPTION)
    body["v"] = str(int(time.time() * 1000))
    body["dpPZl"] = dp_pzl
    return json.dumps(
        {
            "type": "SUBSCRIBE",
            "destination": "ODDS",
            "body": body,
            "id": str(uuid.uuid4()),
        },
        separators=(",", ":"),
    )


def summarize_shape(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {"kind": "object", "keys": sorted(str(key) for key in value)}
    if isinstance(value, list):
        return {"kind": "array", "count": len(value)}
    if isinstance(value, str):
        return {"kind": "string", "length": len(value)}
    if value is None:
        return {"kind": "null"}
    return {"kind": type(value).__name__}


def display_message(message: str | bytes, ws: websocket.WebSocketApp | None = None) -> None:
    if isinstance(message, bytes):
        log(f"收到二进制帧，{len(message)} 字节（内容未打印）")
        return

    try:
        payload = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        log(f"收到非 JSON 文本帧，{len(message)} 字符（内容未打印）")
        return

    if isinstance(payload, dict) and payload.get("type") == "PING":
        if ws is not None:
            ws.send('{"type":"PONG","destination":"ALL"}')
            log("收到应用层 PING，已回复 PONG")
        else:
            log("收到应用层 PING")
        return

    if isinstance(payload, dict) and payload.get("type") == "UPDATE_ODDS":
        odds = payload.get("odds")
        if isinstance(odds, dict) and odds.get("refreshAll") is True:
            try:
                events, leagues = parse_complete_odds_update(payload, parse_response)
            except UnsupportedOddsUpdate as error:
                log(f"完整赔率快照无法解析：{error}")
                return
            else:
                log(f"完整赔率快照：{len(events)} 场足球比赛，{len(leagues)} 个联赛")
                for event in events:
                    summary = {
                        "id": event.get("id"),
                        "league": event.get("league"),
                        "live": event.get("is_live"),
                        "home": event.get("home"),
                        "away": event.get("away"),
                        "score": event.get("score"),
                        "minute": event.get("minute"),
                        "markets": event.get("markets"),
                    }
                    log(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
                return

        update_summary = {
            "type": "UPDATE_ODDS",
            "refreshAll": odds.get("refreshAll") if isinstance(odds, dict) else None,
            "fields": sorted(str(key) for key in odds) if isinstance(odds, dict) else [],
            "u": summarize_shape(odds.get("u")) if isinstance(odds, dict) else {"kind": "missing"},
            "l": summarize_shape(odds.get("l")) if isinstance(odds, dict) else {"kind": "missing"},
        }
        log("赔率增量摘要：" + json.dumps(update_summary, ensure_ascii=False, separators=(",", ":")))
        return

    if isinstance(payload, dict):
        log("服务器消息摘要：" + json.dumps({
            "type": payload.get("type"),
            "keys": sorted(str(key) for key in payload),
        }, ensure_ascii=False, separators=(",", ":")))
    else:
        log(f"收到 JSON {type(payload).__name__} 消息（内容未打印）")


def main() -> int:
    try:
        url, cookie = read_credentials()
    except (EOFError, KeyboardInterrupt):
        print("已取消。", file=sys.stderr)
        return 130
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2

    retry_delay = 3
    try:
        while True:
            state = {"opened": False, "auth_rejected": False}

            def on_open(_ws: websocket.WebSocketApp) -> None:
                state["opened"] = True
                _ws.send(make_odds_subscription(cookie_value(cookie, "dpPZl") or ""))
                log("WSS 已连接，已订阅足球赔率；按 Ctrl+C 停止。")

            def on_message(_ws: websocket.WebSocketApp, message: str | bytes) -> None:
                display_message(message, _ws)

            def on_error(_ws: websocket.WebSocketApp, error: Exception) -> None:
                status = getattr(error, "status_code", None)
                if status is None:
                    status = getattr(error, "status", None)
                if status in (401, 403, "401", "403"):
                    state["auth_rejected"] = True
                    log(
                        "握手被服务器拒绝（认证状态异常）。旧会话不能继续重试，"
                        "请从已登录页面复制新生成的 Request URL 和 Cookie。"
                    )
                    return
                log(f"连接错误: {type(error).__name__}")

            def on_close(
                _ws: websocket.WebSocketApp,
                code: int | None,
                _reason: str | None,
            ) -> None:
                log(f"连接关闭: code={code}")

            client = websocket.WebSocketApp(
                url,
                header=[
                    "Cache-Control: no-cache",
                    "Accept-Language: zh-CN,zh;q=0.9",
                    "Pragma: no-cache",
                    f"User-Agent: {USER_AGENT}",
                ],
                cookie=cookie,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            try:
                client.run_forever(
                    origin=ORIGIN,
                    ping_interval=25,
                    ping_timeout=10,
                )
            except KeyboardInterrupt:
                raise
            except Exception as error:
                log(f"连接异常: {type(error).__name__}")

            if state["auth_rejected"]:
                try:
                    url, cookie = read_credentials()
                except (EOFError, KeyboardInterrupt):
                    log("已停止监听")
                    return 130
                except ValueError as error:
                    print(error, file=sys.stderr)
                    return 2
                retry_delay = 3
                continue

            delay = 3 if state["opened"] else retry_delay
            log(f"{delay} 秒后重连")
            time.sleep(delay)
            if not state["opened"]:
                retry_delay = min(retry_delay * 2, 60)
            else:
                retry_delay = 3
    except KeyboardInterrupt:
        log("已停止监听")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
