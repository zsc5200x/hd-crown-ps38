import http.client
import json
import threading
import unittest
from datetime import datetime, timedelta
from http.server import ThreadingHTTPServer
from unittest.mock import patch

import app
from pinnacle_ws_parser import parse_incremental_odds_update


def incremental_live_tree(home_price="0.84"):
    match = [
        12345, "主队", "客队", None, 1790000000000, None, None, None,
        {"0": [[["-0.5", "0.5", "-0.5", home_price, "1.02"]], [], []]},
    ]
    return [[29, "Soccer", [[77, "测试联赛", [match]]]]]


class MemoryCredentialStore:
    def __init__(self, saved=None):
        self.saved = saved

    def load(self):
        return dict(self.saved) if self.saved else None

    def save(self, username, password):
        self.saved = {"username": username.strip(), "password": password}


class FakeBrowserController:
    def __init__(self, started=True):
        self.started = started
        self.calls = []
        self.is_running = True
        self.refresh_calls = 0

    def start(self, username, password):
        self.calls.append((username, password))
        return self.started

    def refresh(self):
        self.refresh_calls += 1
        return self.is_running


class PinnacleBrowserApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.host, cls.port = cls.server.server_address

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    def request(self, method, path, payload=None):
        connection = http.client.HTTPConnection(self.host, self.port, timeout=3)
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        status = response.status
        raw = response.read()
        connection.close()
        return status, raw.decode("utf-8")

    def test_get_credentials_never_returns_password(self):
        store = MemoryCredentialStore({"username": "demo", "password": "secret-value"})
        with patch.object(app, "_pinnacle_credentials", store):
            status, body = self.request("GET", "/api/pinnacle/credentials")

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"configured": True, "username": "demo"})
        self.assertNotIn("secret-value", body)

    def test_save_credentials_does_not_authenticate(self):
        store = MemoryCredentialStore()
        with patch.object(app, "_pinnacle_credentials", store):
            status, body = self.request(
                "POST", "/api/pinnacle/credentials",
                {"username": " demo ", "password": "secret-value"},
            )

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"ok": True, "configured": True, "username": "demo"})
        self.assertEqual(store.saved, {"username": "demo", "password": "secret-value"})
        self.assertNotIn("secret-value", body)
        self.assertFalse(hasattr(app, "_get_pinnacle_manager"))

    def test_browser_open_uses_saved_credentials(self):
        store = MemoryCredentialStore({"username": "demo", "password": "secret-value"})
        browser = FakeBrowserController()
        with patch.object(app, "_pinnacle_credentials", store), patch.object(
            app, "_get_ps3838_browser", return_value=browser
        ):
            status, body = self.request("POST", "/api/pinnacle/browser/open")

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"ok": True, "started": True})
        self.assertEqual(browser.calls, [("demo", "secret-value")])

    def test_browser_open_requires_saved_credentials(self):
        store = MemoryCredentialStore()
        with patch.object(app, "_pinnacle_credentials", store), patch.object(
            app, "_get_ps3838_browser", side_effect=AssertionError("must not start")
        ):
            status, body = self.request("POST", "/api/pinnacle/browser/open")

        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["code"], "credentials_missing")

    def test_browser_refresh_queues_reload_on_running_controller(self):
        browser = FakeBrowserController()
        with patch.object(app, "_ps3838_browser", browser):
            status, body = self.request("POST", "/api/pinnacle/browser/refresh")

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"ok": True, "queued": True})
        self.assertEqual(browser.refresh_calls, 1)

    def test_browser_refresh_requires_running_controller(self):
        browser = FakeBrowserController()
        browser.is_running = False
        with patch.object(app, "_ps3838_browser", browser):
            status, body = self.request("POST", "/api/pinnacle/browser/refresh")

        self.assertEqual(status, 409)
        self.assertEqual(browser.refresh_calls, 0)

    def test_events_api_keeps_live_and_local_today_only(self):
        today = datetime.now().astimezone().date()

        def local_timestamp(day):
            return int(datetime(day.year, day.month, day.day, 12).astimezone().timestamp() * 1000)

        events = [
            {"id": "live", "is_live": True},
            {"id": "today", "is_live": False, "start_ms": local_timestamp(today)},
            {"id": "tomorrow", "is_live": False, "start_ms": local_timestamp(today + timedelta(days=1))},
        ]
        with patch.dict(app.state, {"events": events, "leagues": []}):
            status, body = self.request("GET", "/api/events")

        self.assertEqual(status, 200)
        self.assertEqual([event["id"] for event in json.loads(body)["events"]], ["live", "today"])

    def test_events_api_returns_hk_display_and_all_alternate_lines(self):
        today = datetime.now().astimezone().date()
        start_ms = int(datetime(today.year, today.month, today.day, 12).astimezone().timestamp() * 1000)
        events = [{
            "id": "fixture", "is_live": False, "start_ms": start_ms,
            "home": "主队", "away": "客队",
            "markets": {
                "handicap": [
                    {"line": "0.5-1", "home_line": "-0.75", "away_line": "0.75", "a": "0.91", "b": "0.99"},
                    {"line": "0", "home_line": "0", "away_line": "0", "a": "0.88", "b": "1.02"},
                    {"line": "0.5", "home_line": "0.5", "away_line": "-0.5", "a": "1.10", "b": "0.82"},
                ],
                "total": [{"line": "3.5-4", "a": "0.93", "b": "0.95"}],
                "moneyline": ["2.10", "3.30", "3.60"],
            },
        }]
        with patch.dict(app.state, {"events": events, "leagues": ["联赛"]}), patch.object(
            app, "PINNACLE_ODDS_FORMAT", "HK"
        ):
            status, body = self.request("GET", "/api/events")

        event = json.loads(body)["events"][0]
        self.assertEqual(status, 200)
        self.assertEqual(event["odds_format_label"], "港赔（HK）")
        self.assertEqual(len(event["display_markets"]["handicap"]), 3)
        self.assertEqual(event["display_markets"]["handicap"][0]["display_line"], "0.5/1")
        self.assertEqual(event["display_markets"]["handicap"][0]["home_display_line"], "-0.5/1")
        self.assertEqual(event["display_markets"]["total"][0]["display_line"], "3.5/4")
        self.assertEqual(event["display_markets"]["total"][0]["a"], "0.93")

    def test_incremental_odds_merge_reaches_events_api_without_dropping_other_lines(self):
        existing_event = {
            "id": "12345", "is_live": True, "home": "主队", "away": "客队",
            "league": "测试联赛", "start_ms": 1790000000000,
            "score": "0 - 0", "minute": "20", "period": "1H",
            "markets": {
                "handicap": [
                    {"line": "0", "home_line": "0", "away_line": "0", "a": "0.90", "b": "1.00"},
                    {"line": "-0.5", "home_line": "-0.5", "away_line": "0.5", "a": "0.91", "b": "0.99"},
                ],
                "total": [{"line": "2.5", "a": "0.88", "b": "1.02"}],
                "moneyline": [],
            },
            "match_quotes": [],
        }
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "l": incremental_live_tree("0.84"), "u": []},
        }
        partial_events, leagues = parse_incremental_odds_update(message, app.parse_response)
        frame_summary = {
            "refresh_all": False,
            "frame_received_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        }

        with patch.object(app, "_pinnacle_section_events", {"n": [], "l": [existing_event]}), patch.dict(
            app.state,
            {
                "events": [existing_event], "leagues": ["测试联赛"],
                "updated_at": None, "received_at": None, "request_duration_ms": None,
                "upstream_http_date": None, "error": None,
                "browser_status": "Chrome 已收到赔率快照",
            },
        ):
            app._on_browser_odds_update(partial_events, leagues, frame_summary)
            status, body = self.request("GET", "/api/events")

        self.assertEqual(status, 200)
        payload = json.loads(body)
        event = payload["events"][0]
        self.assertEqual(event["display_markets"]["handicap"][0]["a"], "0.84")
        self.assertEqual(len(event["markets"]["handicap"]), 2)
        retained_line = next(item for item in event["markets"]["handicap"] if item["line"] == "0")
        self.assertEqual(retained_line["a"], "0.90")
        self.assertEqual(event["markets"]["total"][0]["a"], "0.88")
        self.assertTrue(payload["updated_at"])

    def test_unmatched_incremental_events_are_counted_without_creating_placeholders(self):
        frame_summary = {"refresh_all": False}
        update = {
            "id": "event-1",
            "markets": {"moneyline": ["1.2", "3.4", "5.6"]},
        }

        with patch.object(app, "_pinnacle_section_events", {"n": [], "l": []}), patch.dict(
            app.state,
            {
                "events": [], "leagues": [], "updated_at": None, "received_at": None,
                "request_duration_ms": None, "upstream_http_date": None, "error": None,
                "browser_status": "Chrome 已收到赔率帧",
                "browser_login_status": "状态未确认",
            },
        ):
            app._on_browser_odds_update([update], [], frame_summary)
            self.assertEqual(
                app.state["browser_status"],
                "收到赔率增量但缺少赛事基础列表（登录状态未确认）",
            )

        self.assertEqual(frame_summary["cache_events_before"], 0)
        self.assertEqual(frame_summary["incremental_cache_matches"], 0)
        self.assertEqual(frame_summary["incremental_cache_misses"], 1)
        self.assertEqual(app.state["events"], [])

    def test_incremental_u_prices_update_the_matching_cached_event(self):
        def row(market_type, selection, line, price):
            return [0, market_type, selection, line, -100.0, price, 3781686540, 3781686267, 0, 0, "", 0, 12345]

        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "n": None, "l": None, "u": [[29, [
                row(1, 0, None, "1.132"), row(1, 1, None, "16.520"), row(1, 2, None, "7.010"),
                row(2, 0, -0.5, "0.95"), row(2, 1, 0.5, "0.97"),
                row(2, 0, 0, "0.91"), row(2, 1, 0, "0.92"),
                row(3, 3, 2.5, "0.91"), row(3, 4, 2.5, "0.99"),
            ], 1790000000000, 1]]},
        }
        partial_events, leagues = parse_incremental_odds_update(message, app.parse_response)
        existing_event = {
            "id": "12345", "is_live": True, "home": "主队", "away": "客队", "league": "测试联赛",
            "markets": {
                "handicap": [
                    {"line": "0.5", "home_line": "0.5", "away_line": "-0.5", "a": "0.70", "b": "0.80"},
                    {"line": "0.5", "home_line": "-0.5", "away_line": "0.5", "a": "0.90", "b": "1.00"},
                    {"line": "0", "home_line": "0", "away_line": "0", "a": "0.85", "b": "0.95"},
                ],
                "total": [{"line": "2.5", "a": "0.88", "b": "1.02"}],
                "moneyline": ["2.10", "3.30", "3.60"],
            },
            "match_quotes": [
                {"market_type": "moneyline", "period": "full", "selection": "home", "line": None, "hk_odds": 2.1},
                {"market_type": "moneyline", "period": "full", "selection": "draw", "line": None, "hk_odds": 3.3},
                {"market_type": "moneyline", "period": "full", "selection": "away", "line": None, "hk_odds": 3.6},
                {"market_type": "handicap", "period": "full", "selection": "home", "line": "-0.5", "hk_odds": 0.9},
                {"market_type": "handicap", "period": "full", "selection": "away", "line": "0.5", "hk_odds": 1.0},
                {"market_type": "total", "period": "full", "selection": "over", "line": "2.5", "hk_odds": 0.88},
                {"market_type": "total", "period": "full", "selection": "under", "line": "2.5", "hk_odds": 1.02},
            ],
        }
        frame_summary = {"refresh_all": False, "frame_received_at": datetime.now().astimezone().isoformat()}

        with patch.object(app, "_pinnacle_section_events", {"n": [], "l": [existing_event]}), patch.dict(
            app.state,
            {
                "events": [existing_event], "leagues": ["测试联赛"], "updated_at": None,
                "received_at": None, "request_duration_ms": None, "upstream_http_date": None,
                "error": None, "browser_status": "Chrome 已收到赔率快照",
            },
        ):
            app._on_browser_odds_update(partial_events, leagues, frame_summary)
            status, body = self.request("GET", "/api/events")

        self.assertEqual(status, 200)
        event = json.loads(body)["events"][0]
        self.assertEqual(event["markets"]["moneyline"], ["16.520", "1.132", "7.010"])
        opposite_home = event["markets"]["handicap"][0]
        opposite_away = event["markets"]["handicap"][1]
        draw_line = event["markets"]["handicap"][2]
        self.assertEqual((opposite_home["a"], opposite_home["b"]), ("0.70", "0.80"))
        self.assertEqual((opposite_away["a"], opposite_away["b"]), ("0.95", "0.97"))
        self.assertEqual((draw_line["a"], draw_line["b"]), ("0.91", "0.92"))
        self.assertEqual(event["markets"]["total"][0]["a"], "0.91")
        self.assertEqual(event["markets"]["total"][0]["b"], "0.99")
        quotes = {(quote["market_type"], quote["selection"], quote["line"]): quote["hk_odds"] for quote in event["match_quotes"]}
        self.assertEqual(quotes[("handicap", "home", "-0.5")], 0.95)
        self.assertEqual(quotes[("handicap", "away", "0.5")], 0.97)
        self.assertEqual(quotes[("handicap", "home", "0")], 0.91)
        self.assertEqual(quotes[("handicap", "away", "0")], 0.92)
        self.assertEqual(quotes[("total", "over", "2.5")], 0.91)

    def test_incremental_zero_handicap_updates_row_from_parsed_full_snapshot(self):
        full_match = [None] * 9
        full_match[8] = {"0": [[[0, 0, 0.0, "0.85", "0.95"]], [], []]}
        markets = app.extract_markets(full_match)
        previous = {"id": "12345", "markets": {
            "handicap": markets["handicap"],
            "total": markets["total"],
            "moneyline": markets["moneyline"],
        }, "match_quotes": markets["match_quotes"]}
        update = {
            "id": "12345",
            "markets": {"handicap": [{"selection": "home", "side_line": "0", "price": "0.91"}]},
            "match_quotes": [],
        }

        merged = app._merge_incremental_event(previous, update)

        self.assertEqual(merged["markets"]["handicap"][0]["a"], "0.91")
        quote = next(
            quote for quote in merged["match_quotes"]
            if quote["market_type"] == "handicap" and quote["selection"] == "home"
        )
        self.assertEqual((quote["line"], quote["hk_odds"]), ("0", 0.91))

    def test_ambiguous_incremental_handicap_match_is_skipped(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "n": None,
                "l": None,
                "u": [[29, [[
                    0, 2, 0, -0.5, -100.0, "0.95", 1, 2, 0, 0, "", 0, 12345,
                ]], 1790000000000, 1]],
            },
        }
        partial_events, leagues = parse_incremental_odds_update(message, app.parse_response)
        existing_event = {
            "id": "12345", "is_live": True, "home": "主队", "away": "客队", "league": "测试联赛",
            "markets": {
                "handicap": [
                    {"line": "0.5", "home_line": "-0.5", "away_line": "0.5", "a": "0.90", "b": "1.00"},
                    {"line": "0.5/1", "home_line": "-0.5", "away_line": "1.0", "a": "0.91", "b": "0.99"},
                ],
                "total": [], "moneyline": [],
            },
            "match_quotes": [
                {"market_type": "handicap", "period": "full", "selection": "home", "line": "-0.5", "hk_odds": 0.90},
            ],
        }
        original_event = json.loads(json.dumps(existing_event))
        frame_summary = {"refresh_all": False, "frame_received_at": "2026-10-27T00:00:00+00:00"}

        with patch.object(app, "_pinnacle_section_events", {"n": [], "l": [existing_event]}), patch.dict(
            app.state,
            {
                "events": [existing_event], "leagues": ["测试联赛"],
                "updated_at": "before", "received_at": "before", "request_duration_ms": None,
                "upstream_http_date": None, "error": None, "browser_status": "Chrome 已收到赔率快照",
            },
        ):
            app._on_browser_odds_update(partial_events, leagues, frame_summary)

            self.assertEqual(app.state["events"][0], original_event)
            self.assertEqual(app.state["updated_at"], "before")

    def test_handicap_freshness_requires_a_unique_matching_side_and_line(self):
        previous = {
            "markets": {"handicap": [
                {"home_line": "0.5", "away_line": "-0.5"},
            ]},
        }

        self.assertTrue(app._incremental_patch_is_applicable(previous, {
            "markets": {"handicap": [{"selection": "away", "side_line": "-0.5", "price": "0.90"}]},
        }))
        self.assertFalse(app._incremental_patch_is_applicable(previous, {
            "markets": {"handicap": [{"selection": "home", "side_line": "-0.5", "price": "0.90"}]},
        }))
        self.assertFalse(app._incremental_patch_is_applicable({
            "markets": {"handicap": [
                {"home_line": "-0.5", "away_line": "0.5"},
                {"home_line": "-0.5", "away_line": "1.5"},
            ]},
        }, {
            "markets": {"handicap": [{"selection": "home", "side_line": "-0.5", "price": "0.90"}]},
        }))

    def test_websocket_reconnect_keeps_cached_events_until_new_snapshot(self):
        cached = {
            "id": "12345", "is_live": True, "home": "主队", "away": "客队",
            "league": "测试联赛", "markets": {"handicap": [], "total": [], "moneyline": []},
        }
        sections = {"n": [], "l": [cached]}
        with patch.object(app, "_pinnacle_section_events", sections), patch.dict(
            app.state,
            {
                "events": [cached], "leagues": ["测试联赛"],
                "updated_at": "before", "received_at": "before",
                "browser_status": "Chrome 赔率数据新鲜",
            },
        ):
            app._on_browser_status("websocket_detected")

            self.assertEqual(app.state["events"], [cached])
            self.assertEqual(app.state["leagues"], ["测试联赛"])
            self.assertEqual(sections["l"], [cached])
            self.assertEqual(app.state["updated_at"], "before")
            self.assertEqual(app.state["received_at"], "before")
            self.assertEqual(app.state["browser_status"], "已检测到平博赔率 WebSocket")

    def test_legacy_direct_login_routes_are_disabled(self):
        for path in ("/api/pinnacle/login", "/api/pinnacle/session/check"):
            with self.subTest(path=path):
                status, body = self.request("POST", path, {})
                self.assertEqual(status, 410)
                self.assertEqual(json.loads(body)["code"], "browser_required")


if __name__ == "__main__":
    unittest.main()
