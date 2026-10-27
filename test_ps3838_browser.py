import json
import unittest
from unittest.mock import patch

from ps3838_browser import (
    Ps3838BrowserController,
    is_odds_websocket_url,
    parse_odds_frame,
    summarize_other_protocol_message,
    summarize_odds_frame,
    summarize_incremental_tree_parse,
)


def parse_response(payload):
    return payload["n"], payload["l"]


class Ps3838BrowserTests(unittest.TestCase):
    def test_controller_accepts_explicit_parser_and_update_callback(self):
        on_update = lambda _prematch, _live, _summary: None
        controller = Ps3838BrowserController("/tmp/ps3838-profile", parse_response, on_update)

        self.assertIs(controller.parse_response, parse_response)
        self.assertIs(controller.on_odds_update, on_update)

    def test_auto_login_opens_login_dialog_before_filling_saved_credentials(self):
        class FakeLocator:
            def __init__(self, state, kind):
                self.state = state
                self.kind = kind
                self.first = self

            def is_visible(self):
                if self.kind in {"username", "password"}:
                    return self.state["dialog_open"] and not self.state["submitted"]
                if self.kind == "guest_banner":
                    return False
                return (
                    self.kind == "login_button"
                    and not self.state["dialog_open"]
                    and not self.state["submitted"]
                )

            def count(self):
                return 0 if self.kind == "guest_banner" else 1

            def click(self, timeout=None):
                if self.kind == "login_button":
                    self.state["dialog_open"] = True
                elif self.kind == "submit_button":
                    self.state["submitted"] = True
                    self.state["dialog_open"] = False

            def wait_for(self, state, timeout):
                if state == "visible":
                    self.assert_visible()
                elif state == "hidden" and not self.state["submitted"]:
                    raise TimeoutError("dialog is still visible")

            def assert_visible(self):
                if not self.is_visible():
                    raise TimeoutError("element is not visible")

            def fill(self, value):
                self.state[self.kind] = value

            def locator(self, _selector):
                return FakeLocator(self.state, "form")

            def get_by_role(self, role, name):
                if role != "button":
                    raise AssertionError(role)
                return FakeLocator(self.state, "submit_button")

        class FakePage:
            def __init__(self):
                self.state = {"dialog_open": False, "submitted": False}

            def locator(self, selector):
                kind = {"#loginId": "username", "#pass": "password"}[selector]
                return FakeLocator(self.state, kind)

            def get_by_role(self, role, name):
                if role != "button" or "登录" not in name.pattern:
                    raise AssertionError("unexpected login button locator")
                return FakeLocator(self.state, "login_button")

            def get_by_text(self, _text, exact=False):
                return FakeLocator(self.state, "guest_banner")

        statuses = []
        controller = Ps3838BrowserController(
            "/tmp/ps3838-profile", parse_response, lambda *_args: None, statuses.append,
        )
        credentials = {"username": "test-user", "password": "test-pass"}
        controller._credentials = credentials
        page = FakePage()

        controller._submit_credentials_once(page)

        self.assertTrue(page.state["submitted"])
        self.assertEqual(page.state["username"], "test-user")
        self.assertEqual(page.state["password"], "test-pass")
        self.assertEqual(statuses, ["login_submitted", "login_confirmed"])
        self.assertIsNone(controller._credentials)
        self.assertEqual(credentials, {})

    def test_parses_only_complete_odds_snapshot(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": True, "n": [["prematch"]], "l": [["live"]]},
        }

        self.assertEqual(
            parse_odds_frame(json.dumps(message), parse_response),
            ([["prematch"]], [["live"]]),
        )
        self.assertEqual(
            parse_odds_frame(json.dumps(message).encode("utf-8"), parse_response),
            ([["prematch"]], [["live"]]),
        )

    def test_ignores_incremental_updates(self):
        message = {"type": "UPDATE_ODDS", "odds": {"refreshAll": False, "u": []}}

        self.assertIsNone(parse_odds_frame(json.dumps(message), parse_response))

    def test_odds_frame_summary_contains_only_tree_shape_and_counts(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": True,
                "n": [[29, "SECRET_LEAGUE", [[77, "SECRET_EVENT", [[123, "SECRET_HOME", "SECRET_AWAY"]]]]]],
                "l": [],
                "u": [],
            },
        }

        summary = summarize_odds_frame(json.dumps(message))

        self.assertEqual(summary["n"], {
            "present": True, "kind": "list", "sports": 1, "leagues": 1, "matches": 1,
        })
        self.assertEqual(summary["l"]["matches"], 0)
        self.assertEqual(summary["u"]["kind"], "array")
        self.assertEqual(summary["u"]["length"], 0)
        self.assertNotIn("SECRET", json.dumps(summary))

    def test_odds_frame_summary_marks_missing_sections(self):
        summary = summarize_odds_frame(json.dumps({
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "u": []},
        }))

        self.assertFalse(summary["refresh_all"])
        self.assertFalse(summary["n"]["present"])
        self.assertFalse(summary["l"]["present"])

    def test_odds_frame_summary_describes_auxiliary_fields_without_values(self):
        summary = summarize_odds_frame(json.dumps({
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "n": None,
                "l": None,
                "u": [],
                "e": {"opaque-id": ["SECRET_TEAM", "SECRET_PRICE"]},
                "e1": ["SECRET_VALUE", 17],
            },
        }))

        self.assertEqual(summary["refresh_all_kind"], "boolean")
        self.assertEqual(summary["auxiliary_shapes"]["e"]["kind"], "object")
        self.assertEqual(summary["auxiliary_shapes"]["e"]["size"], 1)
        self.assertEqual(summary["auxiliary_shapes"]["e1"]["length"], 2)
        self.assertNotIn("SECRET", json.dumps(summary))
        self.assertNotIn("opaque-id", json.dumps(summary))

    def test_other_protocol_message_summary_keeps_only_type_and_shapes(self):
        summary = summarize_other_protocol_message({
            "type": "INITIAL_EVENTS",
            "events": [{"name": "SECRET_TEAM", "id": "123456"}],
            "token": "SECRET_TOKEN",
        })

        self.assertEqual(summary["message_type"], "INITIAL_EVENTS")
        self.assertEqual(summary["field_shapes"]["events"], {"kind": "array", "size": 1, "sample_item_kinds": ["object"]})
        self.assertNotIn("SECRET", json.dumps(summary))
        self.assertNotIn("123456", json.dumps(summary))
        self.assertNotIn("opaque", json.dumps(summary))

    def test_incremental_summary_counts_row_shapes_without_retaining_values(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "u": [[29, [
                    ["SECRET_HOME", "SECRET_AWAY", 9, 12345],
                    [0, 2, 0, -0.5, -100.0, 0.91, 1, 2, 0, 0, "", 0, 12345],
                ], 987654, 1]],
            },
        }

        summary = summarize_odds_frame(json.dumps(message))

        self.assertEqual(summary["u_rows"]["groups"], 1)
        self.assertEqual(summary["u_rows"]["records"], 2)
        self.assertEqual(summary["u_rows"]["record_length_counts"], {"4": 1, "13": 1})
        encoded = json.dumps(summary)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("12345", encoded)
        self.assertNotIn("987654", encoded)

    def test_incremental_tree_diagnostic_reports_counts_without_event_data(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "n": [[29, "SECRET_LEAGUE", [[77, "SECRET_EVENT", [[123, "SECRET_HOME", "SECRET_AWAY"]]]]]],
                "l": None,
                "u": [[1, [2, 3, 4], 5]],
            },
        }

        def parse_for_test(payload):
            events = []
            if payload["n"]:
                events.append({"is_live": False, "home": "SECRET_HOME"})
            if payload["l"]:
                events.append({"is_live": True, "home": "SECRET_HOME"})
            return events, []

        summary = summarize_incremental_tree_parse(json.dumps(message), parse_for_test)

        self.assertNotIn("parser_accepted", summary)
        self.assertTrue(summary["incremental_tree_parsed"])
        self.assertEqual(summary["tree_parsed_events"], 1)
        self.assertEqual(summary["tree_parsed_prematch"], 1)
        self.assertNotIn("SECRET", json.dumps(summary))

    def test_ignores_malformed_or_incomplete_frames(self):
        malformed = b"\xff\xfe"
        incomplete = json.dumps({
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": True, "n": None, "l": None},
        })

        self.assertIsNone(parse_odds_frame(malformed, parse_response))
        self.assertIsNone(parse_odds_frame("not-json", parse_response))
        self.assertIsNone(parse_odds_frame(incomplete, parse_response))

    def test_matches_only_ps3838_odds_socket(self):
        self.assertTrue(is_odds_websocket_url("wss://www.ps3838.com/sports-websocket/ws?token=hidden"))
        self.assertFalse(is_odds_websocket_url("wss://example.com/sports-websocket/ws"))
        self.assertFalse(is_odds_websocket_url("wss://www.ps3838.com/other"))

    def test_selects_today_tab_and_confirms_active_state(self):
        class FakeTab:
            def __init__(self):
                self.class_name = "market-tab"
                self.clicks = 0

            def count(self):
                return 1

            def is_visible(self):
                return True

            def get_attribute(self, name):
                return self.class_name if name == "class" else None

            def click(self, timeout):
                self.clicks += 1
                self.class_name += " active"

        class FakePage:
            def __init__(self):
                self.tab = FakeTab()

            def locator(self, selector):
                self.selector = selector
                return self

            def filter(self, has_text):
                self.filter_text = has_text
                return self

            @property
            def first(self):
                return self.tab

            def wait_for_timeout(self, _timeout):
                pass

        statuses = []
        controller = Ps3838BrowserController(
            "/tmp/ps3838-profile", parse_response, lambda _events, _leagues: None,
            statuses.append,
        )
        page = FakePage()

        self.assertTrue(controller._select_today_soccer(page))
        self.assertEqual(page.selector, ".SportMenuItemTabComponent .market-tab")
        self.assertEqual(page.filter_text.pattern, "今天")
        self.assertEqual(page.tab.clicks, 1)
        self.assertEqual(statuses, ["today_subscription_selected"])

    def test_forwards_complete_snapshot_from_browser_websocket(self):
        class FakeWebSocket:
            url = "wss://www.ps3838.com/sports-websocket/ws?token=not-logged"

            def __init__(self):
                self.handlers = {}

            def on(self, event, callback):
                self.handlers[event] = callback

        updates = []
        statuses = []
        diagnostics = []
        controller = Ps3838BrowserController(
            "/tmp/ps3838-profile", parse_response,
            lambda events, leagues, summary: updates.append((events, leagues, summary)),
            statuses.append,
            on_frame_diagnostic=diagnostics.append,
        )
        websocket = FakeWebSocket()
        controller._attach_websocket(websocket)
        frame = json.dumps({
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": True, "n": [["prematch"]], "l": [["live"]]},
        })
        with patch("ps3838_browser.json.loads", wraps=json.loads) as loads:
            websocket.handlers["framereceived"](frame)
        websocket.handlers["close"](websocket)

        expected_snapshot = (
            [["prematch"]],
            [["live"]],
        )
        self.assertEqual(updates, [(*expected_snapshot, diagnostics[0])])
        self.assertEqual(len(diagnostics), 1)
        self.assertTrue(diagnostics[0]["parser_accepted"])
        self.assertEqual(loads.call_count, 1)
        self.assertTrue(diagnostics[0]["frame_received_at"])
        self.assertGreaterEqual(diagnostics[0]["frame_decode_ms"], 0)
        self.assertGreaterEqual(diagnostics[0]["frame_parse_ms"], 0)
        self.assertGreaterEqual(diagnostics[0]["state_commit_ms"], 0)
        self.assertEqual(diagnostics[0]["n"]["sports"], 1)
        self.assertEqual(statuses, ["websocket_detected", "websocket_frame_received", "websocket_closed"])

    def test_incremental_runtime_frame_does_not_parse_empty_trees(self):
        class FakeWebSocket:
            url = "wss://www.ps3838.com/sports-websocket/ws?token=hidden"

            def __init__(self):
                self.handlers = {}

            def on(self, event, callback):
                self.handlers[event] = callback

        def fail_if_parsed(_payload):
            raise AssertionError("incremental frames must not invoke the full event parser")

        diagnostics = []
        updates = []
        controller = Ps3838BrowserController(
            "/tmp/ps3838-profile", fail_if_parsed,
            lambda *args: updates.append(args),
            on_frame_diagnostic=diagnostics.append,
        )
        websocket = FakeWebSocket()
        controller._attach_websocket(websocket)
        websocket.handlers["framereceived"](json.dumps({
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "n": None, "l": None, "u": [[1, [2, 3], 4]]},
        }))

        self.assertEqual(updates, [])
        self.assertEqual(len(diagnostics), 1)
        self.assertFalse(diagnostics[0]["parser_accepted"])
        self.assertNotIn("parsed_events", diagnostics[0])
        self.assertEqual(diagnostics[0]["parser_rejection"], "增量消息没有可验证的赛事赔率")
        self.assertEqual(diagnostics[0]["incremental_u"]["non_soccer_groups"], 1)
        self.assertNotIn("hidden", json.dumps(diagnostics[0]))
        self.assertGreaterEqual(diagnostics[0]["frame_processing_ms"], 0)

    def test_reports_non_odds_protocol_message_shape_without_values(self):
        class FakeWebSocket:
            url = "wss://www.ps3838.com/sports-websocket/ws?token=hidden"

            def __init__(self):
                self.handlers = {}

            def on(self, event, callback):
                self.handlers[event] = callback

        diagnostics = []
        controller = Ps3838BrowserController(
            "/tmp/ps3838-profile", parse_response, lambda *_args: None,
            on_frame_diagnostic=diagnostics.append,
        )
        websocket = FakeWebSocket()
        controller._attach_websocket(websocket)
        websocket.handlers["framereceived"](json.dumps({
            "type": "INITIAL_EVENTS",
            "events": [{"id": "123456", "name": "SECRET_TEAM"}],
        }))

        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["message_type"], "INITIAL_EVENTS")
        self.assertEqual(diagnostics[0]["field_shapes"]["events"]["size"], 1)
        self.assertNotIn("SECRET", json.dumps(diagnostics[0]))
        self.assertNotIn("123456", json.dumps(diagnostics[0]))


if __name__ == "__main__":
    unittest.main()
