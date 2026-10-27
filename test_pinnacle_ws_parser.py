import unittest

import app
from pinnacle_ws_parser import (
    UnsupportedOddsUpdate,
    parse_complete_odds_update,
    parse_incremental_odds_update,
)


def full_live_tree():
    match = [12345, "主队", "客队", None, None]
    league = [77, "测试联赛", [match]]
    return [[29, "Soccer", [league]]]


def full_prematch_tree():
    match = [54321, "赛前主队", "赛前客队", None, None]
    league = [88, "赛前联赛", [match]]
    return [[29, "Soccer", [league]]]


def incremental_live_tree(home_price="0.84"):
    market_row = ["-0.5", "0.5", "-0.5", home_price, "1.02"]
    match = [
        12345, "主队", "客队", None, 1790000000000, None, None, None,
        {"0": [[market_row], [], []]},
    ]
    league = [77, "测试联赛", [match]]
    return [[29, "Soccer", [league]]]


class PinnacleWebSocketParserTests(unittest.TestCase):
    def test_refresh_all_parses_prematch_and_live_trees(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": True,
                "n": full_prematch_tree(),
                "l": full_live_tree(),
            },
        }

        events, leagues = parse_complete_odds_update(message, app.parse_response)

        self.assertEqual({event["id"] for event in events}, {"12345", "54321"})
        self.assertEqual(
            {event["id"]: event["is_live"] for event in events},
            {"12345": True, "54321": False},
        )
        self.assertEqual(leagues, ["测试联赛", "赛前联赛"])

    def test_refresh_all_accepts_empty_live_tree(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": True, "n": full_prematch_tree(), "l": None},
        }

        events, _ = parse_complete_odds_update(message, app.parse_response)

        self.assertEqual([event["id"] for event in events], ["54321"])
        self.assertFalse(events[0]["is_live"])

    def test_incremental_u_frame_is_rejected_without_interpreting_indices(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "u": "opaque", "l": None},
        }

        with self.assertRaises(UnsupportedOddsUpdate):
            parse_complete_odds_update(message, app.parse_response)

    def test_incremental_compact_tree_parses_only_valid_event_odds(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "n": None,
                "l": incremental_live_tree(),
                "u": [[999, [1, 2, 3], 888]],
            },
        }

        events, leagues = parse_incremental_odds_update(message, app.parse_response)

        self.assertEqual([event["id"] for event in events], ["12345"])
        self.assertTrue(events[0]["is_live"])
        self.assertEqual(events[0]["markets"]["handicap"][0]["a"], "0.84")
        self.assertEqual(leagues, ["测试联赛"])

    def test_incremental_tree_without_valid_prices_is_rejected(self):
        tree = incremental_live_tree("")
        tree[0][2][0][2][0][8]["0"][0][0][4] = ""
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "l": tree, "u": []},
        }

        with self.assertRaises(UnsupportedOddsUpdate):
            parse_incremental_odds_update(message, app.parse_response)

    def test_incremental_u_market_rows_decode_full_time_prices_by_event_id(self):
        def row(market_type, selection, line, price, event_id=12345, period=0):
            return [period, market_type, selection, line, -100.0, price, 3781686540, 3781686267, 0, 0, "", 0, event_id]

        message = {
            "type": "UPDATE_ODDS",
            "odds": {
                "refreshAll": False,
                "n": None,
                "l": None,
                "u": [[29, [
                    row(1, 0, None, "1.132"),
                    row(1, 1, None, "16.520"),
                    row(1, 2, None, "7.010"),
                    row(2, 0, -0.5, "0.90"),
                    row(2, 1, 0.5, "1.02"),
                    row(3, 3, 2.5, "0.88"),
                    row(3, 4, 2.5, "1.04"),
                    row(1, 1, None, "99.0", period=1),
                    ["1H", "0-0", 9, 12345],
                ], 1790000000000, 1]],
            },
        }

        diagnostics = {}
        events, leagues = parse_incremental_odds_update(
            message, app.parse_response, diagnostics
        )

        self.assertEqual(leagues, [])
        self.assertEqual(len(events), 1)
        self.assertNotIn("is_live", events[0])
        self.assertEqual(events[0]["id"], "12345")
        self.assertEqual(events[0]["markets"]["moneyline"], ["16.520", "1.132", "7.010"])
        self.assertEqual(events[0]["markets"]["handicap"][0], {
            "selection": "home", "side_line": "-0.5", "price": "0.90",
        })
        self.assertEqual(events[0]["markets"]["total"][0], {"line": "2.5", "selection": "over", "price": "0.88"})
        self.assertEqual(diagnostics["incremental_u"], {
            "groups": 1,
            "soccer_groups": 1,
            "non_soccer_groups": 0,
            "malformed_groups": 0,
            "rows_seen": 9,
            "short_rows": 1,
            "invalid_market_columns": 0,
            "non_full_period": 1,
            "invalid_event_ids": 0,
            "invalid_prices": 0,
            "unsupported_market_selection": 0,
            "invalid_lines": 0,
            "accepted_market_rows": 7,
            "events_emitted": 1,
        })
        self.assertNotIn("12345", str(diagnostics))
        self.assertNotIn("1.132", str(diagnostics))

    def test_partial_live_tree_is_rejected_without_refresh_all_marker(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": False, "l": full_live_tree()},
        }

        with self.assertRaises(UnsupportedOddsUpdate):
            parse_complete_odds_update(message, app.parse_response)

    def test_refresh_all_without_compact_trees_is_rejected(self):
        message = {
            "type": "UPDATE_ODDS",
            "odds": {"refreshAll": True, "n": None, "l": None},
        }

        with self.assertRaises(UnsupportedOddsUpdate):
            parse_complete_odds_update(message, app.parse_response)


if __name__ == "__main__":
    unittest.main()
