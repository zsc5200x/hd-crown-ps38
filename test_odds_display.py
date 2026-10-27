import unittest
from datetime import date, datetime
from unittest.mock import patch

import app
from crown_runtime import _normalize_event, _normalize_more_markets


class OddsDisplayTests(unittest.TestCase):
    def test_pinnacle_feed_keeps_live_and_only_local_today_prematch(self):
        today = date(2026, 10, 27)
        def local_timestamp(day):
            return int(datetime(day.year, day.month, day.day, 12).astimezone().timestamp() * 1000)

        events = [
            {"id": "live", "is_live": True},
            {"id": "today", "is_live": False, "start_ms": local_timestamp(today)},
            {"id": "yesterday", "is_live": False, "start_ms": local_timestamp(date(2026, 10, 26))},
            {"id": "tomorrow", "is_live": False, "start_ms": local_timestamp(date(2026, 10, 28))},
            {"id": "unknown", "is_live": False},
        ]

        visible = app._pinnacle_today_events(events, today=today)

        self.assertEqual([event["id"] for event in visible], ["live", "today"])

    def test_partial_full_snapshots_merge_live_and_today_sections(self):
        today = datetime.now().astimezone().date()
        today_event = {
            "id": "today", "is_live": False,
            "start_ms": int(datetime(today.year, today.month, today.day, 12).astimezone().timestamp() * 1000),
            "league": "今日联赛",
        }
        live_event = {"id": "live", "is_live": True, "league": "滚球联赛"}
        n_only = {
            "refresh_all": True,
            "n": {"present": True, "kind": "list"},
            "l": {"present": False, "kind": "NoneType"},
        }
        l_only = {
            "refresh_all": True,
            "n": {"present": False, "kind": "NoneType"},
            "l": {"present": True, "kind": "list"},
        }

        with patch.dict(app.state, {"events": [], "leagues": [], "updated_at": None}), \
                patch.dict(app._pinnacle_section_events, {"n": [], "l": []}):
            app._on_browser_odds_update([today_event], [], n_only)
            app._on_browser_odds_update([live_event], [], l_only)

            self.assertEqual(
                {event["id"] for event in app.state["events"]}, {"today", "live"}
            )

    def test_pinnacle_hk_values_render_in_crown_two_decimal_format(self):
        event = {
            "markets": {
                "handicap": [{"line": "0.5", "a": "1.990", "b": "0.367"}],
                "total": [],
                "moneyline": ["1.990", "3.200", "0.367"],
            }
        }
        with patch.object(app, "PINNACLE_ODDS_FORMAT", "HK"):
            result = app._pinnacle_display_event(event)
        self.assertEqual(result["display_markets"]["handicap"][0]["a_hk"], "1.99")
        self.assertEqual(result["display_markets"]["handicap"][0]["b_hk"], "0.36")
        self.assertEqual(result["display_markets"]["moneyline"], ["1.99", "3.20", "0.36"])
        self.assertEqual(result["markets"]["handicap"][0]["a"], "1.990")

    def test_pinnacle_european_values_convert_to_hong_kong(self):
        event = {"markets": {"handicap": [{"line": "0", "a": "2.50", "b": "1.80"}]}}
        with patch.object(app, "PINNACLE_ODDS_FORMAT", "EU"):
            result = app._pinnacle_display_event(event)
        quote = result["display_markets"]["handicap"][0]
        self.assertEqual((quote["a_hk"], quote["b_hk"]), ("1.50", "0.80"))

    def test_pinnacle_display_keeps_alternates_and_formats_asian_lines(self):
        event = {
            "home": "主队", "away": "客队",
            "markets": {
                "handicap": [
                    {"line": "0.5-1", "home_line": "-0.5", "away_line": "0.5", "a": "0.91", "b": "0.99"},
                    {"line": "0.0", "home_line": "", "away_line": "", "a": "0.88", "b": "1.02"},
                    {"line": "0-0.5", "home_line": "0.5", "away_line": "-0.5", "a": "1.10", "b": "0.82"},
                ],
                "total": [{"line": "3.5-4", "a": "0.93", "b": "0.95"}],
                "moneyline": ["2.10", "3.30", "3.60"],
            },
        }

        with patch.object(app, "PINNACLE_ODDS_FORMAT", "HK"):
            result = app._pinnacle_display_event(event)

        handicap = result["display_markets"]["handicap"]
        self.assertEqual(len(handicap), 3)
        self.assertEqual(handicap[0]["display_line"], "0.5/1")
        self.assertEqual((handicap[0]["home_display_line"], handicap[0]["away_display_line"]), ("-0.5/1", "+0.5/1"))
        self.assertEqual((handicap[1]["home_display_line"], handicap[1]["away_display_line"]), ("0（平手）", "0（平手）"))
        self.assertEqual((handicap[2]["home_display_line"], handicap[2]["away_display_line"]), ("+0/0.5", "-0/0.5"))
        self.assertEqual(result["display_markets"]["total"][0]["display_line"], "3.5/4")
        self.assertEqual(result["display_markets"]["total"][0]["a"], "0.93")
        self.assertEqual(len(event["markets"]["handicap"]), 3)
        self.assertEqual(event["markets"]["total"][0]["line"], "3.5-4")

    def test_pinnacle_parser_retains_home_and_away_handicap_lines(self):
        match = [None] * 9
        match[8] = {"0": [
            [["-0.5", "0.5", "0.5-1", "0.91", "0.99"]],
            [["3.5-4", "3.75", "0.93", "0.95"]],
            [],
        ]}

        result = app.extract_markets(match)
        self.assertEqual(result["handicap"][0]["home_line"], "-0.5")
        self.assertEqual(result["handicap"][0]["away_line"], "0.5")
        self.assertEqual(result["total"][0]["line"], "3.5-4")

    def test_pinnacle_parser_preserves_zero_handicap_side_lines(self):
        match = [None] * 9
        match[8] = {"0": [[[0, 0, 0.0, "0.85", "0.95"]], [], []]}

        result = app.extract_markets(match)

        self.assertEqual(result["handicap"][0]["home_line"], "0")
        self.assertEqual(result["handicap"][0]["away_line"], "0")
        self.assertEqual(
            {(quote["selection"], quote["line"]) for quote in result["match_quotes"]},
            {("home", "0"), ("away", "0")},
        )

    def test_crown_paired_ior_is_converted_using_both_prices(self):
        event = _normalize_event({
            "GID": "1", "TEAM_H": "主队", "TEAM_C": "客队",
            "RATIO_R": "0.5", "IOR_RH": "0.750", "IOR_RC": "1.000",
        }, "today")
        quote = event["markets"]["handicap"][0]
        self.assertEqual((quote["a_hk"], quote["b_hk"]), ("0.75", "1.00"))
        self.assertEqual(quote["odds_format"], "CROWN_HK")

    def test_crown_total_is_exported_as_over_then_under(self):
        event = _normalize_event({
            "GID": "1", "TEAM_H": "主队", "TEAM_C": "客队",
            "RATIO_OUO": "2.5", "IOR_OUH": "0.820", "IOR_OUC": "1.020",
        }, "today")
        quote = event["markets"]["total"][0]
        self.assertEqual((quote["a_hk"], quote["b_hk"]), ("1.02", "0.82"))

    def test_game_more_keeps_full_time_master_and_alternate_lines(self):
        rows = [
            {
                "GID": "100", "GIDM": "200", "HGID": "101", "MASTER": "Y",
                "MODE": "FT", "PTYPE": "", "RATIO": "0", "RATIO_O": "2.5",
                "RATIO_OUCO": "0.5", "RATIO_OUCU": "0.5",
                "IOR_RH": "0.75", "IOR_RC": "1.05",
                "IOR_OUC": "0.95", "IOR_OUH": "0.85",
                "IOR_OUCO": "1.20", "IOR_OUCU": "0.80",
                "IOR_MH": "1.20", "IOR_MN": "3.40", "IOR_MC": "5.60",
            },
            {
                "GID": "102", "GIDM": "200", "HGID": "103", "MASTER": "N",
                "MODE": "FT", "PTYPE": "", "RATIO": "0.5 / 1", "RATIO_O": "3",
                "RATIO_OUCO": "0.5", "RATIO_OUCU": "0.5",
                "IOR_RH": "1.12", "IOR_RC": "0.77",
                "IOR_OUC": "1.19", "IOR_OUH": "0.69",
                "IOR_OUCO": "1.20", "IOR_OUCU": "0.80",
                "IOR_MH": "0", "IOR_MN": "0", "IOR_MC": "0",
            },
        ]
        result = _normalize_more_markets(rows, "CROWN_HK")
        self.assertEqual([item["gid"] for item in result["handicap"]], ["100", "102"])
        self.assertEqual([item["line"] for item in result["handicap"]], ["0", "0.75"])
        self.assertEqual([item["line"] for item in result["total"]], ["2.5", "3"])
        self.assertTrue(all(item["market_type"] in {"handicap", "total"} for item in result["handicap"] + result["total"]))
        self.assertEqual(result["moneyline"][0]["gid"], "100")

    def test_game_more_master_ids_are_attached_to_base_markets(self):
        more = {
            "status": "loaded",
            "handicap": [{"gid": "100", "gidm": "200", "hgid": "101", "market_id": "200", "master": "Y", "line": "0.75"}],
            "total": [{"gid": "110", "gidm": "210", "hgid": "111", "market_id": "210", "master": "Y", "line": "2.5"}],
            "moneyline": [], "rows": [],
        }
        event = _normalize_event({
            "GID": "100", "TEAM_H": "主队", "TEAM_C": "客队",
            "RATIO_R": "0.5 / 1", "IOR_RH": "1.12", "IOR_RC": "0.77",
            "RATIO_OUO": "2.5", "IOR_OUC": "1.19", "IOR_OUH": "0.69",
        }, "today", more_markets=more)
        self.assertEqual(event["markets"]["handicap"][0]["gid"], "100")
        self.assertEqual(event["markets"]["handicap"][0]["market_id"], "200")
        self.assertEqual(event["markets"]["total"][0]["gid"], "110")

    def test_game_more_live_total_uses_rouo_ratio(self):
        result = _normalize_more_markets([{
            "GID": "500", "GIDM": "600", "HGID": "501", "MASTER": "N",
            "MODE": "FT", "PTYPE": "", "RATIO_RE": "0.5",
            "RATIO_ROUO": "2.5", "RATIO_ROUU": "2.5",
            "IOR_REH": "0.80", "IOR_REC": "1.00",
            "IOR_ROUC": "0.90", "IOR_ROUH": "0.80",
        }], "CROWN_HK")
        self.assertEqual(result["total"][0]["line"], "2.5")


if __name__ == "__main__":
    unittest.main()
