import unittest
import threading
import time
from unittest.mock import patch

import app
import crown_runtime
from event_filters import is_excluded_league


class EventFilterTests(unittest.TestCase):
    def test_excludes_special_betting_marker_but_keeps_regular_league(self):
        self.assertTrue(is_excluded_league("阿根廷职业联赛-特别投注"))
        self.assertTrue(is_excluded_league("阿根廷职业联赛 - 特别投注"))
        self.assertTrue(is_excluded_league("电竞足球-GT体育联赛(2 x 6分钟)"))
        self.assertTrue(is_excluded_league("足球电竞联赛"))
        self.assertTrue(is_excluded_league("电子足球联赛"))
        self.assertFalse(is_excluded_league("阿根廷职业联赛"))
        self.assertFalse(is_excluded_league("英格兰 - 超级联赛"))
        self.assertFalse(is_excluded_league("克罗地亚足球联赛"))

    def test_pinnacle_parser_does_not_publish_special_betting_league(self):
        data = {
            "n": [[29, "Soccer", [
                [1, "阿根廷职业联赛", [[101, "主队甲", "客队甲", None, 1792974600000]]],
                [2, "阿根廷职业联赛-特别投注", [[102, "主队乙", "客队乙", None, 1792974600000]]],
            ]]],
            "l": [],
        }

        events, leagues = app.parse_response(data)

        self.assertEqual([event["id"] for event in events], ["101"])
        self.assertEqual(leagues, ["阿根廷职业联赛"])

    def test_crown_refresh_does_not_publish_special_betting_league(self):
        class FakeStore:
            def accounts(self):
                return [{"id": "test", "kind": "crown_odds", "enabled": True, "has_session": True}]

            def load_session(self, account_id):
                return {}

        class FakeClient:
            odds_format = "HK"
            raw_odds_format = "CROWN_HK"

            def __init__(self, account, session):
                pass

            def list_games(self, mode):
                if mode == "live":
                    return {"games": []}
                return {"games": [
                    {"GID": "201", "LEAGUE": "阿根廷职业联赛", "TEAM_H": "主队甲", "TEAM_C": "客队甲"},
                    {"GID": "202", "LEAGUE": "阿根廷职业联赛-特别投注", "TEAM_H": "主队乙", "TEAM_C": "客队乙"},
                    {"GID": "203", "LEAGUE": "电竞足球-GT体育联赛(2 x 6分钟)", "TEAM_H": "虚拟主队", "TEAM_C": "虚拟客队"},
                ]}

        old_state = crown_runtime.state.copy()
        self.addCleanup(crown_runtime.state.update, old_state)
        with patch("crown_runtime.initialize"), \
                patch("crown_runtime.store", FakeStore()), \
                patch("crown_runtime.CrownOddsClient", FakeClient):
            result = crown_runtime.refresh_once()

        self.assertTrue(result["ok"])
        self.assertEqual([event["id"] for event in crown_runtime.state["events"]], ["201"])
        self.assertEqual(crown_runtime.state["leagues"], ["阿根廷职业联赛"])

    def test_crown_refresh_publishes_base_odds_before_slow_details(self):
        details_started = threading.Event()
        release_details = threading.Event()
        details_finished = threading.Event()

        class FakeStore:
            def accounts(self):
                return [{"id": "test", "kind": "crown_odds", "enabled": True, "has_session": True}]

            def load_session(self, account_id):
                return {}

        class SlowDetailsClient:
            odds_format = "HK"
            raw_odds_format = "CROWN_HK"

            def __init__(self, account, session):
                pass

            def list_games(self, mode):
                if mode == "live":
                    return {"games": []}
                return {"games": [{
                    "GID": "slow-details-test",
                    "LID": "1",
                    "ECID": "2",
                    "LEAGUE": "测试联赛",
                    "TEAM_H": "主队",
                    "TEAM_C": "客队",
                    "RATIO_R": "0",
                    "IOR_RH": "0.90",
                    "IOR_RC": "0.90",
                }]}

            def game_more(self, game, mode):
                details_started.set()
                release_details.wait(3)
                details_finished.set()
                return {"games": []}

        old_state = crown_runtime.state.copy()
        self.addCleanup(crown_runtime.state.update, old_state)
        try:
            with patch("crown_runtime.initialize"), \
                    patch("crown_runtime.store", FakeStore()), \
                    patch("crown_runtime.CrownOddsClient", SlowDetailsClient):
                started = time.monotonic()
                result = crown_runtime.refresh_once()
                elapsed = time.monotonic() - started

                self.assertTrue(result["ok"])
                self.assertLess(elapsed, 0.5)
                self.assertTrue(details_started.wait(1))
                self.assertTrue(crown_runtime.state["updated_at"])
                self.assertEqual(len(crown_runtime.state["events"]), 1)
        finally:
            release_details.set()
            self.assertTrue(details_finished.wait(2))


if __name__ == "__main__":
    unittest.main()
