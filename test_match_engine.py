import unittest
from datetime import datetime, timezone

from match_engine import (
    canonical_event_id,
    league_equivalent,
    match_events,
    normalize_team,
    parse_crown_start,
    parse_pinnacle_start,
    team_similarity,
)


class MatchEngineTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 26, 4, 0, tzinfo=timezone.utc)

    def crown(self, **updates):
        event = {
            "id": "c-1", "league": "智利甲级联赛", "home": "帕勒斯蒂诺",
            "away": "科洛科洛", "is_live": False, "score": "",
            "start_label": "10-25 08:00p",
            "markets": {
                "handicap": [{"line": "0 / 0.5", "a": "0.90", "b": "0.90"}],
                "total": [{"line": "2.5", "a": "0.85", "b": "0.95"}],
            },
        }
        event.update(updates)
        return event

    def pinnacle(self, **updates):
        event = {
            "id": "p-1", "league": "智利甲级联赛", "home": "帕莱斯蒂诺",
            "away": "科洛科洛", "is_live": False, "score": "",
            "start_ms": 1792972800000,
            "markets": {
                "handicap": [{"line": "0-0.5", "a": "0.88", "b": "0.92"}],
                "total": [{"line": "2.5", "a": "0.87", "b": "0.94"}],
            },
        }
        event.update(updates)
        return event

    def test_curated_team_aliases_and_traditional_text_normalize(self):
        self.assertEqual(normalize_team("帕勒斯蒂诺"), normalize_team("帕莱斯蒂诺"))
        self.assertEqual(normalize_team("臺北體育會"), "台北体育会")
        self.assertEqual(normalize_team("巴洛克斯"), normalize_team("巴拉卡斯中央"))
        self.assertEqual(normalize_team("里奥夸尔托学生队"), normalize_team("里奥夸尔托大学生"))
        self.assertTrue(league_equivalent("阿根廷职业联赛", "阿根廷 - 职业联赛"))

    def test_league_division_wording_is_not_rewritten_by_matcher(self):
        self.assertFalse(league_equivalent("乌拉圭甲组联赛", "乌拉圭甲级联赛"))

    def test_vendored_team_matcher_is_auxiliary_for_latin_names(self):
        self.assertGreaterEqual(team_similarity("Manchester United", "Man Utd"), 0.95)
        self.assertGreater(team_similarity("Manchester United", "Manchester City"), 0.0)
        self.assertLess(team_similarity("Manchester United", "Manchester City"), 0.9)

    def test_league_label_difference_does_not_block_strong_fixture(self):
        result = match_events(
            [self.crown(league="POR D1")],
            [self.pinnacle(league="Portuguese Primeira Liga")],
            now=self.now,
        )
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertFalse(result["rows"][0]["match_profile"]["league_exact"])

    def test_confirmed_event_gets_stable_canonical_id(self):
        crown, pinnacle = self.crown(), self.pinnacle()
        first = canonical_event_id(crown, pinnacle, now=self.now, crown_timezone="America/New_York")
        second = canonical_event_id(crown, pinnacle, now=self.now, crown_timezone="America/New_York")
        self.assertTrue(first and first.startswith("evt_"))
        self.assertEqual(first, second)

    def test_eventmatcher_time_window_is_a_hard_identity_gate(self):
        """A team-name helper must not override Maduna's kickoff guard."""
        result = match_events(
            [self.crown()],
            [self.pinnacle(start_ms=1792976400000)],  # one hour later
            now=self.now,
        )
        self.assertEqual(result["counts"]["confirmed"], 0)
        self.assertIn(result["rows"][0]["status"], {"candidate", "unmatched"})

    def test_team_matcher_token_alias_is_only_a_fixture_candidate_signal(self):
        """Man Utd/Manchester United can be resolved without league equality."""
        crown = {
            "id": "c-man", "league": "EPL", "home": "Man Utd", "away": "Liverpool",
            "is_live": False, "score": "", "start_label": "10-25 08:00p", "markets": {},
        }
        pinnacle = {
            "id": "p-man", "league": "Premier League", "home": "Manchester United FC",
            "away": "Liverpool FC", "is_live": False, "score": "", "start_ms": 1792972800000,
            "markets": {},
        }
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        profile = result["rows"][0].get("match_profile", {})
        self.assertGreaterEqual(profile.get("team_home_score", 0), 0.84)

    def test_auxiliary_name_similarity_cannot_override_marker_conflict(self):
        crown = {
            "id": "c-woman", "league": "Premier League Women", "home": "Manchester United (Women)",
            "away": "Liverpool (Women)", "is_live": False, "score": "",
            "start_label": "10-25 08:00p", "markets": {},
        }
        pinnacle = {
            "id": "p-man", "league": "Premier League", "home": "Manchester United FC",
            "away": "Liverpool FC", "is_live": False, "score": "", "start_ms": 1792972800000,
            "markets": {},
        }
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 0)
        self.assertEqual(result["rows"][0]["status"], "conflict")

    def test_shared_city_name_is_not_enough_for_event_identity(self):
        crown = {
            "id": "c-city", "league": "England - Premier League", "home": "Manchester United",
            "away": "Liverpool", "is_live": False, "score": "",
            "start_label": "10-25 08:00p", "markets": {},
        }
        pinnacle = {
            "id": "p-city", "league": "England - Premier League", "home": "Manchester City",
            "away": "Liverpool", "is_live": False, "score": "", "start_ms": 1792972800000,
            "markets": {},
        }
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 0)
        self.assertIn(result["rows"][0]["status"], {"candidate", "unmatched"})

    def test_argentine_provider_aliases_confirm_both_fixture_rows(self):
        crown_events = [
            {
                "id": "c-arg-1", "league": "阿根廷职业联赛", "home": "巴洛克斯",
                "away": "里奥夸尔托学生队", "is_live": False, "score": "",
                "start_label": "10-25 08:30p",
                "markets": {"total": [{"line": "2.5", "a": "0.83", "b": "1.04"}]},
            },
            {
                "id": "c-arg-2", "league": "阿根廷职业联赛", "home": "科尔多瓦圣地亚哥中央队",
                "away": "沙士菲", "is_live": False, "score": "",
                "start_label": "10-25 08:30p",
                "markets": {"total": [{"line": "2.5", "a": "0.87", "b": "1.00"}]},
            },
        ]
        pinnacle_events = [
            {
                "id": "p-arg-1", "league": "阿根廷 - 职业联赛", "home": "巴拉卡斯中央",
                "away": "里奥夸尔托大学生", "is_live": False, "score": "",
                "start_ms": 1792974600000,
                "markets": {"total": [{"line": "2.5", "a": "1.45", "b": "0.59"}]},
            },
            {
                "id": "p-arg-2", "league": "阿根廷 - 职业联赛", "home": "科尔多瓦中央",
                "away": "萨斯菲尔德", "is_live": False, "score": "",
                "start_ms": 1792974600000,
                "markets": {"total": [{"line": "2.5", "a": "1.10", "b": "0.79"}]},
            },
        ]
        result = match_events(
            crown_events, pinnacle_events, now=self.now,
            crown_timezone="America/New_York",
        )
        self.assertEqual(result["counts"]["confirmed"], 2)
        self.assertEqual(
            {row["pinnacle"]["id"] for row in result["confirmed"]},
            {"p-arg-1", "p-arg-2"},
        )

    def test_parses_epoch_milliseconds_and_crown_local_label(self):
        self.assertEqual(parse_pinnacle_start(1792972800000), datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc))
        crown_time = parse_crown_start("10-25 08:00p", now=self.now)
        self.assertEqual(crown_time, datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc))

    def test_confirms_alias_match_only_with_matching_league_state_and_time(self):
        result = match_events([self.crown()], [self.pinnacle()], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertEqual(result["rows"][0]["status"], "confirmed")
        self.assertEqual(result["rows"][0]["pinnacle"]["id"], "p-1")

    def test_reversed_home_away_is_not_confirmed(self):
        result = match_events(
            [self.crown()],
            [self.pinnacle(home="科洛科洛", away="帕莱斯蒂诺")],
            now=self.now,
        )
        self.assertEqual(result["rows"][0]["status"], "conflict")
        self.assertIsNone(result["rows"][0]["pinnacle"])

    def test_womens_and_mens_events_are_not_candidates(self):
        result = match_events(
            [self.crown(league="智利女子甲级联赛")],
            [self.pinnacle()],
            now=self.now,
        )
        self.assertEqual(result["rows"][0]["status"], "conflict")
        self.assertEqual(result["counts"]["confirmed"], 0)

    def test_one_pinnacle_event_cannot_confirm_twice(self):
        result = match_events(
            [self.crown(id="c-1"), self.crown(id="c-2")],
            [self.pinnacle()],
            now=self.now,
        )
        self.assertLessEqual(result["counts"]["confirmed"], 1)
        self.assertEqual(result["counts"]["conflict"], 2)

    def test_market_evidence_is_auxiliary_for_automatic_confirmation(self):
        crown = self.crown(markets={})
        pinnacle = self.pinnacle(markets={})
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertEqual(result["rows"][0]["status"], "confirmed")
        self.assertIn("盘口线/水位未对齐，按联赛、球队、时间和状态合并", result["rows"][0]["evidence"])

    def test_different_market_line_does_not_block_core_fixture_match(self):
        crown = self.crown(markets={
            "total": [{"line": "2.5", "a": "0.85", "b": "0.95"}],
        })
        pinnacle = self.pinnacle(markets={
            "total": [{"line": "3.0", "a": "0.85", "b": "0.95"}],
        })
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertEqual(result["rows"][0]["status"], "confirmed")

    def test_same_line_and_similar_total_prices_are_evidence(self):
        result = match_events([self.crown()], [self.pinnacle()], now=self.now)
        self.assertTrue(result["rows"][0]["market_matched"])
        self.assertTrue(any("大小球盘口 2.5 相同" in item for item in result["rows"][0]["evidence"]))

    def test_different_line_is_auxiliary_mismatch_only(self):
        result = match_events(
            [self.crown()],
            [self.pinnacle(markets={"total": [{"line": "3.0", "a": "0.85", "b": "0.95"}]})],
            now=self.now,
        )
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertFalse(result["rows"][0].get("market_matched", False))

    def test_directional_quotes_match_handicap_sign_conventions(self):
        crown = self.crown(markets={}, match_quotes=[
            {"market_type": "handicap", "period": "full", "selection": "home", "line": "-0.5", "hk_odds": 0.80},
            {"market_type": "handicap", "period": "full", "selection": "away", "line": "0.5", "hk_odds": 0.90},
        ])
        pinnacle = self.pinnacle(markets={}, match_quotes=[
            {"market_type": "handicap", "period": "full", "selection": "home", "line": "0.5", "hk_odds": 0.81},
            {"market_type": "handicap", "period": "full", "selection": "away", "line": "-0.5", "hk_odds": 0.91},
        ])
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertTrue(result["rows"][0]["market_matched"])

    def test_directional_quote_selection_mismatch_does_not_block_core_match(self):
        crown = self.crown(markets={}, match_quotes=[
            {"market_type": "total", "period": "full", "selection": "over", "line": "2.5", "hk_odds": 0.80},
            {"market_type": "total", "period": "full", "selection": "under", "line": "2.5", "hk_odds": 0.90},
        ])
        pinnacle = self.pinnacle(markets={}, match_quotes=[
            {"market_type": "total", "period": "full", "selection": "over", "line": "3.0", "hk_odds": 0.80},
            {"market_type": "total", "period": "full", "selection": "under", "line": "3.0", "hk_odds": 0.90},
        ])
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertFalse(result["rows"][0].get("market_matched", False))

    def test_direction_aware_quotes_match_handicap_signs_by_selection(self):
        crown = self.crown(
            markets={},
            match_quotes=[
                {"market_type": "handicap", "period": "full", "selection": "home", "line": "-0.5", "hk_odds": 0.90},
                {"market_type": "handicap", "period": "full", "selection": "away", "line": "0.5", "hk_odds": 0.91},
            ],
        )
        pinnacle = self.pinnacle(
            markets={},
            match_quotes=[
                {"market_type": "handicap", "period": "full", "selection": "home", "line": "0.5", "hk_odds": 0.88},
                {"market_type": "handicap", "period": "full", "selection": "away", "line": "-0.5", "hk_odds": 0.93},
            ],
        )
        result = match_events([crown], [pinnacle], now=self.now)
        self.assertEqual(result["counts"]["confirmed"], 1)
        self.assertTrue(any("让球盘口 0.5 相同" in item for item in result["rows"][0]["evidence"]))


if __name__ == "__main__":
    unittest.main()
