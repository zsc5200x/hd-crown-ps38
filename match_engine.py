"""Conservative Crown/Pinnacle fixture matching helpers.

The engine deliberately distinguishes a useful candidate from a confirmed
match. Only confirmed rows should be presented as the same fixture.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from functools import lru_cache
from pathlib import Path
import re
import sys
import unicodedata
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


# The vendored reference matcher is optional at runtime.  It provides the
# Jaccard/containment signal for Latin-script provider names; this module
# keeps its own Unicode-safe fallbacks for Chinese names and type markers.
_TEAM_MATCHER_SRC = Path(__file__).resolve().parent / "team_matcher-0.1.0" / "src"
if _TEAM_MATCHER_SRC.is_dir() and str(_TEAM_MATCHER_SRC) not in sys.path:
    sys.path.insert(0, str(_TEAM_MATCHER_SRC))
try:  # pragma: no cover - availability depends on the checkout contents
    from team_matcher import Candidate as _ReferenceCandidate
    from team_matcher import rank_candidates as _reference_rank_candidates
    from team_matcher import similarity as _reference_team_similarity
except Exception:  # pragma: no cover
    _ReferenceCandidate = None
    _reference_rank_candidates = None
    _reference_team_similarity = None

try:  # pragma: no cover - installed with the project .venv
    from rapidfuzz import fuzz as _rapidfuzz
except Exception:  # pragma: no cover - keep imports usable for read-only tools
    _rapidfuzz = None


MATCHER_VERSION = "2026-10-26-eventmatcher-team-matcher-v2"


_TRADITIONAL = str.maketrans({
    "體": "体", "育": "育", "會": "会", "隊": "队", "聯": "联", "賽": "赛",
    "國": "国", "際": "际", "學": "学", "術": "术", "龍": "龙", "馬": "马",
    "門": "门", "華": "华", "東": "东", "臺": "台", "灣": "湾", "廣": "广",
    "業": "业", "區": "区", "總": "总", "進": "进", "場": "场", "後": "后",
    "當": "当", "時": "时", "隊": "队", "聯": "联", "會": "会", "奧": "奥",
    "維": "维", "亞": "亚", "達": "达", "貝": "贝", "羅": "罗", "納": "纳",
    "蘇": "苏", "烏": "乌", "愛": "爱", "爾": "尔", "薩": "萨", "倫": "伦",
    "賓": "宾", "萊": "莱", "圖": "图", "農": "农", "將": "将", "勝": "胜",
    "負": "负", "開": "开", "關": "关", "歲": "岁", "婦": "妇", "預": "预",
})

# Curated transliteration variants observed in current provider samples. Keep
# this list explicit: broad fuzzy normalization can silently merge different clubs.
_TEAM_ALIAS_GROUPS = (
    ("巴塞隆拿sc", "巴塞罗那体育俱乐部"),
    ("帕勒斯蒂诺", "帕莱斯蒂诺"),
    ("康塞普森", "康塞普西翁大学"),
    ("胡提卡帕", "胡蒂卡尔帕"),
    ("敦伯霍登", "邓贝霍登"),
    ("加西拉索", "加尔西拉索竞技俱乐部"),
    ("洛杉矶", "洛杉矶足球俱乐部"),
    # Argentine provider transliterations used in the current live feeds.
    ("巴洛克斯", "巴拉卡斯中央"),
    ("里奥夸尔托学生队", "里奥夸尔托大学生"),
    ("科尔多瓦圣地亚哥中央队", "科尔多瓦中央"),
    ("沙士菲", "萨斯菲尔德"),
    # Cross-provider Spanish/Portuguese transliterations observed in the live
    # feeds.  These are deliberately explicit; fuzzy matching alone must not
    # decide that two semantically different clubs are the same.
    ("蒙特维多国民队", "国民足球俱乐部"),
    ("利物浦蒙特维多", "蒙得维的亚利物浦"),
    ("高士路mg女", "克鲁塞罗"),
    ("利伯泰德女", "林伯奥自由"),
    ("国家体育会", "麦德林国民竞技"),
    ("曼特宁独立", "麦德林独立"),
    ("托利马", "托利马体育"),
    ("柏斯度", "帕斯托体育"),
    ("珊卡洛斯", "圣卡洛斯"),
    ("沙普利萨", "萨普里萨"),
    ("阿拉伦斯", "阿拉胡埃伦斯"),
    ("佩雷斯泽内度", "佩雷斯塞莱东"),
    ("艾斯特利", "皇家埃斯特利"),
    ("马纳瓜", "马那瓜"),
    ("迪利安格恩", "迪瑞安根"),
    ("塞瓦科", "clubsportsebaco"),
    ("圣罗伦素体育会", "圣洛伦佐"),
    ("波特诺", "波特诺山丘"),
    ("欧里恩特", "东方石油"),
    ("史庄格斯特", "最强者"),
    ("竞赛会", "阿韦亚内达竞赛"),
    ("阿苏尔", "蓝十字"),
    ("提华纳", "蒂华纳国民足球俱乐部"),
    ("艾特莱", "阿特拉斯"),
    ("阿美利加会", "美洲俱乐部"),
    ("罗伯士upnfm", "upnfm狼"),
    ("cd奥林匹亚", "cd奥林匹克"),
    ("圣迭戈海浪女", "圣地亚哥波浪"),
    ("波士顿勒加斯女", "波士顿遗产"),
    # Confirmed live-feed spelling variant; keep as data, not as fuzzy logic.
    ("莫雷利亚竞技", "莫雷利亚竞技队"),
)

_TEAM_ALIAS = {alias: group[0] for group in _TEAM_ALIAS_GROUPS for alias in group}
_TEAM_ALIAS_NORMALIZED = None

_TYPE_PATTERNS = {
    "women": re.compile(r"(?:女|女子|women|woman|female|fem\.)", re.I),
    "youth": re.compile(r"(?:u\s*[-.]?\s*\d{2}|under\s*\d{2}|青年|青少年|青年队|junior)", re.I),
    "reserve": re.compile(r"(?:预备队|預備隊|二队|二隊|预备|reserve|reserves|\bii\b|\bb\s*team\b)", re.I),
    "futsal": re.compile(r"(?:五人制|室内足球|futsal)", re.I),
}
_NON_WORD = re.compile(r"[^\w\u3400-\u9fff]+", re.UNICODE)
_MADUNA_TOKEN_SPLIT = re.compile(r"[^\w\u3400-\u9fff]+", re.UNICODE)
_MADUNA_CLUB_NOISE = {
    "fc", "sc", "cf", "afc", "mfc", "fk", "club", "clube", "and", "of",
    "the", "de", "da", "do", "dos", "del", "la", "el", "los", "las",
}
_MADUNA_TOKEN_ALIASES = {
    # Aliases shipped by team-matcher; kept as normalization data only.
    "utd": "united", "man": "manchester", "intl": "international",
    "munchen": "munich", "münchen": "munich", "moscou": "moscow",
}
_MADUNA_MARKER_RE = re.compile(
    r"(?:\([^)]*\)|\[[^]]*\]|\bu\s*[-.]?\s*\d{2}\b|\bunder\s*\d{2}\b|"
    r"\b(?:women|woman|female|fem|youth|junior|reserve|reserves|res|ii|b\s*team)\b|"
    r"女足|女子|女性|青年队|青年|青少年|预备队|预备|二队|五人制|室内足球)",
    re.I,
)
_CROWN_TIME = re.compile(
    r"(?<!\d)(\d{1,2})[-/](\d{1,2})\s+(\d{1,2}):(\d{2})\s*([ap])(?:m)?(?:\b|$)",
    re.I,
)


@lru_cache(maxsize=8192)
def _normalize_name_cached(text: str) -> str:
    """Cached string-only normalization used by the hot matching loop."""
    normalized = unicodedata.normalize("NFKC", text).translate(_TRADITIONAL).casefold()
    return _NON_WORD.sub("", normalized)


def normalize_name(value: object) -> str:
    """Normalize punctuation, whitespace, Unicode compatibility, and script."""
    return _normalize_name_cached(str(value or ""))


def _canonical(value: object, aliases: dict[str, str]) -> str:
    normalized = normalize_name(value)
    return aliases.get(normalized, normalized)


def normalize_team(value: object, aliases: dict[str, str] | None = None) -> str:
    """Return a comparison key; optional aliases map normalized names to keys."""
    global _TEAM_ALIAS_NORMALIZED
    if _TEAM_ALIAS_NORMALIZED is None:
        _TEAM_ALIAS_NORMALIZED = {
            normalize_name(key): normalize_name(alias_value)
            for key, alias_value in _TEAM_ALIAS.items()
        }
    mapping = _TEAM_ALIAS_NORMALIZED
    if aliases:
        mapping = dict(mapping)
        mapping.update({normalize_name(k): normalize_name(v) for k, v in aliases.items()})
    return _canonical(value, mapping)


@lru_cache(maxsize=8192)
def _maduna_tokens_cached(text: str, strip_markers: bool) -> tuple[str, ...]:
    """Normalize a name for Maduna's token-sort/token-set comparison.

    Marker guards are evaluated from the original event before this helper is
    called.  The helper therefore removes only generic club wording and keeps
    competition markers available to the guard without making them part of the
    identity decision.
    """
    text = unicodedata.normalize("NFKD", text).translate(_TRADITIONAL).casefold()
    text = "".join(char for char in text if not unicodedata.combining(char))
    if strip_markers:
        text = _MADUNA_MARKER_RE.sub(" ", text)
    tokens = []
    for token in _MADUNA_TOKEN_SPLIT.split(text):
        token = token.strip("_-")
        if token and token not in _MADUNA_CLUB_NOISE:
            tokens.append(_MADUNA_TOKEN_ALIASES.get(token, token))
    return tuple(tokens)


def _maduna_tokens(value: object, *, strip_markers: bool = False) -> tuple[str, ...]:
    return _maduna_tokens_cached(str(value or ""), strip_markers)


def _maduna_similarity(left: object, right: object, *, strip_markers: bool = False) -> float:
    """Return Maduna EventMatcher's 0..1 team-name similarity."""
    left_tokens = _maduna_tokens(left, strip_markers=strip_markers)
    right_tokens = _maduna_tokens(right, strip_markers=strip_markers)
    if not left_tokens or not right_tokens:
        return 0.0
    left_text, right_text = " ".join(left_tokens), " ".join(right_tokens)
    if left_text == right_text:
        return 1.0
    if _rapidfuzz is None:
        # The project .venv installs RapidFuzz. Read-only tools that import
        # this module outside that environment still get the auxiliary score
        # instead of reviving the removed local fuzzy implementation.
        if _reference_team_similarity is None:
            return 0.0
        try:
            return float(_reference_team_similarity(str(left), str(right)))
        except Exception:
            return 0.0
    scores = [_rapidfuzz.token_sort_ratio(left_text, right_text) / 100.0]
    if len(left_tokens) >= 2 and len(right_tokens) >= 2:
        scores.append(_rapidfuzz.token_set_ratio(left_text, right_text) / 100.0)
    return max(scores)


def team_similarity(left: object, right: object, aliases: dict[str, str] | None = None) -> float:
    """Maduna primary team score with explicit provider aliases preserved."""
    left_key, right_key = normalize_team(left, aliases), normalize_team(right, aliases)
    if not left_key or not right_key:
        return 0.0
    if left_key == right_key:
        return 1.0
    return _maduna_similarity(left, right)


def league_similarity(left: object, right: object, aliases: dict[str, str] | None = None) -> float:
    """Return team-matcher league evidence for ranking only."""
    left_key, right_key = normalize_league(left, aliases), normalize_league(right, aliases)
    if not left_key or not right_key:
        return 0.0
    if left_key == right_key:
        return 1.0
    if _reference_team_similarity is None:
        return 0.0
    try:
        return float(_reference_team_similarity(str(left), str(right)))
    except Exception:
        return 0.0


def _sport_key(event: dict) -> str:
    value = str(event.get("sport") or "soccer").strip().casefold()
    return {"football": "soccer", "足球": "soccer", "soccer": "soccer"}.get(value, value)


def canonical_event_id(crown: dict | None, pinnacle: dict | None, *, now: datetime,
                       crown_timezone: str) -> str | None:
    """Create a stable canonical ID after entity resolution.

    Crown is preferred as the canonical source because it carries the local
    provider label; the kickoff is normalized to UTC and rounded to minutes so
    harmless second-level provider changes do not create a new fixture ID.
    """
    event = crown or pinnacle or {}
    home = normalize_team(event.get("home"))
    away = normalize_team(event.get("away"))
    start = _start(event, "crown" if crown else "pinnacle", now, crown_timezone)
    if not home or not away or start is None:
        return None
    payload = "|".join((_sport_key(event), home, away, start.replace(second=0, microsecond=0).isoformat()))
    return "evt_" + hashlib.md5(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


def normalize_league(value: object, aliases: dict[str, str] | None = None) -> str:
    mapping = {normalize_name(k): normalize_name(v) for k, v in (aliases or {}).items()}
    return _canonical(value, mapping)


def league_equivalent(left: object, right: object, aliases: dict[str, str] | None = None) -> bool:
    """Compare only exact normalized provider labels or explicit aliases.

    Maduna deliberately ignores league text for event identity. This helper
    remains an auditable metadata signal and does not rewrite division words.
    """
    left_key, right_key = normalize_league(left, aliases), normalize_league(right, aliases)
    return bool(left_key and right_key and left_key == right_key)


def event_types(event: dict) -> frozenset[str]:
    """Extract explicit competition qualifiers without inferring adult status."""
    text = " ".join(str(event.get(key) or "") for key in ("league", "home", "away"))
    return frozenset(name for name, pattern in _TYPE_PATTERNS.items() if pattern.search(text))


def parse_pinnacle_start(value: object) -> datetime | None:
    """Parse Pinnacle's epoch-millisecond ``start_ms`` as an aware UTC time."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        stamp = float(value) / 1000
        if not (0 < stamp < 100_000_000_000):
            return None
        return datetime.fromtimestamp(stamp, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def parse_crown_start(
    value: object,
    *,
    now: datetime | None = None,
    crown_timezone: str = "America/New_York",
) -> datetime | None:
    """Parse Crown's short ``MM-DD hh:mm a/p`` label into UTC.

    Crown labels do not include year or zone. ``crown_timezone`` is therefore
    explicit and overridable; the closest date among adjacent years is chosen.
    """
    text = str(value or "").strip()
    if not text:
        return None
    try:
        zone = ZoneInfo(crown_timezone)
    except (ZoneInfoNotFoundError, TypeError):
        return None
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    reference = reference.astimezone(timezone.utc)

    # Accept full ISO timestamps if an upstream version starts returning them.
    try:
        iso = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if iso.tzinfo is None:
            iso = iso.replace(tzinfo=zone)
        return iso.astimezone(timezone.utc)
    except ValueError:
        pass

    match = _CROWN_TIME.search(text)
    if not match:
        return None
    month, day, hour, minute = map(int, match.groups()[:4])
    ampm = match.group(5).lower()
    hour = hour % 12 + (12 if ampm == "p" else 0)
    local_reference = reference.astimezone(zone)
    candidates = []
    for year in (local_reference.year - 1, local_reference.year, local_reference.year + 1):
        try:
            local = datetime(year, month, day, hour, minute, tzinfo=zone)
        except ValueError:
            continue
        candidates.append(local.astimezone(timezone.utc))
    return min(candidates, key=lambda candidate: abs(candidate - reference)) if candidates else None


def _start(event: dict, provider: str, now: datetime, crown_timezone: str) -> datetime | None:
    if provider == "pinnacle":
        return parse_pinnacle_start(event.get("start_ms"))
    return parse_crown_start(event.get("start_label"), now=now, crown_timezone=crown_timezone)


def _score_pair(value: object) -> tuple[int, int] | None:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        values = value[:2]
    else:
        match = re.fullmatch(r"\s*(\d{1,3})\s*[-:：]\s*(\d{1,3})\s*", str(value or ""))
        values = match.groups() if match else None
    if values is None:
        return None
    try:
        return int(values[0]), int(values[1])
    except (TypeError, ValueError):
        return None


def _number_text(value: object) -> str | None:
    """Return a stable decimal representation for a market line value."""
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not number.is_finite():
        return None
    text = format(number.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _canonical_line(value: object) -> tuple[str, ...] | None:
    """Normalize provider line spellings such as ``2 / 2.5`` and ``2-2.5``."""
    text = str(value or "").strip().replace("／", "/").replace("－", "-")
    if not text:
        return None
    # Ignore signs here; handicap direction is carried by selection and
    # normalized separately in _quote_line.
    numbers = [_number_text(item) for item in re.findall(r"\d+(?:\.\d+)?", text)]
    if any(item is None for item in numbers) or not numbers:
        return None
    return tuple(numbers)


def _market_items(event: dict, market_type: str) -> list[dict]:
    markets = event.get("markets")
    if not isinstance(markets, dict):
        return []
    items = markets.get(market_type)
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def _quote_line(value: object, market_type: str) -> tuple[str, ...] | None:
    """Canonical line for direction-aware match quotes.

    Handicap feeds use opposite signs for home and away selections. The
    selection field already carries that direction, so the line comparison
    uses the absolute magnitude. Totals keep their ordinary decimal line.
    """
    line = _canonical_line(value)
    if line is None:
        return None
    if market_type == "handicap":
        try:
            return tuple(_number_text(abs(Decimal(item))) for item in line)
        except (InvalidOperation, TypeError, ValueError):
            return line
    return line


def _match_quote_items(event: dict) -> list[dict]:
    items = event.get("match_quotes")
    if not isinstance(items, list):
        return []
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        market_type = str(item.get("market_type") or item.get("type") or "").lower()
        selection = str(item.get("selection") or "").lower()
        if market_type not in {"handicap", "total", "moneyline"} or not selection:
            continue
        try:
            odds = float(item.get("hk_odds"))
        except (TypeError, ValueError):
            continue
        if odds <= 0:
            continue
        result.append({
            "market_type": market_type,
            "period": str(item.get("period") or "full").lower(),
            "selection": selection,
            "line": _quote_line(item.get("line"), market_type),
            "hk_odds": odds,
        })
    return result


def _odds_pair(item: dict) -> tuple[float, float] | None:
    values = (item.get("a_hk", item.get("a")), item.get("b_hk", item.get("b")))
    try:
        pair = (float(values[0]), float(values[1]))
    except (TypeError, ValueError):
        return None
    if not all(value >= 0 for value in pair):
        return None
    return pair


def _pair_similarity(left: tuple[float, float], right: tuple[float, float], *, live: bool,
                     allow_reverse: bool = False) -> tuple[float, bool, float]:
    """Compare two HK pairs; return score, accepted flag, and max absolute delta."""
    candidates = [right]
    if allow_reverse:
        candidates.append((right[1], right[0]))
    best = (0.0, False, float("inf"))
    # Live prices move quickly, so permit a wider but still bounded spread.
    absolute_limit = 0.22 if live else 0.14
    relative_limit = 0.20 if live else 0.13
    for candidate in candidates:
        deltas = [abs(left[index] - candidate[index]) for index in range(2)]
        ratios = [delta / max(1.0, abs(left[index]), abs(candidate[index]))
                  for index, delta in enumerate(deltas)]
        accepted = max(deltas) <= absolute_limit and max(ratios) <= relative_limit
        score = max(0.0, 1.0 - (sum(ratios) / 2.0) / max(relative_limit, 0.001))
        option = (score, accepted, max(deltas))
        if option[0] > best[0]:
            best = option
    return best


def _market_similarity(crown: dict, pinnacle: dict, *, live: bool) -> dict:
    """Find same-line, same-direction market evidence across both providers."""
    evidence: list[str] = []
    matches: list[dict] = []

    # Prefer the direction-aware adapter output. It avoids comparing a Crown
    # home price with a Pinnacle away price when the providers encode handicap
    # signs differently.
    crown_quotes = _match_quote_items(crown)
    pinnacle_quotes = _match_quote_items(pinnacle)
    if crown_quotes and pinnacle_quotes:
        minimum_score = 0.62 if live else 0.70
        crown_groups: dict[tuple, dict[str, float]] = {}
        pinnacle_groups: dict[tuple, dict[str, float]] = {}
        for quote, groups in ((crown_quotes, crown_groups), (pinnacle_quotes, pinnacle_groups)):
            for item in quote:
                key = (item["market_type"], item["period"], item["line"])
                groups.setdefault(key, {})[item["selection"]] = item["hk_odds"]
        for key, left_values in crown_groups.items():
            right_values = pinnacle_groups.get(key)
            if not right_values:
                continue
            market_type, _period, line_tuple = key
            selections = ("over", "under") if market_type == "total" else (
                ("home", "away") if market_type == "handicap" else ("home", "draw", "away"))
            if any(selection not in left_values or selection not in right_values for selection in selections):
                continue
            if len(selections) == 2:
                comparison = _pair_similarity(
                    (left_values[selections[0]], left_values[selections[1]]),
                    (right_values[selections[0]], right_values[selections[1]]), live=live,
                )
                score, accepted, max_delta = comparison
            else:
                deltas = [abs(left_values[selection] - right_values[selection]) for selection in selections]
                ratios = [delta / max(1.0, abs(left_values[selection]), abs(right_values[selection]))
                          for selection, delta in zip(selections, deltas)]
                limit = 0.22 if live else 0.14
                rel_limit = 0.20 if live else 0.13
                accepted = max(deltas) <= limit and max(ratios) <= rel_limit
                score = max(0.0, 1.0 - (sum(ratios) / len(ratios)) / max(rel_limit, 0.001))
                max_delta = max(deltas)
            if accepted and score >= minimum_score:
                line = "/".join(line_tuple) if line_tuple else "独赢"
                label = {"total": "大小球", "handicap": "让球", "moneyline": "独赢"}[market_type]
                evidence.append(f"{label}盘口 {line} 相同，水位相似度 {round(score * 100)}%")
                matches.append({"type": market_type, "line": line,
                                "similarity": round(score, 3), "max_delta": round(max_delta, 3)})
        if matches:
            best = max(item["similarity"] for item in matches)
            return {"matched": True, "score": best, "evidence": evidence, "matches": matches}
        return {"matched": False, "score": 0.0,
                "evidence": ["没有找到同盘口线、同选择且水位相近的市场"], "matches": []}

    # Legacy payloads do not carry an explicit handicap selection direction.
    # Their total rows do carry over/under order, so only use totals as a
    # fallback when direction-aware match_quotes are unavailable.
    for market_type, label, allow_reverse in (("total", "大小球", False),):
        crown_items = _market_items(crown, market_type)
        pinnacle_items = _market_items(pinnacle, market_type)
        minimum_score = 0.62 if live else 0.70
        for left in crown_items:
            left_line = _canonical_line(left.get("line"))
            left_pair = _odds_pair(left)
            if left_line is None or left_pair is None:
                continue
            best = None
            for right in pinnacle_items:
                if _canonical_line(right.get("line")) != left_line:
                    continue
                right_pair = _odds_pair(right)
                if right_pair is None:
                    continue
                comparison = _pair_similarity(left_pair, right_pair, live=live,
                                              allow_reverse=allow_reverse)
                if best is None or comparison[0] > best[0]:
                    best = (*comparison, right)
            if best and best[1] and best[0] >= minimum_score:
                line = "/".join(left_line)
                evidence.append(f"{label}盘口 {line} 相同，水位相似度 {round(best[0] * 100)}%")
                matches.append({"type": market_type, "line": line, "similarity": round(best[0], 3),
                                "max_delta": round(best[2], 3)})
    if not matches:
        return {"matched": False, "score": 0.0, "evidence": ["没有找到同盘口线且水位相近的市场"], "matches": []}
    score = max(item["similarity"] for item in matches)
    return {"matched": True, "score": score, "evidence": evidence, "matches": matches}


def _team_matcher_auxiliary(
    crown: dict,
    pinnacle: dict,
    *,
    start_c: datetime | None,
    start_p: datetime | None,
    home_score: float,
    away_score: float,
) -> tuple[float, float, bool]:
    """Use the vendored team-matcher only for ranking evidence.

    Event identity has already passed Maduna's hard gates before this helper is
    called.  The explicit-score fallback keeps verified provider aliases
    rankable when the package's ASCII tokenizer cannot read a Chinese label.
    """
    if _reference_rank_candidates is not None and _ReferenceCandidate is not None:
        try:
            ranked = _reference_rank_candidates(
                str(crown.get("home") or ""),
                str(crown.get("away") or ""),
                str(crown.get("league") or ""),
                [_ReferenceCandidate(
                    home=str(pinnacle.get("home") or ""),
                    away=str(pinnacle.get("away") or ""),
                    league=str(pinnacle.get("league") or ""),
                    kickoff=start_p,
                    payload=pinnacle,
                )],
                kickoff=start_c,
            )
            if ranked:
                match = ranked[0]
                base_score = float(match.base_score)
                time_bonus = float(match.time_bonus)
                if base_score <= 0:
                    base_score = (home_score + away_score) / 2.0
                return base_score, time_bonus, bool(match.swapped)
        except Exception:
            pass
    return (home_score + away_score) / 2.0, 0.0, False


def _candidate_score(
    crown: dict,
    pinnacle: dict,
    *,
    now: datetime,
    crown_timezone: str,
    team_aliases: dict[str, str] | None,
    league_aliases: dict[str, str] | None,
) -> dict:
    evidence: list[str] = []
    type_c, type_p = event_types(crown), event_types(pinnacle)

    sport_c, sport_p = _sport_key(crown), _sport_key(pinnacle)
    if sport_c != sport_p:
        return {"eligible": False, "status": "conflict", "score": 0,
                "confidence": 0.0, "evidence": [f"运动项目不一致: {sport_c} / {sport_p}"]}

    crown_home, crown_away = normalize_team(crown.get("home"), team_aliases), normalize_team(crown.get("away"), team_aliases)
    pin_home, pin_away = normalize_team(pinnacle.get("home"), team_aliases), normalize_team(pinnacle.get("away"), team_aliases)
    if not all((crown_home, crown_away, pin_home, pin_away)):
        return {"eligible": False, "status": "unmatched", "score": 0,
                "confidence": 0.0, "evidence": ["主客队名称缺失"]}
    if (crown_home, crown_away) == (pin_away, pin_home):
        return {"eligible": False, "status": "conflict", "score": 0,
                "confidence": 0.0, "evidence": ["主客队顺序相反"]}
    home_exact, away_exact = crown_home == pin_home, crown_away == pin_away
    home_ratio, away_ratio = team_similarity(crown.get("home"), pinnacle.get("home"), team_aliases), team_similarity(crown.get("away"), pinnacle.get("away"), team_aliases)
    if home_exact:
        evidence.append("主队标准名/已验证别名一致")
    else:
        evidence.append(f"主队 Maduna 相似度 {home_ratio:.2f}")
    if away_exact:
        evidence.append("客队标准名/已验证别名一致")
    else:
        evidence.append(f"客队 Maduna 相似度 {away_ratio:.2f}")
    marker_home_score = _maduna_similarity(
        crown.get("home"), pinnacle.get("home"), strip_markers=True
    )
    marker_away_score = _maduna_similarity(
        crown.get("away"), pinnacle.get("away"), strip_markers=True
    )
    if type_c != type_p and marker_home_score >= 0.88 and marker_away_score >= 0.88:
        return {"eligible": False, "status": "conflict", "score": 0,
                "confidence": 0.0,
                "evidence": evidence + ["赛事类型标记不一致"]}
    # Maduna EventMatcher requires both ordered team names to clear 0.88.
    # A one-sided or shared-city match must remain unmatched.
    maduna_team_threshold = 0.88
    if home_ratio < maduna_team_threshold or away_ratio < maduna_team_threshold:
        return {"eligible": False, "status": "unmatched", "score": 0,
                "confidence": 0.0, "evidence": evidence + ["至少一支球队名称不匹配"]}

    # Only report a type conflict after the ordered teams identify the same
    # fixture. Comparing type markers before team identity makes unrelated
    # events look like meaningful conflicts and inflates the unmatched count.
    if type_c != type_p:
        return {"eligible": False, "status": "conflict", "score": 0,
                "confidence": 0.0, "evidence": evidence + ["赛事类型标记不一致"]}
    evidence.append("赛事类型标记一致" if type_c else "双方均无显式赛事类型标记")

    crown_league = normalize_league(crown.get("league"), league_aliases)
    pin_league = normalize_league(pinnacle.get("league"), league_aliases)
    league_exact = league_equivalent(crown.get("league"), pinnacle.get("league"), league_aliases)
    league_score = league_similarity(crown.get("league"), pinnacle.get("league"), league_aliases)
    if league_exact:
        evidence.append("联赛标准名/别名一致")
    elif crown_league and pin_league:
        evidence.append(f"联赛标签不同，辅助相似度 {league_score:.2f}")
    else:
        evidence.append("联赛信息不完整")

    crown_live, pin_live = bool(crown.get("is_live")), bool(pinnacle.get("is_live"))
    if crown_live != pin_live:
        evidence.append("比赛状态不一致")
    else:
        evidence.append("比赛状态一致")
    score_c, score_p = _score_pair(crown.get("score")), _score_pair(pinnacle.get("score"))
    score_equal = bool(crown_live and pin_live and score_c is not None and score_p is not None and score_c == score_p)
    score_conflict = bool(crown_live and pin_live and score_c is not None and score_p is not None and score_c != score_p)
    if score_equal:
        evidence.append("滚球比分一致")
    elif score_conflict:
        evidence.append("滚球比分不一致")

    start_c = _start(crown, "crown", now, crown_timezone)
    start_p = _start(pinnacle, "pinnacle", now, crown_timezone)
    delta_minutes = abs((start_c - start_p).total_seconds()) / 60 if start_c and start_p else None
    if delta_minutes is not None:
        evidence.append(f"开赛时间相差 {delta_minutes:.0f} 分钟")
    else:
        evidence.append("开赛时间不可比")

    # Maduna's kickoff window is a hard identity gate.  Rejecting it before
    # parsing markets avoids doing expensive odds comparisons for the vast
    # majority of unrelated provider rows.
    time_ok = delta_minutes is not None and delta_minutes <= 15
    if not time_ok:
        return {
            "eligible": False,
            "status": "unmatched",
            "score": 0,
            "confidence": 0.0,
            "evidence": evidence + ["开赛时间不在 Maduna 的 15 分钟匹配窗口内"],
            "time_delta_minutes": round(delta_minutes, 1) if delta_minutes is not None else None,
            "sport": sport_c,
            "team_home_score": round(home_ratio, 3),
            "team_away_score": round(away_ratio, 3),
            "strict_teams": True,
            "time_ok": False,
            "matcher_version": MATCHER_VERSION,
        }

    auxiliary_pair_score, kickoff_bonus, auxiliary_swapped = _team_matcher_auxiliary(
        crown,
        pinnacle,
        start_c=start_c,
        start_p=start_p,
        home_score=home_ratio,
        away_score=away_ratio,
    )
    score = round((auxiliary_pair_score + kickoff_bonus) * 100, 3)
    evidence.append(
        f"team-matcher 辅助评分 {auxiliary_pair_score:.2f}"
        + (f"，开赛时间加分 {kickoff_bonus:.2f}" if kickoff_bonus else "")
    )

    # The reference architecture treats league labels as provider metadata,
    # not fixture identity.  Ordered teams, sport/type markers, and kickoff
    # proximity are the hard identity gates; league evidence only ranks edges.
    strict_teams = home_ratio >= maduna_team_threshold and away_ratio >= maduna_team_threshold
    auto_live_ok = not (crown_live and pin_live and score_c is not None and score_p is not None) or score_equal

    # Market lines are evidence only.  Calculate them after all identity gates
    # so a feed with many leagues does not spend time comparing unrelated odds.
    market = _market_similarity(crown, pinnacle, live=crown_live and pin_live)
    evidence.extend(market["evidence"])
    # Market lines are provider snapshots and can legitimately differ while
    # the fixture identity stays the same. They remain useful evidence, but
    # they are not a hard gate: a missing market or a temporarily different
    # line must not hide an otherwise unambiguous event.
    if market["matched"]:
        evidence.append("盘口和水位提供辅助一致性证据")
    else:
        evidence.append("盘口线/水位未对齐，按联赛、球队、时间和状态合并")
    confirmed = (strict_teams and crown_live == pin_live and time_ok
                 and auto_live_ok)
    status = "confirmed" if confirmed else "candidate"
    return {"eligible": True, "status": status, "score": score,
            "confidence": min(0.99, round(max(home_ratio, away_ratio), 3)), "evidence": evidence,
            "time_delta_minutes": round(delta_minutes, 1) if delta_minutes is not None else None,
            "score_equal": score_equal, "score_conflict": score_conflict,
            "sport": sport_c, "league_exact": league_exact, "league_score": round(league_score, 3),
            "team_home_score": round(home_ratio, 3), "team_away_score": round(away_ratio, 3),
            "strict_teams": strict_teams, "time_ok": time_ok,
            "auxiliary_pair_score": round(auxiliary_pair_score, 3),
            "kickoff_bonus": round(kickoff_bonus, 3),
            "auxiliary_swapped": auxiliary_swapped,
            "matcher_version": MATCHER_VERSION,
            "market_matched": market["matched"], "market_score": market["score"],
            "market_matches": market["matches"]}


def match_events(
    crown_events: list[dict],
    pinnacle_events: list[dict],
    *,
    confirmed_mappings=(),
    rejected_mappings=(),
    now: datetime | None = None,
    crown_timezone: str = "America/New_York",
    team_aliases: dict[str, str] | None = None,
    league_aliases: dict[str, str] | None = None,
    max_candidates: int = 5,
) -> dict:
    """Match Crown rows to Pinnacle rows, enforcing one-to-one assignment.

    Returns ``rows`` with one entry per Crown fixture. ``confirmed`` rows have
    a unique, high-evidence Pinnacle event. Ambiguous/low-evidence rows remain
    ``candidate`` or ``conflict`` and must not be displayed as a merged game.
    """
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    reference = reference.astimezone(timezone.utc)

    rows: list[dict] = []
    crown_by_id = {str(event.get("id")): index for index, event in enumerate(crown_events)}
    pinnacle_by_id = {str(event.get("id")): index for index, event in enumerate(pinnacle_events)}
    rejected_pairs = set()
    for mapping in rejected_mappings or ():
        if isinstance(mapping, dict):
            rejected_pairs.add((str(mapping.get("crown_id")), str(mapping.get("pinnacle_id"))))
        elif isinstance(mapping, (tuple, list)) and len(mapping) >= 2:
            rejected_pairs.add((str(mapping[0]), str(mapping[1])))
    all_candidates: list[tuple[float, int, int, dict]] = []
    for ci, crown in enumerate(crown_events):
        candidates = []
        hard_conflicts = []
        for pi, pinnacle in enumerate(pinnacle_events):
            if (str(crown.get("id")), str(pinnacle.get("id"))) in rejected_pairs:
                continue
            result = _candidate_score(crown, pinnacle, now=reference,
                                     crown_timezone=crown_timezone,
                                     team_aliases=team_aliases,
                                     league_aliases=league_aliases)
            if result["status"] == "conflict":
                if ("主客队顺序相反" in result["evidence"]
                        or "赛事类型标记不一致" in result["evidence"]
                        or any(str(item).startswith("运动项目不一致") for item in result["evidence"] )):
                    hard_conflicts.append({"event": pinnacle, **result})
                continue
            if not result["eligible"]:
                continue
            candidate = {"event": pinnacle, **result}
            candidates.append(candidate)
            all_candidates.append((result["score"], ci, pi, result))
        candidates.sort(key=lambda item: item["score"], reverse=True)
        rows.append({
            "crown": crown,
            "status": "unmatched",
            "pinnacle": None,
            "candidates": candidates[:max_candidates],
            "evidence": ["没有找到符合主客队名称约束的平博候选"],
            "confidence": 0.0,
            "matcher_version": MATCHER_VERSION,
            "_candidate_indices": [pinnacle_events.index(item["event"]) for item in candidates],
            "_hard_conflicts": hard_conflicts,
        })

    # Apply durable manual confirmations before automatic assignment. A manual
    # pair is accepted only when both IDs exist and neither side was already
    # claimed by another manual mapping; invalid/conflicting overrides remain
    # visible as ``conflict`` instead of silently replacing an existing link.
    manual_entries = []
    manual_conflicts: list[str] = []
    for mapping in confirmed_mappings or ():
        if isinstance(mapping, dict):
            crown_id, pinnacle_id = mapping.get("crown_id"), mapping.get("pinnacle_id")
        elif isinstance(mapping, (tuple, list)) and len(mapping) >= 2:
            crown_id, pinnacle_id = mapping[0], mapping[1]
        else:
            manual_conflicts.append("确认映射格式无效")
            continue
        ci, pi = crown_by_id.get(str(crown_id)), pinnacle_by_id.get(str(pinnacle_id))
        if ci is None or pi is None:
            manual_conflicts.append(f"确认映射不存在: {crown_id}->{pinnacle_id}")
            continue
        if (str(crown_id), str(pinnacle_id)) in rejected_pairs:
            manual_conflicts.append(f"人工确认与拒绝记录冲突: {crown_id}->{pinnacle_id}")
            continue
        result = _candidate_score(crown_events[ci], pinnacle_events[pi], now=reference,
                                  crown_timezone=crown_timezone,
                                  team_aliases=team_aliases,
                                  league_aliases=league_aliases)
        if not result.get("eligible") or result.get("status") == "conflict":
            manual_conflicts.append(f"确认映射证据冲突: {crown_id}->{pinnacle_id}")
            continue
        manual_entries.append((ci, pi, result, str(crown_id), str(pinnacle_id)))

    crown_mapping_counts: dict[int, int] = {}
    pinnacle_mapping_counts: dict[int, int] = {}
    for ci, pi, _, _, _ in manual_entries:
        crown_mapping_counts[ci] = crown_mapping_counts.get(ci, 0) + 1
        pinnacle_mapping_counts[pi] = pinnacle_mapping_counts.get(pi, 0) + 1
    invalid_manual_crown = {ci for ci, count in crown_mapping_counts.items() if count > 1}
    invalid_manual_pinnacle = {pi for pi, count in pinnacle_mapping_counts.items() if count > 1}
    manually_claimed_crown: set[int] = set()
    manually_claimed_pinnacle: set[int] = set()
    for ci, pi, result, crown_id, pinnacle_id in manual_entries:
        if ci in invalid_manual_crown or pi in invalid_manual_pinnacle:
            for affected_ci, affected_pi, _, _, _ in manual_entries:
                if affected_ci == ci or affected_pi == pi:
                    rows[affected_ci]["status"] = "conflict"
                    rows[affected_ci]["pinnacle"] = None
                    rows[affected_ci]["evidence"] = ["人工确认映射违反一对一约束"]
            manual_conflicts.append(f"确认映射一对一冲突: {crown_id}->{pinnacle_id}")
            continue
        rows[ci].update({"status": "confirmed", "pinnacle": pinnacle_events[pi],
                         "evidence": result.get("evidence", []) + ["人工确认映射"],
                         "confidence": 1.0, "matcher_version": result.get("matcher_version", MATCHER_VERSION),
                         "match_profile": result})
        manually_claimed_crown.add(ci)
        manually_claimed_pinnacle.add(pi)

    # A near tie for a single Crown row is inherently ambiguous. Defer it.
    ambiguous_crowns = set()
    for ci, row in enumerate(rows):
        candidates = row["candidates"]
        if len(candidates) > 1 and candidates[0]["score"] - candidates[1]["score"] < 8:
            ambiguous_crowns.add(ci)

    # Claim only strict-confirmation edges. Sort globally by evidence so that
    # each provider fixture can be consumed once. Ties are deliberately left
    # unresolved instead of depending on source list order.
    confirmed_edges = []
    for score, ci, pi, result in all_candidates:
        if result["status"] == "confirmed":
            confirmed_edges.append((score, ci, pi, result))
    confirmed_edges.sort(reverse=True, key=lambda item: item[0])
    claimed_crown: set[int] = set()
    claimed_pinnacle: set[int] = set()

    claimed_crown.update(manually_claimed_crown)
    claimed_pinnacle.update(manually_claimed_pinnacle)
    for _, ci, pi, result in confirmed_edges:
        if ci in ambiguous_crowns or ci in claimed_crown or pi in claimed_pinnacle:
            continue
        # If the same Pinnacle fixture is a strict option for multiple Crown
        # rows at a similar score, neither row should win arbitrarily.
        competing = [edge for edge in confirmed_edges if edge[2] == pi and edge[1] != ci and abs(edge[0] - result["score"]) < 8]
        if competing:
            rows[ci]["status"] = "conflict"
            rows[ci]["evidence"] = result["evidence"] + ["同一平博赛事被多个皇冠赛事竞争"]
            continue
        row = rows[ci]
        row.update({"status": "confirmed", "pinnacle": pinnacle_events[pi],
                    "evidence": result["evidence"], "confidence": result["confidence"],
                    "market_matched": result.get("market_matched", False),
                    "market_score": result.get("market_score", 0.0),
                    "market_matches": result.get("market_matches", []),
                    "matcher_version": result.get("matcher_version", MATCHER_VERSION),
                    "match_profile": result})
        claimed_crown.add(ci)
        claimed_pinnacle.add(pi)

    for ci, row in enumerate(rows):
        if row["status"] in {"confirmed", "conflict"}:
            continue
        candidates = row["candidates"]
        if ci in ambiguous_crowns:
            row["status"] = "conflict"
            row["evidence"] = candidates[0]["evidence"] + ["存在分数接近的多个候选"]
            row["confidence"] = candidates[0]["confidence"]
            row["match_profile"] = candidates[0]
        elif row["status"] == "conflict":
            # Preserve a hard conflict produced by one-to-one resolution or
            # an invalid manual mapping.
            row["confidence"] = row.get("confidence", 0.0)
        elif candidates:
            top = candidates[0]
            row["status"] = "candidate"
            row["evidence"] = top["evidence"]
            row["confidence"] = top["confidence"]
            row["market_matched"] = top.get("market_matched", False)
            row["market_score"] = top.get("market_score", 0.0)
            row["market_matches"] = top.get("market_matches", [])
            row["matcher_version"] = top.get("matcher_version", MATCHER_VERSION)
            row["match_profile"] = top
        elif row["_hard_conflicts"]:
            row["status"] = "conflict"
            row["evidence"] = row["_hard_conflicts"][0]["evidence"]
        row.pop("_candidate_indices", None)
        row.pop("_hard_conflicts", None)

    # Attach a stable canonical event identity only after a Crown/Pinnacle pair
    # has been resolved.  Candidates keep their provider IDs and evidence but
    # do not masquerade as a confirmed shared fixture.
    for row in rows:
        if row.get("status") != "confirmed" or not row.get("pinnacle"):
            continue
        canonical_id = canonical_event_id(
            row.get("crown"), row.get("pinnacle"), now=reference,
            crown_timezone=crown_timezone,
        )
        if canonical_id:
            row["canonical_event_id"] = canonical_id
            profile = row.get("match_profile")
            if isinstance(profile, dict):
                profile["canonical_event_id"] = canonical_id

    used_pinnacle_ids = {
        str(row["pinnacle"].get("id")) for row in rows
        if row["status"] == "confirmed" and row.get("pinnacle") is not None
    }
    matched_indices = {
        next((index for index, event in enumerate(pinnacle_events)
              if str(event.get("id")) == event_id), -1)
        for event_id in used_pinnacle_ids
    }
    unmatched_pinnacle = [event for index, event in enumerate(pinnacle_events) if index not in matched_indices]
    confirmed_rows = [row for row in rows if row["status"] == "confirmed"]
    candidate_rows = [row for row in rows if row["status"] == "candidate"]
    unmatched_rows = [row for row in rows if row["status"] == "unmatched"]
    conflict_rows = [row for row in rows if row["status"] == "conflict"]
    return {
        "rows": rows,
        "confirmed": confirmed_rows,
        "candidates": candidate_rows,
        "unmatched": unmatched_rows,
        "conflicts": conflict_rows,
        "unmatched_pinnacle": unmatched_pinnacle,
        "counts": {
            "crown": len(crown_events),
            "pinnacle": len(pinnacle_events),
            "confirmed": sum(row["status"] == "confirmed" for row in rows),
            "candidate": sum(row["status"] == "candidate" for row in rows),
            "unmatched": sum(row["status"] == "unmatched" for row in rows),
            "conflict": sum(row["status"] == "conflict" for row in rows),
        },
        "crown_timezone": crown_timezone,
        "matcher_version": MATCHER_VERSION,
        "manual_conflicts": manual_conflicts,
    }
