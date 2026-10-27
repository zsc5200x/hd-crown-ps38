"""Parse read-only Crown football snapshots without inferring unknown markets.

The transport owns response byte limits.  This module does not request a site,
place bets, or use a session.  A quote is usable only when ``status == 'open'``;
unknown switches and invalid prices are retained as unavailable diagnostics.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import math
import re
import unicodedata
import xml.etree.ElementTree as ET

from .odds import to_hk_water


_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)\Z")
_MARKET_FIELD = re.compile(r"(?:RATIO|IOR|SW)_[A-Z0-9_]{1,40}\Z")
_SAFE_GAME_FIELDS = frozenset({
    "ID", "GID", "GIDM", "HGID", "ECID", "LID", "LEAGUE", "TEAM_H", "TEAM_C",
    "TEAM_H_ID", "TEAM_C_ID", "GNUM_H", "GNUM_C", "EVENTID", "DATETIME", "SYSTIME",
    "SCORE_H", "SCORE_C", "FT_SCROE_H", "FT_SCROE_C", "ET_SCROE_H", "ET_SCROE_C",
    "HSC_H", "HSC_C", "IS_RB", "GTYPE", "SHOWTYPE", "STRONG", "HSTRONG", "GOPEN",
    "HGOPEN", "RUNNING", "RETIMESET", "TIMEEDITPOINT", "LASTESTSCORE", "REDCARD_H",
    "REDCARD_C", "MIDFIELD", "MORE", "PTYPE", "PTYPE_MAP", "RECV", "NOW_MODEL",
    "HRECV", "RATIO", "HRATIO", "MODE", "MASTER", "GIDFL", "PTYPE_ID",
})
_SAFE_METADATA = frozenset({
    "datacount", "totaldatacount", "pagecount", "pagedata", "status", "code",
    "gtype", "showtype", "rtype", "datetime", "systime", "timestamp", "isrb", "is_rb",
    "all_close", "nowmode",
    "ecid",
})
_CHINESE_LINES = {
    "平": "0", "平手": "0", "平半": "0.25", "半": "0.5", "半球": "0.5",
    "半一": "0.75", "一": "1", "一球": "1", "球半": "1.5", "一球半": "1.5",
    "两": "2", "兩": "2", "二": "2", "两球": "2", "兩球": "2", "二球": "2",
    "两球半": "2.5", "兩球半": "2.5", "二球半": "2.5", "三": "3", "三球": "3",
    "三球半": "3.5", "四": "4", "四球": "4", "四球半": "4.5", "五": "5",
    "五球": "5", "五球半": "5.5", "六": "6", "六球": "6", "六球半": "6.5",
    "七": "7", "七球": "7", "七球半": "7.5", "八": "8", "八球": "8",
}


def _tag(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def parse_games_xml(body):
    """Return allowlisted scalar game fields and top-level response metadata.

    Field names in ``games`` are upper case for both list/more response styles.
    Credentials and unknown response fields are never copied.  DTD/entity
    declarations and non-UTF-8 byte documents are rejected before parsing.
    Invalid input raises ``ValueError`` without echoing response data.
    """
    if not isinstance(body, (str, bytes)):
        raise ValueError("Crown XML must be text or UTF-8 bytes")
    try:
        text = body.decode("utf-8-sig") if isinstance(body, bytes) else body.lstrip("\ufeff")
    except UnicodeDecodeError:
        raise ValueError("Crown XML must be UTF-8") from None
    if "\x00" in text or re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", text, re.IGNORECASE):
        raise ValueError("Crown XML DTD and entities are forbidden")
    try:
        root = ET.fromstring(text)
    except (ET.ParseError, ValueError):
        raise ValueError("Invalid Crown XML") from None
    if _tag(root.tag).lower() != "serverresponse":
        raise ValueError("Unexpected Crown XML root")

    metadata = {}
    for child in root:
        name = _tag(child.tag)
        if name.lower() in _SAFE_METADATA and not len(child):
            metadata[name] = (child.text or "").strip()

    games = []

    pending = [(root, "")]
    while pending:
        node, ecid = pending.pop()
        name = _tag(node.tag).lower()
        if name == "ec":
            attrs = {str(key).lower(): value for key, value in node.attrib.items()}
            ecid = str(attrs.get("ecid") or attrs.get("id") or ecid)
            ecid = next(((field.text or "").strip() for field in node
                         if _tag(field.tag).upper() == "ECID" and not len(field)), ecid)
        if name == "game":
            game = {}
            for key, value in node.attrib.items():
                key = _tag(key).upper()
                if key in _SAFE_GAME_FIELDS:
                    value = str(value).strip()
                    if key in game and game[key] != value:
                        raise ValueError("Conflicting Crown game fields")
                    game[key] = value
            for child in node:
                key = _tag(child.tag).upper()
                if len(child) or not (key in _SAFE_GAME_FIELDS or _MARKET_FIELD.fullmatch(key)):
                    continue
                value = (child.text or "").strip()
                if key in game and game[key] != value:
                    raise ValueError("Conflicting Crown game fields")
                game[key] = value
            if ecid:
                game.setdefault("ECID", ecid)
            if game.get("ID"):
                game.setdefault("GID", game["ID"])
            games.append(game)
            continue
        pending.extend((child, ecid) for child in reversed(node))

    return {"games": games, "metadata": metadata}


def _line_atom(value):
    sign = ""
    if value.startswith(("+", "-")):
        sign, value = value[0], value[1:]
    value = _CHINESE_LINES.get(value, value)
    token = sign + value
    if not _NUMBER.fullmatch(token):
        return None
    try:
        number = Decimal(token)
    except InvalidOperation:
        return None
    if not number.is_finite() or abs(number) > 10000:
        return None
    return number


def normalize_line(value):
    """Normalize quarter lines, including ``2.5/3 == 2.75``.

    A leading minus applies to both halves in ``-0.5/1``; explicitly conflicting
    signs are rejected. Split lines must be adjacent half-goal endpoints. Bare
    lines must be a multiple of 0.25. No guessing of a selection or team occurs.
    """
    if value is None or isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    text = unicodedata.normalize("NFKC", str(value)).replace("−", "-")
    text = re.sub(r"\s+", "", text)
    if not text or len(text) > 64:
        return None
    halves = text.split("/")
    if len(halves) == 1:
        result = _line_atom(halves[0])
    elif len(halves) == 2:
        left, right = halves
        a, b = _line_atom(left), _line_atom(right)
        if a is None or b is None:
            return None
        if left.startswith("-") and not right.startswith(("+", "-")):
            b = -abs(b)
        elif left.startswith("+") and right.startswith("-"):
            return None
        if left.startswith("-") and right.startswith("+"):
            return None
        if a * b < 0 or abs(a - b) != Decimal("0.5") or a % Decimal("0.5") or b % Decimal("0.5"):
            return None
        result = (a + b) / 2
    else:
        return None
    if result is None or result % Decimal("0.25"):
        return None
    return Decimal(0) if result == 0 else result


def _decimal_text(value):
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def crown_hk_pair(home_raw, away_raw):
    """Return the source-verified Crown ``H`` display prices as Decimals.

    Crown's wire ``ior_*`` values are an internal *pair*, even when the account
    display is HK. ``get_HK_ior`` converts the high side using both values and
    truncates to two places. These prices must not be treated as ordinary HK
    independently. Zero/negative/unknown values invalidate the pair.
    """
    values = []
    for raw in (home_raw, away_raw):
        if isinstance(raw, bool) or not isinstance(raw, (str, int, float, Decimal)):
            return None
        text = str(raw).strip()
        if len(text) > 40 or not _NUMBER.fullmatch(text):
            return None
        try:
            value = Decimal(text)
            if not value.is_finite() or value <= 0 or value > 2000:
                return None
            # chg_ior first clips the wire number to three decimal places,
            # then scales decimal wire ratios while retaining integer inputs.
            value = (value * 1000 + Decimal("0.001")).to_integral_value(rounding=ROUND_FLOOR) / 1000
            if value < 11:
                value *= 1000
            if not 0 < value < 2000:
                return None
            values.append(value)
        except InvalidOperation:
            return None
    home, away = values

    def clip(value):
        return (value / 10 + Decimal("0.0001")).to_integral_value(rounding=ROUND_FLOOR) / 100

    if home <= 1000 and away <= 1000:
        result = (clip(home), clip(away))
    else:
        line = Decimal(2000) - home - away
        low = min(home, away)
        remaining = Decimal(2000) - line - low
        now = -(low + line) if remaining > 1000 else remaining
        if now < 0:
            high = (abs(Decimal(1000) / now) * 1000).to_integral_value(rounding=ROUND_FLOOR)
        else:
            high = Decimal(2000) - line - now
        result = (clip(high), clip(low)) if home > away else (clip(low), clip(high))
    return result if all(value > 0 for value in result) else None


def _hk_odds(raw, odds_format):
    if not isinstance(raw, str) or len(raw) > 40 or not _NUMBER.fullmatch(raw):
        return None
    return to_hk_water(raw, odds_format)


def _score(game):
    home, away = str(game.get("SCORE_H", "")), str(game.get("SCORE_C", ""))
    if re.fullmatch(r"[0-9]{1,3}", home) and re.fullmatch(r"[0-9]{1,3}", away):
        return [int(home), int(away)]
    return None


def extract_quotes(game, *, inplay: bool, odds_format: str):
    """Extract ordinary football handicap/total quotes for full/first half.

    The caller supplies the verified input odds format. HK/MY/EU/ID denote
    already-formatted standard odds; CROWN_HK denotes raw Crown pairs displayed
    by the site's verified H conversion. Unknown formats raise ``ValueError``.
    Only verified Y/N market ``sw_*`` flags are used.
    A missing line is omitted; a known line with blocked/invalid pricing remains
    in the result with ``status='unavailable'`` and ``hk_odds=None``.
    """
    if not isinstance(inplay, bool):
        raise ValueError("Crown quote inplay must be explicit")
    if not isinstance(odds_format, str) or odds_format.upper() not in {"HK", "MY", "EU", "ID", "CROWN_HK"}:
        raise ValueError("Crown quote odds format must be explicit and supported")
    odds_format = odds_format.upper()
    fields = {str(key).upper(): str(value).strip() if value is not None else ""
              for key, value in game.items()}
    # More rows include alternate goals/corners/cards under the same teams.
    # Only the unqualified ordinary-football rows belong in this extractor.
    if (fields.get("GTYPE", "FT").upper() != "FT" or fields.get("PTYPE", "")
            or fields.get("MODE", "FT").upper() != "FT"):
        return []
    more = any(key in fields for key in ("MODE", "MASTER", "RATIO", "HRATIO", "RATIO_O", "RATIO_U", "RATIO_HO", "RATIO_HU"))
    quotes = []
    plans = (
        ("full", "RE" if inplay else "R", "ROU" if inplay else "OU", "STRONG"),
        ("first_half", "HRE" if inplay else "HR", "HROU" if inplay else "HOU", "HSTRONG"),
    )
    score = _score(fields)

    def append(period, market_type, market, selection, line, ratio_key, odds_key, direction_known=True):
        raw_odds = fields.get(odds_key, "")
        if odds_format == "CROWN_HK":
            prefix = odds_key[:-1]
            pair = crown_hk_pair(fields.get(prefix + "H", ""), fields.get(prefix + "C", ""))
            converted = float(pair[0 if odds_key.endswith("H") else 1]) if pair else None
        else:
            converted = _hk_odds(raw_odds, odds_format)
        reasons = []
        if fields.get("GOPEN") != "Y":
            reasons.append("game_not_open")
        if not more and period == "first_half" and "HGOPEN" in fields and fields["HGOPEN"] != "Y":
            reasons.append("period_not_open")
        if "IS_RB" in fields and fields["IS_RB"] not in ({"Y"} if inplay else {"N"}):
            reasons.append("inplay_unconfirmed")
        # Verified game_more FT parseData checks gopen and sw_<market>.
        # SW_OUH/SW_OUC belong to team totals, not the sides of ordinary OU.
        switch_key = "SW_" + market
        if more or switch_key in fields:
            switch = fields.get(switch_key)
            if switch in (None, "N"):
                reasons.append("market_not_open")
            elif switch != "Y":
                reasons.append("unknown_switch")
        if not direction_known:
            reasons.append("unknown_handicap_direction")
        if converted is None:
            reasons.append("invalid_odds")
        quotes.append({
            "type": market_type, "market_type": market_type, "period": period,
            "selection": selection, "line": _decimal_text(line) if line is not None else None,
            "inplay": inplay, "score": score, "hk_odds": converted if not reasons else None,
            "raw_odds": raw_odds, "odds_format": odds_format,
            "status": "unavailable" if reasons else "open", "unavailable_reasons": reasons,
            "gid": fields.get("GID", fields.get("ID", "")),
            "gidm": fields.get("GIDM", ""), "hgid": fields.get("HGID", ""),
            "market_id": fields.get("GIDM") or fields.get("GID", fields.get("ID", "")),
            "ecid": fields.get("ECID", ""),
            "source": {"market": market, "ratio_field": ratio_key, "odds_field": odds_key,
                       "raw_line": fields.get(ratio_key, ""),
                       "schema": "more" if more else "list",
                       "output_odds_format": "HK",
                       "switches": {switch_key: fields[switch_key]} if switch_key in fields else {},
                       "flags": {key: fields[key] for key in ("GOPEN", "HGOPEN", "RECV", "HRECV") if key in fields}},
        })

    for period, handicap, total, strong_key in plans:
        ratio_key = "RATIO_" + handicap
        if ratio_key not in fields and more and not inplay:
            ratio_key = "RATIO" if period == "full" else "HRATIO"
        line = normalize_line(fields.get(ratio_key))
        if line is not None:
            # RATIO_R/RE are magnitudes. Only STRONG identifies which side gives.
            # A negative ratio would conflict with this wire representation.
            strong = fields.get(strong_key, "")
            known = line == 0 or (line > 0 and strong in {"H", "C"})
            home_line = (-line if strong == "H" else line) if known else None
            append(period, "handicap", handicap, "home", home_line, ratio_key,
                   "IOR_" + handicap + "H", known)
            append(period, "handicap", handicap, "away", -home_line if home_line is not None else None,
                   ratio_key, "IOR_" + handicap + "C", known)
        # Verified member page switchConRtype: OUC/ROUC -> ratio_o/ratio_rouo
        # (ART_game_over); OUH/ROUH -> ratio_u/ratio_rouu (ART_game_under).
        for selection, suffix, price_suffix in (("over", "O", "C"), ("under", "U", "H")):
            ratio_key = "RATIO_" + total + suffix
            if ratio_key not in fields and more and not inplay:
                ratio_key = "RATIO_" + ("H" if period == "first_half" else "") + suffix
            line = normalize_line(fields.get(ratio_key))
            if line is not None and line >= 0:
                append(period, "total", total, selection, line, ratio_key, "IOR_" + total + price_suffix)
    return quotes
