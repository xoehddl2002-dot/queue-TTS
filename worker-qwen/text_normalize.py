"""한국어 숫자·날짜·시각·전화번호를 읽는 그대로 풀어쓰는 정규화 (선택 기능).

SUPERTONIC 워커에서 가져온 규칙이다. 거기서는 모델의 G2P 한계를 메우려고 **항상** 켜져 있지만,
Qwen3-TTS 는 LLM 기반이라 숫자를 자체적으로 읽을 가능성이 높고 이미 풀어쓴 한글을 다시 받으면
오히려 어색해질 수 있다. 그래서 이 프로젝트에서는 **기본값이 꺼짐**이고
``QUEUETTS_QWEN_TEXT_NORMALIZE=1`` 로 켠다.

켤지 말지는 실제 샘플 A/B 로 정한다 — docs/design.md "텍스트 전처리" 참고.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

from common.text_processing import prepare_generation_text

_NUMERIC_TOKEN_RE = re.compile(r"(?<!\d)[+-]?\d+(?:[.,:/-]\d+)*(?:%)?")
_ISO_DATE_RE = re.compile(r"(?<!\d)(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)")
_SLASH_DATE_RE = re.compile(r"(?<!\d)(\d{4})[./](\d{1,2})[./](\d{1,2})(?!\d)")
_TIME_WITH_MINUTE_RE = re.compile(r"(?<!\d)(\d{1,2})\s*시\s*(\d{1,2})\s*분")
_TIME_HALF_RE = re.compile(r"(?<!\d)(\d{1,2})\s*시\s*반")
_TIME_HOUR_RE = re.compile(r"(?<!\d)(\d{1,2})\s*시")
_MINUTE_RE = re.compile(r"(?<!\d)(\d{1,2})\s*분")
_MONTH_DAY_RE = re.compile(r"(?<!\d)(\d{1,2})\s*월\s*(\d{1,2})\s*일")
_MONTH_RE = re.compile(r"(?<!\d)(\d{1,2})\s*월")
_DAY_RE = re.compile(r"(?<!\d)(\d{1,2})\s*일")
_SINO_DIGITS = ["영", "일", "이", "삼", "사", "오", "육", "칠", "팔", "구"]
_SINO_DIGITS_BY_CHAR = dict(zip("0123456789", _SINO_DIGITS))
_PHONE_DIGITS_BY_CHAR = {**_SINO_DIGITS_BY_CHAR, "0": "공"}
_INTEGER_TOKEN_RE = re.compile(r"^\d[\d,]*$")
_DECIMAL_TOKEN_RE = re.compile(r"^\d[\d,]*\.\d+$")
_HANGUL_WORD_RE = re.compile(r"[가-힣]+")
_TERMINAL_PUNCTUATION_RE = re.compile(r"[.!?。？！…]['\"”’)\]}]*\s*$")
_PHONE_NUMBER_RE = re.compile(
    r"(?<!\d)("
    r"01[016-9]\d{7,8}"      # 휴대폰 010/011/016-019
    r"|02\d{7,8}"            # 서울 02
    r"|0[3-6][1-5]\d{6,8}"   # 지역번호 031-064
    r"|0[78]0\d{7,8}"        # 070/080
    r"|050\d{7,8}"           # 050 평생번호
    r"|1[5-9]\d{2}\d{4}"     # 대표번호 15XX/16XX/18XX/19XX
    r")(?!\d)"
)
_NATIVE_HOURS = {
    1: "한", 2: "두", 3: "세", 4: "네", 5: "다섯", 6: "여섯",
    7: "일곱", 8: "여덟", 9: "아홉", 10: "열", 11: "열한", 12: "열두",
}


def ensure_terminal_punctuation(text: str) -> str:
    """문장 끝에 종결 부호를 붙인다. 정규화를 꺼도 이건 항상 적용한다."""
    text = text.rstrip()
    if not text or _TERMINAL_PUNCTUATION_RE.search(text):
        return text
    return f"{text}."


def _read_sino_under_10000(value: int) -> str:
    parts: list[str] = []
    for offset, unit in (
        (10000000, "천만"), (1000000, "백만"), (100000, "십만"), (10000, "만"),
        (1000, "천"), (100, "백"), (10, "십"), (1, ""),
    ):
        digit = value // offset
        value %= offset
        if digit == 0:
            continue
        if digit == 1 and unit:
            parts.append(unit)
        else:
            parts.append(f"{_SINO_DIGITS[digit]}{unit}")
    return "".join(parts) or _SINO_DIGITS[0]


def _read_sino_integer(value: int) -> str:
    if value == 0:
        return _SINO_DIGITS[0]
    if value < 10000:
        return _read_sino_under_10000(value)
    return str(value)


def _read_hour(value: int) -> str:
    return _NATIVE_HOURS.get(value, _read_sino_integer(value))


def _read_month(value: int) -> str:
    if value == 6:
        return "유"
    if value == 10:
        return "시"
    return _read_sino_integer(value)


def _spoken_iso_date(match: re.Match[str]) -> str:
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    return f"{_read_sino_integer(year)}년 {_read_month(month)}월 {_read_sino_integer(day)}일"


def normalize_time_date_numbers(text: str) -> str:
    text = _ISO_DATE_RE.sub(_spoken_iso_date, text)
    text = _SLASH_DATE_RE.sub(_spoken_iso_date, text)
    text = _TIME_WITH_MINUTE_RE.sub(
        lambda m: f"{_read_hour(int(m.group(1)))} 시 {_read_sino_integer(int(m.group(2)))} 분", text
    )
    text = _TIME_HALF_RE.sub(lambda m: f"{_read_hour(int(m.group(1)))} 시 반", text)
    text = _TIME_HOUR_RE.sub(lambda m: f"{_read_hour(int(m.group(1)))} 시", text)
    text = _MINUTE_RE.sub(lambda m: f"{_read_sino_integer(int(m.group(1)))} 분", text)
    text = _MONTH_DAY_RE.sub(
        lambda m: f"{_read_month(int(m.group(1)))}월 {_read_sino_integer(int(m.group(2)))}일", text
    )
    text = _MONTH_RE.sub(lambda m: f"{_read_month(int(m.group(1)))}월", text)
    return _DAY_RE.sub(lambda m: f"{_read_sino_integer(int(m.group(1)))}일", text)


@lru_cache(maxsize=1)
def _g2pk_numeral_tools():
    try:
        # g2pK 패키지를 통째로 import 하면 G2P 스택 전체가 초기화된다. numerals.py 만 필요하다.
        package_spec = importlib.machinery.PathFinder.find_spec("g2pk2")
        if package_spec is None or not package_spec.submodule_search_locations:
            return None

        numerals_path = Path(next(iter(package_spec.submodule_search_locations))) / "numerals.py"
        numerals_spec = importlib.util.spec_from_file_location("_queuetts_g2pk_numerals", numerals_path)
        if numerals_spec is None or numerals_spec.loader is None:
            return None

        numerals = importlib.util.module_from_spec(numerals_spec)
        numerals_spec.loader.exec_module(numerals)
    except (ImportError, OSError):
        return None

    process_num = getattr(numerals, "process_num", None)
    if process_num is None:
        return None
    bound_nouns = tuple(sorted(getattr(numerals, "BOUND_NOUNS", "").split(), key=len, reverse=True))
    return process_num, bound_nouns


def read_remaining_numbers(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        spoken = _read_numeric_token(match.group(0), text[match.end():])
        if spoken is None:
            if _is_hyphenated_phone_token(match.group(0)):
                return match.group(0)
            return _wrap_if_touching_letters(match, text)
        return spoken

    return _NUMERIC_TOKEN_RE.sub(replace, text)


def _read_numeric_token(token: str, following_text: str) -> Optional[str]:
    tools = _g2pk_numeral_tools()
    if tools is None:
        return None

    process_num, bound_nouns = tools
    sign = ""
    percent = ""

    if token.endswith("%"):
        token = token[:-1]
        percent = " 퍼센트"
    if token.startswith(("+", "-")):
        sign = "플러스 " if token[0] == "+" else "마이너스 "
        token = token[1:]
    if not token:
        return None

    next_word_match = _HANGUL_WORD_RE.match(following_text)
    next_word = next_word_match.group(0) if next_word_match else ""
    use_native = bool(next_word) and any(next_word.startswith(noun) for noun in bound_nouns)

    try:
        if _INTEGER_TOKEN_RE.fullmatch(token):
            spoken = process_num(token, sino=not use_native)
        elif _DECIMAL_TOKEN_RE.fullmatch(token):
            whole, fraction = token.split(".", 1)
            spoken = f"{process_num(whole, sino=True)}점{''.join(_SINO_DIGITS_BY_CHAR[d] for d in fraction)}"
        else:
            return None
    except Exception:
        return None

    return f"{sign}{spoken}{percent}"


def _read_phone_digits(token: str) -> str:
    return "".join(_PHONE_DIGITS_BY_CHAR[digit] for digit in token)


def _split_phone_number_digits(digits: str) -> list[str]:
    if digits.startswith("02"):
        rest = digits[2:]
        mid = 3 if len(rest) == 7 else 4
        return ["02", rest[:mid], rest[mid:]]
    if digits.startswith("1") and len(digits) == 8:
        return [digits[:4], digits[4:]]
    rest = digits[3:]
    mid = 3 if len(rest) == 7 else 4
    return [digits[:3], rest[:mid], rest[mid:]]


def read_phone_numbers_as_digits(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        groups = _split_phone_number_digits(match.group(0))
        return " ".join(_read_phone_digits(part) for part in groups)

    return _PHONE_NUMBER_RE.sub(replace, text)


def read_hyphenated_numbers_as_phone_digits(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        token = match.group(0)
        if not _is_hyphenated_phone_token(token):
            return token
        return " ".join(_read_phone_digits(part) for part in token.split("-"))

    return _NUMERIC_TOKEN_RE.sub(replace, text)


def _is_hyphenated_phone_token(token: str) -> bool:
    parts = token.split("-")
    if len(parts) < 2 or not all(part.isdigit() for part in parts):
        return False
    digit_count = sum(len(part) for part in parts)
    return digit_count >= 7 and len(parts[-1]) == 4 and all(2 <= len(part) <= 4 for part in parts)


def _wrap_if_touching_letters(match: re.Match[str], text: str) -> str:
    start, end = match.span()
    touches_left = start > 0 and _touches_letter(text[start - 1])
    touches_right = end < len(text) and _touches_letter(text[end])
    if not touches_left and not touches_right:
        return match.group(0)
    return f"({match.group(0)})"


def _touches_letter(char: str) -> bool:
    return char.isalnum() and not char.isdigit()


def normalize_korean_text(text: str) -> str:
    """정규화 전체 파이프라인. ``QUEUETTS_QWEN_TEXT_NORMALIZE=1`` 일 때만 호출된다."""
    text = prepare_generation_text(text)
    text = normalize_time_date_numbers(text)
    text = read_phone_numbers_as_digits(text)
    text = read_remaining_numbers(text)
    return read_hyphenated_numbers_as_phone_digits(text)
