"""Text normalization helpers applied before TTS generation."""

import re
from typing import Optional

try:
    import phonenumbers
except ImportError:
    phonenumbers = None


KOREAN_PHONE_CANDIDATE_RE = re.compile(
    r'(?<![0-9A-Za-z])\(?[0-9](?:[0-9 .\-\u2010-\u2015()]*[0-9]){7,}\)?(?![0-9A-Za-z])'
)
KOREAN_MONTH_DAY_SLASH_RE = re.compile(
    r'(?<![0-9A-Za-z/])'
    r'(0?[1-9]|1[0-2])'
    r'\s*/\s*'
    r'(0?[1-9]|[12][0-9]|3[01])'
    r'\s*일'
    r'(?![0-9A-Za-z/])'
)
KOREAN_TIME_UNIT_NUMBER_RE = re.compile(
    r'(?<![0-9A-Za-z.])'
    r'([0-9]+)'
    r'\s*'
    r'(년|월|일|시|분|초)'
    r'(?![0-9A-Za-z])'
)
KOREAN_GENERAL_NUMBER_RE = re.compile(
    r'(?<![0-9A-Za-z.,/\-])'
    r'([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)'
    r'(\s*)'
    r'(?=[가-힣])'
)
KOREAN_PHONE_DIGIT_WORDS = {
    "0": "공",
    "1": "일",
    "2": "이",
    "3": "삼",
    "4": "사",
    "5": "오",
    "6": "육",
    "7": "칠",
    "8": "팔",
    "9": "구",
}
KOREAN_SINO_DIGITS = {
    0: "",
    1: "일",
    2: "이",
    3: "삼",
    4: "사",
    5: "오",
    6: "육",
    7: "칠",
    8: "팔",
    9: "구",
}
KOREAN_SINO_SMALL_UNITS = ("", "십", "백", "천")
KOREAN_SINO_LARGE_UNITS = ("", "만", "억", "조")
KOREAN_NATIVE_HOUR_WORDS = {
    0: "영",
    1: "한",
    2: "두",
    3: "세",
    4: "네",
    5: "다섯",
    6: "여섯",
    7: "일곱",
    8: "여덟",
    9: "아홉",
    10: "열",
    11: "열한",
    12: "열두",
}
KOREAN_AREA_CODES = {
    "031", "032", "033",
    "041", "042", "043", "044",
    "051", "052", "053", "054", "055",
    "061", "062", "063", "064",
}
KOREAN_MOBILE_PREFIXES = {"010", "011", "016", "017", "018", "019"}
KOREAN_THREE_DIGIT_PHONE_PREFIXES = KOREAN_AREA_CODES | KOREAN_MOBILE_PREFIXES | {"070", "080"}
KOREAN_SERVICE_PREFIXES = {
    "1522", "1544", "1566", "1577", "1588", "1599",
    "1600", "1644", "1661", "1666", "1688",
    "1800", "1833", "1855", "1877", "1899",
}
SENTENCE_BOUNDARY_CHARS = ".?!。！？…"
SENTENCE_CLOSING_CHARS = "\"'”’)]}）】〉》」』"
ENGLISH_ABBREVIATIONS = {
    "mr.", "mrs.", "ms.", "dr.", "prof.", "sr.", "jr.",
    "vs.", "etc.", "e.g.", "i.e.", "a.m.", "p.m.",
    "u.s.", "u.s.a.",
}


def split_korean_phone_digits(digits: str):
    """한국 전화번호 digit 문자열을 읽기 좋은 그룹으로 나눔."""
    length = len(digits)

    if length == 8 and digits[:4] in KOREAN_SERVICE_PREFIXES:
        return [digits[:4], digits[4:]]

    if digits.startswith("02") and length in (9, 10):
        middle_len = length - 6
        return [digits[:2], digits[2:2 + middle_len], digits[-4:]]

    if digits.startswith("050") and length == 12:
        return [digits[:4], digits[4:8], digits[8:]]

    if digits.startswith("050") and length == 11:
        return [digits[:3], digits[3:7], digits[7:]]

    if digits[:3] in KOREAN_THREE_DIGIT_PHONE_PREFIXES and length in (10, 11):
        middle_len = length - 7
        return [digits[:3], digits[3:3 + middle_len], digits[-4:]]

    return None


def read_korean_phone_digits(digits: str) -> Optional[str]:
    """한국 전화번호 digit 문자열을 TTS용 한글 읽기로 변환."""
    groups = split_korean_phone_digits(digits)
    if not groups:
        return None

    return " ".join(
        "".join(KOREAN_PHONE_DIGIT_WORDS[digit] for digit in group)
        for group in groups
    )


def read_korean_phone_with_libphonenumber(raw: str, digits: str) -> Optional[str]:
    """phonenumbers가 설치되어 있으면 한국 번호 검증/그룹 포맷에 활용."""
    if phonenumbers is None:
        return None

    for candidate in (raw, digits):
        try:
            parsed = phonenumbers.parse(candidate, "KR")
        except phonenumbers.NumberParseException:
            continue

        if phonenumbers.region_code_for_number(parsed) != "KR":
            continue
        if not phonenumbers.is_valid_number(parsed):
            continue

        formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
        groups = re.findall(r'\d+', formatted)
        if not groups:
            continue
        return " ".join(
            "".join(KOREAN_PHONE_DIGIT_WORDS[digit] for digit in group)
            for group in groups
        )

    return None


def normalize_korean_phone_numbers(text: str) -> str:
    """텍스트 안의 한국 전화번호를 괄호 친 한글 digit 읽기로 변환."""
    def replace_phone(match):
        raw = match.group(0)
        digits = re.sub(r'\D', '', raw)
        spoken = read_korean_phone_with_libphonenumber(raw, digits) or read_korean_phone_digits(digits)
        if not spoken:
            return raw
        return f"({spoken})"

    return KOREAN_PHONE_CANDIDATE_RE.sub(replace_phone, text)


def normalize_korean_month_day_slashes(text: str) -> str:
    """`4/14일` 같은 월/일 표기를 `4월 14일`로 변환."""
    def replace_month_day(match):
        month = int(match.group(1))
        day = int(match.group(2))
        return f"{month}월 {day}일"

    return KOREAN_MONTH_DAY_SLASH_RE.sub(replace_month_day, text)


def read_korean_sino_number(number: int) -> str:
    """정수를 한국어 한자어 수사로 변환."""
    if number == 0:
        return "영"

    parts = []
    group_index = 0
    while number > 0:
        group = number % 10000
        if group:
            parts.append(_read_korean_sino_under_10000(group) + KOREAN_SINO_LARGE_UNITS[group_index])
        number //= 10000
        group_index += 1

    return "".join(reversed(parts))


def _read_korean_sino_under_10000(number: int) -> str:
    parts = []
    digits = list(map(int, str(number)))
    length = len(digits)

    for index, digit in enumerate(digits):
        if digit == 0:
            continue
        unit_index = length - index - 1
        digit_word = "" if digit == 1 and unit_index > 0 else KOREAN_SINO_DIGITS[digit]
        parts.append(digit_word + KOREAN_SINO_SMALL_UNITS[unit_index])

    return "".join(parts)


def read_korean_time_unit_number(raw_number: str, unit: str) -> str:
    """시간 관련 단위 앞의 숫자를 TTS용 한국어 읽기로 변환."""
    number = int(raw_number)
    if unit == "시" and number in KOREAN_NATIVE_HOUR_WORDS:
        return KOREAN_NATIVE_HOUR_WORDS[number] + unit
    return read_korean_sino_number(number) + unit


def normalize_korean_time_unit_numbers(text: str) -> str:
    """`2023년`, `01시`, `5분` 같은 시간 단위 숫자를 한글 읽기로 변환."""
    def replace_time_unit(match):
        return read_korean_time_unit_number(match.group(1), match.group(2))

    return KOREAN_TIME_UNIT_NUMBER_RE.sub(replace_time_unit, text)


def normalize_korean_general_numbers(text: str) -> str:
    """한글 앞 일반 숫자를 의미 변환 없이 한자어 숫자 읽기로 변환."""
    def replace_number(match):
        raw_number = match.group(1)
        spacing = match.group(2)
        spoken = read_korean_sino_number(int(raw_number.replace(",", "")))
        suffix_start = match.end()
        following = match.string[suffix_start:suffix_start + 2]
        if following.startswith("번") and not following.startswith("번째"):
            return f"{spoken} "
        return f"{spoken}{spacing}"

    return KOREAN_GENERAL_NUMBER_RE.sub(replace_number, text)


def prepare_generation_text(text: str) -> str:
    """모델 생성 직전에 적용할 텍스트 전처리."""
    normalized = normalize_korean_phone_numbers(text)
    normalized = normalize_korean_month_day_slashes(normalized)
    normalized = normalize_korean_time_unit_numbers(normalized)
    normalized = normalize_korean_general_numbers(normalized)
    if normalized != text:
        print(f"[text_processing] 생성 텍스트 전처리: '{text[:80]}' -> '{normalized[:80]}'")
    return normalized


def split_generation_sentences(text: str):
    """TTS 생성을 위해 문장 단위로 분할하되 소수점/URL/말줄임표 내부는 보존."""
    text = text.strip()
    if not text:
        return []

    sentences = []
    start = 0
    i = 0
    length = len(text)

    while i < length:
        char = text[i]
        if char not in SENTENCE_BOUNDARY_CHARS:
            i += 1
            continue

        if char == "." and _is_decimal_point(text, i):
            i += 1
            continue

        cluster_end = _consume_boundary_cluster(text, i)
        segment_end = _consume_closing_chars(text, cluster_end)
        next_index = segment_end
        while next_index < length and text[next_index].isspace():
            next_index += 1

        if _is_sentence_boundary(text, i, segment_end, next_index):
            sentence = text[start:segment_end].strip()
            if sentence:
                sentences.append(sentence)
            start = next_index
            i = next_index
            continue

        i = cluster_end

    tail = text[start:].strip()
    if tail:
        sentences.append(tail)

    return sentences or [text]


def _is_decimal_point(text: str, index: int) -> bool:
    return (
        index > 0
        and index + 1 < len(text)
        and text[index - 1].isdigit()
        and text[index + 1].isdigit()
    )


def _consume_boundary_cluster(text: str, index: int) -> int:
    while index < len(text) and text[index] in SENTENCE_BOUNDARY_CHARS:
        index += 1
    return index


def _consume_closing_chars(text: str, index: int) -> int:
    while index < len(text) and text[index] in SENTENCE_CLOSING_CHARS:
        index += 1
    return index


def _is_sentence_boundary(text: str, boundary_index: int, segment_end: int, next_index: int) -> bool:
    if next_index >= len(text):
        return True

    if text[boundary_index] == "." and _ends_with_english_abbreviation(text[:segment_end]):
        return False

    next_char = text[next_index]
    if next_index > segment_end:
        return True
    return _is_hangul(next_char) or next_char in "\"'“‘([{（【〈《「『"


def _ends_with_english_abbreviation(prefix: str) -> bool:
    compact = prefix.rstrip().lower()
    return any(compact.endswith(abbr) for abbr in ENGLISH_ABBREVIATIONS)


def _is_hangul(char: str) -> bool:
    return "\uac00" <= char <= "\ud7a3"


def parse_repeat_pattern(text: str):
    """텍스트에서 반복 패턴 감지.

    Returns:
        (unit, count, remainder)
        예) "A A A B" -> ("A", 3, "B")
            "A A A"   -> ("A", 3, "")
            "ABCD"    -> ("ABCD", 1, "")
    """
    text = re.sub(r'\s+', ' ', text.strip())
    n = len(text)

    for unit_len in range(1, n // 2 + 1):
        unit = text[:unit_len]
        if unit != unit.strip():  # 앞뒤 공백 있는 단위 제외
            continue

        # 공백 구분자 방식: "A B A B A B"
        count, pos = 0, 0
        while pos < n:
            if text[pos:pos + unit_len] == unit:
                count += 1
                pos += unit_len
                if pos < n and text[pos] == ' ':
                    pos += 1  # 구분 공백 skip
            else:
                break
        if count >= 2:
            return unit, count, text[pos:].strip()

        # 구분자 없는 방식: "AAAB"
        count, pos = 0, 0
        while pos + unit_len <= n and text[pos:pos + unit_len] == unit:
            count += 1
            pos += unit_len
        if count >= 2:
            return unit, count, text[pos:].strip()

    return text, 1, ""
