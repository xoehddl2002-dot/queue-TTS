from __future__ import annotations

import importlib
import importlib.machinery
import importlib.util
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

from common.text_processing import prepare_generation_text


def _env_int_or_default(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# i5-7500 4코어 환경에서 ONNX auto(=4)보다 2/1이 더 빠름 (supertomic3와 동일 기본값).
# 다른 CPU 코어 수에서는 SUPERTONIC_INTRA_THREADS / SUPERTONIC_INTER_THREADS 환경변수로 조정.
DEFAULT_INTRA_THREADS = _env_int_or_default("SUPERTONIC_INTRA_THREADS", 2)
DEFAULT_INTER_THREADS = _env_int_or_default("SUPERTONIC_INTER_THREADS", 1)

DEFAULT_MODEL = os.getenv("SUPERTONIC_MODEL", "supertonic-3")
DEFAULT_VOICE = os.getenv("SUPERTONIC_VOICE", "Na-in-ae")
MAX_SEED = 4294967295
MIN_CHUNK_LENGTH = 10
SPEED_MIN = 0.8
SPEED_DEFAULT = 1.05
SPEED_MAX = 1.3
STEPS_MIN = 5
STEPS_DEFAULT = 12
STEPS_MAX = 12
DEFAULT_MAX_CHUNK_LENGTH = 200
SPEED_REFERENCE = SPEED_DEFAULT
EDGE_TRIM_KEEP_MS = 140
EDGE_TRIM_THRESHOLD = 0.003
OUTPUT_EDGE_PADDING_MS = 250
SLOW_KO_SENTENCE_CHUNK_SPEED = 0.9
_CHUNK_BREAK_CHARS = set(" \t\r\n,，、;；:：.!?。？！…")
_SENTENCE_END_CHARS = set(".!?。？！…")
_TERMINAL_PUNCTUATION_RE = re.compile(r"[.!?。？！…]['\"”’)\]}]*\s*$")

LANG_CHOICES = [
    "auto",
    #"na",
    "en",
    #"ja",
    "ko",
    #"ar",
    #"bg",
    #"cs",
    #"da",
    #"de",
    #"el",
    #"es",
    #"et",
    #"fi",
    #"fr",
    #"hi",
    #"hr",
    #"hu",
    #"id",
    #"it",
    #"lt",
    #"lv",
    #"nl",
    #"pl",
    #"pt",
    #"ro",
    #"ru",
    #"sk",
    #"sl",
    #"sv",
    #"tr",
    #"uk",
    #"vi",
]
FORMAT_CHOICES = ["wav","flac", "ogg"]
ONNX_RUNTIME_ENV = "QUEUETTS_ONNX_RUNTIME"
ONNX_PROVIDERS_ENV = "QUEUETTS_ONNX_PROVIDERS"
ONNX_PRELOAD_DLLS_ENV = "QUEUETTS_ONNX_PRELOAD_DLLS"
SUPERTONIC_ONNX_PROVIDERS_ENV = "SUPERTONIC_ONNX_PROVIDERS"
DEFAULT_ONNX_RUNTIME = "cpu"
CPU_ONNX_PROVIDERS = ["CPUExecutionProvider"]
CUDA_ONNX_PROVIDERS = ["CUDAExecutionProvider", "CPUExecutionProvider"]
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
_ONNX_DLLS_PRELOADED = False


@dataclass(frozen=True)
class TTSParams:
    text: str
    voice: str = DEFAULT_VOICE
    lang: str = "auto"
    speed: float = SPEED_DEFAULT
    steps: int = STEPS_DEFAULT
    response_format: str = "wav"
    seed: Optional[int] = 0
    max_chunk_length: int = DEFAULT_MAX_CHUNK_LENGTH
    silence_duration: float = 0.1


@dataclass(frozen=True)
class AudioResult:
    content: bytes
    response_format: str
    media_type: str
    sample_rate: int
    duration_s: float
    processed_text: str


@dataclass(frozen=True)
class StyleInfo:
    name: str
    kind: str
    path: Optional[str] = None


class QueueTtsError(RuntimeError):
    pass


def _normalize_onnx_runtime(value: Optional[str]) -> str:
    runtime = (value or os.getenv(ONNX_RUNTIME_ENV) or DEFAULT_ONNX_RUNTIME).strip().lower()
    aliases = {
        "cpu": "cpu",
        "gpu": "gpu",
        "cuda": "gpu",
        "cudaexecutionprovider": "gpu",
        "auto": "auto",
    }
    if runtime not in aliases:
        raise QueueTtsError(
            f"Unsupported ONNX runtime '{runtime}'. Use one of: cpu, gpu, auto."
        )
    return aliases[runtime]


def _parse_onnx_providers(value: str) -> list[str]:
    providers = [item.strip() for item in re.split(r"[,;\s]+", value) if item.strip()]
    if not providers:
        raise QueueTtsError("ONNX provider list is empty.")
    if "CPUExecutionProvider" not in providers:
        providers.append("CPUExecutionProvider")
    return providers


def _available_onnx_providers() -> list[str]:
    try:
        import onnxruntime as ort
    except Exception as exc:  # noqa: BLE001
        raise QueueTtsError(f"ONNX Runtime is not available: {exc}") from exc
    _preload_onnx_runtime_dlls(ort)
    return list(ort.get_available_providers())


def _preload_onnx_runtime_dlls(ort) -> None:
    global _ONNX_DLLS_PRELOADED
    if _ONNX_DLLS_PRELOADED:
        return

    if os.getenv(ONNX_PRELOAD_DLLS_ENV, "1") in {"0", "false", "False"}:
        return

    preload_dlls = getattr(ort, "preload_dlls", None)
    if preload_dlls is None:
        _ONNX_DLLS_PRELOADED = True
        return

    try:
        preload_dlls()
    except Exception:
        # Missing CUDA/cuDNN DLLs are reported clearly when CUDAExecutionProvider is requested.
        pass
    finally:
        _ONNX_DLLS_PRELOADED = True


def resolve_onnx_runtime(runtime: Optional[str] = None, providers: Optional[str] = None) -> dict:
    provider_override = (
        providers
        or os.getenv(ONNX_PROVIDERS_ENV)
        or os.getenv(SUPERTONIC_ONNX_PROVIDERS_ENV)
    )
    if provider_override:
        requested_providers = _parse_onnx_providers(provider_override)
        requested_runtime = runtime or "custom"
        selected_runtime = (
            "gpu" if "CUDAExecutionProvider" in requested_providers else "cpu"
        )
    else:
        requested_runtime = runtime or os.getenv(ONNX_RUNTIME_ENV) or DEFAULT_ONNX_RUNTIME
        selected_runtime = _normalize_onnx_runtime(runtime)
        if selected_runtime == "gpu":
            requested_providers = CUDA_ONNX_PROVIDERS.copy()
        elif selected_runtime == "auto":
            available = _available_onnx_providers()
            selected_runtime = "gpu" if "CUDAExecutionProvider" in available else "cpu"
            requested_providers = (
                CUDA_ONNX_PROVIDERS.copy()
                if selected_runtime == "gpu"
                else CPU_ONNX_PROVIDERS.copy()
            )
        else:
            requested_providers = CPU_ONNX_PROVIDERS.copy()

    available_providers = _available_onnx_providers()
    active_providers = [
        provider for provider in requested_providers if provider in available_providers
    ]
    if not active_providers:
        active_providers = CPU_ONNX_PROVIDERS.copy()

    if (
        selected_runtime == "gpu"
        and "CUDAExecutionProvider" in requested_providers
        and "CUDAExecutionProvider" not in available_providers
    ):
        raise QueueTtsError(
            "GPU runtime was requested, but CUDAExecutionProvider is not available. "
            "Install a CUDA-enabled ONNX Runtime build such as onnxruntime-gpu, "
            "then start again with QUEUETTS_ONNX_RUNTIME=gpu."
        )

    return {
        "requested_runtime": str(requested_runtime).lower(),
        "runtime": selected_runtime,
        "requested_providers": requested_providers,
        "active_providers": active_providers,
        "available_providers": available_providers,
    }


def apply_supertonic_onnx_runtime(
    runtime: Optional[str] = None, providers: Optional[str] = None
) -> dict:
    status = resolve_onnx_runtime(runtime=runtime, providers=providers)
    requested_providers = status["requested_providers"]

    config_module = importlib.import_module("supertonic.config")
    config_module.DEFAULT_ONNX_PROVIDERS = requested_providers

    loader_module = sys.modules.get("supertonic.loader")
    if loader_module is not None:
        loader_module.DEFAULT_ONNX_PROVIDERS = requested_providers

    return status


def ensure_supertonic_importable() -> None:
    try:
        importlib.import_module("supertonic")
    except ModuleNotFoundError as exc:
        raise QueueTtsError(
            "The 'supertonic' package is not installed in this app environment. "
            "Run '.\\.venv\\Scripts\\python -m pip install -r requirements.txt' "
            "inside D:\\workspace\\ai-tts\\ai-tts-queuetts."
        ) from exc


def clean_lang(lang: Optional[str]) -> Optional[str]:
    if not lang or lang == "auto":
        return None
    return lang


def clean_format(response_format: str) -> str:
    value = (response_format or "wav").lower().strip()
    if value not in FORMAT_CHOICES:
        raise QueueTtsError(f"Unsupported format '{response_format}'. Use: {', '.join(FORMAT_CHOICES)}")
    return value


def clean_seed(seed: Optional[int]) -> Optional[int]:
    if seed is None:
        return None
    value = int(seed)
    if value < 0:
        return None
    if value > MAX_SEED:
        raise QueueTtsError(f"Seed must be between -1 and {MAX_SEED}. Use -1 for random generation.")
    return value


def clean_speed(speed: Optional[float]) -> float:
    try:
        value = float(speed)
    except (TypeError, ValueError):
        return SPEED_DEFAULT
    if not math.isfinite(value):
        return SPEED_DEFAULT
    return min(max(value, SPEED_MIN), SPEED_MAX)


def clean_steps(steps: Optional[int]) -> int:
    try:
        value = int(steps)
    except (TypeError, ValueError):
        return STEPS_DEFAULT
    return min(max(value, STEPS_MIN), STEPS_MAX)


def clean_max_chunk_length(max_chunk_length: Optional[int]) -> int:
    if not max_chunk_length or max_chunk_length <= 0:
        return DEFAULT_MAX_CHUNK_LENGTH
    value = int(max_chunk_length)
    if value < MIN_CHUNK_LENGTH:
        raise QueueTtsError(f"Max chunk length must be at least {MIN_CHUNK_LENGTH}.")
    return value


def prefer_sentence_chunks(lang: str, speed: float) -> bool:
    if lang != "ko":
        return False
    try:
        return float(speed) <= SLOW_KO_SENTENCE_CHUNK_SPEED
    except (TypeError, ValueError):
        return False


def chunk_text_for_tts(
    text: str,
    max_chunk_length: int,
    chunk_text_fn,
    *,
    keep_sentence_chunks: bool = False,
) -> list[str]:
    chunks = _split_sentences_for_tts(text) if keep_sentence_chunks else chunk_text_fn(text, max_chunk_length)
    safe_chunks: list[str] = []
    for chunk in chunks:
        safe_chunks.extend(_split_oversized_chunk(chunk, max_chunk_length))
    return [chunk for chunk in safe_chunks if chunk]


def _split_sentences_for_tts(text: str) -> list[str]:
    sentences: list[str] = []
    start = 0
    index = 0
    while index < len(text):
        if text[index] not in _SENTENCE_END_CHARS:
            index += 1
            continue

        end = index + 1
        while end < len(text) and text[end] in "'\"”’)]}":
            end += 1

        sentence = text[start:end].strip()
        if sentence:
            sentences.append(sentence)

        while end < len(text) and text[end].isspace():
            end += 1
        start = end
        index = end

    tail = text[start:].strip()
    if tail:
        sentences.append(tail)
    return sentences


def _split_oversized_chunk(text: str, max_chunk_length: int) -> list[str]:
    text = text.strip()
    if len(text) <= max_chunk_length:
        return [text] if text else []

    chunks: list[str] = []
    remaining = text
    min_soft_break = max(MIN_CHUNK_LENGTH, max_chunk_length // 2)

    while len(remaining) > max_chunk_length:
        split_at = -1
        search_end = min(len(remaining), max_chunk_length) - 1
        for idx in range(search_end, min_soft_break - 1, -1):
            if remaining[idx] in _CHUNK_BREAK_CHARS:
                split_at = idx + 1
                break

        if split_at < MIN_CHUNK_LENGTH:
            split_at = max_chunk_length

        tail_len = len(remaining) - split_at
        if 0 < tail_len < MIN_CHUNK_LENGTH:
            split_at = max(MIN_CHUNK_LENGTH, len(remaining) - MIN_CHUNK_LENGTH)

        part = remaining[:split_at].strip()
        if part:
            chunks.append(part)
        remaining = remaining[split_at:].strip()

    if remaining:
        chunks.append(remaining)
    return chunks


def ensure_terminal_punctuation(text: str) -> str:
    text = text.rstrip()
    if not text or _TERMINAL_PUNCTUATION_RE.search(text):
        return text
    return f"{text}."


def _read_sino_under_10000(value: int) -> str:
    parts: list[str] = []
    for offset, unit in ((10000000, "천만"),(1000000, "백만"),(100000, "십만"),(10000, "만"),(1000, "천"), (100, "백"), (10, "십"), (1, "")):
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


def normalize_time_date_numbers_for_tts(text: str) -> str:
    text = _ISO_DATE_RE.sub(_spoken_iso_date, text)
    text = _SLASH_DATE_RE.sub(_spoken_iso_date, text)
    text = _TIME_WITH_MINUTE_RE.sub(
        lambda match: f"{_read_hour(int(match.group(1)))} 시 {_read_sino_integer(int(match.group(2)))} 분",
        text,
    )
    text = _TIME_HALF_RE.sub(lambda match: f"{_read_hour(int(match.group(1)))} 시 반", text)
    text = _TIME_HOUR_RE.sub(lambda match: f"{_read_hour(int(match.group(1)))} 시", text)
    text = _MINUTE_RE.sub(lambda match: f"{_read_sino_integer(int(match.group(1)))} 분", text)
    text = _MONTH_DAY_RE.sub(
        lambda match: f"{_read_month(int(match.group(1)))}월 {_read_sino_integer(int(match.group(2)))}일",
        text,
    )
    text = _MONTH_RE.sub(lambda match: f"{_read_month(int(match.group(1)))}월", text)
    return _DAY_RE.sub(lambda match: f"{_read_sino_integer(int(match.group(1)))}일", text)


def prepare_text_for_tts(text: str) -> str:
    text = prepare_generation_text(text)
    text = normalize_time_date_numbers_for_tts(text)
    text = read_phone_numbers_as_digits(text)
    text = read_remaining_numbers_with_g2pk(text)
    text = read_remaining_hyphenated_numbers_as_phone_digits(text)
    return ensure_terminal_punctuation(text)


@lru_cache(maxsize=1)
def _get_g2pk_numeral_tools():
    try:
        # g2pK's package import initializes the full G2P stack; this path only needs numerals.py.
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
    bound_nouns = getattr(numerals, "BOUND_NOUNS", "")
    if process_num is None:
        return None

    bound_nouns = tuple(sorted(bound_nouns.split(), key=len, reverse=True))
    return process_num, bound_nouns


def read_remaining_numbers_with_g2pk(text: str) -> str:
    """Read simple remaining numeric tokens with g2pK's numeral rules."""

    def replace(match: re.Match[str]) -> str:
        spoken = _read_numeric_token_with_g2pk(match.group(0), text[match.end() :])
        if spoken is None:
            if _is_hyphenated_phone_token(match.group(0)):
                return match.group(0)
            return _wrap_number_match_if_touching_letters(match, text)
        return spoken

    return _NUMERIC_TOKEN_RE.sub(replace, text)


def _read_numeric_token_with_g2pk(token: str, following_text: str) -> Optional[str]:
    tools = _get_g2pk_numeral_tools()
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
    """Read non-hyphenated Korean phone numbers as separate Hangul digits."""

    def replace(match: re.Match[str]) -> str:
        groups = _split_phone_number_digits(match.group(0))
        return " ".join(_read_phone_digits(part) for part in groups)

    return _PHONE_NUMBER_RE.sub(replace, text)


def read_remaining_hyphenated_numbers_as_phone_digits(text: str) -> str:
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


def _wrap_number_match_if_touching_letters(match: re.Match[str], text: str) -> str:
    start, end = match.span()
    touches_left = start > 0 and _touches_letter(text[start - 1])
    touches_right = end < len(text) and _touches_letter(text[end])
    if not touches_left and not touches_right:
        return match.group(0)
    return f"({match.group(0)})"


def _touches_letter(char: str) -> bool:
    return char.isalnum() and not char.isdigit()


class LocalTTSService:
    def __init__(self, model: str = DEFAULT_MODEL) -> None:
        self.model_name = model
        self._lock = threading.Lock()
        self._tts = None
        self._styles_store = None
        self._custom_styles: dict[str, Path] = {}
        self._style_cache = {}

    def load(self):
        if self._tts is not None:
            return self._tts

        with self._lock:
            if self._tts is not None:
                return self._tts

            ensure_supertonic_importable()
            apply_supertonic_onnx_runtime()
            import style_store
            from supertonic import TTS

            auto_download = os.getenv("SUPERTONIC_AUTO_DOWNLOAD", "1") not in {"0", "false", "False"}
            self._tts = TTS(
                model=self.model_name,
                auto_download=auto_download,
                intra_op_num_threads=DEFAULT_INTRA_THREADS,
                inter_op_num_threads=DEFAULT_INTER_THREADS,
            )
            print(f"[queuetts] TTS loaded with intra={DEFAULT_INTRA_THREADS} inter={DEFAULT_INTER_THREADS}")
            self._styles_store = style_store
            self.reload_custom_styles()
            return self._tts

    def reload_custom_styles(self) -> None:
        if self._styles_store is None:
            return
        self._custom_styles = self._styles_store.scan(
            self._styles_store.default_custom_styles_dir(self.model_name)
        )
        self._style_cache = {
            name: style for name, style in self._style_cache.items() if name not in self._custom_styles
        }

    def style_infos(self) -> list[StyleInfo]:
        tts = self.load()
        builtin = [StyleInfo(name=name, kind="builtin") for name in tts.voice_style_names]
        return builtin + self.custom_style_infos()

    def custom_style_infos(self) -> list[StyleInfo]:
        self.load()
        custom = [
            StyleInfo(name=name, kind="custom", path=str(path))
            for name, path in sorted(self._custom_styles.items())
        ]
        return custom

    def voices(self) -> list[str]:
        return [style.name for style in self.style_infos()]

    def custom_voices(self) -> list[str]:
        return [style.name for style in self.custom_style_infos()]

    def _style(self, voice: str):
        tts = self.load()
        voice = voice or DEFAULT_VOICE
        if voice in self._style_cache:
            return self._style_cache[voice]

        if voice in tts.voice_style_names:
            style = tts.get_voice_style(voice)
        elif voice in self._custom_styles:
            style = tts.get_voice_style_from_path(self._custom_styles[voice])
        else:
            raise QueueTtsError(f"Unknown voice '{voice}'. Refresh voices and choose one from the list.")
        self._style_cache[voice] = style
        return style

    def synthesize(self, params: TTSParams) -> AudioResult:
        if not params.text.strip():
            raise QueueTtsError("Text is empty.")

        t_total = time.perf_counter()
        ensure_supertonic_importable()
        import numpy as np
        from common.queuetts_audio import coerce_response_format, duration_seconds, encode_audio, format_to_mime
        from supertonic.utils import chunk_text

        fmt = coerce_response_format(clean_format(params.response_format))
        seed = clean_seed(params.seed)
        t0 = time.perf_counter()
        tts = self.load()
        t_load = time.perf_counter() - t0

        t0 = time.perf_counter()
        voice_style = self._style(params.voice)
        t_style = time.perf_counter() - t0

        steps = clean_steps(params.steps)
        speed = clean_speed(params.speed)
        silence_duration = float(params.silence_duration)
        lang = clean_lang(params.lang)
        effective_lang = lang if lang is not None else "ko"

        # 청크 자체 끝 무음을 우리가 직접 다듬으려면 supertonic의 synthesize 통째 호출 대신
        # 모델을 직접 청크 단위로 부른다.
        max_chunk_length = clean_max_chunk_length(params.max_chunk_length)

        t0 = time.perf_counter()
        prepared = prepare_text_for_tts(params.text)
        t_prep = time.perf_counter() - t0

        t0 = time.perf_counter()
        text_chunks = chunk_text_for_tts(
            prepared,
            max_chunk_length,
            chunk_text,
            keep_sentence_chunks=prefer_sentence_chunks(effective_lang, speed),
        )
        t_chunk = time.perf_counter() - t0
        if not text_chunks:
            raise QueueTtsError("Text produced no chunks.")

        with self._lock:
            if seed is not None:
                np.random.seed(seed)
            t0 = time.perf_counter()
            wav = self._synthesize_chunks_with_tight_gaps(
                tts=tts,
                text_chunks=text_chunks,
                voice_style=voice_style,
                steps=steps,
                speed=speed,
                lang=effective_lang,
                silence_duration=silence_duration,
            )
            t_synth = time.perf_counter() - t0
            t0 = time.perf_counter()
            content = encode_audio(wav, tts.sample_rate, fmt)
            t_encode = time.perf_counter() - t0

        t_all = time.perf_counter() - t_total
        prepared_chars = len(prepared)
        chunk_chars = sum(len(c) for c in text_chunks)
        print(
            f"[queuetts.timing] total={t_all:.3f}s | load={t_load:.3f} style={t_style:.3f} "
            f"prep={t_prep:.3f} chunk={t_chunk:.3f} synth={t_synth:.3f} encode={t_encode:.3f} "
            f"| in_chars={len(params.text)} prepared_chars={prepared_chars} chunk_chars={chunk_chars} "
            f"chunks={len(text_chunks)} max_chunk_len={max_chunk_length}"
        )

        return AudioResult(
            content=content,
            response_format=fmt,
            media_type=format_to_mime(fmt),
            sample_rate=int(tts.sample_rate),
            duration_s=duration_seconds(wav, tts.sample_rate),
            processed_text=prepared,
        )

    @staticmethod
    def _synthesize_chunks_with_tight_gaps(
        *,
        tts,
        text_chunks: list[str],
        voice_style,
        steps: int,
        speed: float,
        lang: str,
        silence_duration: float,
    ):
        """청크별 합성 후 앞뒤 무음을 다듬고 짧은 무음으로 이어붙임."""
        import numpy as np

        sample_rate = tts.sample_rate
        # 모델이 청크 앞뒤로 600~750ms 정도 자체 무음을 만들 수 있어 다듬는다.
        # 완성본 앞뒤에는 별도 패딩을 두므로 어두/어미가 플레이어에서 물리지 않는다.
        keep_edge_ms = LocalTTSService._edge_keep_ms_for_speed(speed)
        keep_edge_samples = int(sample_rate * keep_edge_ms / 1000)
        threshold = EDGE_TRIM_THRESHOLD

        gap_samples = max(int(silence_duration * sample_rate), 0)
        gap = np.zeros((1, gap_samples), dtype=np.float32) if gap_samples > 0 else None

        pieces = []
        infer_total = 0.0
        trim_total = 0.0
        for index, chunk in enumerate(text_chunks):
            t0 = time.perf_counter()
            wav, _ = tts.model([chunk], voice_style, steps, speed, lang)
            infer_total += time.perf_counter() - t0
            # wav shape: (1, samples)
            t0 = time.perf_counter()
            trimmed = LocalTTSService._trim_edges(wav, threshold, keep_edge_samples)
            trim_total += time.perf_counter() - t0
            pieces.append(trimmed)
            if gap is not None and index < len(text_chunks) - 1:
                pieces.append(gap)

        result = LocalTTSService._pad_output_edges(np.concatenate(pieces, axis=1), sample_rate)
        print(f"[queuetts.timing.synth] infer={infer_total:.3f}s trim+concat={trim_total:.3f}s n_chunks={len(text_chunks)}")
        return result

    @staticmethod
    def _edge_keep_ms_for_speed(speed: float) -> int:
        try:
            speed_value = float(speed)
        except (TypeError, ValueError):
            speed_value = SPEED_REFERENCE
        if not speed_value > 0:
            speed_value = SPEED_REFERENCE
        if speed_value >= SPEED_REFERENCE:
            return EDGE_TRIM_KEEP_MS
        return int(round(EDGE_TRIM_KEEP_MS * (SPEED_REFERENCE / speed_value)))

    @staticmethod
    def _trim_edges(
        wav,
        threshold: float,
        keep_edge_samples: int,
        *,
        trim_leading: bool = True,
        trim_trailing: bool = True,
    ):
        """앞뒤로 진폭이 threshold 미만인 구간을 keep_edge_samples만 남기고 잘라낸다."""
        import numpy as np

        if not trim_leading and not trim_trailing:
            return wav

        signal = wav[0]
        abs_signal = np.abs(signal)
        n = len(signal)
        if n == 0:
            return wav

        leading = 0
        if trim_leading:
            while leading < n and abs_signal[leading] < threshold:
                leading += 1
        trailing = 0
        if trim_trailing:
            while trailing < n and abs_signal[n - 1 - trailing] < threshold:
                trailing += 1

        # 모두 무음이면 그대로 둠.
        if leading + trailing >= n:
            return wav

        start = max(0, leading - keep_edge_samples) if trim_leading else 0
        end = n - max(0, trailing - keep_edge_samples) if trim_trailing else n
        return wav[:, start:end]

    @staticmethod
    def _pad_output_edges(wav, sample_rate: int):
        """플레이어가 첫/끝 음절을 물지 않도록 완성본 앞뒤에 짧은 무음을 둔다."""
        import numpy as np

        padding_samples = int(sample_rate * OUTPUT_EDGE_PADDING_MS / 1000)
        if padding_samples <= 0:
            return wav
        padding = np.zeros((wav.shape[0], padding_samples), dtype=wav.dtype)
        return np.concatenate([padding, wav, padding], axis=1)


LOCAL_SERVICE = LocalTTSService()
