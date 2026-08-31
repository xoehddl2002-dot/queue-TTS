"""Qwen3-TTS 합성 백엔드.

`qwen-tts` 패키지의 `Qwen3TTSModel` 을 감싸 워커가 쓰는 형태로 노출한다.

Supertonic 워커(`worker-supertonic`)와 **Redis 계약·출력 포맷은 동일**하지만
엔진 내부 규격은 다르다 — `speed`/`steps` 대응 개념이 없고, 언어는 코드가 아니라 이름
(`"Korean"`)이며, 보이스는 Gateway가 소유하는 참조 음성 clone style이다.

이 워커는 **Base 체크포인트 전용**이다. 화자 인코더를 가진 것은 Base 뿐이라 클로닝도 Base 만
할 수 있다 — CustomVoice/VoiceDesign 체크포인트는 `generate_voice_clone` 을 아예 거절한다.
그래서 내장 화자 목록이 없고, `custom_voices/`는 speakerName 기반 prompt 파생 캐시로만 쓴다.
자세한 대응 관계는 docs/design.md 참고.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import clone_prompt
import voice_store
from clone_prompt import ClonePrompt
from voice_store import StyleRef, default_style_cache_dir

log = logging.getLogger("queuetts.qwen")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", ""}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# 클로닝은 Base 체크포인트에만 있다. 다른 타입을 넣으면 로드는 되고 첫 합성에서 터지므로
# 기동 시점에 확인한다(QwenTTSService._check_capability).
MODEL_ID = os.getenv("QWEN_MODEL_ID", "Qwen/Qwen3-TTS-12Hz-1.7B-Base")
REQUIRED_MODEL_TYPE = "base"
DEVICE_MAP = os.getenv("QWEN_DEVICE_MAP", "cuda:0")
DTYPE = os.getenv("QWEN_DTYPE", "bfloat16")
DEFAULT_LANGUAGE = os.getenv("QWEN_DEFAULT_LANGUAGE", "Korean")

# Qwen 은 롱폼에 강해 Supertonic(200)보다 크게 잡는다. 너무 크면 지연이 튀므로 조절 가능하게 둔다.
DEFAULT_MAX_CHUNK_LENGTH = _env_int("QWEN_MAX_CHUNK_LENGTH", 400)
MIN_CHUNK_LENGTH = 10
MAX_SEED = 4294967295

# 청크 앞뒤 무음 트리밍은 **기본 꺼짐**이다. Supertonic 에서 쓰던 임계값이 Qwen 출력에도 맞는지
# 검증되지 않았고, 잘못 켜면 어두/어미를 깎아낸다. 특성을 파악한 뒤 켠다.
EDGE_TRIM_ENABLED = _env_flag("QWEN_EDGE_TRIM", False)
EDGE_TRIM_KEEP_MS = _env_int("QWEN_EDGE_TRIM_KEEP_MS", 200)
EDGE_TRIM_THRESHOLD = float(os.getenv("QWEN_EDGE_TRIM_THRESHOLD", "0.003"))
OUTPUT_EDGE_PADDING_MS = _env_int("QWEN_OUTPUT_PADDING_MS", 250)

TEXT_NORMALIZE = _env_flag("QUEUETTS_QWEN_TEXT_NORMALIZE", False)

FORMAT_CHOICES = ["wav", "flac", "ogg"]

# Gateway/UI 는 ISO 코드로 보내고 Qwen 은 언어 이름을 받는다.
LANGUAGE_BY_CODE = {
    "ko": "Korean",
    "en": "English",
    "ja": "Japanese",
    "zh": "Chinese",
    "de": "German",
    "fr": "French",
    "ru": "Russian",
    "pt": "Portuguese",
    "es": "Spanish",
    "it": "Italian",
}
SUPPORTED_LANGUAGES = set(LANGUAGE_BY_CODE.values())

_CHUNK_BREAK_CHARS = set(" \t\r\n,，、;；:：.!?。？！…")
_SENTENCE_END_CHARS = set(".!?。？！…")


class QueueTtsError(RuntimeError):
    pass


@dataclass(frozen=True)
class TTSParams:
    text: str
    voice: str = ""
    # 게이트웨이가 speaker registry 에서 해석해 실어 보내는 참조. wire 이름도 speaker* 다.
    # (프롬프트 캐시 내부는 아직 style 용어를 쓴다 — 그쪽 rename 이 오면 함께 따라가면 된다.)
    speaker_name: str = ""
    reference_digest: str = ""
    speaker_blob_key: str = ""
    speaker_mode: str = "icl"
    speaker_ref_text: Optional[str] = None
    lang: str = "auto"
    response_format: str = "wav"
    seed: Optional[int] = 0
    max_chunk_length: int = DEFAULT_MAX_CHUNK_LENGTH
    silence_duration: float = 0.1
    # Supertonic 계열 파라미터. Qwen 에는 대응 개념이 없어 무시하고 로그만 남긴다.
    speed: Optional[float] = None
    steps: Optional[int] = None
    do_sample: Optional[bool] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    top_k: Optional[int] = None
    repetition_penalty: Optional[float] = None
    max_new_tokens: Optional[int] = None
    subtalker_dosample: Optional[bool] = None
    subtalker_temperature: Optional[float] = None
    subtalker_top_p: Optional[float] = None
    subtalker_top_k: Optional[int] = None

    def generate_kwargs(self) -> dict[str, Any]:
        names = (
            "do_sample", "temperature", "top_p", "top_k", "repetition_penalty",
            "max_new_tokens", "subtalker_dosample", "subtalker_temperature",
            "subtalker_top_p", "subtalker_top_k",
        )
        return {name: getattr(self, name) for name in names if getattr(self, name) is not None}


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


# ── 파라미터 정리 ────────────────────────────────────────────────────────────────
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


def clean_max_chunk_length(max_chunk_length: Optional[int]) -> int:
    if not max_chunk_length or max_chunk_length <= 0:
        return DEFAULT_MAX_CHUNK_LENGTH
    value = int(max_chunk_length)
    if value < MIN_CHUNK_LENGTH:
        raise QueueTtsError(f"Max chunk length must be at least {MIN_CHUNK_LENGTH}.")
    return value


def clean_language(lang: Optional[str], fallback: Optional[str] = None) -> str:
    """``auto``/ISO 코드/언어 이름을 Qwen 이 받는 언어 이름으로 바꾼다.

    ``auto`` 이거나 비어 있으면 ``fallback``(보통 style 기본 언어)으로, 그것도 없으면
    ``QWEN_DEFAULT_LANGUAGE`` 로 떨어진다. **명시된 언어가 style 기본 언어보다 우선한다.**
    """
    value = (lang or "").strip()
    if not value or value.lower() == "auto":
        return clean_language(fallback) if fallback else DEFAULT_LANGUAGE
    mapped = LANGUAGE_BY_CODE.get(value.lower())
    if mapped:
        return mapped
    normalized = value.capitalize() if value.islower() else value
    if normalized in SUPPORTED_LANGUAGES:
        return normalized
    raise QueueTtsError(
        f"Unsupported language '{lang}'. Use one of: "
        f"{', '.join(sorted(SUPPORTED_LANGUAGES))} (or codes: {', '.join(sorted(LANGUAGE_BY_CODE))})."
    )


# ── 텍스트 준비 ─────────────────────────────────────────────────────────────────
def prepare_text(text: str) -> str:
    if TEXT_NORMALIZE:
        from text_normalize import ensure_terminal_punctuation, normalize_korean_text

        return ensure_terminal_punctuation(normalize_korean_text(text))

    from text_normalize import ensure_terminal_punctuation

    return ensure_terminal_punctuation(text.strip())


def chunk_text(text: str, max_chunk_length: int) -> list[str]:
    """문장 경계로 자른 뒤 ``max_chunk_length`` 까지 **다시 묶는다.**

    묶는 쪽이 핵심이다 — 청크 하나가 곧 `generate_voice_clone` 호출 하나이고 호출마다
    독립적으로 샘플링하므로, 문장을 낱개로 흘려보내면 문장마다 음색이 미세하게 달라져
    이어붙인 결과가 여러 사람이 번갈아 읽는 것처럼 들린다. 호출을 줄이면 그 자리가 없어진다.

    ``max_chunk_length`` 는 상한이다. 문장 하나가 그보다 길면 구두점/공백에서 다시 쪼갠다.
    """
    pieces: list[str] = []
    for sentence in _split_sentences(text):
        pieces.extend(_split_oversized(sentence, max_chunk_length))

    # `_split_oversized` 가 조각마다 상한을 지키므로, 이어붙일 때는 길이만 보면 된다.
    chunks: list[str] = []
    for piece in pieces:
        if not piece:
            continue
        if chunks and len(chunks[-1]) + 1 + len(piece) <= max_chunk_length:
            chunks[-1] = f"{chunks[-1]} {piece}"
        else:
            chunks.append(piece)
    return chunks


def _split_sentences(text: str) -> list[str]:
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


def _split_oversized(text: str, max_chunk_length: int) -> list[str]:
    text = text.strip()
    if len(text) <= max_chunk_length:
        return [text] if text else []

    chunks: list[str] = []
    remaining = text
    min_soft_break = max(MIN_CHUNK_LENGTH, max_chunk_length // 2)

    while len(remaining) > max_chunk_length:
        split_at = -1
        for idx in range(min(len(remaining), max_chunk_length) - 1, min_soft_break - 1, -1):
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


# ── 오디오 후처리 ───────────────────────────────────────────────────────────────
def _as_2d_mono(wav) -> Any:
    """모델 출력이 어떤 형태로 오든 ``(1, samples)`` float32 로 맞춘다.

    `generate_*` 는 batch 를 염두에 둔 ``wavs`` 를 돌려주므로 list / 1-D / 2-D 를 모두 받는다.
    """
    import numpy as np

    if isinstance(wav, (list, tuple)):
        if not wav:
            raise QueueTtsError("model returned an empty waveform batch")
        wav = wav[0]

    array = np.asarray(wav)
    if hasattr(array, "detach"):  # torch tensor 가 그대로 온 경우
        array = array.detach().cpu().numpy()
    array = np.asarray(array, dtype=np.float32)

    if array.ndim == 1:
        return array.reshape(1, -1)
    if array.ndim == 2:
        # (samples, channels) 로 온 경우도 (1, samples) 로 맞춘다.
        if array.shape[0] > array.shape[1]:
            array = array.T
        return array[:1]
    raise QueueTtsError(f"unexpected waveform shape {array.shape}")


def _trim_edges(wav, threshold: float, keep_edge_samples: int):
    import numpy as np

    signal = wav[0]
    abs_signal = np.abs(signal)
    n = len(signal)
    if n == 0:
        return wav

    leading = 0
    while leading < n and abs_signal[leading] < threshold:
        leading += 1
    trailing = 0
    while trailing < n and abs_signal[n - 1 - trailing] < threshold:
        trailing += 1

    if leading + trailing >= n:  # 전부 무음이면 그대로 둔다
        return wav

    start = max(0, leading - keep_edge_samples)
    end = n - max(0, trailing - keep_edge_samples)
    return wav[:, start:end]


def _pad_edges(wav, sample_rate: int, padding_ms: int):
    import numpy as np

    padding_samples = int(sample_rate * padding_ms / 1000)
    if padding_samples <= 0:
        return wav
    padding = np.zeros((wav.shape[0], padding_samples), dtype=wav.dtype)
    return np.concatenate([padding, wav, padding], axis=1)


# ── 서비스 ──────────────────────────────────────────────────────────────────────
class QwenTTSService:
    """Qwen3-TTS 모델 한 벌을 로드해 합성 요청을 처리한다.

    ``_lock`` 은 모델 로드와 추론을 함께 보호한다 — 전역 RNG(torch/numpy)를 여러 스레드가
    동시에 건드리면 seed 를 준 요청의 재현성이 깨지기 때문이다.
    """

    def __init__(self, model_id: str = MODEL_ID) -> None:
        self.model_name = model_id
        self._lock = threading.Lock()
        self._model = None
        self._styles: dict[str, StyleRef] = {}
        # 정확한 prompt 캐시 키: (speakerName, referenceDigest, model id, model revision).
        self._prompts: dict[tuple[str, str, str, str], ClonePrompt] = {}
        self._warned_ignored_params = False

    # ── 수명주기 ──
    def load(self):
        if self._model is not None:
            return self._model

        with self._lock:
            if self._model is not None:
                return self._model

            try:
                import torch
                from qwen_tts import Qwen3TTSModel
            except ModuleNotFoundError as exc:
                raise QueueTtsError(
                    "The 'qwen-tts' package (and torch) is not installed in this environment. "
                    "Run '.\\.venv\\Scripts\\python -m pip install -r requirements.txt'."
                ) from exc

            dtype = getattr(torch, DTYPE, None)
            if dtype is None:
                raise QueueTtsError(f"Unsupported QWEN_DTYPE '{DTYPE}'.")

            started = time.perf_counter()
            self._model = Qwen3TTSModel.from_pretrained(
                self.model_name, device_map=DEVICE_MAP, dtype=dtype
            )
            log.info(
                "qwen model loaded: id=%s device=%s dtype=%s in %.1fs",
                self.model_name, DEVICE_MAP, DTYPE, time.perf_counter() - started,
            )
            self._check_capability()
            self.reload_style_cache()
            return self._model

    def _check_capability(self) -> None:
        """로드된 체크포인트가 클로닝을 할 수 있는지 기동 시점에 확인한다.

        CustomVoice/VoiceDesign 체크포인트도 로드 자체는 되므로, 확인하지 않으면 **첫 합성
        요청**에서야 `does not support generate_voice_clone` 로 터진다. 그때는 이미 job 이
        실패한 뒤다.

        타입을 읽을 수 없으면(스텁 모델 등) 통과시킨다 — 진짜 모델이 아니면 판단할 근거가 없다.
        """
        model_type = getattr(getattr(self._model, "model", None), "tts_model_type", None)
        if model_type is None:
            return
        if model_type != REQUIRED_MODEL_TYPE:
            raise QueueTtsError(
                f"'{self.model_name}' is a '{model_type}' checkpoint, but this worker clones voices "
                f"and needs a '{REQUIRED_MODEL_TYPE}' one (e.g. Qwen/Qwen3-TTS-12Hz-1.7B-Base). "
                "Only the Base checkpoint ships the speaker encoder."
            )

    @property
    def model_revision(self) -> str:
        model = self._model
        config = getattr(model, "config", None)
        if config is None:
            config = getattr(getattr(model, "model", None), "config", None)
        return str(getattr(config, "_commit_hash", None) or "unknown")

    def reload_style_cache(self) -> None:
        self._styles = voice_store.scan(default_style_cache_dir())
        self._prompts = {}

    def reload_custom_voices(self) -> None:
        """Compatibility alias used by older local callers."""
        self.reload_style_cache()

    # ── voice ──
    def style_infos(self) -> list[StyleInfo]:
        """이 worker 가 현재 캐시하고 있는 clone style 목록(진단용)."""
        return [
            StyleInfo(name=speaker_name, kind="cached", path=str(ref.source) if ref.source else None)
            for speaker_name, ref in sorted(self._styles.items())
        ]

    def voices(self) -> list[str]:
        return [style.name for style in self.style_infos()]

    def custom_voices(self) -> list[str]:
        return sorted(self._styles)

    def _resolve_style(self, params: TTSParams) -> StyleRef:
        speaker_name = voice_store.clean_speaker_name(params.speaker_name)
        digest = (params.reference_digest or "").strip()
        if not digest:
            raise QueueTtsError("Qwen payload is missing referenceDigest")
        cached = self._styles.get(speaker_name)
        if cached is not None and cached.reference_digest == digest:
            return cached
        return StyleRef(
            speaker_name=speaker_name,
            reference_digest=digest,
            mode=clone_prompt.clean_mode(params.speaker_mode),
            ref_text=params.speaker_ref_text,
            prompt_path=default_style_cache_dir() / f"{voice_store.cache_stem(speaker_name)}{clone_prompt.PROMPT_SUFFIX}",
            source=default_style_cache_dir() / f"{voice_store.cache_stem(speaker_name)}.json",
        )

    # ── 클론 prompt ──
    def _cache_key(self, ref: StyleRef) -> tuple[str, str, str, str]:
        return ref.speaker_name, ref.reference_digest, self.model_name, self.model_revision

    def _load_prompt(self, ref: StyleRef) -> ClonePrompt | None:
        key = self._cache_key(ref)
        cached = self._prompts.get(key)
        if cached is not None:
            return cached
        if ref.prompt_path is None:
            return None
        cached = clone_prompt.load(
            ref.prompt_path,
            model_id=self.model_name,
            model_revision=self.model_revision,
            reference_digest=ref.reference_digest,
        )
        if cached is not None:
            self._prompts[key] = cached
        return cached

    def _build_prompt(self, ref: StyleRef, audio: bytes, audio_format: str) -> ClonePrompt:
        model = self._model or self.load()
        suffix = f".{audio_format.lstrip('.')}"
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp:
                temp.write(audio)
                temp_path = Path(temp.name)
            started = time.perf_counter()
            prompt = clone_prompt.build(model, ref_audio=temp_path, ref_text=ref.ref_text, mode=ref.mode)
            log.info("clone prompt built for '%s' (mode=%s) in %.2fs", ref.speaker_name, ref.mode, time.perf_counter() - started)
            if ref.prompt_path is not None:
                ref.prompt_path.parent.mkdir(parents=True, exist_ok=True)
                clone_prompt.save(
                    ref.prompt_path,
                    prompt,
                    model_id=self.model_name,
                    model_revision=self.model_revision,
                    reference_digest=ref.reference_digest,
                )
            self._prompts[self._cache_key(ref)] = prompt
            return prompt
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def forget_style(self, speaker_name: str) -> bool:
        speaker_name = voice_store.clean_speaker_name(speaker_name)
        with self._lock:
            removed = voice_store.forget(default_style_cache_dir(), speaker_name)
            self._styles.pop(speaker_name, None)
            for key in [key for key in self._prompts if key[0] == speaker_name]:
                self._prompts.pop(key, None)
        return removed

    def _prompt_for(self, ref: StyleRef, blob_key: str, blob_loader: Callable[[str], bytes] | None) -> ClonePrompt:
        cached = self._load_prompt(ref)
        if cached is not None:
            return cached
        if blob_loader is None or not blob_key:
            raise QueueTtsError(f"prompt cache miss for {ref.speaker_name} and no style blob loader/key was supplied")
        audio = blob_loader(blob_key)
        if not audio:
            raise QueueTtsError(f"reference audio blob is missing: {blob_key}")
        audio_format = voice_store.audio_format(audio)
        prompt = self._build_prompt(ref, audio, audio_format)
        stored = voice_store.save_ref(default_style_cache_dir(), ref)
        self._styles[stored.speaker_name] = stored
        return prompt

    # ── 합성 ──
    def synthesize(
        self,
        params: TTSParams,
        *,
        blob_loader: Callable[[str], bytes] | None = None,
    ) -> AudioResult:
        if not params.text.strip():
            raise QueueTtsError("Text is empty.")

        t_total = time.perf_counter()
        from common.queuetts_audio import coerce_response_format, duration_seconds, encode_audio, format_to_mime

        fmt = coerce_response_format(clean_format(params.response_format))
        seed = clean_seed(params.seed)
        max_chunk_length = clean_max_chunk_length(params.max_chunk_length)
        self._warn_ignored_params(params)

        t0 = time.perf_counter()
        self.load()
        t_load = time.perf_counter() - t0

        ref = self._resolve_style(params)
        language = clean_language(params.lang, fallback=ref.language)

        t0 = time.perf_counter()
        prepared = prepare_text(params.text)
        t_prep = time.perf_counter() - t0

        t0 = time.perf_counter()
        text_chunks = chunk_text(prepared, max_chunk_length)
        t_chunk = time.perf_counter() - t0
        if not text_chunks:
            raise QueueTtsError("Text produced no chunks.")

        with self._lock:
            prompt = self._prompt_for(ref, params.speaker_blob_key, blob_loader)
            self._apply_seed(seed)
            t0 = time.perf_counter()
            wav, sample_rate = self._render(text_chunks, prompt, language, params)
            t_synth = time.perf_counter() - t0
            t0 = time.perf_counter()
            content = encode_audio(wav, sample_rate, fmt)
            t_encode = time.perf_counter() - t0

        log.info(
            "[queuetts.timing] engine=qwen total=%.3fs | load=%.3f prep=%.3f chunk=%.3f synth=%.3f encode=%.3f "
            "| voice=%s lang=%s in_chars=%d prepared_chars=%d chunks=%d max_chunk_len=%d sr=%d",
            time.perf_counter() - t_total, t_load, t_prep, t_chunk, t_synth, t_encode,
            ref.speaker_name, language, len(params.text), len(prepared), len(text_chunks),
            max_chunk_length, sample_rate,
        )

        return AudioResult(
            content=content,
            response_format=fmt,
            media_type=format_to_mime(fmt),
            sample_rate=int(sample_rate),
            duration_s=duration_seconds(wav, sample_rate),
            processed_text=prepared,
        )

    def _warn_ignored_params(self, params: TTSParams) -> None:
        """speed/steps 는 Qwen 에 대응이 없다. 조용히 무시하면 디버깅이 어려우니 한 번은 알린다."""
        if self._warned_ignored_params:
            return
        ignored = [
            name for name, value in (("speed", params.speed), ("steps", params.steps))
            if value is not None
        ]
        if ignored:
            log.warning(
                "payload carries %s, which Qwen3-TTS has no equivalent for — ignoring "
                "(logged once per process)", " and ".join(ignored),
            )
            self._warned_ignored_params = True

    @staticmethod
    def _apply_seed(seed: Optional[int]) -> None:
        if seed is None:
            return
        import numpy as np
        import torch

        np.random.seed(seed)
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)

    def _render(self, text_chunks, prompt, language, params):
        """청크를 모두 합성한 뒤 sample rate 를 확정하고, 다듬어 이어붙인다.

        Qwen 은 sample rate 를 **호출 결과로** 돌려주므로(모델 속성이 아니라), 트리밍 샘플 수와
        청크 간 무음 길이는 첫 합성 뒤에야 계산할 수 있다. 청크마다 sr 이 다르면 이어붙일 때
        피치가 틀어지므로 즉시 실패시킨다.
        """
        import numpy as np

        rendered: list[Any] = []
        sample_rate: Optional[int] = None
        infer_total = 0.0

        for chunk in text_chunks:
            t0 = time.perf_counter()
            wav, chunk_sr = self._generate(chunk, prompt, language, params.generate_kwargs())
            infer_total += time.perf_counter() - t0

            chunk_sr = int(chunk_sr)
            if sample_rate is None:
                sample_rate = chunk_sr
            elif chunk_sr != sample_rate:
                raise QueueTtsError(
                    f"model returned inconsistent sample rates ({sample_rate} then {chunk_sr}); "
                    "cannot concatenate chunks safely"
                )
            rendered.append(_as_2d_mono(wav))

        assert sample_rate is not None  # text_chunks 가 비어 있지 않음은 호출부에서 보장

        t0 = time.perf_counter()
        keep_edge_samples = int(sample_rate * EDGE_TRIM_KEEP_MS / 1000)
        gap_samples = max(int(params.silence_duration * sample_rate), 0)
        gap = np.zeros((1, gap_samples), dtype=np.float32) if gap_samples > 0 else None

        pieces: list[Any] = []
        for index, piece in enumerate(rendered):
            if EDGE_TRIM_ENABLED:
                piece = _trim_edges(piece, EDGE_TRIM_THRESHOLD, keep_edge_samples)
            pieces.append(piece)
            if gap is not None and index < len(rendered) - 1:
                pieces.append(gap)

        result = _pad_edges(np.concatenate(pieces, axis=1), sample_rate, OUTPUT_EDGE_PADDING_MS)
        log.info(
            "[queuetts.timing.synth] infer=%.3fs trim+concat=%.3fs n_chunks=%d",
            infer_total, time.perf_counter() - t0, len(text_chunks),
        )
        return result, sample_rate

    def _generate(self, text: str, prompt: ClonePrompt, language: str, generate_kwargs: dict[str, Any]):
        """미리 만들어 둔 prompt 로 합성한다.

        ``ref_audio``/``ref_text`` 를 매번 넘기는 대신 ``voice_clone_prompt`` 를 준다 — 그래야
        오디오 디코드·speech tokenizer encode·화자 임베딩 추출을 청크마다 다시 하지 않는다.
        """
        return self._model.generate_voice_clone(
            text=text,
            language=language,
            voice_clone_prompt=[prompt.as_model_item()],
            **generate_kwargs,
        )


LOCAL_SERVICE = QwenTTSService()
