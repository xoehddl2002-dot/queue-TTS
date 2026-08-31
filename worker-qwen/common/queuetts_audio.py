"""Audio encoding helpers used by the Redis TTS processor."""

from __future__ import annotations

import io
from typing import Optional

import numpy as np
import soundfile as sf

_FORMATS = {
    "wav": ("WAV", "PCM_16", "audio/wav"),
    "flac": ("FLAC", "PCM_16", "audio/flac"),
    "ogg": ("OGG", "VORBIS", "audio/ogg"),
}


class UnsupportedAudioFormat(ValueError):
    pass


def format_to_mime(fmt: str) -> str:
    try:
        return _FORMATS[fmt][2]
    except KeyError as exc:
        raise UnsupportedAudioFormat(fmt) from exc


def encode_audio(wav: np.ndarray, sample_rate: int, fmt: str) -> bytes:
    try:
        sf_format, subtype, _ = _FORMATS[fmt]
    except KeyError as exc:
        raise UnsupportedAudioFormat(fmt) from exc

    if wav.ndim == 2:
        wav = wav.squeeze(0)

    buffer = io.BytesIO()
    sf.write(buffer, wav, sample_rate, format=sf_format, subtype=subtype)
    return buffer.getvalue()


def duration_seconds(wav: np.ndarray, sample_rate: int) -> float:
    return float(wav.shape[-1]) / float(sample_rate)


def coerce_response_format(value: Optional[str]) -> str:
    normalized = (value or "wav").lower().strip()
    if normalized not in _FORMATS:
        raise UnsupportedAudioFormat(value)
    return normalized
