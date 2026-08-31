"""Gateway-owned Qwen style prompt cache.

The Gateway owns reference audio and metadata. A worker stores only derived prompt tensors plus enough metadata
to diagnose and reuse them after restart. Losing this directory is safe: a TTS job can read raw audio from the
Redis blob key supplied by the Gateway and rebuild the cache.
"""
from __future__ import annotations

import io
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from clone_prompt import MODE_ICL, MODE_X_VECTOR, PROMPT_SUFFIX, PromptError, clean_mode

logger = logging.getLogger(__name__)

MAX_SPEAKER_NAME_LENGTH = 64
AUDIO_SUFFIXES = {".wav", ".flac", ".ogg", ".mp3"}
_FORMAT_SUFFIX = {
    "WAV": ".wav",
    "WAVEX": ".wav",
    "FLAC": ".flac",
    "OGG": ".ogg",
    "MPEG": ".mp3",
}


class StyleCacheError(RuntimeError):
    pass


@dataclass(frozen=True)
class StyleRef:
    speaker_name: str
    reference_digest: str
    mode: str
    ref_text: str | None = None
    language: str | None = None
    prompt_path: Path | None = None
    source: Path | None = None


def default_style_cache_dir() -> Path:
    # Keep the existing mounted directory path; only its ownership and contents changed.
    return Path(__file__).resolve().parent / "custom_voices"


def default_custom_voices_dir() -> Path:
    """Compatibility alias for existing deployment configuration."""
    return default_style_cache_dir()


def clean_speaker_name(speaker_name: str) -> str:
    value = (speaker_name or "").strip()
    if not value or len(value) > MAX_SPEAKER_NAME_LENGTH:
        raise StyleCacheError(f"speakerName must be 1..{MAX_SPEAKER_NAME_LENGTH} characters")
    if any(char in "/\\" or ord(char) < 32 or ord(char) == 127 for char in value):
        raise StyleCacheError("speakerName contains a path separator or control character")
    return value


def cache_stem(speaker_name: str) -> str:
    """Map a logical name key to a safe, stable local filename."""
    name = clean_speaker_name(speaker_name)
    return "speaker_" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:24]


def scan(directory: Path) -> dict[str, StyleRef]:
    refs: dict[str, StyleRef] = {}
    if not directory.is_dir():
        return refs
    for path in sorted(directory.glob("speaker_*.json")):
        ref = _load_ref(path)
        if ref is not None:
            refs[ref.speaker_name] = ref
    return refs


def _load_ref(path: Path) -> StyleRef | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise StyleCacheError("top level must be an object")
        speaker_name = clean_speaker_name(str(payload.get("speaker_name") or ""))
        digest = str(payload.get("reference_digest") or "").strip()
        if not digest:
            raise StyleCacheError("reference_digest is required")
        mode = clean_mode(payload.get("mode"))
        ref_text = str(payload["ref_text"]).strip() if payload.get("ref_text") else None
        if mode == MODE_ICL and not ref_text:
            raise StyleCacheError("ref_text is required for mode 'icl'")
        prompt_path = path.with_suffix(PROMPT_SUFFIX)
        if not prompt_path.is_file():
            raise StyleCacheError("prompt cache is missing")
        return StyleRef(
            speaker_name=speaker_name,
            reference_digest=digest,
            mode=mode,
            ref_text=None if mode == MODE_X_VECTOR else ref_text,
            language=str(payload["language"]) if payload.get("language") else None,
            prompt_path=prompt_path,
            source=path,
        )
    except (OSError, json.JSONDecodeError, PromptError, StyleCacheError) as exc:
        logger.warning("Skipping unreadable style cache %s: %s", path, exc)
        return None


def save_ref(directory: Path, ref: StyleRef) -> StyleRef:
    directory.mkdir(parents=True, exist_ok=True)
    speaker_name = clean_speaker_name(ref.speaker_name)
    mode = clean_mode(ref.mode)
    ref_text = (ref.ref_text or "").strip() or None
    if mode == MODE_ICL and not ref_text:
        raise StyleCacheError("ref_text is required for mode 'icl'")
    if mode == MODE_X_VECTOR:
        ref_text = None
    stem = cache_stem(speaker_name)
    meta_path = directory / f"{stem}.json"
    stored = StyleRef(
        speaker_name=speaker_name,
        reference_digest=ref.reference_digest,
        mode=mode,
        ref_text=ref_text,
        language=ref.language,
        prompt_path=directory / f"{stem}{PROMPT_SUFFIX}",
        source=meta_path,
    )
    payload = {
        "speaker_name": stored.speaker_name,
        "reference_digest": stored.reference_digest,
        "mode": stored.mode,
        "ref_text": stored.ref_text,
        "language": stored.language,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    temp = meta_path.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps({k: v for k, v in payload.items() if v is not None}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp.replace(meta_path)
    return stored


def forget(directory: Path, speaker_name: str) -> bool:
    stem = cache_stem(speaker_name)
    removed = False
    for suffix in (".json", PROMPT_SUFFIX):
        path = directory / f"{stem}{suffix}"
        if path.is_file():
            path.unlink()
            removed = True
    return removed


def snapshot(directory: Path) -> tuple[tuple[str, int, int], ...]:
    if not directory.is_dir():
        return ()
    entries: list[tuple[str, int, int]] = []
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.suffix.lower() not in {".json", PROMPT_SUFFIX}:
            continue
        stat = path.stat()
        entries.append((path.name, stat.st_mtime_ns, stat.st_size))
    return tuple(entries)


def audio_format(content: bytes) -> str:
    """Read only the format needed to build a prompt; registry policy is enforced by the Gateway."""
    import soundfile as sf

    try:
        info = sf.info(io.BytesIO(content))
    except Exception as exc:  # noqa: BLE001
        raise StyleCacheError(f"reference audio could not be decoded: {exc}") from exc
    suffix = _FORMAT_SUFFIX.get(str(info.format).upper())
    if suffix is None:
        raise StyleCacheError(
            f"Unsupported reference audio format '{info.format}'. "
            f"Use one of: {', '.join(sorted(AUDIO_SUFFIXES))}"
        )
    return suffix.removeprefix(".")
