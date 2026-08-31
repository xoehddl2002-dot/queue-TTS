"""On-disk storage for processor-managed custom voice styles."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Iterable

from supertonic.config import DEFAULT_MODEL
from supertonic.utils import validate_voice_style_format

logger = logging.getLogger(__name__)
_NAME_RE = re.compile(r"[A-Za-z0-9_\-]{1,64}")


class InvalidStyleName(ValueError):
    pass


class StyleNameConflict(ValueError):
    pass


def default_custom_styles_dir(_model: str = DEFAULT_MODEL) -> Path:
    """항상 model 프로젝트 내부의 custom_styles 디렉터리를 사용한다."""
    return Path(__file__).resolve().parent / "custom_styles"


def sanitize_name(name: str) -> str:
    normalized = (name or "").strip()
    if not _NAME_RE.fullmatch(normalized):
        raise InvalidStyleName(
            f"Invalid style name {normalized!r}: must match [A-Za-z0-9_-]{{1,64}}"
        )
    return normalized


def scan(directory: Path) -> dict[str, Path]:
    styles: dict[str, Path] = {}
    if not directory.exists():
        return styles

    for path in sorted(directory.glob("*.json")):
        try:
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            if not validate_voice_style_format(payload):
                logger.warning("Skipping invalid voice style file: %s", path)
                continue
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Skipping unreadable voice style file %s: %s", path, exc)
            continue
        styles[path.stem] = path
    return styles


def save(
    directory: Path,
    name: str,
    payload: dict,
    *,
    builtin_names: Iterable[str] = (),
    overwrite: bool = False,
) -> Path:
    normalized = sanitize_name(name)
    if normalized in set(builtin_names):
        raise StyleNameConflict(
            f"Name {normalized!r} is a built-in voice and cannot be overwritten"
        )
    if not validate_voice_style_format(payload):
        raise ValueError("voice style JSON is missing required keys/fields")

    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{normalized}.json"
    if target.exists() and not overwrite:
        raise StyleNameConflict(f"Style {normalized!r} already exists")

    temporary = target.with_suffix(".json.tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(payload, file)
    temporary.replace(target)
    return target
