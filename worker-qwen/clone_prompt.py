"""보이스 클론 prompt 캐시.

``Qwen3TTSModel.create_voice_clone_prompt()`` 이 만든 참조 표현(참조 speech code + 화자 임베딩)을
디스크에 저장해 두고 합성 때 재사용한다. 캐시가 있으면 요청 경로에서 **오디오 디코드 →
speech tokenizer encode → 화자 임베딩 추출**이 사라진다.

ICL 모드에서 참조 코드를 생성 앞에 붙이는 비용은 **캐시해도 남는다** — 그건 저장할 수 있는
전처리가 아니라 매 생성마다 모델이 다시 하는 일이다. 그래서 참조 음성은 짧을수록 좋다.

저장물은 텐서 둘과 스칼라뿐이라 ``torch.load(..., weights_only=True)`` 로 읽는다. 등록 API 로
들어온 파일을 읽는 경로이므로 이 플래그는 켠 채로 둔다.

캐시는 **style 참조와 정확한 모델 snapshot 양쪽에 종속**된다. ``reference_digest`` 는 오디오,
mode, ref_text를 묶고, ``model_revision`` 은 Hugging Face snapshot commit을 담는다. repo id가
같아도 revision이 바뀌면 캐시를 버리고 다시 만든다.

텐서는 **CPU 로 내려 저장**한다. 모델이 받을 때 알아서 옮기므로(``ref_spk_embedding`` 은
``.to(talker.device).to(talker.dtype)``, ``ref_code`` 는 ``.to(talker.device)``) device 나 dtype 을
파일에 굳혀 둘 이유가 없다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("queuetts.qwen.prompt")

FORMAT_VERSION = 2

MODE_ICL = "icl"
MODE_X_VECTOR = "x_vector"
MODES = (MODE_ICL, MODE_X_VECTOR)

PROMPT_SUFFIX = ".pt"


class PromptError(RuntimeError):
    pass


def clean_mode(mode: str | None) -> str:
    value = (mode or MODE_ICL).strip().lower()
    if value not in MODES:
        raise PromptError(f"Unsupported clone mode '{mode}'. Use one of: {', '.join(MODES)}")
    return value


@dataclass(frozen=True)
class ClonePrompt:
    """한 style 의 클론 prompt.

    필드 이름은 ``qwen_tts.VoiceClonePromptItem`` 과 맞춰 둔다 — 모델에 넘길 때 그 클래스로
    바꿔 주기 때문이다(:meth:`as_model_item`).
    """

    ref_code: Optional[Any]          # (T,) 또는 (T, Q) 텐서. x_vector 모드면 None
    ref_spk_embedding: Any           # (D,) 텐서
    x_vector_only_mode: bool
    icl_mode: bool
    ref_text: Optional[str] = None

    @property
    def mode(self) -> str:
        return MODE_X_VECTOR if self.x_vector_only_mode else MODE_ICL

    def as_model_item(self):
        """모델이 받는 ``VoiceClonePromptItem`` 으로 바꾼다.

        ``qwen_tts`` 를 import 할 수 없으면(스텁 테스트) 자기 자신을 돌려준다 — 모델 쪽은
        속성만 읽으므로 그대로 동작한다.
        """
        try:
            from qwen_tts import VoiceClonePromptItem
        except Exception:  # noqa: BLE001 — 스텁 환경
            return self
        return VoiceClonePromptItem(
            ref_code=self.ref_code,
            ref_spk_embedding=self.ref_spk_embedding,
            x_vector_only_mode=self.x_vector_only_mode,
            icl_mode=self.icl_mode,
            ref_text=self.ref_text,
        )


def _to_cpu(tensor):
    if tensor is None:
        return None
    detach = getattr(tensor, "detach", None)
    if detach is not None:
        tensor = detach()
    to = getattr(tensor, "to", None)
    return to("cpu") if to is not None else tensor


def build(model, *, ref_audio: Path | str, ref_text: str | None, mode: str) -> ClonePrompt:
    """참조 음성에서 prompt 를 만든다. 모델(Base 체크포인트)이 로드돼 있어야 한다.

    ``mode="icl"`` 은 ``ref_text`` 가 필수다 — 모델이 참조 텍스트와 참조 speech code 를 함께
    조건으로 쓴다. ``mode="x_vector"`` 는 화자 임베딩만 쓰므로 ``ref_text`` 를 무시한다.
    """
    mode = clean_mode(mode)
    x_vector_only = mode == MODE_X_VECTOR
    if not x_vector_only and not (ref_text or "").strip():
        raise PromptError("ref_text is required for mode 'icl'. Use mode 'x_vector' to clone without a transcript.")

    items = model.create_voice_clone_prompt(
        ref_audio=str(ref_audio),
        ref_text=None if x_vector_only else ref_text,
        x_vector_only_mode=x_vector_only,
    )
    if not items:
        raise PromptError("create_voice_clone_prompt returned nothing")

    item = items[0]
    return ClonePrompt(
        ref_code=_to_cpu(getattr(item, "ref_code", None)),
        ref_spk_embedding=_to_cpu(getattr(item, "ref_spk_embedding", None)),
        x_vector_only_mode=bool(getattr(item, "x_vector_only_mode", x_vector_only)),
        icl_mode=bool(getattr(item, "icl_mode", not x_vector_only)),
        ref_text=getattr(item, "ref_text", None if x_vector_only else ref_text),
    )


def save(
    path: Path,
    prompt: ClonePrompt,
    *,
    model_id: str,
    model_revision: str,
    reference_digest: str,
) -> None:
    import torch

    payload = {
        "format": FORMAT_VERSION,
        "model_id": model_id,
        "model_revision": model_revision,
        "reference_digest": reference_digest,
        "ref_code": prompt.ref_code,
        "ref_spk_embedding": prompt.ref_spk_embedding,
        "x_vector_only_mode": prompt.x_vector_only_mode,
        "icl_mode": prompt.icl_mode,
        "ref_text": prompt.ref_text,
    }
    # 같은 파일을 읽는 중인 워커가 반쪽짜리를 보지 않도록 임시 파일에 쓰고 바꿔 끼운다.
    temp = path.with_suffix(path.suffix + ".tmp")
    torch.save(payload, temp)
    temp.replace(path)


def load(
    path: Path,
    *,
    model_id: str,
    model_revision: str,
    reference_digest: str,
) -> ClonePrompt | None:
    """캐시를 읽는다. 없거나·형식이 다르거나·모델/오디오가 바뀌었으면 ``None``.

    ``None`` 은 오류가 아니라 "다시 만들라"는 뜻이다. 호출부가 :func:`build` 로 되살린다.
    """
    if not path.is_file():
        return None

    import torch

    try:
        payload = torch.load(path, map_location="cpu", weights_only=True)
    except Exception as exc:  # noqa: BLE001 — 손상/구버전 캐시는 버리고 다시 만든다
        log.warning("discarding unreadable prompt cache %s: %s", path, exc)
        return None

    if not isinstance(payload, dict) or payload.get("format") != FORMAT_VERSION:
        log.warning("discarding prompt cache %s: unexpected format", path)
        return None
    if payload.get("model_id") != model_id:
        log.info(
            "discarding prompt cache %s: built for model %s, running %s",
            path, payload.get("model_id"), model_id,
        )
        return None
    if payload.get("model_revision") != model_revision:
        log.info(
            "discarding prompt cache %s: built for revision %s, running %s",
            path, payload.get("model_revision"), model_revision,
        )
        return None
    if payload.get("reference_digest") != reference_digest:
        log.info("discarding prompt cache %s: style reference changed", path)
        return None

    embedding = payload.get("ref_spk_embedding")
    if embedding is None:
        log.warning("discarding prompt cache %s: no speaker embedding", path)
        return None

    return ClonePrompt(
        ref_code=payload.get("ref_code"),
        ref_spk_embedding=embedding,
        x_vector_only_mode=bool(payload.get("x_vector_only_mode")),
        icl_mode=bool(payload.get("icl_mode")),
        ref_text=payload.get("ref_text"),
    )
