"""Stub-only tests for Gateway-owned Qwen style caching and synthesis."""
from __future__ import annotations

import io
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import numpy as np
import soundfile as sf
import torch

import clone_prompt
import qwen_backend
from qwen_backend import LOCAL_SERVICE, TTSParams

SPEAKER_NAME = "나인애"
FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    print(f"  {'PASS' if condition else 'FAIL'} {label}{': ' + detail if detail else ''}")
    if not condition:
        FAILURES.append(label)


class StubModel:
    def __init__(self, revision: str = "rev-1") -> None:
        self.config = SimpleNamespace(_commit_hash=revision)
        self.prompt_calls: list[dict] = []
        self.calls: list[dict] = []

    def create_voice_clone_prompt(self, ref_audio, ref_text=None, x_vector_only_mode=False):
        self.prompt_calls.append({
            "ref_audio": ref_audio,
            "ref_text": ref_text,
            "x_vector_only_mode": x_vector_only_mode,
        })
        return [SimpleNamespace(
            ref_code=None if x_vector_only_mode else torch.arange(4, dtype=torch.int64),
            ref_spk_embedding=torch.ones(8, dtype=torch.float32),
            x_vector_only_mode=x_vector_only_mode,
            icl_mode=not x_vector_only_mode,
            ref_text=ref_text,
        )]

    def generate_voice_clone(self, text, language, voice_clone_prompt, **kwargs):
        self.calls.append({
            "text": text,
            "language": language,
            "prompt": voice_clone_prompt[0],
            "kwargs": kwargs,
        })
        return np.zeros(max(1200, len(text) * 200), dtype=np.float32), 24000


def wav_bytes(seconds: float = 3.0, sample_rate: int = 24000, freq: float = 220.0) -> bytes:
    timeline = np.linspace(0, seconds, int(sample_rate * seconds), endpoint=False, dtype=np.float32)
    wave = (0.2 * np.sin(2 * np.pi * freq * timeline)).astype(np.float32)
    buffer = io.BytesIO()
    sf.write(buffer, wave, sample_rate, format="WAV", subtype="PCM_16")
    return buffer.getvalue()


@contextmanager
def cache_dir():
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp)
        original = qwen_backend.default_style_cache_dir
        qwen_backend.default_style_cache_dir = lambda: directory
        try:
            LOCAL_SERVICE._styles = {}
            LOCAL_SERVICE._prompts = {}
            yield directory
        finally:
            qwen_backend.default_style_cache_dir = original
            LOCAL_SERVICE._styles = {}
            LOCAL_SERVICE._prompts = {}


def install(model: StubModel) -> StubModel:
    LOCAL_SERVICE._model = model
    LOCAL_SERVICE.model_name = "Qwen/Test-Base"
    LOCAL_SERVICE._styles = {}
    LOCAL_SERVICE._prompts = {}
    return model


def params(**overrides) -> TTSParams:
    values = dict(
        text="테스트 문장입니다.",
        voice=SPEAKER_NAME,
        speaker_name=SPEAKER_NAME,
        reference_digest="digest-1",
        speaker_blob_key="qwen:style:blob:" + SPEAKER_NAME,
        speaker_mode="icl",
        speaker_ref_text="참조 발화",
        lang="ko",
        seed=-1,
    )
    values.update(overrides)
    return TTSParams(**values)


def test_lazy_prompt_and_synthesize() -> None:
    print("\n[1] lazy prompt 생성과 합성")
    with cache_dir() as directory:
        model = install(StubModel())
        loads: list[str] = []
        result = LOCAL_SERVICE.synthesize(
            params(),
            blob_loader=lambda key: loads.append(key) or wav_bytes(),
        )
        check("첫 합성에서 blob GET", loads == ["qwen:style:blob:" + SPEAKER_NAME])
        check("첫 합성에서 prompt 생성", len(model.prompt_calls) == 1)
        check("원본 오디오를 로컬에 보관하지 않음", not list(directory.glob("*.wav")))
        check("파생 캐시 저장", bool(list(directory.glob("*.pt"))) and bool(list(directory.glob("*.json"))))
        check("합성 성공", result.content and model.calls[-1]["language"] == "Korean")
        check("speakerName 캐시 사용", SPEAKER_NAME in LOCAL_SERVICE.custom_voices())


def test_read_through_and_cache_keys() -> None:
    print("\n[2] read-through와 캐시 무효화")
    with cache_dir():
        model = install(StubModel())
        loads: list[str] = []

        def loader(key: str) -> bytes:
            loads.append(key)
            return wav_bytes(freq=330.0)

        LOCAL_SERVICE.synthesize(params(), blob_loader=loader)
        check("캐시 미스에서 blob GET", loads == ["qwen:style:blob:" + SPEAKER_NAME])
        check("prompt 1회 생성", len(model.prompt_calls) == 1)

        LOCAL_SERVICE._prompts = {}
        LOCAL_SERVICE.synthesize(params(), blob_loader=loader)
        check("디스크 캐시 재사용", len(model.prompt_calls) == 1 and len(loads) == 1)

        LOCAL_SERVICE._styles = {}
        LOCAL_SERVICE._prompts = {}
        LOCAL_SERVICE.synthesize(params(reference_digest="digest-2"), blob_loader=loader)
        check("digest 변경 시 재생성", len(model.prompt_calls) == 2 and len(loads) == 2)

        LOCAL_SERVICE._styles = {}
        LOCAL_SERVICE._prompts = {}
        model.config._commit_hash = "rev-2"
        LOCAL_SERVICE.synthesize(params(reference_digest="digest-2"), blob_loader=loader)
        check("model revision 변경 시 재생성", len(model.prompt_calls) == 3 and len(loads) == 3)


def test_generate_params_and_xvector() -> None:
    print("\n[3] 생성 파라미터와 x_vector")
    with cache_dir():
        model = install(StubModel())
        LOCAL_SERVICE.synthesize(
            params(
                speaker_mode="x_vector",
                speaker_ref_text=None,
                temperature=0.7,
                top_k=42,
                do_sample=False,
            ),
            blob_loader=lambda _key: wav_bytes(),
        )
        kwargs = model.calls[-1]["kwargs"]
        check("허용 생성 파라미터 전달", kwargs == {"do_sample": False, "temperature": 0.7, "top_k": 42}, str(kwargs))
        check("미지정 키 제거", "top_p" not in kwargs)
        check("x_vector prompt", model.prompt_calls[-1]["x_vector_only_mode"] is True)


def test_prompt_roundtrip() -> None:
    print("\n[4] prompt 직렬화 키")
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "prompt.pt"
        prompt = clone_prompt.ClonePrompt(
            ref_code=torch.arange(4),
            ref_spk_embedding=torch.ones(8),
            x_vector_only_mode=False,
            icl_mode=True,
            ref_text="참조",
        )
        clone_prompt.save(
            path,
            prompt,
            model_id="m1",
            model_revision="r1",
            reference_digest="d1",
        )
        loaded = clone_prompt.load(path, model_id="m1", model_revision="r1", reference_digest="d1")
        check("정확한 키로 로드", loaded is not None)
        check("revision 불일치 거절", clone_prompt.load(path, model_id="m1", model_revision="r2", reference_digest="d1") is None)
        check("digest 불일치 거절", clone_prompt.load(path, model_id="m1", model_revision="r1", reference_digest="d2") is None)


def test_chunking() -> None:
    """청크 하나가 생성 호출 하나다. 문장을 낱개로 보내면 문장마다 음색이 흔들린다."""
    print("\n[5] 청크 묶기")
    text = "안녕하세요. Qwen 클론 화자 테스트입니다. 오늘 날씨가 참 좋네요!"

    check("상한 안의 문장들은 한 청크로", qwen_backend.chunk_text(text, 400) == [text])

    tight = qwen_backend.chunk_text(text, 20)
    check("상한을 넘으면 나뉨", len(tight) > 1 and all(len(chunk) <= 20 for chunk in tight), str(tight))

    long_sentence = "가나다라마바사아자차카타파하 " * 8 + "끝."
    split = qwen_backend.chunk_text(long_sentence, 60)
    check("문장 하나가 길어도 상한 준수", all(len(chunk) <= 60 for chunk in split), str([len(c) for c in split]))

    with cache_dir():
        model = install(StubModel())
        LOCAL_SERVICE.synthesize(params(text=text), blob_loader=lambda _key: wav_bytes())
        check("세 문장이 생성 호출 1회", len(model.calls) == 1, f"{len(model.calls)}회")


def main() -> None:
    test_lazy_prompt_and_synthesize()
    test_read_through_and_cache_keys()
    test_generate_params_and_xvector()
    test_prompt_roundtrip()
    test_chunking()
    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILED -> {FAILURES}")
        raise SystemExit(1)
    print("RESULT: ALL PASSED")


if __name__ == "__main__":
    main()
