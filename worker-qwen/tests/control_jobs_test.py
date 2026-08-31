"""Wire-contract tests for speaker_forget and lazy TTS prompt creation."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import redis_worker
import stub_pipeline_test
from redis_messages import (
    JobType,
    RedisJobMessage,
    SpeakerForgetPayload,
    TtsPayload,
)
from stub_pipeline_test import SPEAKER_NAME, StubModel, cache_dir, check, install, params, wav_bytes


def job(job_type: JobType, payload: dict) -> RedisJobMessage:
    return RedisJobMessage.from_redis_fields({
        "jobId": "req_test",
        "type": job_type.value,
        "payload": json.dumps(payload, ensure_ascii=False),
    })


def test_payload_parsing() -> None:
    print("\n[1] payload 파싱")
    forgotten = job(JobType.SPEAKER_FORGET, {"speakerName": SPEAKER_NAME}).payload
    check("SpeakerForgetPayload", isinstance(forgotten, SpeakerForgetPayload))

    tts = job(JobType.TTS, {
        "text": "안녕",
        "speakerName": SPEAKER_NAME,
        "referenceDigest": "digest-1",
        "speakerBlobKey": "blob",
        "speakerMode": "icl",
        "speakerRefText": "참조",
        "temperature": 0.8,
        "top_k": 30,
        "do_sample": False,
    }).payload
    check("TtsPayload", isinstance(tts, TtsPayload))
    check("speaker 참조", tts.speaker_name == SPEAKER_NAME and tts.speaker_blob_key == "blob")
    check("생성 파라미터", tts.temperature == 0.8 and tts.top_k == 30 and tts.do_sample is False)

    # 게이트웨이가 speaker* 로 바뀌기 전에 발행된 in-flight 잡도 계속 읽혀야 한다.
    legacy = job(JobType.TTS, {"text": "안녕", "styleId": "style_a1b2c3d4e5f6", "styleBlobKey": "blob"}).payload
    check("레거시 style* 키 fallback", legacy.speaker_name == "style_a1b2c3d4e5f6" and legacy.speaker_blob_key == "blob")


def test_control_roundtrip() -> None:
    print("\n[2] lazy TTS → styles(cache) → forget")
    with cache_dir():
        install(StubModel())
        redis_worker.LOCAL_SERVICE.synthesize(params(), blob_loader=lambda _key: wav_bytes())

        styles = redis_worker.handle_control_job(job(JobType.STYLES, {})).to_dict()
        check("styles는 캐시 진단 목록", [item["name"] for item in styles["styles"]] == [SPEAKER_NAME])
        check("kind=cached", styles["styles"][0]["kind"] == "cached")

        forgotten = redis_worker.handle_control_job(
            job(JobType.SPEAKER_FORGET, {"speakerName": SPEAKER_NAME})
        ).to_dict()
        check("forget applied", forgotten["applied"] is True)
        check("forget 응답은 이름", forgotten["speakerName"] == SPEAKER_NAME)
        check("캐시 목록 비움", redis_worker.LOCAL_SERVICE.style_infos() == [])


def main() -> None:
    test_payload_parsing()
    test_control_roundtrip()
    print()
    if stub_pipeline_test.FAILURES:
        print(f"RESULT: {len(stub_pipeline_test.FAILURES)} FAILED -> {stub_pipeline_test.FAILURES}")
        raise SystemExit(1)
    print("RESULT: ALL PASSED")


if __name__ == "__main__":
    main()
