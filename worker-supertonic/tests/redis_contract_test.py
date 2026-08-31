"""Gateway ↔ worker Redis 메시지 계약 테스트.

여기가 깨지면 합성 품질이 아니라 **연동 자체가** 끊긴다. 필드 이름 하나만 바뀌어도
Gateway 가 결과를 읽지 못하는데, 런타임에는 조용히 "unknown job" 로그만 남으므로
알아채기 어렵다. 규약 문서는 `gateway-api/docs/redis-queue-contract.md`.

모델도 Redis 도 필요 없다 — 순수 직렬화/역직렬화만 본다.
"""
from __future__ import annotations

import json

from _harness import check, equals, raises, report

import redis_messages as m


def test_job_parsing() -> None:
    print("[1] job 메시지 파싱")
    job = m.RedisJobMessage.from_redis_fields({
        "jobId": "job_abc",
        "type": "tts",
        # Gateway 는 모든 값을 문자열로 싣는다. 숫자 필드도 문자열로 도착한다.
        "payload": json.dumps({"text": "안녕", "voice": "Na-in-ae", "speed": "1.1", "steps": "12", "seed": "7"}),
    })
    equals("jobId", job.job_id, "job_abc")
    equals("job type", job.job_type, m.JobType.TTS)
    equals("text", job.payload.text, "안녕")
    equals("voice", job.payload.voice, "Na-in-ae")
    # 문자열로 온 숫자를 파이썬 타입으로 바꿔 준다. 안 그러면 clean_speed 등이 문자열을 받는다.
    equals("speed 는 float", job.payload.speed, 1.1)
    equals("steps 는 int", job.payload.steps, 12)
    equals("seed 는 int", job.payload.seed, 7)

    # type 을 생략한 옛 발행자를 위한 기본값. 바뀌면 기존 호출자가 조용히 깨진다.
    default_type = m.RedisJobMessage.from_redis_fields({"jobId": "j", "payload": '{"text":"x"}'}).job_type
    equals("type 생략 시 tts", default_type, m.JobType.TTS)

    # styles 는 인자가 없는 제어 요청이라 payload 모델이 다르다.
    styles = m.RedisJobMessage.from_redis_fields({"jobId": "req_1", "type": "styles", "payload": "{}"})
    check("styles 는 EmptyPayload", isinstance(styles.payload, m.EmptyPayload))


def test_job_parsing_rejects_bad_input() -> None:
    print("\n[2] 잘못된 job 메시지 거절")
    # 이 셋은 worker 가 처리할 수 없는 레코드다. 조용히 넘기면 job 이 영원히 pending 으로 남는다.
    raises("jobId 없으면 거절", m.RedisModelError,
           m.RedisJobMessage.from_redis_fields, {"type": "tts", "payload": "{}"})
    raises("모르는 type 이면 거절", m.RedisModelError,
           m.RedisJobMessage.from_redis_fields, {"jobId": "j", "type": "nope", "payload": "{}"})
    raises("text 없으면 거절", m.RedisModelError,
           m.RedisJobMessage.from_redis_fields, {"jobId": "j", "type": "tts", "payload": "{}"})
    raises("payload 가 JSON 이 아니면 거절", m.RedisModelError,
           m.RedisJobMessage.from_redis_fields, {"jobId": "j", "type": "tts", "payload": "not json"})


def test_success_result_fields() -> None:
    print("\n[3] 성공 결과 직렬화")
    message = m.RedisResultMessage.succeeded(
        job_id="job_abc",
        worker_id="tts-worker-1",
        batch_id="batch_1",
        result=m.AudioResult(1.5, 44100, "audio/wav", "wav", "안녕."),
        artifact=m.ArtifactContent("job_abc.wav", "audio/wav", "QUJD"),
    )
    fields = message.to_redis_fields()

    equals("필드 집합", sorted(fields), ["artifact", "batchId", "jobId", "result", "state", "workerId"])
    equals("state", fields["state"], "succeeded")
    equals("jobId 는 받은 값 그대로", fields["jobId"], "job_abc")

    # Gateway 의 AudioJobResult 가 이 키들을 읽는다.
    result = json.loads(fields["result"])
    equals("result 키", sorted(result), ["audioFormat", "durationS", "mediaType", "processedText", "sampleRate"])
    equals("durationS 는 숫자", result["durationS"], 1.5)
    # sampleRate 만 문자열이다. Gateway DTO 가 문자열로 읽으므로 숫자로 바꾸면 깨진다.
    equals("sampleRate 는 문자열", result["sampleRate"], "44100")

    artifact = json.loads(fields["artifact"])
    equals("artifact 키", sorted(artifact), ["contentBase64", "fileName", "mediaType"])
    equals("contentBase64", artifact["contentBase64"], "QUJD")

    # 한글이 \uXXXX 로 이스케이프되면 Gateway 로그와 이력에서 읽을 수 없다.
    check("한글을 이스케이프하지 않음", "안녕." in fields["result"])


def test_failure_and_running_fields() -> None:
    print("\n[4] 실패 · running 결과 직렬화")
    failed = m.RedisResultMessage.failed(
        job_id="job_abc", worker_id="tts-worker-1", batch_id="batch_1", message="boom",
    ).to_redis_fields()
    equals("state", failed["state"], "failed")
    equals("error 메시지", json.loads(failed["error"]), {"message": "boom"})
    check("실패에는 artifact 가 없다", "artifact" not in failed)
    check("실패에는 result 가 없다", "result" not in failed)

    running = m.RedisResultMessage.running(
        job_id="job_abc", worker_id="tts-worker-1", batch_id="batch_1",
    ).to_redis_fields()
    equals("state", running["state"], "running")
    # Gateway 는 startedAt 을 OffsetDateTime.parse 로 읽는다. 없으면 job 시작 시각이 비고,
    # 형식이 어긋나면 리스너가 그 레코드를 통째로 버린다.
    check("startedAt 이 있다", "startedAt" in running)
    check("startedAt 은 타임존을 포함한 ISO-8601", running["startedAt"].endswith("+00:00"))


def test_styles_result_fields() -> None:
    print("\n[5] styles 제어 응답 직렬화")
    fields = m.RedisResultMessage.succeeded(
        job_id="req_1",
        worker_id="tts-worker-1",
        batch_id="batch_1",
        result=m.StylesResult(
            styles=[m.StyleInfo("Na-in-ae", "builtin"), m.StyleInfo("custom-a", "custom", "/styles/custom-a.json")],
            worker_id="tts-worker-1",
            batch_size=2,
        ),
    ).to_redis_fields()

    result = json.loads(fields["result"])
    equals("styles 개수", len(result["styles"]), 2)
    equals("batch_size", result["batch_size"], 2)
    # Gateway 가 overview 의 batchSize 로 쓰는 값이라 키 이름이 바뀌면 조용히 null 이 된다.
    check("worker_id 키가 있다", "worker_id" in result)
    # path 는 있을 때만 싣는다 — builtin 보이스에 null path 를 넣으면 UI 가 빈 경로를 그린다.
    check("path 없는 style 은 키 자체가 없다", "path" not in result["styles"][0])
    equals("path 있는 style", result["styles"][1]["path"], "/styles/custom-a.json")


def main() -> None:
    test_job_parsing()
    test_job_parsing_rejects_bad_input()
    test_success_result_fields()
    test_failure_and_running_fields()
    test_styles_result_fields()
    report()


if __name__ == "__main__":
    main()
