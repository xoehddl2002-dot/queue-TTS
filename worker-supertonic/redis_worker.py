"""Redis Streams 기반 QueueTTS processor.

- Gateway가 발행한 ``tts:jobs:{priority}`` 작업을 처리하고 결과를 ``tts:results``에 기록한다.
- 기본 voice는 Supertonic 모델에서, custom voice는 이 프로젝트의 ``custom_styles/*.json``에서 읽는다.
- custom JSON 변경은 다음 작업을 처리하기 직전에 자동으로 다시 읽는다.
- 다른 worker가 남긴 pending 작업은 XAUTOCLAIM으로 회수한다.

공유 구간
---------
아래 함수들은 **모든 워커 저장소가 글자 하나 안 틀리고 같다** (Qwen worker =
``worker-qwen``). 고칠 때는 반드시 모든 워커 저장소를 함께 고친다 —
한쪽만 고치면 두 워커가 큐를 다르게 다루면서도 예외도 로그도 없이 조용히 갈라진다.

    new_id                _log_value              redis_fields_for_log
    log_received_batch    log_received_priority_batch
    refresh_lease         batch_lease             flush_batch_results
    publish_running_events                        reclaim_pending
    ensure_groups         _drain_stream           _id_gt
    stream_has_undelivered                        _selected_record_count
    _drain_streams_in_priority_order              _extend_priority_batch
    read_new_jobs         job_loop                shutdown    healthcheck

엔진별로 달라야 하는 것(voice 규격·생성 파라미터·voice catalog 동기화·모델 로딩)은 이
목록에 없다. 같은 이유로 공유되는 ``text_processing`` / ``queuetts_audio`` 는 ``common/`` 에
있다 — ``common/README.md`` 참고.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
from contextlib import contextmanager

import redis
from dotenv import load_dotenv

load_dotenv()

from backend import (
    DEFAULT_VOICE,
    LOCAL_SERVICE,
    ONNX_PROVIDERS_ENV,
    ONNX_RUNTIME_ENV,
    QueueTtsError,
    TTSParams,
    clean_format,
    clean_lang,
    clean_max_chunk_length,
    clean_seed,
    clean_speed,
    clean_steps,
)
from redis_messages import (
    ArtifactContent,
    AudioResult,
    JobType,
    RedisJobMessage,
    RedisResultMessage,
    StyleInfo,
    StylesResult,
    TtsPayload,
)
from style_store import default_custom_styles_dir

log = logging.getLogger("queuetts.processor")

# ── 설정 (gateway 의 queuetts.queue.* / queuetts.voice.* 와 반드시 일치해야 한다) ──────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_URL = os.getenv("REDIS_URL", "").strip()
REDIS_USERNAME = os.getenv("REDIS_USERNAME") or None
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
REDIS_SOCKET_TIMEOUT_S = float(os.getenv("REDIS_SOCKET_TIMEOUT_S", "30"))
REDIS_CONNECT_TIMEOUT_S = float(os.getenv("REDIS_CONNECT_TIMEOUT_S", "5"))
REDIS_HEALTH_CHECK_INTERVAL_S = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL_S", "15"))

JOB_STREAM_PREFIX = os.getenv("QUEUETTS_JOB_STREAM", "tts:jobs")
JOB_PRIORITIES = ("urgent", "high", "normal", "low")
JOB_STREAMS = tuple(f"{JOB_STREAM_PREFIX}:{priority}" for priority in JOB_PRIORITIES)
JOB_GROUP = os.getenv("QUEUETTS_JOB_GROUP", "tts-workers")
RESULT_STREAM = os.getenv("QUEUETTS_RESULT_STREAM", "tts:results")
DEAD_STREAM = os.getenv("QUEUETTS_DEAD_STREAM", "tts:jobs:dead")

# 결과/사망 스트림의 근사(approximate) MAXLEN 상한.
#
# 정상 운영에서 결과 스트림은 거의 비어 있다 — Gateway 가 ACK 직후 레코드를 삭제한다. 이 상한이
# 의미를 갖는 건 **Gateway 가 소비하지 못하는 동안**뿐이다. 결과 레코드에는 base64 오디오가 통째로
# 실리므로(건당 수백 KB~수 MB) 상한이 없으면 Gateway 정지 시간에 비례해 Redis 메모리가 무한히
# 늘고, 잡 큐·voice catalog·speaker blob 까지 함께 얹혀 있는 Redis 가 OOM 으로 죽는다.
#
# 기본값의 근거: Gateway 는 worker-timeout-seconds(기본 600초) 안에 결과가 도착하지 않으면 그 job 을
# 이미 실패시킨 뒤다. 그보다 한참 오래된 결과는 되살릴 대상이 없으므로, 그 창을 넉넉히 덮는 선에서
# 끊는다. **Redis maxmemory 에 맞춰 조정할 것** — `상한 x 평균 결과 크기` 가 최악의 점유량이다.
RESULT_STREAM_MAX_LENGTH = max(100, int(os.getenv("QUEUETTS_RESULT_STREAM_MAX_LENGTH", "2000")))
# 감사용 기록이라 오디오가 실리지 않는다(원본 job payload 만). 더 길게 잡아도 부담이 적다.
DEAD_STREAM_MAX_LENGTH = max(100, int(os.getenv("QUEUETTS_DEAD_STREAM_MAX_LENGTH", "10000")))

# 재시작해도 유지되는 고유 이름이어야 한다 (docker 에서는 container hostname 이 수명 내 고정)
WORKER_ID = os.getenv("QUEUETTS_WORKER_ID", f"tts-{socket.gethostname()}")

# 처리 중인 worker 가 죽었다고 판단해 pending 을 회수(→실패 처리)하기까지의 idle 임계값(ms).
# 살아있는 worker 는 LEASE_REFRESH_S 마다 idle 을 0 으로 리셋하므로, 그보다 충분히 커야
# 살아있는 job 을 잘못 실패시키지 않는다 (여기선 약 4배 여유).
CLAIM_MIN_IDLE_MS = int(os.getenv("QUEUETTS_CLAIM_MIN_IDLE_MS", "40000"))
# 처리 중 pending 의 idle 을 리셋하는 주기(초). CLAIM_MIN_IDLE_MS 보다 충분히 작아야 한다.
LEASE_REFRESH_S = int(os.getenv("QUEUETTS_LEASE_REFRESH_S", "10"))
# 한 번에 Redis 에서 가져와 batch 로 처리할 작업 수 (우선순위 Stream 단위).
BATCH_SIZE = max(1, int(os.getenv("QUEUETTS_BATCH_SIZE", "2")))
# urgent 가 비고 더 낮은 우선순위에만 작업이 있을 때, 그걸 claim 하기 전에 같은 버스트의
# 더 높은 우선순위가 도착하는지 기다리는 최대 시간(ms). 0 이면 기다리지 않고 바로 claim.
BATCH_WAIT_MS = max(0, int(os.getenv("QUEUETTS_BATCH_WAIT_MS", "200")))
# 유휴(할 일 없음) 상태에서 우선순위 큐를 다시 훑기 전 잠깐 자는 폴링 간격(ms).
# blocking 읽기를 쓰지 않으므로, 깨어날 때 우선순위와 무관한 작업이 손에 배달되지 않는다.
POLL_INTERVAL_MS = max(1, int(os.getenv("QUEUETTS_POLL_INTERVAL_MS", "50")))

# ── Voice catalog 동기화 (Gateway 의 queuetts.queue.* 와 반드시 일치) ──────
# Worker 는 시작 시 자기 voice 목록을 Redis catalog 에 반영하고, 다른 worker 의 catalog 와
# 비교해 목록이 일치할 때만 기동한다(불일치 시 자가 종료). 이는 worker 간 voice 일관성을
# 보장하기 위한 것으로, Gateway 는 catalog 를 읽지 않는다(보이스 목록은 GET /api/styles 로 노출).
#
# "살아있는 worker" 판정은 잡 스트림 consumer group(XINFO CONSUMERS) 하나만 본다. 잡 처리와
# voice 동기화가 같은 liveness 신호를 공유하므로 별도의 worker 상태 key(TTL heartbeat)는 두지 않는다.
VOICE_KEY_PREFIX = os.getenv("QUEUETTS_VOICE_KEY_PREFIX", "tts")
# 다른 worker 를 "살아있음"으로 볼 idle 상한(ms). Gateway 의 queuetts.queue.worker-active-idle-ms 와 맞춘다.
WORKER_ACTIVE_IDLE_MS = max(1000, int(os.getenv("QUEUETTS_WORKER_ACTIVE_IDLE_MS", "30000")))

CATALOG_KEY = f"{VOICE_KEY_PREFIX}:voice-catalog"
CATALOG_STATE_KEY = f"{VOICE_KEY_PREFIX}:voice-catalog:state"
CATALOG_LOCK_KEY = f"{VOICE_KEY_PREFIX}:voice-catalog:lock"


def new_id(prefix: str) -> str:
    """Create an identifier using the same format as Gateway job IDs."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


LOG_VALUE_LIMIT = max(200, int(os.getenv("QUEUETTS_LOG_VALUE_LIMIT", "2000")))


def _log_value(value, key: str = ""):
    if key.lower() in {"contentbase64", "audio_base64"} and isinstance(value, str):
        return f"<base64 chars={len(value)}>"
    if isinstance(value, dict):
        return {str(k): _log_value(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        preview = [_log_value(item) for item in value[:10]]
        if len(value) > 10:
            preview.append(f"<... {len(value) - 10} more items>")
        return preview
    if isinstance(value, str):
        if key in {"payload", "result", "artifact", "error"}:
            try:
                return _log_value(json.loads(value), key)
            except (TypeError, json.JSONDecodeError):
                pass
        if len(value) > LOG_VALUE_LIMIT:
            return f"{value[:LOG_VALUE_LIMIT]}<... {len(value) - LOG_VALUE_LIMIT} more chars>"
    return value


def redis_fields_for_log(fields: dict) -> dict:
    return {str(key): _log_value(value, str(key)) for key, value in fields.items()}


def log_received_batch(batch_id: str, stream: str, records: list, source: str) -> None:
    details = {
        "source": source,
        "batchId": batch_id,
        "stream": stream,
        "count": len(records),
        "records": [
            {"recordId": record_id, **redis_fields_for_log(fields or {})}
            for record_id, fields in records
        ],
    }
    log.info("redis.batch.received %s", json.dumps(details, ensure_ascii=False, default=str))


def log_received_priority_batch(batch_id: str, selected_batches: list[tuple[str, list]], source: str) -> None:
    if len(selected_batches) == 1:
        stream, records = selected_batches[0]
        log_received_batch(batch_id, stream, records, source)
        return
    details = {
        "source": source,
        "batchId": batch_id,
        "stream": "mixed",
        "count": sum(len(records) for _, records in selected_batches),
        "streams": [
            {
                "stream": stream,
                "count": len(records),
                "records": [
                    {"recordId": record_id, **redis_fields_for_log(fields or {})}
                    for record_id, fields in records
                ],
            }
            for stream, records in selected_batches
        ],
    }
    log.info("redis.batch.received %s", json.dumps(details, ensure_ascii=False, default=str))


def create_redis_client() -> redis.Redis:
    connection_options = {
        "decode_responses": True,
        "socket_timeout": REDIS_SOCKET_TIMEOUT_S,
        "socket_connect_timeout": REDIS_CONNECT_TIMEOUT_S,
        "socket_keepalive": True,
        "health_check_interval": REDIS_HEALTH_CHECK_INTERVAL_S,
        "retry_on_timeout": True,
    }
    if REDIS_URL:
        return redis.Redis.from_url(REDIS_URL, **connection_options)
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        **connection_options,
    )

r = create_redis_client()

running = True
_custom_style_snapshot: tuple[tuple[str, int, int], ...] = ()


def custom_style_snapshot() -> tuple[tuple[str, int, int], ...]:
    """model 프로젝트의 custom_styles JSON 상태를 반환한다."""
    directory = default_custom_styles_dir(LOCAL_SERVICE.model_name)
    if not directory.is_dir():
        return ()
    return tuple(
        (path.name, path.stat().st_mtime_ns, path.stat().st_size)
        for path in sorted(directory.glob("*.json"))
        if path.is_file()
    )


def refresh_custom_styles_if_changed(*, force: bool = False) -> None:
    """새 작업을 처리하기 전에 변경된 로컬 JSON을 모델에 다시 읽힌다."""
    global _custom_style_snapshot
    current = custom_style_snapshot()
    if not force and current == _custom_style_snapshot:
        return
    LOCAL_SERVICE.reload_custom_styles()
    _custom_style_snapshot = current
    log.info("custom styles loaded from %s: %s", default_custom_styles_dir(), LOCAL_SERVICE.custom_voices())

# ── Job 처리 ─────────────────────────────────────────────────────────────────────
def params_from_payload(payload: TtsPayload) -> TTSParams:
    return TTSParams(
        text=str(payload.text or ""),
        voice=str(payload.voice or DEFAULT_VOICE),
        lang=clean_lang(payload.lang) or "auto",
        speed=clean_speed(payload.speed),
        steps=clean_steps(payload.steps),
        response_format=clean_format(str(payload.response_format or "wav")),
        seed=clean_seed(payload.seed),
        max_chunk_length=clean_max_chunk_length(payload.max_chunk_length),
        silence_duration=float(payload.silence_duration if payload.silence_duration is not None else 0.1),
    )


def handle_audio_job(job: RedisJobMessage) -> tuple[AudioResult, ArtifactContent]:
    if not isinstance(job.payload, TtsPayload):
        raise QueueTtsError(f"job {job.job_id} does not contain an audio payload")
    params = params_from_payload(job.payload)
    result = LOCAL_SERVICE.synthesize(params)
    return (
        AudioResult(
            duration_s=result.duration_s,
            sample_rate=result.sample_rate,
            media_type=result.media_type,
            audio_format=result.response_format,
            processed_text=result.processed_text,
        ),
        ArtifactContent(
            file_name=f"{job.job_id}.{result.response_format}",
            media_type=result.media_type,
            content_base64=base64.b64encode(result.content).decode("ascii"),
        ),
    )


def handle_control_job(job: RedisJobMessage) -> StylesResult:
    """styles — 사용 가능한 보이스/스타일 목록을 JSON 응답의 result 필드로 돌려준다.

    voice 목록은 큐로 조회하지 않는다. 각 worker 가 시작 시 Redis 의
    voice catalog(``tts:voice-catalog``)를 유지하며 worker 간 목록 일관성을 스스로 맞춘다.
    Gateway 는 catalog 를 읽지 않고, 사용 가능한 보이스 목록은 ``GET /api/styles`` 로 노출한다.
    (:func:`register_voice_catalog` 참고)
    """
    if job.job_type == JobType.STYLES:
        return StylesResult(
            styles=[
                StyleInfo(name=style.name, kind=style.kind, path=style.path)
                for style in LOCAL_SERVICE.style_infos()
            ],
            worker_id=WORKER_ID,
            batch_size=BATCH_SIZE,
        )
    raise QueueTtsError(f"unsupported job type: {job.job_type}")


def refresh_lease(streams_record_ids: dict[str, list[str]], stop_event: threading.Event) -> None:
    """처리 동안 주기적으로 소유한 모든 레코드에 XCLAIM 해 idle 을 리셋한다.

    아직 ACK 되지 않은 pending 레코드에만 효과가 있고, 이미 ACK 된 레코드엔 영향이 없다.
    한 번의 loop 반복에서 우선순위별로 여러 배치를 순차 처리할 수 있으므로, 아직 처리 순서를
    기다리는 (우선순위가 낮은) 레코드까지 idle 을 0 으로 리셋해, 다른 worker 가 XAUTOCLAIM 으로
    회수해 중복 처리하는 것을 막는다.
    """
    while not stop_event.wait(LEASE_REFRESH_S):
        for stream, record_ids in streams_record_ids.items():
            if not record_ids:
                continue
            try:
                r.xclaim(
                    stream,
                    JOB_GROUP,
                    WORKER_ID,
                    min_idle_time=0,
                    message_ids=record_ids,
                    justid=True,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("lease refresh failed for %s/%s: %s", stream, record_ids, exc)


@contextmanager
def batch_lease(streams_record_ids: dict[str, list[str]]):
    """소유한 모든 레코드가 ACK 될 때까지 lease 를 살려 두는 컨텍스트 매니저.

    with 블록 안에서 처리 + flush(ACK) 를 마친 뒤 lease 스레드를 정지한다.
    """
    stop = threading.Event()
    lease = threading.Thread(target=refresh_lease, args=(streams_record_ids, stop), daemon=True)
    lease.start()
    try:
        yield
    finally:
        stop.set()


def process_record(stream: str, record_id: str, fields: dict, batch_id: str = "") -> tuple[str, dict] | None:
    """한 작업을 처리해 ``(record_id, result_fields)`` 를 만든다.

    결과 발행(XADD)과 ACK 는 여기서 하지 않는다 — 같은 batch 의 다른 작업까지 모두 끝난 뒤
    ``flush_batch_results`` 가 한꺼번에 처리한다. 처리 중 idle 리셋(lease)은 호출자가
    ``batch_lease`` 로 batch 전체에 대해 유지한다. 처리할 수 없는 레코드(jobId 없음)는
    즉시 ACK 하고 ``None`` 을 돌려준다.
    """
    job_id = fields.get("jobId")
    if not job_id:
        log.warning("record %s/%s has no jobId, discarding", stream, record_id)
        r.xack(stream, JOB_GROUP, record_id)
        return None

    # 이 작업이 속한 batch 식별자. 결과에 태깅해 downstream 이 묶음을 인식하게 한다.
    batch_id = batch_id or new_id("batch")

    try:
        refresh_custom_styles_if_changed()
        job = RedisJobMessage.from_redis_fields(fields)
        if job.job_type == JobType.TTS:
            result, artifact = handle_audio_job(job)
            completion = RedisResultMessage.succeeded(
                job_id=job.job_id,
                worker_id=WORKER_ID,
                batch_id=batch_id,
                result=result,
                artifact=artifact,
            )
        else:
            completion = RedisResultMessage.succeeded(
                job_id=job.job_id,
                worker_id=WORKER_ID,
                batch_id=batch_id,
                result=handle_control_job(job),
            )
        result_fields = completion.to_redis_fields()
    except Exception as exc:  # noqa: BLE001
        log.warning("job %s (batch %s) failed: %s", job_id, batch_id or "-", exc)
        result_fields = RedisResultMessage.failed(
            job_id=job_id,
            worker_id=WORKER_ID,
            batch_id=batch_id,
            message=str(exc),
        ).to_redis_fields()
    return record_id, result_fields


def flush_batch_results(stream: str, batch_id: str, outcomes: list[tuple[str, dict]]) -> None:
    """batch 의 모든 작업이 끝난 뒤, 모아 둔 결과를 한꺼번에 Redis 로 발행하고 원본 작업을 ACK 한다.

    결과 XADD 는 파이프라인으로 묶어 한 번에 보내고, 결과가 스트림에 들어간 뒤에야
    원본 작업들을 ACK 한다 (at-least-once).
    """
    if not outcomes:
        return
    log.info(
        "redis.batch.result.send %s",
        json.dumps(
            {"stream": RESULT_STREAM, "batchId": batch_id, "count": len(outcomes),
             "results": [redis_fields_for_log(result_fields) for _, result_fields in outcomes]},
            ensure_ascii=False,
            default=str,
        ),
    )
    pipe = r.pipeline(transaction=False)
    for _record_id, result_fields in outcomes:
        pipe.xadd(RESULT_STREAM, result_fields, maxlen=RESULT_STREAM_MAX_LENGTH, approximate=True)
    result_record_ids = pipe.execute()
    r.xack(stream, JOB_GROUP, *[record_id for record_id, _ in outcomes])
    log.info(
        "redis.batch.result.sent %s",
        json.dumps(
            {"stream": RESULT_STREAM, "batchId": batch_id, "count": len(result_record_ids),
             "recordIds": result_record_ids},
            ensure_ascii=False,
            default=str,
        ),
    )


def publish_running_events(batch_id: str, selected_batches: list[tuple[str, list]]) -> None:
    """worker 가 새 batch 를 확보한 직후 각 job 의 running 상태를 발행한다."""
    messages = [
        RedisResultMessage.running(
            job_id=str(fields["jobId"]),
            worker_id=WORKER_ID,
            batch_id=batch_id,
        ).to_redis_fields()
        for _stream, records in selected_batches
        for _record_id, fields in records
        if fields and fields.get("jobId")
    ]
    if not messages:
        return

    log.info(
        "redis.batch.running.send %s",
        json.dumps(
            {"stream": RESULT_STREAM, "batchId": batch_id, "count": len(messages)},
            ensure_ascii=False,
        ),
    )
    pipe = r.pipeline(transaction=False)
    for message in messages:
        pipe.xadd(RESULT_STREAM, message, maxlen=RESULT_STREAM_MAX_LENGTH, approximate=True)
    record_ids = pipe.execute()
    log.info(
        "redis.batch.running.sent %s",
        json.dumps(
            {"stream": RESULT_STREAM, "batchId": batch_id, "count": len(record_ids), "recordIds": record_ids},
            ensure_ascii=False,
            default=str,
        ),
    )


def reclaim_pending(stream: str) -> None:
    """처리 중 죽은 worker(idle > CLAIM_MIN_IDLE_MS)가 남긴 pending 메시지를 회수해 '실패'로 떨군다.

    재처리(재합성)하지 않는다 — 크래시된 job 은 그대로 실패 처리해 사용자에게 즉시 실패를 알린다.
    재처리하면 중복 합성 + 긴 대기가 생기므로, 서비스 정책상 '실패'가 낫다는 결정에 따른다.
    감사(audit)용으로 원본 job 은 DEAD_STREAM 에도 남긴다.

    주의: '결과 발행 직후 ACK 전에 죽은' 극히 드문 경우엔 이미 성공한 job 을 실패로 발행할 수 있다.
    Gateway 는 이미 종료(succeeded/failed)된 job 에 대한 결과를 무시해 성공을 실패로 되돌리지 않아야 한다.
    """
    cursor = "0-0"
    while True:
        cursor, claimed, _ = r.xautoclaim(
            stream,
            JOB_GROUP,
            WORKER_ID,
            min_idle_time=CLAIM_MIN_IDLE_MS,
            start_id=cursor,
            count=10,
        )
        if not claimed:
            break
        batch_id = new_id("batch")
        log_received_batch(batch_id, stream, claimed, source="reclaimed")

        with batch_lease({stream: [record_id for record_id, _ in claimed]}):
            outcomes: list[tuple[str, dict]] = []
            for record_id, fields in claimed:
                if fields is None:
                    r.xack(stream, JOB_GROUP, record_id)
                    continue
                job_id = fields.get("jobId") or "?"
                log.warning("job %s reclaimed from a dead worker — failing without retry", job_id)
                r.xadd(
                    DEAD_STREAM,
                    {**fields, "sourceStream": stream, "reason": "worker-crashed"},
                    maxlen=DEAD_STREAM_MAX_LENGTH,
                    approximate=True,
                )
                outcomes.append((
                    record_id,
                    RedisResultMessage.failed(
                        job_id=job_id,
                        worker_id=WORKER_ID,
                        batch_id=batch_id,
                        message="worker crashed mid-processing; job failed (no retry)",
                    ).to_redis_fields(),
                ))
            # 회수한 batch 는 재처리 없이 실패 결과만 발행하고 ACK 한다 (lease 유지 상태에서 ACK).
            flush_batch_results(stream, batch_id, outcomes)
        if cursor == "0-0":
            break


# ── Voice catalog 동기화 ─────────────────────────────────────────────────────────
# 규칙: 살아있는 다른 worker 가 없으면(=사실상 1대) 그 목록을 그대로 catalog 로 쓰고,
#       다른 worker 가 있으면 시작하는 worker 의 목록을 catalog 와 비교해 다르면 스스로 종료한다.
# "살아있는 worker" 는 잡 스트림 consumer group 으로만 판정한다(별도 heartbeat key 없음).
# 전 과정이 Redis 로만 이뤄지며 Gateway 로의 HTTP 호출은 없다.
def _own_voice_catalog() -> dict[str, str]:
    """{voiceId: metadataJSON}. 현재 metadata 는 비어 있다(version/hash 없음)."""
    return {name: "{}" for name in LOCAL_SERVICE.voices()}


def _other_active_worker_count() -> int:
    """자신을 뺀, 잡 group 에서 idle 이 임계값 이하인(=살아있는) consumer 수. Gateway 의 판정과 동일한 신호."""
    active: set[str] = set()
    for stream in JOB_STREAMS:
        try:
            consumers = r.xinfo_consumers(stream, JOB_GROUP)
        except redis.RedisError:
            continue
        for consumer in consumers:
            name = consumer.get("name")
            if not name or name == WORKER_ID:
                continue
            if int(consumer.get("idle", 0)) <= WORKER_ACTIVE_IDLE_MS:
                active.add(name)
    return len(active)


def _cached_catalog() -> dict[str, str] | None:
    if r.get(CATALOG_STATE_KEY) != "ready":
        return None
    return r.hgetall(CATALOG_KEY) or {}


def _save_catalog(voices: dict[str, str]) -> None:
    pipe = r.pipeline()
    pipe.delete(CATALOG_KEY)
    if voices:
        pipe.hset(CATALOG_KEY, mapping=voices)
    pipe.set(CATALOG_STATE_KEY, "ready")
    pipe.execute()


def _catalog_difference(reference: dict[str, str], actual: dict[str, str]) -> tuple[list[str], list[str]]:
    """(missing, extra): 기준에 있는데 내게 없는 voice / 내게만 있는 voice."""
    ref, act = set(reference), set(actual)
    return sorted(ref - act), sorted(act - ref)


def _acquire_catalog_lock(token: str, timeout_s: float = 10.0) -> bool:
    """catalog check-and-set 을 직렬화하는 짧은 락. 동시에 기동하는 worker 들의 경쟁을 막는다."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if r.set(CATALOG_LOCK_KEY, token, nx=True, ex=10):
            return True
        time.sleep(0.1)
    return False


def register_voice_catalog() -> None:
    """시작 시 voice 목록을 catalog 와 동기화한다. 다른 worker 와 목록이 다르면 SystemExit 으로 종료.

    ``ensure_groups()`` 이후에 호출해야 자신이 consumer 로 등록돼 있고, 다른 worker 를 정확히 센다.
    """
    voices = _own_voice_catalog()
    token = uuid.uuid4().hex
    locked = _acquire_catalog_lock(token)
    try:
        others = _other_active_worker_count()
        catalog = _cached_catalog()
        if others == 0 or catalog is None:
            # 살아있는 다른 worker 가 없거나 catalog 가 아직 없으면 내가 기준이 된다.
            _save_catalog(voices)
            log.info("voice catalog seeded by %s (%d voices)", WORKER_ID, len(voices))
            return
        missing, extra = _catalog_difference(catalog, voices)
        if missing or extra:
            log.error(
                "voice list does not match catalog; shutting down. missing=%s extra=%s",
                missing,
                extra,
            )
            raise SystemExit(3)
        log.info("voice list matches catalog (%d voices)", len(voices))
    finally:
        if locked and r.get(CATALOG_LOCK_KEY) == token:
            r.delete(CATALOG_LOCK_KEY)


def ensure_groups() -> None:
    for stream in JOB_STREAMS:
        try:
            r.xgroup_create(stream, JOB_GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        r.xgroup_createconsumer(stream, JOB_GROUP, WORKER_ID)


def _drain_stream(stream: str, count: int) -> list:
    """한 우선순위 Stream 에서 아직 배달되지 않은(>) 레코드를 최대 count 개 non-blocking 으로 읽는다."""
    if count <= 0:
        return []
    entries = r.xreadgroup(JOB_GROUP, WORKER_ID, {stream: ">"}, count=count)
    return list(entries[0][1]) if entries and entries[0][1] else []


def _id_gt(a: str, b: str) -> bool:
    """Redis Stream ID('ms-seq') 크기 비교: a > b 이면 True."""
    def parse(value: str) -> tuple[int, int]:
        ms, _, seq = str(value).partition("-")
        return (int(ms or 0), int(seq or 0))
    return parse(a) > parse(b)


def stream_has_undelivered(stream: str) -> bool:
    """이 스트림에 그룹으로 아직 배달되지 않은(>) 작업이 있는지, 읽지(claim 하지) 않고 확인한다.

    폴링에서 낮은 우선순위를 곧바로 claim 하지 않고 "후보가 있는지"만 엿보는 데 쓴다.
    Redis 7+ 의 xinfo group ``lag`` 를 우선 사용하고, 없으면 last-delivered-id 로 비교한다.
    """
    try:
        groups = r.xinfo_groups(stream)
    except redis.RedisError:
        return False
    group = next((g for g in groups if g.get("name") == JOB_GROUP), None)
    if group is None:
        return False
    lag = group.get("lag")
    if lag is not None:
        return lag > 0
    try:
        last_id = r.xinfo_stream(stream).get("last-generated-id", "0-0")
    except redis.RedisError:
        return False
    return _id_gt(last_id, group.get("last-delivered-id", "0-0"))


def _selected_record_count(selected_batches: list[tuple[str, list]]) -> int:
    return sum(len(records) for _, records in selected_batches)


def _drain_streams_in_priority_order(streams: tuple[str, ...], count: int) -> list[tuple[str, list]]:
    selected_batches: list[tuple[str, list]] = []
    remaining = count
    for stream in streams:
        records = _drain_stream(stream, remaining)
        if not records:
            continue
        selected_batches.append((stream, records))
        remaining -= len(records)
        if remaining <= 0:
            break
    return selected_batches


def _extend_priority_batch(
    selected_batches: list[tuple[str, list]],
    streams: tuple[str, ...],
) -> list[tuple[str, list]]:
    remaining = BATCH_SIZE - _selected_record_count(selected_batches)
    if remaining <= 0:
        return selected_batches
    selected_batches.extend(_drain_streams_in_priority_order(streams, remaining))
    return selected_batches


def read_new_jobs() -> list[tuple[str, list]]:
    """폴링 방식으로 처리할 배치를 우선순위 순서대로 최대 BATCH_SIZE 만큼 고른다.

    blocking 읽기를 쓰지 않으므로 "깨어날 때 우선순위와 무관한 작업이 손에 배달되는" 일이 없다.
    항상 urgent 부터 읽어 claim 하므로, urgent 가 있으면 절대 낮은 우선순위를 먼저 claim 하지 않는다.
    높은 우선순위에서 BATCH_SIZE 를 채우지 못하면 남은 슬롯은 낮은 우선순위에서 순서대로 채운다.

    버스트 대비: urgent 가 비고 더 낮은 우선순위에만 작업이 있으면, 그 낮은 걸 곧바로 claim 하지
    않고 BATCH_WAIT_MS 동안 (후보보다 높은 우선순위를) 다시 확인한다. 같은 버스트의 urgent 가
    조금 늦게 도착해도 그게 먼저 처리된다. 후보 유무 확인은 xinfo 로 non-consuming 하게 한다.
    """
    # 1) urgent 는 언제나 바로 claim 해도 안전하다 (더 높은 우선순위가 없음).
    selected_batches = _drain_streams_in_priority_order(JOB_STREAMS[:1], BATCH_SIZE)
    if selected_batches:
        return _extend_priority_batch(selected_batches, JOB_STREAMS[1:])

    # 2) urgent 가 비었다. 아직 claim 하지 않은 채로 가장 높은 우선순위 "후보"를 엿본다.
    candidate = next((s for s in JOB_STREAMS[1:] if stream_has_undelivered(s)), None)
    if candidate is None:
        return []  # 아무 데도 없음 → job_loop 이 잠깐 자고 다시 폴링한다.

    # 3) 후보를 곧바로 claim 하지 않고, 후보보다 높은 우선순위가 버스트로 도착하는지 잠깐 기다린다.
    higher_streams = JOB_STREAMS[: JOB_STREAMS.index(candidate)]
    if BATCH_WAIT_MS > 0 and higher_streams:
        deadline = time.monotonic() + BATCH_WAIT_MS / 1000.0
        while running and time.monotonic() < deadline:
            selected_batches = _drain_streams_in_priority_order(higher_streams, BATCH_SIZE)
            if selected_batches:
                return _extend_priority_batch(
                    selected_batches,
                    JOB_STREAMS[JOB_STREAMS.index(candidate):],
                )  # 더 높은 우선순위 도착 → 그걸 먼저 처리하고 남은 슬롯은 아래에서 채움
            time.sleep(POLL_INTERVAL_MS / 1000.0)

    # 4) 대기 동안 더 높은 우선순위가 안 왔다 → 이제 후보부터 낮은 우선순위까지 claim 한다.
    selected_batches = _drain_streams_in_priority_order(JOB_STREAMS[JOB_STREAMS.index(candidate):], BATCH_SIZE)
    if selected_batches:
        return selected_batches
    # 후보가 그새 다른 worker 에게 갔다 → 빈손으로 반환, 다음 폴링에서 재시도.
    return []


def job_loop() -> None:
    last_reclaim = 0.0
    while running:
        try:
            if time.monotonic() - last_reclaim > 30:
                for stream in JOB_STREAMS:
                    reclaim_pending(stream)
                last_reclaim = time.monotonic()

            selected_batches = read_new_jobs()
            if not selected_batches:
                time.sleep(POLL_INTERVAL_MS / 1000.0)  # 할 일 없음 → 잠깐 자고 다시 폴링
                continue

            # read_new_jobs 는 우선순위 순서대로 최대 BATCH_SIZE 만큼 여러 스트림에서 채울 수 있다.
            # 하나의 logical batch 로 묶어 같은 batchId 를 쓰고, ACK 전까지 전체 lease 를 유지한다.
            batch_id = new_id("batch")
            log_received_priority_batch(batch_id, selected_batches, source="new")

            streams_record_ids = {
                stream: [record_id for record_id, _ in records]
                for stream, records in selected_batches
            }
            outcomes_by_stream: dict[str, list[tuple[str, dict]]] = {
                stream: []
                for stream, _ in selected_batches
            }
            with batch_lease(streams_record_ids):
                publish_running_events(batch_id, selected_batches)
                for stream, records in selected_batches:
                    for record_id, fields in records:
                        outcome = process_record(stream, record_id, fields, batch_id=batch_id)
                        if outcome:
                            outcomes_by_stream[stream].append(outcome)
                # batch 의 모든 작업을 끝낸 뒤 결과를 stream 별로 발행하고 원본을 ACK 한다 (lease 유지 중 ACK).
                for stream, outcomes in outcomes_by_stream.items():
                    flush_batch_results(stream, batch_id, outcomes)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            log.warning("redis connection interrupted, reconnecting: %s", exc)
            r.connection_pool.disconnect()
            time.sleep(3)
        except Exception as exc:  # noqa: BLE001
            log.warning("job loop error: %s", exc)
            time.sleep(1)

# ── graceful shutdown (docker stop / scale-in) ──────────────────────────────────
def shutdown(signum, _frame) -> None:
    global running
    log.info("signal %s received, shutting down", signum)
    running = False


def cleanup() -> None:
    # catalog(기준 목록)는 지우지 않는다 — 남은 worker 들이 그대로 쓴다. 마지막 worker 가 떠나
    # active worker 가 0 이 되어도 catalog 는 남지만, 다음에 기동하는 worker 가 active peer 0 을
    # 확인하면 자기 목록을 새 기준으로 덮어쓴다(register_voice_catalog).
    try:
        # pending 작업이 있으면 consumer를 남겨 다른 worker가 XAUTOCLAIM하도록 한다.
        for stream in JOB_STREAMS:
            if not r.xpending_range(stream, JOB_GROUP, "-", "+", 1, consumername=WORKER_ID):
                r.xgroup_delconsumer(stream, JOB_GROUP, WORKER_ID)
    except Exception as exc:  # noqa: BLE001
        log.warning("shutdown cleanup failed: %s", exc)

def _run() -> None:
    LOCAL_SERVICE.load()
    refresh_custom_styles_if_changed(force=True)
    ensure_groups()  # consumer 등록을 먼저 해야 voice 동기화가 다른 worker 를 정확히 센다
    register_voice_catalog()  # 시작 시 voice 목록 동기화 (불일치 시 SystemExit 으로 종료)
    job_loop()


def healthcheck() -> bool:
    """Redis 연결과 이 consumer의 생성 여부를 검사한다."""
    try:
        if not r.ping():
            return False
        return any(
            any(consumer["name"] == WORKER_ID for consumer in r.xinfo_consumers(stream, JOB_GROUP))
            for stream in JOB_STREAMS
        )
    except redis.RedisError:
        return False

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the QueueTTS Redis processor.")
    parser.add_argument(
        "--runtime",
        "--device",
        choices=["cpu", "gpu", "cuda", "auto"],
        default=None,
        help="ONNX Runtime device mode. gpu requires CUDAExecutionProvider.",
    )
    parser.add_argument(
        "--onnx-providers",
        default=None,
        help="Comma-separated ONNX provider list.",
    )
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Exit successfully only when this processor is active in Redis.",
    )
    return parser.parse_args()


def main() -> None:
    global running
    args = _parse_args()
    if args.runtime:
        os.environ[ONNX_RUNTIME_ENV] = args.runtime
    if args.onnx_providers:
        os.environ[ONNX_PROVIDERS_ENV] = args.onnx_providers
    if args.healthcheck:
        raise SystemExit(0 if healthcheck() else 1)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info(
        "starting Redis processor: workerId=%s redis=%s jobStream=%s batchSize=%s batchWaitMs=%s pollIntervalMs=%s",
        WORKER_ID,
        "REDIS_URL" if REDIS_URL else f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        JOB_STREAM_PREFIX,
        BATCH_SIZE,
        BATCH_WAIT_MS,
        POLL_INTERVAL_MS,
    )

    running = True
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        _run()
    except Exception:  # noqa: BLE001
        log.exception("Redis processor stopped unexpectedly")
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()
