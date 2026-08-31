# Redis Queue 연동 규약 (Gateway ↔ TTS Worker)

Gateway 와 TTS worker 는 서로의 존재(URL, 갯수, 위치)를 전혀 모른 채 Redis Streams 로만 통신한다.

```
클라이언트 → Gateway ── XADD ──▶ tts:jobs:{priority}  ── XREADGROUP ──▶ Supertonic Worker
                       (model 로 분기)                                  │ (TTS 처리)
                   └── XADD ──▶ qwen:jobs:{priority} ── XREADGROUP ──▶ Qwen Worker
                                                                        │
클라이언트 ← Gateway ◀── XREADGROUP ── tts:results ◀── XADD ────────────┘
```

## 0. 합성 엔진(모델)별 큐 분리

엔진마다 voice 목록과 파라미터 규격이 달라 **워커 풀을 엔진별로 나눈다.** 같은 스트림·그룹을
쓰면 서로의 잡을 가져가 엔진별 payload 를 잘못 해석한다.

| 모델 | job stream prefix | consumer group | voice catalog | 보이스/speaker 원본 |
|---|---|---|---|---|
| `supertonic` (기본) | `tts:jobs` | `tts-workers` | `tts` | `worker-supertonic` |
| `qwen` | `qwen:jobs` | `qwen-workers` | 대상 아님 | Gateway `tts_style` + Redis blob |

- Gateway 는 job payload 의 **`model`** 로 발행 대상 스트림을 고른다. 없으면
  `queuetts.queue.default-model`(기본 `supertonic`) — 기존 호출자는 그대로 동작한다.
- 등록되지 않은 `model` 은 접수 단계에서 **400** 으로 거절한다(발행까지 미루면 어느 스트림에도
  안 들어간 job 이 timeout 될 때까지 방치된다).
- **결과 스트림(`tts:results`)은 모든 풀이 공유한다.** 결과는 `jobId` 로 식별되므로 Gateway 는
  result stream 하나만 소비한다.
- 설정은 `queuetts.queue.models.<model>.job-stream` / `.job-group` 이며, 각 워커의
  `QUEUETTS_JOB_STREAM` / `QUEUETTS_JOB_GROUP` 과 값이 일치해야 한다.

아래 설명의 `tts:jobs` 는 기본 모델 기준이며, 다른 모델은 자기 prefix 로 읽으면 된다.

- Gateway 는 job 을 우선순위에 맞는 `{prefix}:urgent|high|normal|low` 스트림에 넣기만 한다.
- Worker 는 각 스트림의 `tts-workers` consumer group 에서 높은 우선순위부터 job 을 가져간 직후
  `running` 이벤트를 발행하고, 처리 후 terminal 결과를 `tts:results` 스트림에 넣은 다음 job 메시지를 `XACK` 한다.
- Gateway 는 `gateway` consumer group 으로 진행/결과 이벤트를 소비해 job 상태를 갱신하고,
  필요한 API 호출자에게 결과를 반환한다.
- Worker 갯수/상태는 우선순위별 `XINFO CONSUMERS <stream> tts-workers` 결과를 합쳐 파악한다
  (설정이 아니라 실제 Redis consumer 기준이므로 중복 집계가 없다).

스트림/그룹 이름은 gateway 의 `queuetts.queue.*` 설정으로 바뀔 수 있다. 아래는 기본값 기준.

## 1. 우선순위별 Job 스트림: `tts:jobs:{priority}`

| 우선순위 | Stream | 소비 순서 |
|---|---|---|
| `urgent` | `tts:jobs:urgent` | 1 |
| `high` | `tts:jobs:high` | 2 |
| `normal` | `tts:jobs:normal` | 3 |
| `low` | `tts:jobs:low` | 4 |

동일 우선순위 안에서는 FIFO다. worker가 이미 시작한 낮은 우선순위 작업을 선점하지는 않으며,
아직 가져가지 않은 작업들 사이에서만 우선순위가 적용된다. 등록 후 우선순위 변경은 지원하지 않는다.

Gateway 가 XADD 하는 엔트리 필드 (모든 값은 문자열):

| 필드 | 설명 |
|---|---|
| `jobId` | gateway job id (예: `job_ab12cd34ef56`). 결과 보고 시 그대로 돌려줘야 한다. `req_` 접두사는 일회성 제어 요청(아래 참고). |
| `type` | 아래 "type 별 처리" 참고 |
| `payload` | JSON 문자열. `type=tts` 이면 `{"text": ..., "voice": ..., "speed": ..., "response_format": ..., "model": ...}`. `model` 은 Gateway 가 라우팅에 쓰는 값이라 worker 는 읽지 않아도 된다(자기 스트림만 소비하므로 이미 대상이 정해져 있다). |
| `priority` | `urgent` / `high` / `normal` / `low` — 대상 스트림과 일치해야 함 |
| `source` | job 출처 (없을 수 있음) |
| `enqueuedAt` | ISO-8601 UTC |

### type 별 처리

| type | payload | 응답 |
|---|---|---|
| `tts` | TTS job payload | 오디오 → result 스트림에 `artifact` 포함 |
| `styles` | (빈 객체) | worker가 캐시한 style 목록 + `batch_size` (진단/overview용) |
| `speaker_forget` | `{"speakerName":"나인애"}` | 이름 기반 로컬 prompt 캐시 삭제 결과 |

- `styles`/`speaker_forget` 는 `jobId` 가 `req_` 접두사로 온다.
  gateway 쪽에서 job 으로 관리되지 않는 일회성 request/reply 이며, 응답 대기 timeout 이
  짧으므로(기본 30초, `queuetts.queue.control-request-timeout-seconds`) 우선 처리하는 것이 좋다.
- consumer group 특성상 제어 요청은 worker 한 대만 받는다. Qwen speaker 캐시는 worker마다 달라도
  정상이며, TTS 캐시 미스는 Redis blob read-through로 복구한다.

### Qwen speaker prompt와 삭제

공개 리소스는 `/api/qwen/speaker`이고 원본과 오디오 검증은 Gateway가 소유한다. Gateway는 raw 참조
음성을 `{speaker-blob-prefix}:style:blob:{speakerName}` Redis STRING에 쓰지만 준비 제어 요청은 보내지
않는다. `speaker_forget`은 삭제 때 파생 캐시만 지우며 DB 행과 blob 삭제는 Gateway가 담당한다.

Qwen TTS payload에는 `speakerName`, `referenceDigest`, `speakerBlobKey`, `speakerMode`,
`speakerRefText`가 포함된다. 캐시가 없으면 worker가 `speakerBlobKey`를 GET해 prompt를 재생성한다.
생성 파라미터는 `do_sample`, `temperature`, `top_p`, `top_k`, `repetition_penalty`,
`max_new_tokens`, `subtalker_dosample`, `subtalker_temperature`, `subtalker_top_p`,
`subtalker_top_k`만 전달하며, 미지정/null 키는 payload에서 제거한다.

> 식별 필드는 예전에 `styleId`, 그다음 `speakerId`였고 현재는 `speakerName`이다. 나머지 필드는
> 예전에 `styleBlobKey`/`styleMode`/`styleRefText`라는 이름이었다.
> worker 는 전환 기간 동안 옛 이름도 읽어 주지만(`redis_messages.TtsPayload`), Gateway 는
> 새 이름만 발행한다. 큐가 한 번 비고 나면 worker 쪽 fallback 은 지워도 된다.

**엔진마다 payload 규격이 다르고, Gateway 가 접수 단계에서 검사한다.** Supertonic 은 `speed`/
`steps` 를, Qwen 은 위 생성 파라미터를 받으며, 그 엔진에 없는 필드가 오면 `POST /api/jobs` 가
400 이다(예전에는 조용히 무시됐다). 규격은 `dto/JobDto.kt` 의 모델별 구현체가 정의한다.
반대로 **큐에 직접 `XADD` 하는 경로는 이 검사를 우회**하므로, worker 도 대응 없는 파라미터를
받으면 프로세스당 한 번 경고를 남긴다.

Worker 규칙:

1. 시작 시 네 스트림 각각에 consumer group 이 없으면 생성한다 (gateway 도 시작 시 `MKSTREAM` 으로
   만들어 두므로 보통 이미 존재): `XGROUP CREATE tts:jobs:<priority> tts-workers 0 MKSTREAM`
   (BUSYGROUP 오류는 무시).
2. **consumer 이름은 worker 인스턴스마다 고유하고, 재시작해도 같은 이름을 유지**해야 한다
   (예: `tts-worker-<host>-<gpu번호>`). 이 이름이 곧 gateway 가 보는 workerId 이며,
   재시작마다 이름이 바뀌면 유령 consumer 가 쌓인다
   (pending 0 인 채 5분(`worker-evict-idle-ms`) 이상 idle 이면 gateway 가 자동 삭제하긴 한다).
3. 새 작업은 `urgent → high → normal → low` 순서로 각 스트림을 non-blocking `XREADGROUP` 한다.
   모두 비었을 때만 짧게 대기한 뒤 다시 높은 우선순위부터 확인한다.
4. job 을 가져온 직후 **`state=running` 이벤트를 result 스트림에 XADD** 하고 처리를 시작한다.
   처리 완료 후 **terminal 결과를 result 스트림에 XADD 한 다음** job 엔트리를 `XACK` 한다.
   (crash 시 pending 으로 남아 `XAUTOCLAIM` 으로 다른 worker 가 회수 가능)
5. idle 상태에서도 네 스트림을 주기적으로 XREADGROUP 하면 idle 시간이 짧게 유지되어
   gateway 가 active worker 로 집계한다 (기준: idle ≤ 30초, `worker-active-idle-ms`).

파일 기반 voice pool은 시작 시 자기 voice 목록을 Redis catalog에 기록해 비교할 수 있다.
Gateway 소유 Qwen clone speaker는 이 검증 대상이 아니다. 자세한 경계는 `voice-sync-design.md`를 참고한다.

## 2. Result 스트림: `tts:results`

Worker 가 XADD 하는 엔트리 필드 (모든 값은 문자열):

| 필드 | 필수 | 설명 |
|---|---|---|
| `jobId` | O | job 스트림에서 받은 값 그대로 |
| `workerId` | O | 자신의 consumer 이름 |
| `batchId` | O | worker 가 묶어 처리하는 논리 batch id |
| `state` | O | `running`, `succeeded`, `failed` 중 하나 |
| `startedAt` | running 시 | worker 가 job 처리를 시작한 ISO-8601 UTC 시각 |
| `result` | X | JSON 문자열. 오디오 job 은 `{"durationS": 1.25, "sampleRate": "44100", "mediaType": "audio/wav", "audioFormat": "wav"}`, 제어 요청(`styles` 등)은 응답 JSON 전체 |
| `error` | 실패 시 | JSON 문자열. 예: `{"message": "..."}` |
| `artifact` | 오디오 성공 시 | JSON 문자열: `{"fileName": "job_x.wav", "mediaType": "audio/wav", "contentBase64": "<base64 오디오>"}` (제어 요청은 생략) |

- artifact 크기 제한은 gateway 의 `queuetts.job.artifact-max-bytes` (기본 100MB, base64 디코딩 후 기준).
- gateway 가 결과를 처리하면 해당 엔트리를 XACK + XDEL 하므로, **gateway 가 소비하고 있는 동안은**
  result 스트림이 쌓이지 않는다.
- 그 전제가 깨지는 순간(gateway 정지·리스너 장애·소비 지연)에는 base64 오디오가 실린 엔트리가
  그대로 누적된다. 그래서 **worker 는 XADD 할 때 반드시 근사 MAXLEN 상한을 함께 건다**
  (`maxlen=..., approximate=True`). 상한이 없으면 gateway 정지 시간에 비례해 Redis 메모리가
  무한히 늘고, 잡 큐·voice catalog·speaker blob 까지 얹혀 있는 Redis 가 OOM 으로 죽는다.
  기존 worker 의 기본값은 `QUEUETTS_RESULT_STREAM_MAX_LENGTH=2000` 이다.

## 3. Python worker 예시 (redis-py)

```python
import base64, json, socket, time, uuid, redis
from datetime import datetime, timezone

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
JOB_STREAMS = ["tts:jobs:urgent", "tts:jobs:high", "tts:jobs:normal", "tts:jobs:low"]
JOB_GROUP = "tts-workers"
RESULT_STREAM = "tts:results"
# gateway 가 소비하지 못하는 동안 결과(=base64 오디오)가 무한히 쌓이지 않도록 거는 안전장치.
RESULT_STREAM_MAX_LENGTH = 2000
CONSUMER = f"tts-worker-{socket.gethostname()}-0"   # 고정된 고유 이름

for stream in JOB_STREAMS:
    try:
        r.xgroup_create(stream, JOB_GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

while True:
    selected = None
    for stream in JOB_STREAMS:  # 반드시 높은 우선순위부터 확인
        entries = r.xreadgroup(JOB_GROUP, CONSUMER, {stream: ">"}, count=1)
        if entries:
            selected = entries[0]
            break
    if selected is None:
        time.sleep(0.1)
        continue

    stream, records = selected
    batch_id = f"batch_{uuid.uuid4().hex[:12]}"
    for record_id, fields in records:
        payload = json.loads(fields["payload"])
        r.xadd(RESULT_STREAM, maxlen=RESULT_STREAM_MAX_LENGTH, approximate=True, fields={
            "jobId": fields["jobId"],
            "workerId": CONSUMER,
            "batchId": batch_id,
            "state": "running",
            "startedAt": datetime.now(timezone.utc).isoformat(),
        })
        try:
            audio_bytes, media_type = run_tts(payload)   # 실제 TTS 처리
            r.xadd(RESULT_STREAM, maxlen=RESULT_STREAM_MAX_LENGTH, approximate=True, fields={
                "jobId": fields["jobId"],
                "workerId": CONSUMER,
                "batchId": batch_id,
                "state": "succeeded",
                "result": json.dumps({"mediaType": media_type,
                                      "audioFormat": payload.get("response_format", "wav")}),
                "artifact": json.dumps({
                    "fileName": f"{fields['jobId']}.{payload.get('response_format', 'wav')}",
                    "mediaType": media_type,
                    "contentBase64": base64.b64encode(audio_bytes).decode(),
                }),
            })
        except Exception as e:
            r.xadd(RESULT_STREAM, maxlen=RESULT_STREAM_MAX_LENGTH, approximate=True, fields={
                "jobId": fields["jobId"],
                "workerId": CONSUMER,
                "batchId": batch_id,
                "state": "failed",
                "error": json.dumps({"message": str(e)}),
            })
        r.xack(stream, JOB_GROUP, record_id)
```

## 4. Gateway 쪽 동작 요약

- `POST /api/jobs` 등 모든 작업 생성은 priority에 해당하는 job 스트림 XADD 로 이어진다.
- 작업 상태는 `wait → running → succeeded | failed | cancelled` 흐름으로 결과 스트림을 통해 반영되며,
  클라이언트는 `GET /api/jobs/{jobId}` 폴링
  또는 `/api/jobs/{jobId}/events`(SSE)로 확인한다.
  오래 완료되지 않은 job 은 `JobTimeoutSweeper` 가 `failed` 로 닫고
  (timeout: `queuetts.worker-timeout-seconds`, 기본 60초), 이후 같은 jobId 로 늦게 도착한 결과는 무시한다.
- `/api/styles` 는 파일 기반 풀만 `type=styles` 제어 요청으로 조회한다. Qwen 항목은 Gateway DB의
  active style에서 직접 채우므로 Qwen worker가 없어도 목록이 나온다.
  - `?model=` 을 주면 그 풀만, 생략하면 **살아있는 worker 가 있는 모든 풀을 병렬로 조회해 합친다**
    (순차로 돌면 죽은 풀 하나마다 `control-request-timeout-seconds` 만큼 응답이 밀린다).
    worker 가 없는 풀에는 아예 보내지 않는다.
  - 응답의 각 style 에는 `model` 이 붙고, 일부 풀이 실패하면 나머지는 그대로 주되 `errors` 에
    실패 사유를 남긴다. 전부 실패했을 때만 503 이다.
- Qwen clone speaker CRUD는 `/api/qwen/speaker`에 있다. `DELETE`는 DB 행·blob·worker 캐시를 함께
  지우며 되돌릴 수 없다. 상세 계약은 `speakers-registry.md`를 따른다.
- `/api/health`, `/api/admin/job-gateway/overview` 의 `workers` 는 우선순위별 Redis consumer 정보를 합친
  (이름 / pending / idleMs / active 여부) 기반이다.
- 결과 메시지가 유실된 job 은 `POST /api/jobs/{jobId}/requeue` 로 재발행할 수 있다.
