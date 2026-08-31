# 새 TTS 모델 추가하기

엔진을 하나 더 붙일 때 무엇을 건드려야 하는지에 대한 체크리스트다.
배경(왜 엔진마다 프로젝트를 나누는지)은
[worker-qwen/docs/design.md](../worker-qwen/docs/design.md) 참고.

## 한눈에

| 대상 | 작업량 |
|---|---|
| **gateway-api** | **큐 라우팅은 설정 3줄 × 환경 3개. 파라미터 규격은 payload 클래스 하나** |
| worker-`<엔진>` | 새 워커 디렉터리 (`worker-qwen` 복사가 출발점) |

무거운 건 워커뿐이다. 엔진마다 파라미터·voice 규격이 달라 그 부분은 매번 새로 짜야 한다.

## 1. Gateway — 큐 라우팅 (설정만)

세 환경 파일(`src/main/resources-env/{local,dev,prod}/application.yml`)의
`queuetts.queue.models` 에 항목을 추가한다.

```yaml
queuetts:
  queue:
    default-model: supertonic
    models:
      supertonic:
        job-stream: tts:jobs
        job-group: tts-workers
      qwen:
        job-stream: qwen:jobs
        job-group: qwen-workers
      cosyvoice:            # ← 추가
        job-stream: cosy:jobs
        job-group: cosy-workers
```

이것만으로 발행 라우팅·consumer group 생성·worker 집계·`/api/styles` 병합·`/api/health` 태깅이
전부 따라온다. 코드를 고칠 필요가 없다는 사실은
`QueueModelExtensibilityTests` 가 세 번째 엔진으로 고정하고 있다 — **그 테스트가 깨지면
"설정만으로 되는" 성질이 무너진 것이다.**

### 겹치면 안 되는 값

| 항목 | 이유 |
|---|---|
| `job-stream` | 겹치면 두 엔진이 서로의 잡을 가져가 `Unknown voice` 로 실패한다 |
| `job-group` | 위와 같다 |
| 워커의 `QUEUETTS_VOICE_KEY_PREFIX` | 겹치면 voice 목록 불일치로 나중에 뜬 워커가 자가 종료한다 |

**Gateway 는 기동 시 이걸 검증하고, 겹치면 뜨지 않는다**(`RedisStreamQueueClient.validateModelQueues`).
조용히 오작동하는 것보다 낫다는 판단이다. `default-model` 오타, 대소문자만 다른 모델 이름,
빈 값도 같은 방식으로 막는다.

반대로 **result stream(`tts:results`)은 모든 엔진이 공유한다.** 결과는 `jobId` 로 식별되므로
Gateway 는 result stream 하나만 소비한다 — 여기는 나누지 말 것.

## 2. Gateway — 파라미터 규격 (payload 클래스)

라우팅과 달리 **생성 파라미터는 설정으로 끝나지 않는다.** 엔진마다 받는 knob 이 달라서,
그 엔진의 payload 타입을 하나 만들어야 한다.

`dto/JobDto.kt` 에 구현체를 추가하고 `JobPayloadTypes` 에 한 줄 등록한다.

```kotlin
@JsonInclude(JsonInclude.Include.NON_NULL)
data class CosyVoiceJobPayload(
    // 봉투 9개는 인터페이스가 요구한다 (text/model/voice/lang/response_format/seed/
    // max_chunk_length/silence_duration/source) — 다른 구현체에서 복사한다.
    ...
    // 여기부터 이 엔진 전용 knob
    val instruct: String? = null,
) : JobPayload { ... }
```

```kotlin
private val byName = mapOf(
    "supertonic" to SupertonicJobPayload::class.java,
    "qwen" to QwenJobPayload::class.java,
    "cosyvoice" to CosyVoiceJobPayload::class.java,   // ← 추가
)
```

**이 타입에 없는 필드가 오면 400 이다.** 예전에는 모든 엔진의 파라미터가 한 DTO 에 union 으로
들어 있어서, qwen 잡에 `speed` 를 실어도 조용히 사라졌다 — "슬라이더를 움직였는데 왜 그대로냐"를
추적할 수 없던 원인이다. 지금은 접수 단계에서 막힌다.

봉투(text·voice·lang·출력 포맷·seed·청크 설정)는 어느 엔진에서도 뜻이 같은 값이라 인터페이스가
소유한다. **엔진 전용 knob 을 봉투로 올리지 말 것** — 올리는 순간 다른 엔진도 그 필드를 받는다.

### voice 등록소가 따로 있는 엔진이면

`voice` 이름을 Gateway 가 해석해 줘야 하는 엔진(qwen 의 style registry 처럼)은
`service/JobService.kt` 의 `VoiceResolver` 구현을 하나 추가한다. worker 가 voice 카탈로그를 소유하는
엔진(supertonic)은 해석할 것이 없으므로 resolver 도 필요 없다 — 요청의 `voice` 가 그대로 간다.

### Redis 로 나가는 모양은 그대로 평면이다

payload 타입이 갈려도 **worker 가 보는 JSON 은 예전과 같은 평면 구조**다. 각 구현체가 자기
필드만 `NON_NULL` 로 내보내므로, 워커는 자기 파라미터만 담긴 객체를 받는다. 그래서 이 구분
때문에 워커를 고칠 일은 없다.

## 3. 워커 저장소

`worker-qwen` 을 복사해 시작한다. 그대로 쓰는 것과 새로 짜는 것이 갈린다.

**그대로 쓰는 것** (Gateway 계약이라 사실상 안 바뀐다)

```text
redis_messages.py   메시지 계약
redis_worker.py     큐 로직 — 우선순위 폴링, lease, 회수, batch, voice catalog
queuetts_audio.py      wav/flac/ogg 인코딩
```

`redis_worker.py` 에서 바꿀 곳은 스트림 기본값과 백엔드 import 정도다.

**새로 짜는 것**

```text
<engine>_backend.py  모델 로드 / voice 해석 / 청크 추론 / 파라미터 매핑
voice_store.py       커스텀 voice 형식 (엔진마다 다르다)
requirements.txt     런타임 스택
Dockerfile / compose
```

### 워커 환경변수

```bash
QUEUETTS_JOB_STREAM=cosy:jobs
QUEUETTS_JOB_GROUP=cosy-workers
QUEUETTS_DEAD_STREAM=cosy:jobs:dead
QUEUETTS_VOICE_KEY_PREFIX=cosy
QUEUETTS_RESULT_STREAM=tts:results   # 공유 — 바꾸지 말 것
```

### 파라미터 매핑을 먼저 정하라

엔진마다 대응 없는 파라미터가 생긴다(Qwen 은 `speed`/`steps` 가 없다). 그 목록이 곧 §2 의
payload 클래스이므로 **워커 규격을 먼저 확정하고 payload 를 그 모양으로 적는다.**

Gateway 가 접수 단계에서 막아 주지만, 워커에서도 대응 없는 값을 받으면 프로세스당 한 번은
경고를 남긴다 — 큐에 직접 `XADD` 하는 경로(§4-2)는 Gateway 검증을 우회한다.

## 4. 검증 순서

Gateway 를 건드리기 **전에** 워커만으로 검증할 수 있다.

1. 워커를 새 스트림으로 띄운다.
2. `cosy:jobs:normal` 에 직접 `XADD` 해서 합성이 되는지 본다 (Gateway 무관).
3. 그 다음 Gateway 설정과 payload 클래스를 추가하고 `POST /api/jobs` 에 `{"model": "cosyvoice"}` 로 확인한다.
4. 그 엔진에 **없는 파라미터**를 일부러 실어 보내 400 이 나는지 확인한다 (payload 클래스가 실제로 걸리는지).
5. `GET /api/health` 에 새 워커가 `model` 태그와 함께 뜨는지 확인한다.
6. `GET /api/styles` 에 새 엔진 voice 가 합쳐 나오는지, `?model=cosyvoice` 로 단독 조회되는지 확인한다.
7. `model` 없는 기존 요청이 여전히 기본 모델로 가는지 확인한다.

## 5. 아직 안 된 것

- **모델별 잡 집계.** `/api/jobs` 의 `counts` 요약에 모델 구분이 없다 — 목록·상세는 엔진별로
  걸러지지만 집계는 전 엔진 합계로 나온다. 소비자가 엔진별 대시보드를 그리려면 여기가 먼저다.

## 6. 안 하기로 한 것

- **모델별 job timeout.** `queuetts.worker-timeout-seconds` 는 전역 단일값으로 둔다.
  분리하자는 이야기가 나왔던 이유는 "느린 엔진을 붙이면 긴 텍스트가 timeout 으로 실패한다"
  였는데, **느린 엔진은 애초에 서비스 요건에서 탈락한다.** 전역 상한을 못 맞추는 엔진은
  붙이지 않는 것이지 상한을 엔진마다 늘려 주는 것이 아니다.

  즉 이 값은 튜닝 대상이 아니라 **엔진 채택 기준**에 가깝다. 새 엔진이 여기 걸리면
  timeout 을 나누지 말고 그 엔진을 다시 검토한다.
