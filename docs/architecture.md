# 시스템 개요

텍스트를 받아 음성을 돌려주는 TTS 백엔드다. 클라이언트는 Gateway 의 HTTP API 하나만 알면 되고,
실제 합성은 별도 프로세스인 Worker 가 한다. **Gateway 와 Worker 는 서로의 주소·개수·위치를
전혀 모른 채 Redis 로만 통신한다.**

이 문서는 프로젝트들을 묶는 지도다 — 각 계층의 상세는 해당 디렉터리 문서로 링크한다.

## 구성 요소

| 디렉터리 | 계층 | 스택 | 역할 |
|---|---|---|---|
| [gateway-api](../gateway-api/README.md) | 게이트웨이 | Kotlin · Spring Boot 3.5 · Java 21 | HTTP 프론트도어, 잡 관리, 이력 영속화, speaker 레지스트리 |
| [worker-supertonic](../worker-supertonic/README.md) | 워커 | Python · Supertonic · ONNX | 합성 엔진. voice 카탈로그를 워커가 소유 |
| [worker-qwen](../worker-qwen/README.md) | 워커 | Python · Qwen3-TTS · torch | 합성 엔진. 보이스 클로닝(참조 음성으로 speaker 등록) |
| 인프라 | 큐/저장 | Redis · PostgreSQL | Redis = 작업·결과·감사·voice 카탈로그, PostgreSQL = 잡 이력·샘플 |

워커는 **HTTP 서버를 띄우지 않는다.** Redis 스트림을 소비하는 프로세스일 뿐이라 스케일 아웃이
"프로세스를 더 띄운다"로 끝난다.

## 흐름

```
                  HTTP (REST)
   클라이언트 ───────────────▶ ┌──────────────────┐
              ◀─────────────── │   gateway-api    │
                               │  (Spring Boot)   │
                               └───┬──────────┬───┘
                    XADD           │          │  XREADGROUP (결과)
              (payload.model 로 분기)│          │
                                   ▼          ▲
        ┌──────────────────────────────────────────────────────┐
        │  REDIS Streams                                       │
        │    tts:jobs:{urgent|high|normal|low}   tts:jobs:dead  │
        │    qwen:jobs:{urgent|high|normal|low}  qwen:jobs:dead │
        │    tts:results   ← 두 워커 풀이 공유                    │
        │    tts:voice-catalog · {prefix}:style:blob:{name}     │
        └───┬──────────────────────────────────────────┬───────┘
   XREADGROUP│                                          │XREADGROUP
            ▼                                          ▼
  ┌────────────────────┐                    ┌────────────────────┐
  │  worker-supertonic │                    │    worker-qwen     │
  │   (ONNX, CPU/GPU)  │                    │   (torch, GPU)     │
  └────────────────────┘                    └────────────────────┘

        gateway-api ──▶ PostgreSQL (완료 잡 이력, speaker 메타, 샘플 비교)
```

- **엔진별로 잡 스트림·consumer group·voice 카탈로그를 분리한다.** 같은 스트림을 쓰면 서로의
  잡을 가져가 payload 를 잘못 해석한다. Gateway 는 job payload 의 `model` 로 대상 스트림을
  고른다(없으면 `queuetts.queue.default-model`).
- **결과 스트림(`tts:results`)만 공유한다.** 결과는 `jobId` 로 식별되므로 Gateway 는 result
  stream 하나만 소비하면 된다.
- **워커 생존 판정에 별도 heartbeat 가 없다.** 잡 스트림 consumer group 의
  `XINFO CONSUMERS` 하나로 판정한다 — 잡을 처리하는 신호와 살아있다는 신호가 같아야
  "죽었는데 살아있다고 보고하는" 상태가 생기지 않는다.

## 공유 개념

| 용어 | 뜻 |
|---|---|
| Job / Priority | 합성·제어 단위. `urgent > high > normal > low` 로 스트림이 나뉜다 |
| Control request | `req_` 접두사의 일회성 요청/응답(styles 조회 등). 잡으로 영속화되지 않는다 |
| Artifact | 완성된 오디오. base64 로 결과 스트림에 실려 온다 |
| Voice catalog | 사용자에게 노출하는 검증된 voice 목록. Worker 가 Redis 에 기록하고 Gateway 는 읽기만 한다 |
| Speaker | 보이스 클로닝용 `(model, name)` 쌍. Gateway 가 소유하고 Worker 는 파생 캐시만 갖는다 |

## 더 깊이

- **Gateway ↔ Worker 통합 아키텍처**: [gateway-api/docs/architecture.md](../gateway-api/docs/architecture.md)
- **Redis 큐 메시지 규약**: [gateway-api/docs/redis-queue-contract.md](../gateway-api/docs/redis-queue-contract.md)
- **speaker 레지스트리 설계**: [gateway-api/docs/speakers-registry.md](../gateway-api/docs/speakers-registry.md)
- **voice 목록 동기화**: [gateway-api/docs/voice-sync-design.md](../gateway-api/docs/voice-sync-design.md)
- **Gateway HTTP API 목록**: [gateway-api/README.md](../gateway-api/README.md)
- **엔진이 왜 별도 프로세스인지**: [worker-qwen/docs/design.md](../worker-qwen/docs/design.md)
- **새 엔진 추가 체크리스트**: [adding-a-model.md](adding-a-model.md)

## 배포 형태

각 디렉터리가 자기 `docker-compose` 를 갖는다. Redis 와 PostgreSQL 은 공유 인프라이며,
**Gateway 와 Worker 가 같은 Redis 를 봐야 한다** — 스트림 이름·consumer group·키 prefix 설정이
양쪽에서 일치해야 하고, 어긋나면 잡이 어느 쪽에도 도착하지 않는다.

| 파일 | 대상 |
|---|---|
| `gateway-api/docker-compose.yml` | Gateway |
| `worker-supertonic/docker-compose.cpu.yml` · `.gpu.yml` | Supertonic 워커 (CPU/GPU) |
| `worker-qwen/docker-compose.gpu.yml` | Qwen 워커 |
