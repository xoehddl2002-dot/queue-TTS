# AI TTS Server

텍스트를 음성으로 합성하는 백엔드입니다. **HTTP 프론트도어(Spring Boot)**와 **GPU 합성 워커(Python)**를
분리하고, 둘 사이를 **Redis Streams 작업 큐**로 연결했습니다.

> 실무에서 접한 문제를 일반화하여 공개 자료와 오픈소스만으로 독립 재구현한 포트폴리오
> 프로젝트이며, 회사 원본 코드와 내부 데이터는 포함하지 않습니다.

핵심은 "TTS를 붙였다"가 아니라 **느리고(수 초~수십 초) 불안정하며 GPU를 점유하는 작업을
HTTP 요청 위에서 어떻게 다룰 것인가**입니다. 동기 호출로 워커를 부르면 워커가 죽는 순간 요청이
함께 실패하고, 워커를 늘리면 게이트웨이가 워커 목록을 알아야 합니다. 그래서 둘을 큐로 분리했습니다.

```
클라이언트 ──HTTP──▶ gateway-api ──XADD──▶ Redis Streams ──XREADGROUP──▶ worker (N개)
                          ▲                                                  │
                          └────────────── tts:results ◀──────────────────────┘
```

Gateway는 워커의 주소나 개수를 알지 못합니다. 워커는 HTTP 서버를 띄우지 않습니다. 스케일 아웃은
**프로세스를 더 띄우는 것**으로 완료되며, 워커가 종료되어도 작업은 큐에 남습니다.

| 디렉터리 | 스택 | 역할 |
|---|---|---|
| [`gateway-api`](gateway-api/README.md) | Kotlin · Spring Boot 3.5 · Java 21 · PostgreSQL | HTTP API, 잡 수명주기, 이력 영속화, speaker 레지스트리 |
| [`worker-supertonic`](worker-supertonic/README.md) | Python · ONNX Runtime | 합성 워커. voice 카탈로그를 워커가 소유 |
| [`worker-qwen`](worker-qwen/README.md) | Python · Qwen3-TTS · torch | 합성 워커. 참조 음성 기반 보이스 클로닝 |

전체 지도는 [docs/architecture.md](docs/architecture.md), wire 계약은
[gateway-api/docs/redis-queue-contract.md](gateway-api/docs/redis-queue-contract.md).

---

## 설계에서 신경 쓴 것

### 1. 우선순위 큐와 "죽은 워커" 처리

작업은 `urgent > high > normal > low` 네 개의 스트림으로 나뉘며, 워커는 높은 우선순위부터 가져갑니다.
처리 중인 작업은 consumer group의 pending으로 남고, 워커가 주기적으로 lease를 갱신합니다.

종료된 워커가 남긴 pending은 **재처리하지 않고 실패 처리합니다.** 재처리가 기본값이면 "GPU를
40초간 점유한 후 OOM으로 종료되는 작업"이 워커를 옮겨 다니며 큐 전체를 마비시킬 수 있기 때문입니다. 원본
메시지는 `{prefix}:jobs:dead`에 감사용으로 남깁니다.

### 2. 엔진 추가는 설정으로 끝난다

엔진(모델)마다 작업 스트림·consumer group·voice 목록이 분리되어 있고, Gateway는 요청 payload의
`model`로 대상 스트림을 선택합니다. **새 엔진을 추가할 때 Gateway에서 수정하는 것은 환경 설정 3줄과
payload 클래스 하나**이며, 큐 라우팅·워커 집계·`/api/styles` 병합·health 태깅이 모두 따라옵니다.

이 성질은 문서가 아니라 테스트로 보장합니다. `QueueModelExtensibilityTests`는 "설정만 추가한
세 번째 엔진"을 고정하여, 코드를 수정해야만 엔진이 추가되는 구조로 퇴화하면 테스트가 실패합니다.
체크리스트는 [docs/adding-a-model.md](docs/adding-a-model.md).

엔진 전용 파라미터는 공통 봉투로 올리지 않습니다. 기존에는 모든 엔진의 파라미터가 한 DTO에
union으로 들어 있어서, 해당 엔진에 없는 값을 전달해도 **조용히 사라졌습니다.** 이로 인해 "슬라이더를 움직였는데
왜 소리가 그대로인가?"를 추적하기 어려웠습니다. 현재는 접수 단계에서 400 응답을 반환합니다.

### 3. 들어오는 문은 엄격하게, 저장된 것은 관대하게

요청 바인딩은 알 수 없는 키를 400으로 거절합니다. Spring Boot가 꺼 두는
`FAIL_ON_UNKNOWN_PROPERTIES`를 읽는 지점에서 명시적으로 활성화합니다. 반대로 이력 행의 jsonb
payload를 읽을 때는 알 수 없는 키를 무시합니다. **규격이 분리되기 전에 쌓인 데이터로 인해 과거
조회가 깨지면 안 되기 때문입니다.**

### 4. 실패를 예외로 던지지 않는다

서비스 계층은 `Either<DomainError, T>`(Arrow)로 실패를 **반환**합니다. 이 프로젝트에는
`@ControllerAdvice`가 없어서, 예외를 던지는 순간 해당 응답만 봉투 모양이 달라집니다. 실패를
값으로 다루면 컨트롤러가 도메인 에러 → HTTP 상태 매핑을 한곳에서 처리할 수 있습니다.

### 5. 인증과 감사

health 계열을 제외한 모든 엔드포인트가 API Key(`X-API-Key`)를 요구하고, `/api/admin/**`는 admin
role 키만 통과시킵니다. **작업을 접수한 키가 작업에 함께 기록되어**(`caller_id`·`caller_role`) 이력
테이블에 남습니다. 운영 프로파일은 키에 기본값이 없으므로, 키를 주입하지 않으면 기동에 실패합니다. 이로써 무방비 상태로
서버가 기동되는 것을 방지합니다.

### 6. 스키마는 Flyway, 워커 공용 코드는 복제

DB 스키마는 `db/migration/V*.sql`로 관리하여 "적용한 후 파일을 삭제하는 방식"을 구조적으로 방지합니다.

워커 두 벌의 `common/`(문장 정규화·오디오 인코딩)과 큐 프로토콜 함수는 **글자 하나까지 동일해야
합니다.** 어느 엔진을 사용하더라도 "15000원"은 같게 읽혀야 하며, 한쪽만 수정하면 예외나 로그 없이
결과가 달라질 수 있기 때문입니다. 근거와 목록은 [worker-supertonic/common/README.md](worker-supertonic/common/README.md)에서 확인할 수 있습니다.

---

## 규모

| | |
|---|---|
| Gateway 프로덕션 코드 | Kotlin 41개 파일 · 약 6,000줄 |
| Gateway 테스트 | 25개 파일 · `@Test` 163개 (컨트롤러·서비스·DTO 바인딩·큐 계약·타임아웃 E2E) |
| Worker | Python 약 6,300줄 (워커 2벌 + 공용 모듈) |

## 실행

인프라(Redis·PostgreSQL)가 먼저 필요합니다. Gateway와 워커는 **같은 Redis**를 바라봐야 합니다.

접속 정보는 코드에 없습니다. 각 디렉터리의 `.env.example`을 `.env`로 복사한 후 값을 채우십시오
(`.env`는 커밋되지 않습니다).

**1. Redis** — Gateway 와 워커가 **같은 인스턴스**를 봐야 한다.

```powershell
cd gateway-api/redis-docker; docker compose up -d
```

PostgreSQL은 별도로 기동하고 데이터베이스만 생성하면 됩니다. 스키마는 Gateway 기동 시
Flyway가 `db/migration`을 통해 적용합니다.

**2. Gateway**

```powershell
cd gateway-api; .\gradlew.bat bootRun
```

기본 프로파일은 `local`(포트 8080, Swagger UI `/swagger-ui.html`). 접속 정보는
`REDIS_HOST` / `REDIS_PASSWORD` / `POSTGRES_HOST` / `POSTGRES_USER` / `POSTGRES_PASSWORD`
환경변수로 주입하며, `local` 프로파일은 같은 값을 `gateway-api/.env`에서도 읽습니다
(`spring.config.import`). docker compose도 같은 파일을 읽습니다.

**3. Worker**

```powershell
cd worker-supertonic
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe redis_worker.py
```

`worker-qwen`도 같은 방식이며 GPU와 Qwen3-TTS **Base** 체크포인트가 필요합니다. Docker로 기동할
때는 `.env.docker.example`을 `.env.docker`로 복사한 후 compose 파일을 사용합니다.

워커의 `QUEUETTS_JOB_STREAM` / `QUEUETTS_JOB_GROUP` 은 Gateway 의 `queuetts.queue.models.<model>` 과
값이 같아야 합니다. 일치하지 않으면 작업이 어느 워커에도 도착하지 않고 timeout으으로 실패합니다.

## 테스트

```powershell
cd gateway-api; .\gradlew.bat test --console=plain
```

```powershell
cd worker-qwen; .\.venv\Scripts\python.exe tests\control_jobs_test.py
cd worker-qwen; .\.venv\Scripts\python.exe tests\stub_pipeline_test.py
```

```powershell
cd worker-supertonic; .\.venv\Scripts\python.exe tests\redis_contract_test.py
cd worker-supertonic; .\.venv\Scripts\python.exe tests\text_pipeline_test.py
```

워커 테스트는 pytest 가 아니라 직접 실행하는 스크립트다(`RESULT: ALL PASSED` 를 출력한다).
워커 이미지에 테스트 전용 의존성을 넣지 않으려는 선택이며, **모델도 GPU 도 Redis 도 없이**
메시지 규약과 텍스트 파이프라인을 검증한다. 두 검증 모두 런타임에는 조용히 실패한다 —
필드 이름이 어긋나면 "unknown job" 로그만 남고, 숫자 읽기가 깨지면 합성은 성공하고 소리만
이상해진다.

## 저장소 범위

- 공개 구현 범위는 서버 측 3개 컴포넌트(API·워커 2벌)다. UI 데모는 포함하지 않는다.
- 접속 정보와 API Key 는 모두 환경변수 주입으로 바꿨고, 저장소에는 `.env.example` 과 기본값만
  남겼다. 커밋된 자격증명은 없다.
- 모델 가중치(수 GB), 합성 결과물, 로그, 샘플 음원은 포함하지 않는다.
