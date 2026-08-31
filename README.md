# AI TTS Server

텍스트를 음성으로 합성하는 백엔드다. **HTTP 프론트도어(Spring Boot)** 와 **GPU 합성 워커(Python)**
를 분리하고, 둘 사이를 **Redis Streams 작업 큐**로 이었다.

> 실무에서 접한 문제를 일반화하여 공개 자료와 오픈소스만으로 독립 재구현한 포트폴리오
> 프로젝트이며, 회사 원본 코드와 내부 데이터는 포함하지 않는다.

핵심은 "TTS 를 붙였다"가 아니라 **느리고(수 초~수십 초) 불안정하며 GPU 를 점유하는 작업을
HTTP 요청 위에서 어떻게 다룰 것인가**다. 동기 호출로 워커를 부르면 워커가 죽는 순간 요청이
같이 죽고, 워커를 늘리면 게이트웨이가 워커 목록을 알아야 한다. 그래서 둘을 큐로 끊었다.

```
클라이언트 ──HTTP──▶ gateway-api ──XADD──▶ Redis Streams ──XREADGROUP──▶ worker (N개)
                          ▲                                                  │
                          └────────────── tts:results ◀──────────────────────┘
```

Gateway 는 워커의 주소도 개수도 모른다. 워커는 HTTP 서버를 띄우지 않는다. 스케일 아웃은
**프로세스를 더 띄우는 것**으로 끝나고, 워커가 죽어도 잡은 큐에 남는다.

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

잡은 `urgent > high > normal > low` 네 스트림으로 나뉘고, 워커는 높은 우선순위부터 가져간다.
처리 중인 잡은 consumer group 의 pending 으로 남고 워커가 주기적으로 lease 를 갱신한다.

죽은 워커가 남긴 pending 은 **재처리하지 않고 실패로 떨군다.** 재처리가 기본값이면 "GPU 를
40초 점유하다 OOM 으로 죽는 잡"이 워커를 옮겨 다니며 큐 전체를 마비시키기 때문이다. 원본
메시지는 `{prefix}:jobs:dead` 에 감사용으로 남긴다.

### 2. 엔진 추가는 설정으로 끝난다

엔진(모델)마다 잡 스트림·consumer group·voice 목록이 분리돼 있고, Gateway 는 요청 payload 의
`model` 로 대상 스트림을 고른다. **새 엔진을 붙일 때 Gateway 에서 고치는 것은 환경 설정 3줄과
payload 클래스 하나**이며, 큐 라우팅·워커 집계·`/api/styles` 병합·health 태깅이 전부 따라온다.

이 성질은 문서가 아니라 테스트가 지킨다 — `QueueModelExtensibilityTests` 가 "설정만 추가한
세 번째 엔진"을 고정하고 있어서, 코드를 고쳐야만 엔진이 붙는 구조로 퇴화하면 빨간불이 뜬다.
체크리스트는 [docs/adding-a-model.md](docs/adding-a-model.md).

엔진 전용 파라미터는 공통 봉투로 올리지 않는다. 예전에는 모든 엔진의 파라미터가 한 DTO 에
union 으로 들어 있어서, 그 엔진에 없는 값을 실어도 **조용히 사라졌다** — "슬라이더를 움직였는데
왜 소리가 그대로냐"를 추적할 수 없던 원인이다. 지금은 접수 단계에서 400 이다.

### 3. 들어오는 문은 엄격하게, 저장된 것은 관대하게

요청 바인딩은 모르는 키를 400 으로 거절한다(Spring Boot 가 꺼 두는
`FAIL_ON_UNKNOWN_PROPERTIES` 를 읽는 지점에서 명시적으로 켠다). 반대로 이력 행의 jsonb
payload 를 읽을 때는 모르는 키를 흘려보낸다 — **규격이 갈리기 전에 쌓인 데이터 때문에 과거
조회가 깨지면 안 되기 때문이다.**

### 4. 실패를 예외로 던지지 않는다

서비스 계층은 `Either<DomainError, T>`(Arrow)로 실패를 **반환**한다. 이 프로젝트에는
`@ControllerAdvice` 가 없어서, 예외를 던지는 순간 그 응답만 봉투 모양이 달라진다. 실패를
값으로 다루면 컨트롤러가 도메인 에러 → HTTP 상태 매핑을 한곳에서 한다.

### 5. 인증과 감사

health 계열을 뺀 모든 엔드포인트가 API Key(`X-API-Key`)를 요구하고, `/api/admin/**` 은 admin
role 키만 통과한다. **잡을 접수한 키가 잡에 함께 기록되어**(`caller_id`·`caller_role`) 이력
테이블에 남는다. 운영 프로파일은 키에 기본값이 없어 미주입 시 기동에 실패한다 — 무방비 상태로
뜨는 것보다 안 뜨는 편이 낫다.

### 6. 스키마는 Flyway, 워커 공용 코드는 복제

DB 스키마는 `db/migration/V*.sql` 로 관리해 "적용하고 파일을 지웠다"가 구조적으로 불가능하다.

워커 두 벌의 `common/`(문장 정규화·오디오 인코딩)과 큐 프로토콜 함수는 **글자 하나까지 동일해야
한다.** 어느 엔진을 쓰든 "15000원"은 같게 읽혀야 하는데, 한쪽만 고치면 예외도 로그도 없이
조용히 갈라지기 때문이다. 근거와 목록은 [worker-supertonic/common/README.md](worker-supertonic/common/README.md).

---

## 규모

| | |
|---|---|
| Gateway 프로덕션 코드 | Kotlin 41개 파일 · 약 6,000줄 |
| Gateway 테스트 | 25개 파일 · `@Test` 163개 (컨트롤러·서비스·DTO 바인딩·큐 계약·타임아웃 E2E) |
| Worker | Python 약 6,300줄 (워커 2벌 + 공용 모듈) |

## 실행

인프라(Redis·PostgreSQL)가 먼저 필요하다. Gateway 와 워커는 **같은 Redis** 를 봐야 한다.

접속 정보는 코드에 없다. 각 디렉터리의 `.env.example` 을 `.env` 로 복사해 값을 채운다
(`.env` 는 커밋되지 않는다).

**1. Redis** — Gateway 와 워커가 **같은 인스턴스**를 봐야 한다.

```powershell
cd gateway-api/redis-docker; docker compose up -d
```

PostgreSQL 은 별도로 띄우고 데이터베이스만 만들어 두면 된다 — 스키마는 Gateway 기동 시
Flyway 가 `db/migration` 으로 적용한다.

**2. Gateway**

```powershell
cd gateway-api; .\gradlew.bat bootRun
```

기본 프로파일은 `local`(포트 8080, Swagger UI `/swagger-ui.html`). 접속 정보는
`REDIS_HOST` / `REDIS_PASSWORD` / `POSTGRES_HOST` / `POSTGRES_USER` / `POSTGRES_PASSWORD`
환경변수로 주입하며, `local` 프로파일은 같은 값을 `gateway-api/.env` 에서도 읽는다
(`spring.config.import`). docker compose 도 같은 파일을 읽는다.

**3. Worker**

```powershell
cd worker-supertonic
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe redis_worker.py
```

`worker-qwen` 도 같은 방식이며 GPU 와 Qwen3-TTS **Base** 체크포인트가 필요하다. Docker 로 띄울
때는 `.env.docker.example` 을 `.env.docker` 로 복사한 뒤 compose 파일을 쓴다.

워커의 `QUEUETTS_JOB_STREAM` / `QUEUETTS_JOB_GROUP` 은 Gateway 의 `queuetts.queue.models.<model>` 과
값이 같아야 한다. 어긋나면 잡이 어느 워커에도 도착하지 않고 timeout 으로 실패한다.

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
