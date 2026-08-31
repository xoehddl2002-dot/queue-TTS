# gateway-api — TTS Gateway (Spring Boot)

HTTP 프론트도어. 합성 요청을 잡으로 접수해 Redis Streams 로 워커에 넘기고, 결과를 돌려주며
이력을 PostgreSQL 에 남깁니다. 워커의 주소를 모르고 HTTP 로 부르지도 않습니다.

## Stack

- Kotlin
- Gradle Kotlin DSL
- Spring Boot 3.5.15
- Java 21
- YAML configuration

## Run

Gateway와 TTS worker는 HTTP 주소 목록 없이 Redis Streams로 통신합니다. 두 프로젝트의 전체 관계와
용어는 [docs/architecture.md](docs/architecture.md)에 정리돼 있습니다. 작업 큐 계약은
[docs/redis-queue-contract.md](docs/redis-queue-contract.md), worker voice 목록 동기화는
[docs/voice-sync-design.md](docs/voice-sync-design.md)를 참고하세요.

DB 연결도 `application.yml`에서 관리합니다.

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/postgres
    username: postgres
    password:
```

## Security

`GET /api/health`, `GET /actuator/health`, `GET /actuator/info` 를 제외한 **모든 엔드포인트는 API Key 가 필요**합니다.

```http
X-API-Key: <발급받은 키>
```

`Authorization: Bearer <키>` 형태도 받습니다. 키는 `queuetts.security.keys` 에 role 별로 등록하며,
`role: admin` 키만 `/api/admin/...` 을 호출할 수 있고 `role: client` 키는 나머지 API만 호출할 수 있습니다.

```yaml
queuetts:
  security:
    enabled: true
    header-name: X-API-Key
    keys:
      - id: prod-admin
        key: ${QUEUETTS_ADMIN_API_KEY}
        role: admin
      - id: prod-client
        key: ${QUEUETTS_CLIENT_API_KEY}
        role: client
    cors:
      # 브라우저에서 직접 호출하는 소비자의 Origin 만 나열한다. 비어 있으면 교차 출처 호출 전면 차단.
      allowed-origins: []
```

- 실패 응답은 다른 API 와 같은 봉투(`{ "code": ..., "message": ... }`)로 나갑니다.
  키 없음 `401 MISSING_API_KEY`, 등록되지 않은 키 `401 INVALID_API_KEY`, 권한 부족 `403 FORBIDDEN`.
- **운영 키는 환경변수(`QUEUETTS_ADMIN_API_KEY` / `QUEUETTS_CLIENT_API_KEY`)로 주입**합니다. prod 프로파일에는
  기본값이 없어 미주입 시 애플리케이션이 기동에 실패합니다(무방비 상태로 뜨는 것을 막기 위함).
  키가 비었거나 16자 미만이거나 중복이면 역시 기동 시점에 실패합니다.
- local/dev 프로파일에는 개발용 기본 키가 들어 있습니다(`local-admin-0000000000000000` /
  `local-client-000000000000000`). 외부에 노출되는 환경이면 반드시 환경변수로 덮어쓰세요.
- Swagger UI 는 운영(prod)에서 꺼져 있습니다(`springdoc.*.enabled: false`). local/dev 에서는 문서 경로가
  무인증으로 열려 있고, Authorize 버튼으로 API Key 를 넣어 호출할 수 있습니다.
- SSE(`/api/jobs/{jobId}/events`)도 키가 필요합니다. 브라우저 `EventSource` 는 헤더를 실을 수 없으므로
  헤더 지정이 가능한 클라이언트(예: `fetch` 기반 SSE 라이브러리)를 쓰거나 서버 측에서 중계해야 합니다.
- **접수한 호출자 기록**: job 을 생성하면 그 요청을 인증한 API Key 가 job 에 함께 기록됩니다.
  조회 응답에 `caller: { "id": ..., "role": "admin"|"client" }` 로 나오고, `tts_job_generation_history`
  테이블의 `caller_id`·`caller_role` 컬럼에 영속화됩니다. 인증이 꺼진 환경(`queuetts.security.enabled=false`)에서
  만들어진 job 은 `caller` 가 `null` 입니다. caller 는 접수 시점에 확정되며 이후 상태 변경으로 덮어쓰지 않습니다.
- 인증을 끄려면 `queuetts.security.enabled=false` (로컬 디버깅 전용).

## APIs

health 계열을 제외한 모든 경로는 `X-API-Key` 헤더가 필요합니다.

- `GET /api/health`
- `GET /api/styles`
- `GET /api/admin/samples`
- `GET /api/admin/samples/{sampleKey}`
- `GET /api/admin/samples/{sampleKey}/legacy-audio`
- `GET /api/admin/samples/{sampleKey}/current-audio`
- `POST /api/jobs`
- `GET /api/jobs`
- `GET /api/jobs/{jobId}`
- `DELETE /api/jobs/{jobId}`
- `POST /api/jobs/{jobId}/requeue`
- `GET /api/jobs/{jobId}/download`
- `GET /api/jobs/{jobId}/events`
- `GET /api/admin/job-gateway/overview`

Actuator health endpoint:

```http
GET /actuator/health
```

## Notes

- `/api/styles`는 gateway가 직접 TTS worker를 호출하지 않고 Redis Streams 잡 큐(`queuetts.queue.*`)에 발행/구독하는 방식으로 처리합니다. 자세한 메시지 규약은 [docs/redis-queue-contract.md](docs/redis-queue-contract.md) 참고.
- 사용 가능한 보이스 목록은 `/api/styles`(빌트인 보이스 + 커스텀 스타일 이름)로 노출합니다. worker들은 시작 시 Redis catalog(`{prefix}:voice-catalog`)로 서로의 voice 목록 일관성을 검증하지만(2대째부터 목록 불일치 시 worker 자가 종료), gateway는 이 catalog를 읽지 않습니다. 자세한 내용은 [docs/voice-sync-design.md](docs/voice-sync-design.md) 참고.
- **합성 엔진(모델)별로 워커 풀이 나뉩니다.** `POST /api/jobs`의 payload에 `model`(`supertonic`/`qwen`)을 넣으면 해당 풀로 라우팅되고, 생략하면 `queuetts.queue.default-model`로 갑니다 — 기존 호출자는 바뀌는 게 없습니다. 등록되지 않은 `model`은 400입니다.
- `GET /api/styles?model=`로 한 엔진만 조회할 수 있습니다. 생략하면 살아있는 워커가 있는 모든 풀을 병렬 조회해 합치며, 각 style에 `model`이 붙습니다. 일부 풀이 실패해도 나머지 보이스는 그대로 주고 실패 사유는 `errors`에 담깁니다.
- `/api/jobs` 계열과 TTS 생성 이력은 하나로 관리합니다. 진행 중인 job은 런타임(in-memory)에서 추적하고, 완료된 job은 PostgreSQL `tts_job_generation_history`에 영속화하며, 진행 중·완료 job 모두 `/api/jobs`(목록)와 `/api/jobs/{jobId}`(상세)에서 함께 조회합니다.
- `GET /api/jobs` 목록은 `state`·`priority`·`source` 쿼리 파라미터로 필터할 수 있습니다. 예: `?source=tts` 로 특정 호출자(source)의 job만 조회. 소비자는 이를 이용해 관리자 화면(전체)과 테스트 화면(`source=tts`)을 같은 엔드포인트에서 구분합니다.
- job은 생성 시 지정한 우선순위에 따라 `tts:jobs:urgent`, `tts:jobs:high`, `tts:jobs:normal`, `tts:jobs:low` 중 하나에 들어갑니다. worker는 높은 우선순위 Stream부터 확인하며, 등록 후 우선순위 변경은 지원하지 않습니다.
- `/api/admin/samples`는 PostgreSQL `tts_sample_comparison`을 조회합니다.
- DB 테이블 매핑은 Spring Data `@Table` 모델(`model`)에서 관리합니다.
- 테이블 생성은 Flyway가 관리합니다. 마이그레이션은 `src/main/resources/db/migration/V*.sql`에 있고, 기동 시 순서대로 적용되며 적용 이력은 DB의 `flyway_schema_history`에 남습니다. **스키마를 바꿀 때는 기존 파일을 고치지 말고 다음 버전(`V2__...sql`)을 추가하세요** — 적용된 마이그레이션을 수정하면 checksum이 어긋나 기동이 실패합니다.
- 이미 스키마가 올라가 있는 기존 DB는 `baseline-on-migrate`로 `V1`을 적용된 것으로 표시하고 건너뜁니다. 빈 DB만 `V1`을 실제로 실행합니다.

## Worker 상태

- worker 존재/상태는 우선순위별 job Stream의 `XINFO CONSUMERS` 결과 하나로만 판정합니다(별도 heartbeat key 없음). voice 동기화도 같은 신호를 씁니다.
- `idle ≤ worker-active-idle-ms`(기본 30초)인 consumer만 active로 집계합니다.
