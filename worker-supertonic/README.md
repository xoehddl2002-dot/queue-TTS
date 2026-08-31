# QueueTTS

Redis Streams로 들어오는 TTS 작업을 처리하는 Supertonic 기반 model processor입니다.
HTTP 서버는 실행하지 않습니다.

Gateway(`gateway-api`)와의 관계·용어·통신 흐름은 Gateway 저장소의
`docs/architecture.md`(통합 아키텍처)와 `docs/redis-queue-contract.md`(큐 규약),
`docs/voice-sync-design.md`(voice 동기화)에 정리돼 있습니다.

## 처리 흐름

- `tts:jobs:urgent|high|normal|low`를 높은 우선순위부터 소비합니다. 유휴 시 blocking 없이 폴링하여, 깨어날 때 우선순위와 무관한 작업이 배달되지 않습니다.
- 합성 결과를 `tts:results`에 기록한 뒤 입력 메시지를 ACK합니다.
- 처리 중인 작업의 lease를 갱신합니다. 처리 중 죽은 processor가 남긴 pending 작업은 **재처리하지 않고 실패로 떨굽니다**(중복 합성·긴 대기 방지). 원본은 `tts:jobs:dead`에 감사용으로 남깁니다.
- Redis는 작업과 결과 전달에만 사용합니다.

## Voice

기본 voice는 Supertonic 모델에 포함된 `F1`~`F5`, `M1`~`M5`를 바로 사용합니다.

커스텀 voice는 이 프로젝트의 `custom_styles` 폴더에서 JSON 파일을 직접 읽습니다.
Gateway 업로드나 파일 동기화 도구는 필요하지 않습니다.

```text
custom_styles/Na-in-ae.json  -> voice: Na-in-ae
custom_styles/my-voice.json  -> voice: my-voice
```

파일을 추가하거나 교체하면 다음 작업 처리 전에 변경을 자동 감지하여 다시 읽습니다.
여러 model 서버를 운영한다면 필요한 JSON은 각 model 프로젝트의 `custom_styles`에 배포해야 합니다.

### voice 목록 동기화

worker는 작업을 소비하기 전에 자기 voice 목록을 Redis catalog(`tts:voice-catalog`)와 맞춰 봅니다.
Gateway로의 HTTP 호출은 없고, 모두 Redis로만 이뤄집니다.

- 살아있는 다른 worker가 없으면 → 자기 목록을 catalog로 기록합니다(기준이 됨).
- 살아있는 다른 worker가 있으면 → 자기 목록을 catalog와 비교해 **일치하면 기동, 다르면 로그를
  남기고 스스로 종료**합니다. 그래서 여러 worker를 운영할 땐 각 `custom_styles`가 동일해야 합니다.
- "살아있는 worker"는 잡 스트림 consumer group(`XINFO CONSUMERS`) 하나로만 판정합니다. 별도의
  heartbeat key는 없고, 잡 처리 상태와 같은 신호를 씁니다. 마지막 worker가 사라져도 catalog는
  남고, 다음에 기동하는 worker(active peer 0)가 자기 목록을 새 기준으로 덮어씁니다.
  이 catalog는 worker 간 일관성 검증용이며 Gateway는 읽지 않습니다(보이스 목록은 `GET /api/styles`로 노출).

구현: `redis_worker.py`의 `register_voice_catalog()` / `_other_active_worker_count()`.
규칙 상세는 Gateway `docs/voice-sync-design.md` 참고.

## 로컬 실행

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
.\.venv\Scripts\python.exe redis_worker.py
```

`.env`에는 Redis 접속 정보와 processor 설정을 둡니다.

## 테스트

```powershell
.\.venv\Scripts\python.exe tests\redis_contract_test.py
.\.venv\Scripts\python.exe tests\text_pipeline_test.py
```

pytest를 쓰지 않고 스크립트로 직접 실행합니다(워커 이미지에 테스트 전용 의존성을 넣지 않기
위해서입니다). 실패하면 exit code 1과 함께 기대값/실제값을 찍습니다.

모델도 ONNX도 Redis도 필요 없습니다 — `backend`는 ONNX를 함수 안에서 지연 로드하므로 import만으로는
런타임을 건드리지 않습니다. 두 파일이 보는 것은:

- `redis_contract_test.py` — Gateway↔worker 메시지 규약. 필드 이름이 하나만 어긋나도 연동이
  끊기는데 런타임에는 "unknown job" 로그만 남아 알아채기 어렵습니다.
  규약 문서는 [redis-queue-contract.md](../gateway-api/docs/redis-queue-contract.md)입니다.
- `text_pipeline_test.py` — 숫자·날짜·전화번호 읽기, 청크 분할, 파라미터 정리. 숫자 읽기는
  **합성은 성공하고 소리만 이상해지는** 종류라 테스트가 아니면 회귀를 잡기 어렵습니다.

실제 가중치와 ONNX 실행 경로는 별도 환경에서 확인해야 합니다.

## common/ — 다른 워커와 공유하는 코드

`common/` 의 파일은 **모든 TTS 워커 저장소가 똑같이 들고 있다.** 엔진과 무관한 것들이다 —
어느 엔진을 쓰든 "15000원"은 "일만오천원"으로 읽어야 하고 wav 인코딩도 같기 때문이다.

| 파일 | 내용 |
|---|---|
| `common/text_processing.py` | 합성 전 문장 정리 (숫자·날짜·전화번호 읽기, 문장 분리) |
| `common/queuetts_audio.py` | 오디오 인코딩 (출력 형식, 길이 계산, MIME) |

**여기를 고치면 모든 워커 저장소를 함께 고친다.** 한쪽만 고치면 두 워커가 같은 문장을 다르게
읽으면서도 예외도 로그도 없이 조용히 갈라진다. `redis_worker.py` 의 큐 프로토콜 함수 21개도
같은 이유로 공유되니 그 파일 상단의 목록을 함께 볼 것.

자세한 배경과 "왜 별도 저장소로 빼지 않았나"는 [common/README.md](common/README.md) 참고.

## Docker

CPU:

```powershell
docker compose -f docker-compose.cpu.yml up --build
```

GPU:

```powershell
docker compose -f docker-compose.gpu.yml up --build
```

두 Compose 구성 모두 현재 프로젝트의 `./custom_styles`를 컨테이너의
`/app/custom_styles`에 마운트합니다.

## 주요 환경변수

| 이름 | 기본값 | 설명 |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `REDIS_DB` | `0` | Redis DB 번호 |
| `REDIS_USERNAME` | 없음 | Redis ACL 사용자명 |
| `REDIS_PASSWORD` | 없음 | Redis 비밀번호 |
| `REDIS_URL` | 없음 | 지정하면 개별 Redis 연결 설정보다 우선 |
| `QUEUETTS_WORKER_ID` | 호스트명 기반 | processor 고유 ID |
| `QUEUETTS_JOB_STREAM` | `tts:jobs` | 우선순위별 입력 Stream의 prefix |
| `QUEUETTS_JOB_GROUP` | `tts-workers` | Consumer Group |
| `QUEUETTS_RESULT_STREAM` | `tts:results` | 결과 Stream |
| `QUEUETTS_RESULT_STREAM_MAX_LENGTH` | `2000` | 결과 Stream 의 근사 MAXLEN 상한. 정상 운영에서는 Gateway 가 ACK 직후 레코드를 지우므로 거의 비어 있고, 이 값은 **Gateway 가 소비하지 못하는 동안**의 안전장치다. 결과에 base64 오디오가 실리므로 `상한 x 평균 결과 크기` 를 Redis `maxmemory` 에 맞춰 조정할 것 |
| `QUEUETTS_DEAD_STREAM_MAX_LENGTH` | `10000` | `tts:jobs:dead` 의 근사 MAXLEN 상한. 감사용 원본 job 만 실려 오디오가 없다 |
| `QUEUETTS_VOICE_KEY_PREFIX` | `tts` | voice catalog Redis key prefix (Gateway `queuetts.voice.key-prefix`와 일치해야 함) |
| `QUEUETTS_WORKER_ACTIVE_IDLE_MS` | `30000` | 다른 worker를 "살아있음"으로 볼 idle 상한(ms). Gateway `queuetts.queue.worker-active-idle-ms`와 맞춤 |
| `QUEUETTS_POLL_INTERVAL_MS` | `50` | 할 일이 없을 때 우선순위 큐를 다시 훑기 전 잠깐 자는 폴링 간격(ms). blocking 읽기를 쓰지 않아 깨어날 때 우선순위와 무관한 작업이 배달되지 않음 |
| `QUEUETTS_ONNX_RUNTIME` | `cpu` | `cpu`, `gpu`, `auto` 중 하나 |
| `SUPERTONIC_MODEL` | `supertonic-3` | 사용할 모델 |
| `SUPERTONIC_VOICE` | `Na-in-ae` | 요청에 voice가 없을 때 사용할 기본 voice |
