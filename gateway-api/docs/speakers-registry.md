# Speaker 레지스트리 (보이스 클로닝)

클론 보이스(style)를 **Gateway가 소유**하고 worker는 파생 캐시로 두는 설계다.

관련 문서: [redis-queue-contract.md](redis-queue-contract.md)(큐 규약) ·
[voice-sync-design.md](voice-sync-design.md)(voice 동기화) ·
워커 [design.md](../../worker-qwen/docs/design.md)

## 1. 왜 Gateway가 소유하는가

클론 style의 참조 음성은 **사용자가 올린, 재생성할 수 없는 데이터**다. 그것이 GPU worker
컨테이너의 볼륨에만 있으면 볼륨이 사라질 때 복구할 방법이 없다. Gateway에는 이미 Postgres가
있으므로 원본을 거기 둔다.

따라오는 이득이 둘 더 있다.

- **공유 볼륨이 필요 없다.** worker를 여러 대 띄워도 각자 같은 원본에서 자기 캐시를 만든다.
  등록 요청이 한 대만 받는 문제도, voice catalog 불일치로 자가 종료하는 문제도 사라진다.
- **목록 조회가 큐를 타지 않는다.** DB에서 바로 답하므로 worker가 죽어 있어도 목록이 나온다.

worker가 만들 수 없는 것이 하나 있다. **prompt(`.pt`)는 모델이 있어야 만들어진다** — 화자
임베딩 추출과 speech tokenizer encode는 GPU worker의 일이다. 그래서 원본은 Gateway, prompt는
worker라는 분업이 된다.

```
Gateway (원본)                  Redis                        Worker (파생 캐시)
 tts_style (Postgres)  ──mirror──▶ qwen:style:blob:{name} ──GET──▶ prompt(.pt) 생성 후 로컬 보관
 GET /api/styles ← DB                                           캐시 미스면 blob 을 읽어 재생성
```

## 2. 데이터 모델

style 하나 = 행 하나다. **버전 이력은 두지 않는다**(§7 참고).

### `tts_style`

기본키는 **`(model, name)`** 이다. 공개 보이스 키와 저장 키가 같아서 "이름으로 찾아 내부 id 로
쓴다" 같은 왕복이 없다. 대신 rename 이 기본키를 바꾸는 UPDATE 가 되므로 §7 의 트랜잭션 순서를
지켜야 한다. name 은 바뀔 수 있지만 job 이력은 처음부터 name 문자열을 남기므로 잃는 추적은 없다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `model` | text PK | 소속 엔진 풀. 클로닝을 지원하는 풀만 (`qwen`) |
| `name` | text PK | 공개 보이스 키이자 저장 키. **변경 가능** |
| `language` | text NULL | 이 style의 기본 언어. 합성 요청의 `lang`이 우선 |
| `description` | text NULL | |
| `mode` | text | `icl` \| `x_vector` |
| `ref_text` | text NULL | `mode=icl`이면 필수 |
| `audio` | bytea | 참조 음성 원본 |
| `audio_sha256` | text | 오디오 자체의 해시 |
| `reference_digest` | text | **prompt 캐시 무효화 키** (§3) |
| `audio_format` | text | `wav` / `flac` / `ogg` / `mp3` |
| `sample_rate` | int4 | |
| `duration_s` | float8 | |
| `default_params` | jsonb | 생성 파라미터 기본값 (§5) |
| `created_by` | text NULL | 접수한 API Key id |
| `created_at` / `updated_at` | timestamptz | |
| `reference_updated_at` | timestamptz | 참조가 마지막으로 바뀐 시각 |

`reference_updated_at`을 따로 두는 이유: 버전 이력이 없으므로 "언제부터 목소리가 달라졌나"를
되짚을 근거가 이것뿐이다.

DDL은 `src/main/resources/db/migration/V1__initial_schema.sql`에 있다 (Flyway 관리).

## 3. `reference_digest` — prompt 캐시를 무엇으로 무효화하는가

prompt는 참조 오디오만이 아니라 **`mode`와 `ref_text`에도 종속**된다.

- `mode`가 `x_vector`면 `ref_code`가 없고 `icl`이면 있다.
- `icl`은 참조 텍스트를 조건으로 함께 넣는다.

따라서 오디오 해시만으로 캐시를 무효화하면, 오디오는 그대로 두고 `ref_text`의 오타만 고친
경우에 **낡은 prompt를 그대로 쓰게 된다.** 버전을 두지 않아 in-place 수정이 유일한 경로이므로
이 경우가 오히려 흔하다. 그래서 셋을 묶는다.

```
reference_digest = sha256(audio_sha256 + "\n" + mode + "\n" + (ref_text ?: ""))
prompt 캐시 키    = (speaker_name, reference_digest, model_id, model_revision)
```

`model_revision`은 Hugging Face 스냅샷 커밋이다. repo id만으로 비교하면 업스트림이 같은
repo의 가중치를 갱신했을 때 캐시가 조용히 낡는다.

## 4. API — `QwenController` (`/api/qwen/**`)

클론 style은 Qwen 전용 개념이라 **별도 컨트롤러에 둔다.** 공용 `/api/styles`에 얹으면 한
엔드포인트가 풀마다 다르게 동작하게 된다 — 클론 style에는 `mode`/`referenceDigest`가 있고
Supertonic 스타일 벡터에는 없다. 경로를 나누면 그 비대칭이 사라진다.

모두 인증이 필요하다(`X-API-Key`). 대상 worker 풀은 `queuetts.queue.voice-model`(기본 `qwen`)이며,
경로가 이미 엔진을 가리키므로 `?model=`은 받지 않는다.

| | 동작 | worker 왕복 |
|---|---|---|
| `POST /api/qwen/speaker` | 등록 | X |
| `GET /api/qwen/speaker` | 목록 | X |
| `GET /api/qwen/speaker/{name}` | 단건 | X |
| `PATCH /api/qwen/speaker/{name}` | 수정 | X |
| `DELETE /api/qwen/speaker/{name}` | 삭제 (행·blob·worker 캐시) | O |

기존 `GET /api/styles`는 **모든 풀을 합친 읽기 전용 뷰**로 그대로 둔다. qwen 항목은 이제 worker에
묻지 않고 DB에서 채우며, 기존 응답 모양(`name`/`kind`/`model`)을 유지한다.

### `POST /api/qwen/speaker`

이 API는 일반적인 발화 style을 등록하는 것이 아니라 **Qwen3-TTS Base에서 재사용할 clone
목소리**를 등록한다. 공개 필드는 Qwen의 원본 함수와 같은 의미다.

```python
model.create_voice_clone_prompt(
    ref_audio=ref_audio,
    ref_text=ref_text,
    x_vector_only_mode=x_vector_only_mode,
)
```

- `ref_audio`: 필수. `multipart/form-data`의 file part로 업로드한다. Gateway 제한은 16 MiB다.
- `ref_text`: `x_vector_only_mode=false`(기본 ICL)일 때 필수다.
- `x_vector_only_mode=true`: 화자 embedding만 쓰므로 `ref_text`를 요구하지 않지만 clone 품질이
  낮아질 수 있다.
- `name`, `description`, `language`, `default_params`는 Qwen prompt 생성 인자가 아니라
  Gateway registry 메타데이터와 합성 기본값이다.

Gateway가 `wav`/`flac`/`ogg`/`mp3` 헤더와 2~30초 길이를 검증하고 메타데이터를 채운다. prompt는
등록 때 만들지 않으며, 첫 합성을 받은 Qwen worker가 Redis blob을 읽어 lazy 생성한다.

```bash
curl -X POST http://localhost:8080/api/qwen/speaker \
  -H "X-API-Key: ..." \
  -F "name=나인애" \
  -F "ref_audio=@/path/to/reference.wav;type=audio/wav" \
  -F "ref_text=참조 음성에서 실제로 말한 문장을 그대로 적습니다." \
  -F "x_vector_only_mode=false" \
  -F "language=ko" \
  -F "description=상담 안내용" \
  -F 'default_params={"temperature":0.85}'
```

응답:

```json
{
  "name": "나인애",
  "model": "qwen",
  "x_vector_only_mode": false,
  "language": "Korean",
  "ref_text": "참조 음성에서 실제로 말한 문장을 그대로 적습니다.",
  "durationS": 6.5,
  "sampleRate": 24000,
  "audioFormat": "wav",
  "referenceDigest": "9f2c…",
  "defaultParams": { "temperature": 0.85 },
  "promptRebuilt": false,
  "createdAt": "2026-08-19T05:12:44Z"
}
```

참조 음성은 2~30초를 허용하고 5~10초를 권장한다. `icl` 모드는 참조 길이에 비례하는 비용을
**매 합성마다** 내기 때문이다(§6).

### `PATCH /api/qwen/speaker/{name}`

한 엔드포인트로 전부 받고 **Gateway가 내부에서 판단한다.** 참조가 바뀌면 digest를 무효화하고
새 prompt는 다음 합성에서 만든다.

| 바꾸는 필드 | prompt 재생성 | 처리 |
|---|---|---|
| `language` `description` `default_params` | 불필요 | **Gateway 단독**, 즉시 |
| `name` | 다음 합성에서 lazy 생성 | 공개 키·Redis mirror 키 변경 |
| `ref_text` `x_vector_only_mode` `ref_audio` | 다음 합성에서 lazy 생성 | Gateway 검증 → blob/DB 갱신 |

- 메타데이터만 바꿀 때는 기존처럼 `application/json` PATCH를 쓴다.
- `ref_audio`를 교체할 때는 `multipart/form-data` PATCH를 쓴다. 나머지 필드는 같은 form
  field로 함께 보낼 수 있다.

```bash
curl -X PATCH http://localhost:8080/api/qwen/speaker/나인애 \
  -H "X-API-Key: ..." \
  -F "ref_audio=@/path/to/replacement.wav;type=audio/wav" \
  -F "ref_text=교체한 참조 음성의 정확한 전사"
```

### `DELETE /api/qwen/speaker/{name}`

행·Redis blob·worker 파생 캐시(`speaker_forget`)를 **모두 지운다. 되돌릴 수 없다.**

예전에는 기본이 archive(`status`를 바꿔 목록에서만 감추기)였고 `?purge=true`라야 실제로 지웠다.
한 엔드포인트에 동작이 둘이면 호출자가 "지웠는데 왜 아직 있나"를 매번 따져야 하고, 그 값을 읽는
곳도 목록 필터 하나뿐이라 개념째 걷어냈다. `status` 컬럼과 `?includeArchived`, PATCH 의 `status`
필드도 함께 사라졌다.

job 이력 추적은 끊기지 않는다 — 이력은 speaker 행이 아니라 `voice` **문자열**을 남기므로 지운
speaker 로 만든 job 도 이름 그대로 조회된다. 대신 참조 음성 원본은 사라지므로 **보관은 클라이언트
책임이다**(§7 과 같은 이유다).

응답은 `{name, workerId}`다. `workerId`는 캐시 정리에 응답한 worker이며, 정리에 실패해도 삭제는
그대로 끝난다 — prompt 는 파생물이라 참조하는 speaker 가 없으면 쓰이지 않는다.

## 5. `default_params` — Base 생성 파라미터

허용 키는 아래뿐이며, 그 외는 400이다.

| 키 | 모델 기본값 | 비고 |
|---|---|---|
| `do_sample` | `true` | |
| `temperature` | `0.9` | 높을수록 변동 큼 |
| `top_p` / `top_k` | `1.0` / `50` | |
| `repetition_penalty` | `1.05` | 반복 코드 억제 |
| `max_new_tokens` | `8192` | 생성 코덱 토큰 상한 |
| `subtalker_dosample` / `subtalker_temperature` / `subtalker_top_p` / `subtalker_top_k` | `true` / `0.9` / `1.0` / `50` | **12Hz 토크나이저 전용** |

우선순위는 **요청 > style `default_params` > 모델 기본값**이다.

셋째 단계는 Gateway가 알 필요가 없다. worker가 값을 넘기지 않으면
`Qwen3TTSModel._merge_generate_kwargs`가 체크포인트의 `generate_config.json`을, 그것도 없으면
자기 하드 기본값을 쓴다. 그래서 **미지정 필드는 payload에서 아예 빼서 보낸다.**

`speed` / `steps`는 Qwen에 대응 개념이 없다. 받아도 무시되며 worker가 프로세스당 한 번 경고한다.

## 6. job과의 연결

공개 API는 `voice`에 등록된 **이름만** 받는다. Gateway가 기본키 `(model, name)`으로 조회하고
worker payload에도 같은 이름과 digest를 싣는다. 별도의 식별자는 존재하지 않는다.

```
클라이언트   {"model": "qwen", "voice": "나인애", "text": "...", "temperature": 0.8}
     ↓ Gateway 가 해석 + default_params 병합
worker payload {"speakerName": "나인애", "referenceDigest": "9f2c…",
                "speakerBlobKey": "qwen:style:blob:나인애",
                "speakerMode": "icl", "speakerRefText": "참조 음성의 실제 문장",
                "voice": "나인애", "text": "...", "temperature": 0.8, ...}
```

이름이 공개 키이므로 이름을 바꾸면 호출자도 새 이름을 사용해야 한다. `voice`는 정규화된 등록
이름으로 이력(`tts_job_generation_history.voice`)에 남는다.

worker는 `(speakerName, referenceDigest)`로 자기 prompt 캐시를 찾고, 없으면 `speakerBlobKey`를 GET해
`speakerMode`/`speakerRefText` 조건으로 만들어 쓴다(read-through). 이 두 필드가 필요한 이유는 blob이
오디오 raw bytes뿐이고 prompt는 mode와 ref_text에도 종속되기 때문이다. 그래서 **어느 worker가
job을 가져가도 동작하며**, 캐시를 통째로 지워도 스스로 복구한다.

## 7. 버전을 두지 않기로 한 결과

참조 변경은 in-place다. 아래 셋은 설계상 받아들인 것이다.

- `POST /api/jobs/{id}/requeue`는 그때가 아니라 **현재 참조**로 재생성한다. 같은 job을 재실행해도
  음성이 달라질 수 있다.
- 잘못 덮어쓰면 이전 참조를 되돌릴 수 없다. **원본 음성 보관은 클라이언트 책임이다.**
- 처리 중인 job과 수정이 겹치면 그 job은 새 참조로 나갈 수 있다.

## 8. Redis blob 키

| 키 | 타입 | 내용 |
|---|---|---|
| `{queuetts.queue.style-blob-prefix}:style:blob:{speakerName}` | STRING (binary) | 참조 음성 원본 바이트 |

**키 이름은 Gateway가 정해 payload로 알려준다.** worker가 같은 규칙으로 키를 조립하게 하면
양쪽이 맞아야 하는 설정이 하나 더 늘어난다. worker는 받은 키를 GET 할 뿐이다.

**TTL을 두지 않는다.** worker의 캐시 미스는 시점을 예측할 수 없으므로(새 worker 투입, 캐시
삭제, 재시작) 만료된 blob은 복구 불가능한 실패가 된다. DB 행의 거울로 두고 style이 바뀔 때
덮어쓰며 삭제 때 함께 지운다.

base64가 아니라 **raw 바이트로 넣는다.** 33% 오버헤드가 없고, 스트림 엔트리가 아니라 일반
키이므로 정확히 갱신·삭제할 수 있다.

메모리는 style 수에 비례한다 — 10초 24kHz 16bit mono가 약 470KB이므로 style 500개면 약 250MB다.
이보다 커지면 오브젝트 스토리지로 옮기고 키에는 참조만 두는 편이 낫다.

> **왜 job 스트림에 싣지 않는가.** result 스트림은 Gateway가 처리 후 ack + delete 하지만,
> job 스트림은 worker가 XACK만 하고 Gateway가 `MAXLEN 100000`으로 trim한다 — **개수** 기준이라
> 바이트를 보지 않는다. 등록은 드문 작업이라 640KB짜리 엔트리가 사실상 영원히 남는다.

## 9. worker 캐시 삭제 요청

`styles`와 같은 일회성 request/reply다(`jobId`가 `req_` 접두사).

| type | payload | 응답 |
|---|---|---|
| `styles` | (빈 객체) | worker가 **캐시하고 있는** style 목록 + `batch_size`. 사용자 목록이 아니라 진단·표시용이다 |
| `speaker_forget` | `{speakerName}` | `{speakerName, applied, workerId}` |

등록/참조 변경에는 제어 요청이 없다. Gateway가 오디오를 검증하고, worker는 TTS 캐시 미스에서만
blob을 읽어 prompt를 만든다. `speaker_forget`은 삭제 때 남은 파생 캐시를 정리하기 위한 요청이다.

### 등록 순서

```
1. 요청 모양과 오디오 검증 (이름, mode/ref_text 조합, 형식, 길이)
2. duration_s / sample_rate / audio_format 및 reference_digest 계산
3. tts_style 행 INSERT/UPDATE (이름 유니크 판정)
4. SET {prefix}:style:blob:{speakerName} = raw audio
5. mirror 쓰기가 실패하면 DB 원본을 유지하고 첫 합성의 resolver가 이름 키를 다시 채운다
```

수정에서 참조와 메타를 함께 바꿀 때는 **두 UPDATE 를 한 트랜잭션으로 묶는다.** 나눠 쓰면 뒤가
실패했을 때 참조만 바뀌고 이름은 예전 값인 반쪽 상태가 남는다.

**순서도 계약이다.** 참조 UPDATE 가 먼저, 메타 UPDATE 가 나중이다. name 이 기본키라 메타 UPDATE
가 곧 rename 이고, 참조 UPDATE 는 아직 옛 이름을 WHERE 키로 쓴다. 뒤집으면 참조 쓰기가 0행을
갱신하고 조용히 지나간다.

이름 유일성의 최종 판정은 기본키 `tts_style_pkey` 다. 사전 조회와 INSERT 사이에 다른 요청이 같은
이름을 선점하면 `DuplicateKeyException` 이 나며, 이를 **409** 로 옮긴다. rename 충돌도 같은 경로다.

### voice catalog와의 관계

클론 style은 Gateway가 소유하므로 **worker 간 voice catalog 일관성 검증 대상이 아니다.** worker의
캐시는 언제 비어 있어도 정상이고 read-through로 채워진다. catalog 규칙은 Supertonic처럼 파일로
voice를 관리하는 풀에만 남는다.
