# Worker voice 목록 동기화 경계

voice catalog는 **worker 로컬 파일이 원본인 풀**의 worker 간 일관성 검증 장치다. Gateway는
catalog를 읽지 않으며 worker 존재 여부도 catalog가 아니라 job stream consumer group의
`XINFO CONSUMERS`로 판단한다.

## 파일 기반 voice pool

Supertonic처럼 배포된 파일에서 voice 목록을 만드는 풀은 worker마다 같은 파일을 봐야 한다.
이런 풀은 시작 시 자기 목록을 Redis catalog에 기록하고, 살아 있는 peer가 있으면 기존 목록과
비교한다. 불일치하면 잘못된 voice로 합성하는 대신 기동을 중단한다.

기본 키는 다음과 같다.

| Key | 타입 | 기록 주체 | 내용 |
|---|---|---|---|
| `tts:voice-catalog` | HASH | worker | 검증된 voice 목록 |
| `tts:voice-catalog:state` | STRING | worker | `ready` |
| `tts:voice-catalog:lock` | STRING (NX/EX) | worker | 동시 기동 직렬화 락 |

## Gateway 소유 Qwen clone style

Qwen clone style은 catalog 대상이 아니다.

- 원본 메타데이터와 참조 음성은 Gateway의 `tts_style` 테이블이 소유한다.
- 참조 음성은 Redis raw STRING blob으로 미러링한다.
- 각 Qwen worker의 `custom_voices/`에는 prompt `.pt`와 캐시 메타만 있으며 언제 비어도 정상이다.
- worker가 TTS job을 받으면 `speakerBlobKey`를 GET해 read-through로 자기 캐시를 만든다.
- worker별 캐시 목록이 다른 것은 정상이며, 그 차이로 종료하면 안 된다.

따라서 Qwen worker는 시작 시 clone style 목록을 voice catalog에 publish하거나 catalog와 비교하지
않는다. `styles` 제어 요청이 돌려주는 목록도 사용자 레지스트리가 아니라 해당 worker가 현재
캐시한 style의 진단용 목록이다.

## 사용자 조회와 변경

- `GET /api/styles`: 모든 풀의 읽기 전용 합성 view. Qwen 항목은 worker가 아니라 DB에서 읽는다.
- `/api/qwen/speaker`: Qwen clone speaker CRUD. 등록/참조 변경은 Gateway 단독이며 삭제만
  `speaker_forget` 제어 요청을 사용한다.
- Qwen job의 공개 `voice`는 등록된 이름이며 Gateway가 접수 시 `(model, name)`으로 해석한다.

상세 소유권과 실패/삭제 규칙은 [speakers-registry.md](speakers-registry.md), wire payload는
[redis-queue-contract.md](redis-queue-contract.md)를 따른다.
