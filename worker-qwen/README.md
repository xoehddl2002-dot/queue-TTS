# QueueTTS — Qwen worker

Qwen3-TTS **Base checkpoint 전용** Redis Streams worker다. Base에만 speaker encoder와
`generate_voice_clone`이 있으므로 CustomVoice/VoiceDesign checkpoint는 기동 시 거절한다.

확정된 소유권/API 설계는 Gateway의
[`docs/speakers-registry.md`](../gateway-api/docs/speakers-registry.md), wire 계약은
[`docs/redis-queue-contract.md`](../gateway-api/docs/redis-queue-contract.md)가 원본이다.

## style 소유권

- Gateway: `tts_style` DB 행과 참조 음성 원본
- Redis 일반 STRING: Gateway가 미러링한 raw 참조 음성 blob
- worker: 모델로 만든 prompt `.pt` 파생 캐시

`custom_voices/`는 원본 speaker 레지스트리가 아니다. 현재는 speakerName 기반 prompt 캐시
디렉터리이며 통째로 지워도 다음 TTS job에서 blob read-through로 복구된다. 여러 worker가 서로
다른 캐시 목록을 갖는 것도 정상이다.

## Job type

| type | 설명 |
|---|---|
| `tts` | `speakerName`/`referenceDigest`로 prompt를 찾고 캐시 미스면 `speakerBlobKey` GET 후 합성 |
| `styles` | 이 worker가 현재 캐시한 style 목록과 `batch_size` (진단용) |
| `speaker_forget` | speakerName의 로컬 파생 캐시 삭제 |

TTS 생성 파라미터는 `do_sample`, `temperature`, `top_p`, `top_k`, `repetition_penalty`,
`max_new_tokens`, `subtalker_dosample`, `subtalker_temperature`, `subtalker_top_p`,
`subtalker_top_k`만 `generate_voice_clone`에 전달한다. 값이 없는 키는 전달하지 않아 checkpoint의
`generate_config.json` 기본값을 보존한다. `speed`/`steps`는 Qwen에 대응 개념이 없어 한 번 경고 후
무시한다.

## 실행과 테스트

```powershell
python redis_worker.py
python tests/stub_pipeline_test.py
python tests/control_jobs_test.py
```

주요 환경 변수:

| 변수 | 기본값 |
|---|---|
| `QWEN_MODEL_ID` | `Qwen/Qwen3-TTS-12Hz-1.7B-Base` |
| `QWEN_DEVICE_MAP` | `cuda:0` |
| `QWEN_DTYPE` | `bfloat16` |
| `QUEUETTS_JOB_STREAM` / `QUEUETTS_JOB_GROUP` | `qwen:jobs` / `qwen-workers` |
| `QUEUETTS_RESULT_STREAM` | `tts:results` |
| `QUEUETTS_RESULT_STREAM_MAX_LENGTH` | `2000` |
| `QUEUETTS_DEAD_STREAM_MAX_LENGTH` | `10000` |
| `QUEUETTS_WORKER_ID` | `qwen-<hostname>` |
| `QUEUETTS_BATCH_SIZE` | `2` |

`QUEUETTS_RESULT_STREAM_MAX_LENGTH` 는 결과 Stream 의 근사 MAXLEN 상한이다. 정상 운영에서 결과
Stream 은 거의 비어 있고(Gateway 가 ACK 직후 레코드를 삭제한다) 이 상한은 **Gateway 가 소비하지
못하는 동안**에만 의미가 있다. 결과 레코드에는 base64 오디오가 통째로 실리므로 상한이 없으면
Gateway 정지 시간에 비례해 Redis 메모리가 무한히 늘어난다. `상한 x 평균 결과 크기` 가 최악의
점유량이므로 Redis `maxmemory` 에 맞춰 조정할 것.

실제 Base 가중치와 GPU 호출 경로는 별도 환경에서 검증해야 한다. 저장소 테스트는 모델 스텁으로
prompt 직렬화, read-through, cache invalidation, cache 삭제 payload, 생성 파라미터 전달을 검증한다.

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

## 배포와 디스크

`scripts/deploy_model_gpu_push.ps1` 로 빌드 → 태깅 → 예시 private registry 푸시를 한다.

```powershell
.\scripts\deploy_model_gpu_push.ps1 -Tag v1.0
```

GPU 이미지는 약 **10.7GB** 다. 대부분이 torch cu124 휠이 번들해 오는 CUDA 라이브러리
(`site-packages/nvidia/` — cudnn 976MB, cublas 528MB, cufft 281MB, ...)라 더 줄이기 어렵다.
베이스를 `cudnn-runtime` 에서 `base` 로 바꿔 apt 쪽 중복분 약 2.7GB 를 걷어낸 결과다
(자세한 이유는 `Dockerfile.gpu` 주석 참고). 이 크기 자체는 정상이지만, **배포마다 이미지
하나가 통째로 새로 생긴다**는 점 때문에 디스크 관리가 필요하다:

- 기본 빌드는 **레이어 캐시를 쓴다.** `requirements.txt`/`Dockerfile.gpu` 가 바뀌면 pip 레이어는
  알아서 무효화되고, 소스만 바뀌었으면 마지막 `COPY . .` 만 다시 굽는다.
  `-NoCache` 는 캐시가 실제로 오염됐을 때만 쓴다 — 매번 6GB 를 다시 받고, 직전 이미지를
  태그 없는 `<none>` 으로 남긴다.
- 스크립트는 **푸시 성공 후에** `docker image prune` 으로 그 `<none>` 들을 지우고 빌드 캐시를
  `-MaxBuildCache`(기본 20GB) 이하로 자른다. 푸시가 실패하면 정리하지 않으므로 `-SkipBuild` 로
  재시도할 수 있다.

### VHDX 는 지워도 줄지 않는다

Docker Desktop 의 WSL2 디스크(`%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx`)는 한 번 커지면
이미지를 지워도 자동으로 축소되지 않는다. 여기서 호스트 디스크가 차면 VM 안이 ENOSPC 가 되고
데몬이 죽는다. 한 번 sparse 로 바꿔 두면 이후로는 prune 한 만큼 실제 파일도 줄어든다:

```powershell
wsl --manage docker-desktop --set-sparse true
```

(Docker Desktop 을 종료한 상태에서 실행한다. 현재 사용량은 `docker system df` 로 확인.)
