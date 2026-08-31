# common — 워커 공통 모듈

**이 디렉터리의 파일은 모든 TTS 워커 저장소가 똑같이 들고 있다.**
아래 두 곳의 내용은 글자 하나까지 같아야 한다.

```
worker-supertonic/common/
worker-qwen/common/
```

## 규칙

**여기를 고치면 모든 워커 저장소를 함께 고친다.**

한쪽만 고치면 두 워커가 같은 문장을 다르게 읽거나 다른 오디오를 내놓는다. 예외도 로그도 없이
조용히 갈라지므로 운영 중에는 알아채기 어렵고, "왜 이 워커만 발음이 다르지" 를 한참 뒤에
쫓게 된다.

바꾼 뒤에는 양쪽에서 확인한다:

```powershell
# Supertonic 저장소에서
.\.venv\Scripts\python.exe tests\text_pipeline_test.py

# 두 복사본이 실제로 같은지
python -c "import pathlib,hashlib; [print(p, hashlib.sha256(p.read_bytes()).hexdigest()[:12]) for p in pathlib.Path('.').glob('common/*.py')]"
```

## 파일

| 파일 | 내용 |
|---|---|
| `text_processing.py` | 합성 전 문장 정리 — 숫자·날짜·전화번호 읽기, 문장 분리, 반복 패턴 |
| `queuetts_audio.py` | 오디오 인코딩 — 출력 형식 변환, 길이 계산, MIME 타입 |

두 파일 모두 **엔진과 무관하다.** 어느 TTS 엔진을 쓰든 "15000원"은 "일만오천원"으로 읽어야 하고
wav 인코딩도 같다. 그래서 공통이다.

반대로 **엔진마다 달라야 하는 것은 여기 두지 않는다** — voice 규격, 생성 파라미터, prompt 캐시,
모델 로딩은 각 워커의 `backend.py` / `qwen_backend.py` 몫이다.

## 왜 저장소를 따로 만들지 않았나

공용 패키지 저장소를 만들면 복제가 사라지지만, 저장소 하나를 더 만들고 버전을 매기고 두 워커의
`requirements.txt` 와 Dockerfile 을 거기에 묶어야 한다. **워커가 둘뿐인 지금은 그 비용이 얻는 것보다
크다.** 게다가 이 두 파일은 만든 뒤 거의 바뀌지 않았다(각각 커밋 1회).

**세 번째 워커가 생기면 이 계산이 바뀐다.** "두 곳 고치기"는 감당되지만 "세 곳 고치기"는 빠뜨릴
확률이 확 오른다. 그때 별도 패키지로 빼는 것을 다시 검토할 것.

## 여기 없지만 실은 공유되는 것

`redis_worker.py` 의 **큐 프로토콜 함수 21개(약 374줄)도 두 워커가 글자 하나 안 틀리고 같다** —
`reclaim_pending`, `job_loop`, `read_new_jobs`, `flush_batch_results`, `refresh_lease` 등이다.

이것들은 엔진별 로직과 한 파일에 뒤섞여 있고 모듈 전역(`r`, `JOB_GROUP`, `WORKER_ID` 등)에
의존해서, 옮기려면 설정 주입 구조부터 바꿔야 한다. 위험 대비 이득이 맞지 않아 그대로 두었다.
대신 `redis_worker.py` 상단에 어떤 함수가 공유되는지 적어 두었으니, **그 목록에 있는 함수를
고칠 때도 양쪽을 함께 고쳐야 한다.**
