# api-test 참조 음성

`api-test-qwen.http` 가 speaker 등록에 올려 보내는 참조 음성을 두는 곳이다.
`.http` 파일이 **이 디렉터리를 상대경로로** 가리키므로(`< ./api-test-assets/...`), 파일만 넣으면
경로를 고칠 필요 없이 바로 실행된다.

## 넣을 파일

| 파일명 | 쓰이는 곳 | 필수 |
|---|---|---|
| `ref.wav` | 등록(ICL / x-vector), 참조 교체 PATCH | O |
| `ref-replacement.wav` | 참조 음성 교체 PATCH — 앞의 것과 **다른 목소리/발화**여야 교체가 확인된다 | X (없으면 `ref.wav` 재사용) |

## 조건

- **길이 2~30초.** 권장은 5~10초다. 너무 짧으면 prompt 품질이 나쁘고, 너무 길면 느리다.
- `wav` / `flac` / `ogg` / `mp3`. 상한은 `queuetts.job.speaker-audio-max-bytes`(기본 16MB).
- 잡음·배경음악 없이 **한 사람만** 말하는 구간.
- ICL 모드(`x_vector_only_mode=false`)면 `.http` 의 `ref_text` 를 **이 음성에서 실제로 말한 문장
  그대로** 고쳐야 한다. 전사가 어긋나면 prompt 가 그만큼 나빠진다.
  전사를 맞추기 싫으면 `x_vector_only_mode=true` 로 등록하는 요청을 쓰면 된다(전사 불필요).

## 커밋하지 않는다

목소리는 개인 식별 정보이고 파일도 크다. `.gitignore` 가 이 디렉터리의 미디어를 제외하며,
README 와 `.gitignore` 만 추적된다. 팀에 공유할 참조 음성이 필요하면 저장소가 아니라
별도 자산 저장소를 쓸 것.
