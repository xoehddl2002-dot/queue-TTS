# custom_voices (prompt cache)

이 디렉터리는 더 이상 사용자 관리 speaker 원본 저장소가 아니다. Gateway가 소유한 Qwen clone
style의 **worker 로컬 파생 캐시**다.

style 하나가 준비되면 다음 파일이 생긴다.

```text
custom_voices/
  speaker_<name-sha256>.json   # speakerName, referenceDigest, mode, refText, language
  speaker_<name-sha256>.pt     # CPU prompt tensors
```

참조 음성 원본은 이곳에 저장하지 않는다. Gateway DB와 Redis raw blob이 원본/전달 경로다.
이 디렉터리는 삭제해도 안전하며, 다음 TTS cache miss 때 `speakerBlobKey` read-through로 다시 생성된다.

파일명에는 경로 안전성을 위해 name의 SHA-256이 들어가고 JSON 안에 실제 speakerName이 저장된다.
파일을 손으로 만들거나 worker에 직접 등록하지 않는다. 공개 CRUD는 Gateway의
`/api/qwen/speaker`이며, purge의 파생 캐시 정리에만 `speaker_forget`을 사용한다. 여러 worker가
서로 다른 캐시 내용을 가져도 정상이고 clone style은 voice catalog 일관성 검증 대상이 아니다.
