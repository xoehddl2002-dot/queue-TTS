# Qwen clone worker 설계

## 1. 역할

이 worker는 Qwen3-TTS Base 모델의 voice clone 추론과 prompt 생성만 담당한다. 사용자 style의
원본 레지스트리는 Gateway가 소유하며 worker 로컬 파일은 파생 캐시다.

```text
Gateway tts_style ── raw audio mirror ──▶ Redis blob
       │                                    │ GET
       └─ TTS payload ─────────────────────▶ Worker ──▶ prompt .pt cache
```

## 2. 정확한 캐시 키

```text
(speaker_name, reference_digest, model_id, model_revision)
```

- `reference_digest`: audio SHA-256 + mode + ref_text를 Gateway가 묶은 값
- `model_revision`: `model.config._commit_hash`, 없으면 `unknown`

repo id만 비교하면 같은 Hugging Face repository의 가중치가 바뀌었을 때 낡은 prompt를 재사용할
수 있으므로 snapshot commit까지 비교한다. 텐서는 CPU로 저장하고
`torch.load(..., weights_only=True)`로 읽는다.

## 3. 제어 요청

등록과 참조 변경은 Gateway가 오디오를 검증하고 저장하므로 worker 제어 요청이 없다. worker는
TTS 캐시 미스에서만 blob을 디코딩하고 prompt를 만든다.

`speaker_forget`은 로컬 `.json`/`.pt`만 지운다. `styles`는 사용자 레지스트리가 아니라 이 worker의
캐시 상태를 보여주는 진단 요청이다.

## 4. TTS read-through

Gateway는 공개 `voice`를 이름으로 DB에서 해석하고 payload에 `speakerName`,
`referenceDigest`, `speakerBlobKey`, `speakerMode`, `speakerRefText`를 싣는다. worker는 메모리 → 디스크
순서로 prompt를 찾고, 없거나 digest/model revision이 다르면 blob을 GET해 재생성한다.

공개 `voice`와 `speakerName`은 같은 정규화된 등록 이름이며 worker 캐시 식별에도 사용한다.

## 5. 다중 worker와 voice catalog

각 worker의 prompt 캐시는 언제든 달라도 정상이다. clone style은 Gateway 소유이므로 Qwen worker는
이를 voice catalog에 publish하거나 다른 worker와 비교하지 않는다. 공유 볼륨도 필요 없다.

consumer group, 우선순위 polling, lease/XAUTOCLAIM, result stream 규칙은 Gateway
`redis-queue-contract.md`를 따른다.

## 6. 생성 파라미터

Gateway가 요청값 > style 기본값 순으로 병합해 non-null 키만 보낸다. worker는 허용된 10개 키만
`generate_voice_clone(**kwargs)`에 전달한다. 나머지 기본값은 Qwen의
`_merge_generate_kwargs`가 checkpoint 설정, 하드 기본값 순으로 결정한다.

ICL prompt 캐시는 오디오 encode와 speaker embedding 추출을 없애지만, 참조 code prefill 비용은
매 합성마다 남는다. 그래서 5~10초 참조 음성을 권장한다.
