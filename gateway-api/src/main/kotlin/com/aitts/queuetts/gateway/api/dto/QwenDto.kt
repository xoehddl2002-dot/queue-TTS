package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.time.OffsetDateTime

// QwenController(/api/qwen/speakers) 의 요청/응답 DTO. 설계는 docs/speakers-registry.md 참고.

/**
 * Qwen Base 클론 목소리 등록 요청.
 *
 * 공식 `create_voice_clone_prompt(ref_audio, ref_text, x_vector_only_mode)` 인자 중 파일이 아닌
 * 항목을 담는다. `ref_audio`는 JSON이 아니라 multipart file part로 받는다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateSpeakerRequest(
    /** 공개 보이스 키. 풀 안에서 유일하며 CRUD 경로와 합성 `voice`에 사용한다. */
    val name: String,
    /** Qwen `ref_text`. `x_vector_only_mode=false`(기본)에서 필수. */
    @field:JsonProperty("ref_text")
    @field:JsonAlias("refText")
    val refText: String? = null,
    /** Qwen `x_vector_only_mode`. false면 ref audio code + transcript를 쓰는 ICL 모드다. */
    @field:JsonProperty("x_vector_only_mode")
    @field:JsonAlias("xVectorOnlyMode")
    val xVectorOnlyMode: Boolean = false,
    /** 이 style 의 기본 언어. 합성 요청의 `lang` 이 우선한다. */
    val language: String? = null,
    val description: String? = null,
    /** 생성 파라미터 기본값. 받는 키는 [QwenSpeakerParams] 가 정의한다. */
    @field:JsonProperty("default_params")
    @field:JsonAlias("defaultParams")
    @field:JsonDeserialize(using = StrictQwenSpeakerParamsDeserializer::class)
    val defaultParams: QwenSpeakerParams? = null,
)

/**
 * 수정 요청. **주지 않은 필드는 건드리지 않는다.**
 *
 * `refText`/`xVectorOnlyMode`가 바뀌면 digest를 무효화하고 다음 합성에서 prompt를 다시 만든다.
 * 오디오 교체는 multipart PATCH의 `ref_audio` file part로 받는다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class UpdateSpeakerRequest(
    val name: String? = null,
    val language: String? = null,
    val description: String? = null,
    @field:JsonProperty("x_vector_only_mode")
    @field:JsonAlias("xVectorOnlyMode")
    val xVectorOnlyMode: Boolean? = null,
    @field:JsonProperty("ref_text")
    @field:JsonAlias("refText")
    val refText: String? = null,
    @field:JsonProperty("default_params")
    @field:JsonAlias("defaultParams")
    @field:JsonDeserialize(using = StrictQwenSpeakerParamsDeserializer::class)
    val defaultParams: QwenSpeakerParams? = null,
) {
    /** 참조가 바뀌는 요청인지. 이 경우 prompt digest를 다시 계산한다. */
    fun touchesReference(): Boolean = xVectorOnlyMode != null || refText != null
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SpeakerResponse(
    /** 조회·합성·저장을 아우르는 유일한 식별자. `(model, name)` 이 곧 기본키다. */
    val name: String,
    val model: String,
    /** 등록 때 Qwen prompt에 적용된 `x_vector_only_mode`. */
    @field:JsonProperty("x_vector_only_mode")
    val xVectorOnlyMode: Boolean,
    val language: String? = null,
    val description: String? = null,
    @field:JsonProperty("ref_text")
    val refText: String? = null,
    val durationS: Double? = null,
    val sampleRate: Int? = null,
    val audioFormat: String? = null,
    /** prompt 캐시 무효화 키. 디버깅과 job payload 추적에 쓴다. */
    val referenceDigest: String,
    val defaultParams: QwenSpeakerParams = QwenSpeakerParams(),
    /** CRUD는 prompt를 만들지 않으므로 항상 false. 기존 응답 계약을 유지하기 위해 남겨 둔다. */
    val promptRebuilt: Boolean = false,
    /** CRUD는 worker를 왕복하지 않으므로 null. 기존 응답 계약을 유지하기 위해 남겨 둔다. */
    val workerId: String? = null,
    val createdAt: OffsetDateTime? = null,
    val updatedAt: OffsetDateTime? = null,
    val referenceUpdatedAt: OffsetDateTime? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SpeakerListResponse(
    val speakers: List<SpeakerResponse>,
    val model: String,
)

/**
 * 등록된 참조 음성 원본.
 *
 * 이것만 JSON 이 아니다 — 본문이 오디오 바이트 그대로이고, 나머지 필드는 [SpeakerResponse] 처럼
 * 직렬화되는 것이 아니라 컨트롤러가 응답 헤더를 채우는 데 쓴다.
 *
 * `equals`/`hashCode` 는 쓰지 않는다. [content] 가 ByteArray 라 data class 가 만들어 주는 것은
 * 참조 비교이고, 이 타입은 비교 대상이 된 적이 없다.
 */
data class SpeakerAudio(
    val content: ByteArray,
    val fileName: String,
    val mediaType: String,
)

/**
 * 삭제 결과. 삭제는 한 가지뿐이라 어떤 동작이었는지 알려 줄 것이 없다 — 예전에는 archive 와
 * purge 를 구분하려고 `action` 을 실었다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DeleteSpeakerResponse(
    val name: String,
    /** 파생 캐시 정리(`speaker_forget`)에 응답한 worker. 정리에 실패해도 삭제는 끝난다. */
    val workerId: String? = null,
)

/**
 * speaker 하나에 저장해 두는 Qwen 생성 파라미터 기본값.
 *
 * **모든 필드가 nullable 이고 null 은 "지정하지 않음"이다.** NON_NULL 직렬화라 지정하지 않은 키는
 * 저장 JSON 에도 job payload 에도 아예 나타나지 않고, 그러면 worker 가 체크포인트의
 * `generate_config.json` 기본값을 쓴다 — **Gateway 는 모델 기본값을 알 필요가 없다.**
 *
 * 예전에는 `Map<String, Any?>` 에 아무 키나 담고 별도 allowlist 로 걸렀다. 그러면 "이 speaker 가
 * 뭘 받는지"를 소스에서 읽을 수 없고 값 타입도 검사되지 않는다.
 *
 * **요청으로 받을 때는 모르는 키가 400**이다([StrictQwenSpeakerParamsDeserializer]). 반대로 이미
 * 저장된 행을 읽을 때는 관대하다 — 규격에서 빠진 키가 남아 있어도 조회는 되어야 한다.
 *
 * 필드 구성은 [QwenJobPayload] 의 생성 파라미터와 같아야 한다 — 여기 저장한 기본값을 그 payload 로
 * 흘려보내기 때문이다([QwenJobPayload.withDefaults]). 어긋나면 `QwenDtoTests` 가 잡는다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class QwenSpeakerParams(
    @field:JsonProperty("do_sample")
    @field:JsonAlias("doSample")
    val doSample: Boolean? = null,
    val temperature: Double? = null,
    @field:JsonProperty("top_p")
    @field:JsonAlias("topP")
    val topP: Double? = null,
    @field:JsonProperty("top_k")
    @field:JsonAlias("topK")
    val topK: Int? = null,
    @field:JsonProperty("repetition_penalty")
    @field:JsonAlias("repetitionPenalty")
    val repetitionPenalty: Double? = null,
    @field:JsonProperty("max_new_tokens")
    @field:JsonAlias("maxNewTokens")
    val maxNewTokens: Int? = null,
    /** `subtalker_*` 는 2단계 디코더용이다. 이름만 다르고 뜻은 위 항목들과 같다. */
    @field:JsonProperty("subtalker_dosample")
    @field:JsonAlias("subtalkerDosample")
    val subtalkerDosample: Boolean? = null,
    @field:JsonProperty("subtalker_temperature")
    @field:JsonAlias("subtalkerTemperature")
    val subtalkerTemperature: Double? = null,
    @field:JsonProperty("subtalker_top_p")
    @field:JsonAlias("subtalkerTopP")
    val subtalkerTopP: Double? = null,
    @field:JsonProperty("subtalker_top_k")
    @field:JsonAlias("subtalkerTopK")
    val subtalkerTopK: Int? = null,
)

/**
 * 요청 본문에서 [QwenSpeakerParams] 를 읽을 때만 쓰는 엄격 리더 — **모르는 키는 예외**다.
 *
 * 타입에 `@JsonIgnoreProperties(ignoreUnknown = false)` 를 다는 것으로는 안 된다. 그 값은 Jackson
 * 의 기본값이라 "무시를 강제하지 않는다"는 뜻일 뿐이고, 실제 거절 여부는
 * `FAIL_ON_UNKNOWN_PROPERTIES` 가 정하는데 Spring Boot 는 그걸 꺼 둔다. 그래서 읽는 지점에서
 * 명시적으로 켠다.
 *
 * 저장된 행을 읽는 경로(`QwenService.readParams`)는 이 리더를 쓰지 않는다 — 그쪽은 관대해야 한다.
 */
class StrictQwenSpeakerParamsDeserializer : JsonDeserializer<QwenSpeakerParams>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): QwenSpeakerParams {
        val mapper = parser.codec as ObjectMapper
        val node: JsonNode = mapper.readTree(parser)
        return mapper.readerFor(QwenSpeakerParams::class.java)
            .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .readValue(node)
    }
}
