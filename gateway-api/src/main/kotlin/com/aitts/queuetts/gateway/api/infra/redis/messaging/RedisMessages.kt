package com.aitts.queuetts.gateway.api.infra.redis.messaging

import com.aitts.queuetts.gateway.api.dto.AudioJobResult
import com.aitts.queuetts.gateway.api.dto.JobError
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.OffsetDateTime

// Redis Streams 로 주고받는 메시지 계약. 워커의 redis_messages.py 와 대칭을 이룬다.
// (보냄: 우선순위별 job 스트림 / 받음: result 스트림)

/**
 * 게이트웨이가 우선순위별 job 스트림에 XADD 하는 발행 메시지.
 *
 * payload 는 tts 잡이면 [JobPayload], `styles` 처럼 인자가 없는 제어 요청이면 비어 있고,
 * `speaker_forget` 처럼 인자가 있는 제어 요청이면 그 요청의 DTO 다. 어차피 JSON 문자열
 * 한 필드로 나가므로 타입을 하나로 묶지 않는다.
 */
data class RedisJobMessage(
    val jobId: String,
    val type: String,
    val payload: Any?,
    val priority: String,
    val source: String?,
    val enqueuedAt: OffsetDateTime,
) {
    /**
     * Redis Stream 필드 맵으로 직렬화한다. 바깥은 평면 필드, payload 만 JSON 문자열이다.
     *
     * payload 가 [com.aitts.queuetts.gateway.api.dto.JobPayload] 여도 **worker 가 보는 JSON 은 예전
     * 그대로 평면**이다 — 모델별 타입은 Gateway 안의 구분이고, 각 구현체가 자기 엔진의 필드만
     * NON_NULL 로 내보내므로 worker 는 자기 파라미터만 담긴 객체를 받는다. 이 경계 덕분에 이
     * 구조 변경으로 워커를 고치지 않는다.
     */
    fun toRedisFields(objectMapper: ObjectMapper): Map<String, String> = buildMap {
        put("jobId", jobId)
        put("type", type)
        put("payload", objectMapper.writeValueAsString(payload ?: emptyMap<String, Any?>()))
        put("priority", priority)
        source?.let { put("source", it) }
        put("enqueuedAt", enqueuedAt.toString())
    }
}

/** worker 가 result 스트림으로 돌려주는 진행/완료 메시지. */
data class RedisResultMessage(
    @field:JsonAlias("job_id")
    val jobId: String,
    @field:JsonAlias("worker_id")
    val workerId: String = "unknown-worker",
    @field:JsonAlias("batch_id")
    val batchId: String? = null,
    @field:JsonAlias("status")
    val state: String? = null,
    @field:JsonAlias("started_at")
    val startedAt: OffsetDateTime? = null,
    val result: AudioJobResult? = null,
    val styleCatalog: StyleCatalogResult? = null,
    /**
     * 제어 요청 응답의 `result` JSON 원문.
     *
     * 제어 요청마다 응답 모양이 다르므로(styles / speaker_forget 등) 여기서 타입을 정하지 않고
     * 요청을 보낸 쪽이 자기가 아는 타입으로 읽는다. tts 잡에서는 null 이다.
     */
    val rawResult: String? = null,
    val error: JobError? = null,
    val artifact: ArtifactContent? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class StylesCatalogVoice(
    val name: String,
    val kind: String,
    val path: String? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class StyleCatalogResult(
    @field:JsonAlias("styles")
    val styles: List<StylesCatalogVoice> = emptyList(),
    @field:JsonProperty("worker_id")
    @field:JsonAlias("workerId")
    val workerId: String? = null,
    // worker 가 styles 응답에 함께 실어 보내는 자기 batchSize (표시용). worker 소유 값.
    @field:JsonAlias("batch_size")
    val batchSize: Int? = null,
)

/**
 * `speaker_forget` 응답. 등록과 참조 변경은 Gateway 단독으로 처리하므로 prepare 응답은 없다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class SpeakerControlResult(
    @field:JsonAlias("speaker_name", "speakerId", "speaker_id", "styleId", "style_id")
    val speakerName: String = "",
    /** 지울 캐시가 있었는지. */
    val applied: Boolean? = null,
    @field:JsonProperty("worker_id")
    @field:JsonAlias("workerId")
    val workerId: String? = null,
)

/** result 메시지에 실려 오는 생성 아티팩트(오디오) 콘텐츠. */
data class ArtifactContent(
    @field:JsonAlias("file_name")
    val fileName: String? = null,
    @field:JsonAlias("media_type")
    val mediaType: String? = null,
    @field:JsonAlias("content_base64")
    val contentBase64: String? = null,
)
