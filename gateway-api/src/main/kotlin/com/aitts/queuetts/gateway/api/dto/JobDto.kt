package com.aitts.queuetts.gateway.api.dto

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.fasterxml.jackson.annotation.*
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.OffsetDateTime

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class AudioJobResult(
    @field:JsonProperty("durationS")
    @field:JsonAlias("duration_s")
    val durationS: Double? = null,
    @field:JsonProperty("sampleRate")
    @field:JsonAlias("sample_rate")
    val sampleRate: String? = null,
    @field:JsonProperty("mediaType")
    @field:JsonAlias("media_type")
    val mediaType: String? = null,
    @field:JsonProperty("audioFormat")
    @field:JsonAlias("audio_format")
    val audioFormat: String? = null,
    @field:JsonProperty("processedText")
    @field:JsonAlias("processed_text")
    val processedText: String? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
data class JobError(
    val code: String? = null,
    val message: String? = null,
    val detail: String? = null,
    val reason: String? = null,
) {
    fun displayMessage(): String? = sequenceOf(message, detail, reason, code)
        .firstOrNull { !it.isNullOrBlank() }
}

// JobController(/api/jobs) 의 요청/응답 DTO. 잡 도메인 내부 모델(이력·영속화)도 함께 둔다.

/**
 * TTS 작업 페이로드.
 *
 * **엔진 무관한 봉투는 이 인터페이스가, 엔진별 생성 파라미터는 구현체가 정한다.** 두 엔진의 규격
 * 차이는 거의 전부 후자에 몰려 있어서(voice 해석 방식과 생성 knob), 봉투만 공유하고 나머지는
 * 모델별 타입으로 갈랐다. 덕분에 `/api/jobs` 는 엔드포인트 하나로 유지되면서도,
 * **어떤 엔진이 무엇을 받는지는 그 엔진의 구현체만 보면 된다.**
 *
 * 요청 본문은 `model` 로 구현체를 고른다([JobPayloadBinder]). 그 엔진이 모르는 파라미터는
 * 조용히 무시하지 않고 400 이다 — 예전에는 qwen 잡에 `speed` 를 실어도 그냥 사라져서
 * "슬라이더를 움직였는데 왜 그대로냐"를 추적할 수 없었다.
 *
 * 엔진을 추가하려면 구현체 하나를 만들어 [JobPayloadTypes] 에 등록한다. 큐 라우팅은 여전히
 * 설정만으로 끝난다(`queuetts.queue.models`) — 코드가 필요한 것은 파라미터 규격뿐이다.
 */
sealed interface JobPayload {
    /** 합성할 텍스트. */
    val text: String?

    /**
     * 이 잡을 처리할 합성 엔진. 생략하면 `queuetts.queue.default-model`.
     *
     * gateway 는 이 값으로 발행 대상 stream 과 이 payload 의 타입을 고른다. worker 는 자기
     * stream 만 소비하므로 이 필드를 읽지 않는다.
     */
    val model: String?

    /** 호출자가 보낸 voice 이름. registry가 있는 엔진은 등록된 정규 이름으로 바꿔 이력에 남긴다. */
    val voice: String?

    val lang: String?

    /** `wav` / `flac` / `ogg`. worker 의 인코딩 단계가 읽는다. */
    val responseFormat: String?

    val seed: Long?

    /** 긴 텍스트를 나눌 청크 최대 길이. */
    val maxChunkLength: Int?

    /** 청크 사이에 넣을 무음 길이(초). */
    val silenceDuration: Double?

    /** 접수 경로(이력·우선순위 기본값 산정용). 합성에는 쓰이지 않는다. */
    val source: String?

    /** [source] 만 바꾼 사본. 구현체마다 `copy` 시그니처가 달라 인터페이스에서 이름을 하나 준다. */
    fun withSource(source: String?): JobPayload

    /**
     * 클라이언트가 정할 수 없는 필드를 비운 사본.
     *
     * 참조 음성처럼 **Gateway 가 해석해 채우는 값**을 요청 본문으로 받아 주면, 호출자가 남의
     * speaker blob 키를 찍어 보낼 수 있다. 접수 직후 한 번 통과시킨다.
     */
    fun sanitized(): JobPayload
}

/**
 * Supertonic 워커가 받는 payload.
 *
 * voice 는 **worker 소유 카탈로그**의 이름을 그대로 쓴다 — Gateway 가 해석할 것이 없어서
 * 참조 관련 필드가 없다. 생성 knob 은 확산 스텝 수와 말하기 속도 둘뿐이다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class SupertonicJobPayload(
    override val text: String? = null,
    override val model: String? = null,
    override val voice: String? = null,
    override val lang: String? = null,
    @field:JsonProperty("response_format")
    @field:JsonAlias("responseFormat")
    override val responseFormat: String? = null,
    override val seed: Long? = null,
    @field:JsonProperty("max_chunk_length")
    @field:JsonAlias("maxChunkLength")
    override val maxChunkLength: Int? = null,
    @field:JsonProperty("silence_duration")
    @field:JsonAlias("silenceDuration")
    override val silenceDuration: Double? = null,
    @field:JsonAlias("jobSource", "job_source", "apiType", "api_type", "clientType", "client_type", "page", "channel")
    override val source: String? = null,

    // ── 여기부터 Supertonic 전용 생성 파라미터 ──

    /** 말하기 속도 배수. worker 가 청크 분할 방식과 edge trim 을 이 값으로 조정한다. */
    val speed: Double? = null,
    /** 확산 스텝 수. 크면 느리고 품질이 오른다. */
    val steps: Int? = null,
) : JobPayload {
    override fun withSource(source: String?): JobPayload = copy(source = source)

    /** 이 엔진은 Gateway 가 채우는 필드가 없다. */
    override fun sanitized(): JobPayload = this
}

/**
 * Qwen3-TTS 워커가 받는 payload.
 *
 * voice 는 **Gateway 소유 speaker registry**를 거쳐야 하므로, 해석 결과인 `speaker*` 필드들이
 * 함께 실린다 — 이 다섯은 클라이언트가 보내는 값이 아니라 `SpeakerRegistryVoiceResolver` 가
 * 채운다([sanitized] 가 요청분을 버린다).
 *
 * 생성 knob 은 자기회귀 샘플링 파라미터다. `subtalker_*` 는 2단계 디코더용으로 이름만 다르고
 * 뜻은 같다. **여기 없는 파라미터는 400** 이므로, worker 가 새 knob 을 받기 시작하면 여기에
 * 필드를 추가해야 한다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class QwenJobPayload(
    override val text: String? = null,
    override val model: String? = null,
    override val voice: String? = null,
    override val lang: String? = null,
    @field:JsonProperty("response_format")
    @field:JsonAlias("responseFormat")
    override val responseFormat: String? = null,
    override val seed: Long? = null,
    @field:JsonProperty("max_chunk_length")
    @field:JsonAlias("maxChunkLength")
    override val maxChunkLength: Int? = null,
    @field:JsonProperty("silence_duration")
    @field:JsonAlias("silenceDuration")
    override val silenceDuration: Double? = null,
    @field:JsonAlias("jobSource", "job_source", "apiType", "api_type", "clientType", "client_type", "page", "channel")
    override val source: String? = null,

    // ── Gateway 가 speaker registry 에서 해석해 채우는 참조 (요청으로 받지 않는다) ──

    /** 등록된 speaker 이름. worker 는 이 값으로 prompt 캐시를 찾는다. */
    val speakerName: String? = null,
    /** 참조 오디오·mode·ref_text 가 바뀌면 달라지는 prompt 캐시 무효화 키. */
    val referenceDigest: String? = null,
    /** worker 가 read-through 할 참조 음성 Redis STRING 키. */
    val speakerBlobKey: String? = null,
    /** 캐시 미스 시 prompt 를 다시 만들기 위한 clone mode (`icl` / `x_vector`). */
    val speakerMode: String? = null,
    /** ICL 캐시 미스 시 prompt 를 다시 만들기 위한 참조 전사. */
    val speakerRefText: String? = null,

    // ── 여기부터 Qwen 전용 생성 파라미터 ──

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
) : JobPayload {
    override fun withSource(source: String?): JobPayload = copy(source = source)

    override fun sanitized(): JobPayload = copy(
        speakerName = null,
        referenceDigest = null,
        speakerBlobKey = null,
        speakerMode = null,
        speakerRefText = null,
    )

    /**
     * speaker 에 저장된 기본값을 **요청값이 이기도록** 덮어쓴 사본.
     *
     * 양쪽 다 없는 키는 null 로 남고, NON_NULL 직렬화라 worker payload 에 아예 나타나지 않는다 —
     * 그래야 worker 가 체크포인트의 `generate_config.json` 기본값을 쓴다.
     * **Gateway 는 모델 기본값을 알 필요가 없다.**
     */
    @JsonIgnore
    fun withDefaults(defaults: QwenSpeakerParams): QwenJobPayload = copy(
        doSample = doSample ?: defaults.doSample,
        temperature = temperature ?: defaults.temperature,
        topP = topP ?: defaults.topP,
        topK = topK ?: defaults.topK,
        repetitionPenalty = repetitionPenalty ?: defaults.repetitionPenalty,
        maxNewTokens = maxNewTokens ?: defaults.maxNewTokens,
        subtalkerDosample = subtalkerDosample ?: defaults.subtalkerDosample,
        subtalkerTemperature = subtalkerTemperature ?: defaults.subtalkerTemperature,
        subtalkerTopP = subtalkerTopP ?: defaults.subtalkerTopP,
        subtalkerTopK = subtalkerTopK ?: defaults.subtalkerTopK,
    )
}

/**
 * 모델 이름 → payload 타입.
 *
 * **엔진을 추가할 때 코드에서 손대는 곳은 여기 한 줄과 그 payload 클래스뿐이다.** 큐·health·
 * styles 병합은 `queuetts.queue.models` 설정만으로 따라온다(`QueueModelExtensibilityTests`).
 *
 * 이름 매칭은 대소문자를 무시한다 — `QueueTtsGatewayProperties.Queue.canonicalModel` 과 같은 규칙이라
 * 라우팅은 통과하는데 바인딩만 실패하는 어긋남이 생기지 않는다.
 */
object JobPayloadTypes {
    private val byName: Map<String, Class<out JobPayload>> = mapOf(
        "supertonic" to SupertonicJobPayload::class.java,
        "qwen" to QwenJobPayload::class.java,
    )

    /** `model` 이 없는 요청이 바인딩될 타입. `queuetts.queue.default-model` 기본값과 맞춘다. */
    val DEFAULT: Class<out JobPayload> = SupertonicJobPayload::class.java

    val names: Set<String> get() = byName.keys

    fun forModel(model: String?): Class<out JobPayload>? {
        val requested = model?.trim()?.takeIf(String::isNotEmpty) ?: return DEFAULT
        return byName.entries.firstOrNull { it.key.equals(requested, ignoreCase = true) }?.value
    }
}

/**
 * JSON 을 모델별 [JobPayload] 로 바인딩한다.
 *
 * 들어오는 문과 나가는 문에서 **엄격함이 다르다.**
 * - [bindRequest] 는 엄격하다. 모르는 필드는 400 이고, 그게 이 구조 변경의 요점이다.
 * - [readPersisted] 는 관대하다. 이력 행의 `payload` jsonb 에는 모델별 규격이 갈리기 전에
 *   기록된 값이 남아 있어(예: qwen 잡에 `speed` 가 함께 실린 행) 엄격하게 읽으면 **과거 이력
 *   조회가 통째로 깨진다.** 이미 벌어진 일을 지금 규격으로 거절해 봐야 얻을 것이 없다.
 */
object JobPayloadBinder {
    /** payload 필드가 아니라 요청 봉투의 필드. 평면 형태에서 payload 로 읽지 않는다. */
    private val RESERVED_TOP_LEVEL = setOf("payload", "priority")

    /** 바인딩 성공이면 [payload], 실패면 400 으로 내보낼 [error]. 둘 중 하나만 채워진다. */
    data class Result(val payload: JobPayload?, val error: String?)

    fun bindRequest(node: JsonNode, mapper: ObjectMapper): Result {
        if (!node.isObject) return Result(null, null)

        val model = node.get("model")?.takeUnless(JsonNode::isNull)?.asText()
        val type = JobPayloadTypes.forModel(model)
            ?: return Result(
                null,
                "unknown model '$model'. Known models: ${JobPayloadTypes.names.sorted().joinToString(", ")}",
            )

        // priority 는 최상위(평면) 요청에서 payload 와 한 객체에 섞여 온다. 걸러내지 않으면
        // 아래 엄격 읽기가 "모르는 필드"로 거절해 버린다.
        val body = node.deepCopy<ObjectNode>().remove(RESERVED_TOP_LEVEL)

        return try {
            Result(
                mapper.readerFor(type)
                    .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .readValue<JobPayload>(body),
                null,
            )
        } catch (exception: UnrecognizedPropertyException) {
            Result(null, unknownFieldMessage(exception, model ?: "(default)"))
        } catch (exception: Exception) {
            Result(null, exception.originalMessage())
        }
    }

    /**
     * 별칭까지 포함해 `source` 하나만 꺼낸다.
     *
     * 중첩 형태 요청의 **최상위**를 읽을 때 쓴다 — 거기에는 payload 가 아닌 필드가 섞여 있으므로
     * 관대하게 읽어야 하고, 별칭 목록을 여기 또 적지 않으려고 바인딩을 그대로 재사용한다.
     */
    fun sourceOf(node: JsonNode, mapper: ObjectMapper): String? = runCatching {
        mapper.readerFor(JobPayloadTypes.DEFAULT)
            .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .readValue<JobPayload>(node)
            .source
    }.getOrNull()

    fun readPersisted(raw: String?, mapper: ObjectMapper): JobPayload? {
        val node = raw?.takeIf(String::isNotBlank)?.let { mapper.readTree(it) } ?: return null
        if (!node.isObject) return null
        val type = JobPayloadTypes.forModel(node.get("model")?.takeUnless(JsonNode::isNull)?.asText())
            ?: JobPayloadTypes.DEFAULT
        return mapper.readerFor(type)
            .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .readValue(node)
    }

    /**
     * Jackson 기본 메시지는 Kotlin 클래스 FQCN 을 노출해 호출자에게 쓸모가 없다. 어떤 엔진이
     * 무엇을 받는지로 바꿔 준다 — 대개 다른 엔진의 파라미터를 실어 보낸 경우다.
     */
    private fun unknownFieldMessage(exception: UnrecognizedPropertyException, model: String): String {
        val accepted = exception.knownPropertyIds.orEmpty().map(Any::toString).sorted()
        return "model '$model' does not accept '${exception.propertyName}'. " +
                "Accepted fields: ${accepted.joinToString(", ")}"
    }

    private fun Exception.originalMessage(): String =
        (this as? JsonMappingException)?.originalMessage ?: message ?: "invalid job payload"
}

@JsonDeserialize(using = CreateJobRequestDeserializer::class)
data class CreateJobRequest(
    val payload: JobPayload? = null,
    val priority: String? = null,
    /**
     * 본문을 모델별 payload 로 바인딩하지 못한 이유. 성공하면 null 이다.
     *
     * 바인딩 실패는 사실상 요청 검증 실패(모르는 model, 다른 엔진의 파라미터)라서, Jackson 이
     * 예외를 던져 Spring 기본 400 본문으로 나가게 두지 않고 여기 담아 넘긴다. 그래야 다른 실패와
     * 똑같이 `JobError` 형태(code/message)로 응답할 수 있다 — 이 프로젝트에는 `ControllerAdvice`
     * 가 없어서 예외를 던지면 응답 모양이 혼자 달라진다.
     */
    val payloadError: String? = null,
)

/**
 * `/api/jobs` 요청 본문의 두 형태를 모두 [JobPayload] 로 변환한다:
 * 중첩(`{"payload":{...}}`)과 최상위 평면(`{"text":...}`) 형태. source 는 중첩 payload 안에
 * 있으면 그것을, 없으면 최상위 필드(별칭 포함)를 사용한다.
 *
 * 어떤 엔진의 payload 타입으로 바인딩할지와 모르는 필드를 어떻게 거절할지는
 * [JobPayloadBinder] 가 정한다 — 여기서는 두 **본문 모양**만 맞춘다.
 */
class CreateJobRequestDeserializer : JsonDeserializer<CreateJobRequest>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): CreateJobRequest {
        val mapper = parser.codec as ObjectMapper
        val root: JsonNode = mapper.readTree(parser)
        val priority = root.get("priority")?.takeUnless(JsonNode::isNull)?.asText()

        // 중첩 형태면 최상위의 나머지 필드는 payload 가 아니다 — 거기 뭐가 섞여 있든 보지 않고,
        // source 만 payload 에 없을 때 끌어온다.
        root.get("payload")?.takeIf(JsonNode::isObject)?.let { nested ->
            val bound = JobPayloadBinder.bindRequest(nested, mapper)
            bound.error?.let { return CreateJobRequest(priority = priority, payloadError = it) }
            val payload = bound.payload
                ?.let { if (it.source == null) it.withSource(JobPayloadBinder.sourceOf(root, mapper)) else it }
            return CreateJobRequest(payload = payload, priority = priority)
        }

        val flat = JobPayloadBinder.bindRequest(root, mapper)
        flat.error?.let { return CreateJobRequest(priority = priority, payloadError = it) }
        return CreateJobRequest(payload = flat.payload, priority = priority)
    }
}

data class AcceptedJobResponse(
    val status: String,
    val jobId: String,
    val state: String,
    val priority: String,
    val source: String?,
    val statusUrl: String,
) {
    @get:JsonProperty("job_id")
    val jobIdSnakeCase: String get() = jobId
}

data class JobArtifactResponse(
    val path: String,
    val fileName: String?,
    val mediaType: String?,
    val size: Long?,
)

data class JobEventResponse(
    val id: Long,
    val at: OffsetDateTime,
    val state: String,
    val message: String,
    val jobId: String,
)

/**
 * job 을 접수시킨 호출자(API Key).
 *
 * [id] 는 `queuetts.security.keys[].id`, [role] 은 그 키의 권한이다. 인증이 꺼진 환경에서 생성된
 * job 은 caller 가 없어 null 로 남는다.
 */
data class JobCaller(
    val id: String,
    val role: QueueTtsGatewayProperties.ApiKeyRole,
)

@JsonInclude(JsonInclude.Include.ALWAYS)
data class JobResponse(
    val jobId: String,
    val state: String,
    val priority: String,
    val source: String?,
    /** 이 job 을 접수시킨 API Key 와 권한. 인증 없이 만들어진 job 은 null. */
    val caller: JobCaller?,
    val createdAt: OffsetDateTime,
    val updatedAt: OffsetDateTime,
    val startedAt: OffsetDateTime?,
    val finishedAt: OffsetDateTime?,
    val workerId: String?,
    val batchId: String?,
    val result: AudioJobResult?,
    val error: JobError?,
    val artifact: JobArtifactResponse?,
    val downloadUrl: String?,
    val statusUrl: String,
    val historyError: String?,
    val version: Long,
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val payload: JobPayload? = null,
) {
    @get:JsonProperty("job_id")
    val jobIdSnakeCase: String get() = jobId

    @get:JsonProperty("worker_id")
    val workerIdSnakeCase: String? get() = workerId

    @get:JsonProperty("batch_id")
    val batchIdSnakeCase: String? get() = batchId
}

// 목록 필터와 무관한 전체 상태 요약. wait/running 은 진행 중 working set,
// terminal(succeeded/failed/cancelled)은 DB 누적 기준.
data class JobCountsResponse(
    val total: Int,
    val byState: Map<String, Int>,
    val byPriority: Map<String, Int>,
    val wait: Int,
    val running: Int,
    val terminal: Int,
)

data class JobListResponse(
    val items: List<JobResponse>,
    val total: Int,
    val limit: Int,
    val offset: Int,
    val counts: JobCountsResponse,
)

// ── 잡 도메인 내부 모델 (컨트롤러가 직접 노출하지 않음: 이력 조회·영속화 계층에서 사용) ──

data class JobHistoryResponse(
    @field:JsonUnwrapped
    val job: JobResponse,
    val artifactPath: String?,
    val artifactName: String?,
    val artifactMediaType: String?,
    val artifactSize: Long?,
)

data class PersistedGatewayJob(
    val jobId: String,
    val state: String,
    val priority: String,
    /** 접수 시점의 호출자. 이력 조회에서 "누가/어떤 권한으로" 만든 job 인지 보기 위해 함께 저장한다. */
    val caller: JobCaller?,
    val workerId: String?,
    val batchId: String?,
    val payload: JobPayload,
    val result: AudioJobResult?,
    val error: JobError?,
    val artifactPath: String?,
    val artifactName: String?,
    val artifactMediaType: String?,
    val artifactSize: Long?,
    val downloadUrl: String?,
    val sequence: Long,
    val leaseUntil: OffsetDateTime?,
    val batchingToken: String?,
    val batchingUntil: OffsetDateTime?,
    val createdAt: OffsetDateTime,
    val startedAt: OffsetDateTime?,
    val finishedAt: OffsetDateTime?,
    val version: Long,
    val events: List<JobEventResponse>,
)
