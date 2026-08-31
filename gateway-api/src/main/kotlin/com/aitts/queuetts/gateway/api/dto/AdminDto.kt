package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

// AdminController(/api/admin/*) 의 요청/응답 DTO: 게이트웨이 개요, 샘플 비교.

data class JobGatewayOverviewResponse(
    val workers: List<WorkerResponse>,
    val workerCount: Int,
    val activeWorkerCount: Int,
    val redisQueue: RedisQueueOverviewResponse?,
    val jobWorkers: List<WorkerResponse>,
    // worker 가 styles 응답에 실어 보내는 batchSize(표시용). worker 가 없거나 아직 조회 전이면 null.
    // 여러 모델이 있으면 기본 모델 값이며, 모델별 값은 [batchSizes] 에 있다.
    val batchSize: Int?,
    /** 모델별 batchSize. 엔진마다 값이 달라 하나로 뭉뚱그릴 수 없다. */
    val batchSizes: Map<String, Int>? = null,
)

// overview 응답 트리를 구성하는 큐/워커 상태 DTO. WorkerResponse 는 GatewayController 의
// health 응답에서도 재사용된다(같은 패키지라 별도 import 불필요).

data class WorkerResponse(
    val workerId: String,
    val status: String,
    val capacity: Int? = null,
    val currentRunning: Int? = null,
    val pending: Long? = null,
    val idleMs: Long? = null,
    val active: Boolean? = null,
    val firstSeenAt: OffsetDateTime? = null,
    val lastSeenAt: OffsetDateTime? = null,
    val lastHeartbeatAt: OffsetDateTime? = null,
    /** 이 worker 가 속한 합성 엔진(모델) 풀. */
    val model: String? = null,
) {
    @get:JsonProperty("worker_id")
    val workerIdSnakeCase: String get() = workerId

    @get:JsonProperty("current_running")
    val currentRunningSnakeCase: Int? get() = currentRunning
}

data class QueueStreamResponse(val stream: String, val length: Long)

/** 합성 엔진(모델) 하나가 소비하는 큐의 상태. */
data class QueueModelOverviewResponse(
    val jobStreamPrefix: String,
    val jobGroup: String,
    val jobStreams: Map<String, QueueStreamResponse>,
    val jobStreamLength: Long,
    val workerCount: Int,
    val activeWorkerCount: Int,
)

data class RedisQueueOverviewResponse(
    /** 기본 모델의 job stream prefix (기존 소비자 호환). 모델별 값은 [models] 참고. */
    val jobStream: String,
    val jobStreamPrefix: String,
    /** 기본 모델의 우선순위별 stream (기존 소비자 호환). */
    val jobStreams: Map<String, QueueStreamResponse>,
    /** 모든 모델 stream 길이의 합. */
    val jobStreamLength: Long,
    val resultStream: String,
    val resultStreamLength: Long,
    /** 모든 모델의 worker (각 항목의 `model` 로 구분). */
    val workers: List<WorkerResponse>,
    val workerCount: Int,
    val activeWorkerCount: Int,
    val models: Map<String, QueueModelOverviewResponse> = emptyMap(),
    val defaultModel: String? = null,
)

data class SampleComparisonResponse(
    @field:JsonProperty("sample_key")
    val sampleKey: String,
    val text: String,
    @field:JsonProperty("legacy_service")
    val legacyService: String?,
    @field:JsonProperty("legacy_audio_path")
    val legacyAudioPath: String?,
    @field:JsonProperty("legacy_audio_format")
    val legacyAudioFormat: String?,
    @field:JsonProperty("current_service")
    val currentService: String?,
    @field:JsonProperty("current_model")
    val currentModel: String?,
    @field:JsonProperty("current_voice")
    val currentVoice: String?,
    @field:JsonProperty("current_lang")
    val currentLang: String?,
    @field:JsonProperty("current_speed")
    val currentSpeed: Double?,
    @field:JsonProperty("current_steps")
    val currentSteps: Int?,
    @field:JsonProperty("current_response_format")
    val currentResponseFormat: String?,
    @field:JsonProperty("current_seed")
    val currentSeed: Int?,
    @field:JsonProperty("current_section_size")
    val currentSectionSize: String?,
    @field:JsonProperty("current_max_chunk_length")
    val currentMaxChunkLength: Int?,
    @field:JsonProperty("current_silence_duration")
    val currentSilenceDuration: Double?,
    @field:JsonProperty("current_audio_path")
    val currentAudioPath: String?,
    @field:JsonProperty("current_audio_format")
    val currentAudioFormat: String?,
    @field:JsonProperty("current_duration_s")
    val currentDurationS: Double?,
    @field:JsonProperty("current_sample_rate")
    val currentSampleRate: Int?,
    @field:JsonProperty("current_result_info")
    val currentResultInfo: String?,
    @field:JsonProperty("current_generated_at")
    val currentGeneratedAt: OffsetDateTime?,
    @field:JsonProperty("comparison_status")
    val comparisonStatus: String,
    val notes: String?,
    @field:JsonProperty("created_at")
    val createdAt: OffsetDateTime?,
    @field:JsonProperty("updated_at")
    val updatedAt: OffsetDateTime?,
)

data class SampleComparisonListResponse(
    val items: List<SampleComparisonResponse>,
    val limit: Int,
)
