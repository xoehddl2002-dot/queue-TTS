package com.aitts.queuetts.gateway.api.infra.db.repository

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.*
import com.aitts.queuetts.gateway.api.utils.JdbcValueConverters
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
@ConditionalOnProperty(prefix = "queuetts.database", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class TtsJobGenerationHistoryRepository(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val objectMapper: ObjectMapper,
) {
    fun upsert(job: PersistedGatewayJob) {
        val payload = job.payload
        jdbcTemplate.update(
            """
            INSERT INTO tts_job_generation_history (
                job_id, state, priority, caller_id, caller_role, worker_id, batch_id,
                payload, result, error, text, voice, lang, speed, steps, response_format,
                seed, max_chunk_length, silence_duration, artifact_path, artifact_name,
                artifact_media_type, artifact_size, download_url, created_at,
                started_at, finished_at
            ) VALUES (
                :jobId, :state, :priority, :callerId, :callerRole, :workerId, :batchId,
                CAST(:payload AS jsonb), CAST(:result AS jsonb), CAST(:error AS jsonb),
                :text, :voice, :lang, :speed, :steps, :responseFormat, :seed,
                :maxChunkLength, :silenceDuration, :artifactPath, :artifactName,
                :artifactMediaType, :artifactSize, :downloadUrl, :createdAt,
                :startedAt, :finishedAt
            ) ON CONFLICT (job_id) DO UPDATE SET
                state = EXCLUDED.state, priority = EXCLUDED.priority,
                -- caller 는 접수 시점에 확정되는 값이라 한 번 기록된 뒤에는 덮어쓰지 않는다.
                caller_id = COALESCE(tts_job_generation_history.caller_id, EXCLUDED.caller_id),
                caller_role = COALESCE(tts_job_generation_history.caller_role, EXCLUDED.caller_role),
                worker_id = EXCLUDED.worker_id, batch_id = EXCLUDED.batch_id,
                payload = EXCLUDED.payload, result = EXCLUDED.result, error = EXCLUDED.error,
                text = EXCLUDED.text, voice = EXCLUDED.voice, lang = EXCLUDED.lang,
                speed = EXCLUDED.speed, steps = EXCLUDED.steps,
                response_format = EXCLUDED.response_format, seed = EXCLUDED.seed,
                max_chunk_length = EXCLUDED.max_chunk_length,
                silence_duration = EXCLUDED.silence_duration,
                artifact_path = EXCLUDED.artifact_path, artifact_name = EXCLUDED.artifact_name,
                artifact_media_type = EXCLUDED.artifact_media_type,
                artifact_size = EXCLUDED.artifact_size, download_url = EXCLUDED.download_url,
                started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at
            WHERE tts_job_generation_history.state NOT IN ('succeeded', 'failed')
            """.trimIndent(),
            jobParams(job)
                .addValue("text", payload.text)
                .addValue("voice", payload.voice)
                .addValue("lang", payload.lang)
                // speed/steps 는 supertonic 전용 knob 이라 조회 편의 컬럼도 그 엔진에서만 찬다.
                // 다른 엔진의 파라미터까지 컬럼으로 늘리지 않는다 — 전량은 payload jsonb 에 있다.
                .addValue("speed", (payload as? SupertonicJobPayload)?.speed)
                .addValue("steps", (payload as? SupertonicJobPayload)?.steps)
                .addValue("responseFormat", payload.responseFormat)
                .addValue("seed", payload.seed)
                .addValue("maxChunkLength", payload.maxChunkLength)
                .addValue("silenceDuration", payload.silenceDuration),
        )
    }

    fun list(limit: Int): List<JobHistoryResponse> = jdbcTemplate.query(
        "SELECT * FROM tts_job_generation_history ORDER BY created_at DESC LIMIT :limit",
        mapOf("limit" to limit.coerceIn(1, 200)),
        rowMapper,
    )

    fun listJobs(
        state: String?,
        priority: String?,
        source: String?,
        limit: Int,
        excludeJobIds: Set<String>,
    ): List<JobHistoryResponse> {
        val params = MapSqlParameterSource()
            .addValue("limit", limit.coerceIn(1, 1_000))
        val where = jobListWhere(state, priority, source, excludeJobIds, params)
        return jdbcTemplate.query(
            "SELECT * FROM tts_job_generation_history$where ORDER BY created_at DESC LIMIT :limit",
            params,
            rowMapper,
        )
    }

    fun countJobs(
        state: String?,
        priority: String?,
        source: String?,
        excludeJobIds: Set<String>,
    ): Int {
        val params = MapSqlParameterSource()
        val where = jobListWhere(state, priority, source, excludeJobIds, params)
        return jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM tts_job_generation_history$where",
            params,
            Int::class.java,
        ) ?: 0
    }

    /** state 별 누적 건수. overview 의 terminal(succeeded/failed/cancelled) 카운트 소스. */
    fun countByState(): Map<String, Int> = jdbcTemplate.query(
        "SELECT state, COUNT(*) AS cnt FROM tts_job_generation_history GROUP BY state",
        RowMapper { rs, _ -> rs.getString("state") to rs.getInt("cnt") },
    ).toMap()

    fun findByJobId(jobId: String): JobHistoryResponse? = jdbcTemplate.query(
        "SELECT * FROM tts_job_generation_history WHERE job_id = :jobId",
        mapOf("jobId" to jobId),
        rowMapper,
    ).firstOrNull()

    private fun jobListWhere(
        state: String?,
        priority: String?,
        source: String?,
        excludeJobIds: Set<String>,
        params: MapSqlParameterSource,
    ): String {
        val conditions = mutableListOf<String>()
        if (!state.isNullOrBlank()) {
            conditions += "state = :state"
            params.addValue("state", state)
        }
        if (!priority.isNullOrBlank()) {
            conditions += "priority = :priority"
            params.addValue("priority", priority)
        }
        if (!source.isNullOrBlank()) {
            conditions += "lower(payload->>'source') = :source"
            params.addValue("source", source)
        }
        if (excludeJobIds.isNotEmpty()) {
            conditions += "job_id NOT IN (:excludeJobIds)"
            params.addValue("excludeJobIds", excludeJobIds)
        }
        return conditions.takeIf { it.isNotEmpty() }
            ?.joinToString(prefix = " WHERE ", separator = " AND ")
            .orEmpty()
    }

    private val rowMapper = RowMapper<JobHistoryResponse> { rs, _ ->
        val jobId = rs.getString("job_id")
        val createdAt = requireNotNull(JdbcValueConverters.offsetDateTime(rs, "created_at"))
        val finishedAt = JdbcValueConverters.offsetDateTime(rs, "finished_at")
        val artifactPath = rs.getString("artifact_path")
        val artifactName = rs.getString("artifact_name")
        val artifactMediaType = rs.getString("artifact_media_type")
        val artifactSize = JdbcValueConverters.nullableLong(rs, "artifact_size")
        val artifact = artifactPath?.let {
            JobArtifactResponse(it, artifactName, artifactMediaType, artifactSize)
        }
        val payload = parsePayload(rs.getString("payload"))
        val job = JobResponse(
            jobId = jobId,
            state = rs.getString("state"),
            priority = rs.getString("priority"),
            source = payload.source,
            caller = parseCaller(rs.getString("caller_id"), rs.getString("caller_role")),
            createdAt = createdAt,
            updatedAt = finishedAt ?: createdAt,
            startedAt = JdbcValueConverters.offsetDateTime(rs, "started_at"),
            finishedAt = finishedAt,
            workerId = rs.getString("worker_id"),
            batchId = rs.getString("batch_id"),
            result = parseAudioResult(rs.getString("result")),
            error = parseJobError(rs.getString("error")),
            artifact = artifact,
            downloadUrl = rs.getString("download_url"),
            statusUrl = "/api/jobs/$jobId",
            historyError = null,
            version = 0,
            payload = payload,
        )
        JobHistoryResponse(job, artifactPath, artifactName, artifactMediaType, artifactSize)
    }

    private fun jobParams(job: PersistedGatewayJob): MapSqlParameterSource = MapSqlParameterSource()
        .addValue("jobId", job.jobId)
        .addValue("state", job.state)
        .addValue("priority", job.priority)
        .addValue("callerId", job.caller?.id)
        // state/priority 와 같은 소문자 표기로 저장한다.
        .addValue("callerRole", job.caller?.role?.name?.lowercase())
        .addValue("workerId", job.workerId)
        .addValue("batchId", job.batchId)
        .addValue("payload", objectMapper.writeValueAsString(job.payload))
        .addValue("result", job.result?.let(objectMapper::writeValueAsString))
        .addValue("error", job.error?.let(objectMapper::writeValueAsString))
        .addValue("artifactPath", job.artifactPath)
        .addValue("artifactName", job.artifactName)
        .addValue("artifactMediaType", job.artifactMediaType)
        .addValue("artifactSize", job.artifactSize)
        .addValue("downloadUrl", job.downloadUrl)
        .addValue("createdAt", job.createdAt)
        .addValue("startedAt", job.startedAt)
        .addValue("finishedAt", job.finishedAt)

    /**
     * caller 컬럼 → [JobCaller]. caller 컬럼이 추가되기 전에 쌓인 행이나 인증 없이 만들어진 job 은
     * 값이 없어 null 이 되고, 알 수 없는 role 문자열도 조회를 깨뜨리지 않도록 null 로 떨어뜨린다.
     */
    private fun parseCaller(callerId: String?, callerRole: String?): JobCaller? {
        val id = callerId?.takeIf(String::isNotBlank) ?: return null
        val role = QueueTtsGatewayProperties.ApiKeyRole.entries
            .firstOrNull { it.name.equals(callerRole?.trim(), ignoreCase = true) }
            ?: return null
        return JobCaller(id = id, role = role)
    }

    /**
     * 이력 행의 `payload` jsonb → 모델별 payload.
     *
     * 모델별로 규격이 갈리기 전에 쌓인 행에는 지금 그 엔진이 받지 않는 키가 섞여 있다(예: UI 가
     * 모든 엔진에 `speed` 를 실어 보내던 시절의 qwen 잡). 그런 행을 지금 규격으로 거절하면
     * **과거 이력 조회가 통째로 깨지므로** 읽을 때는 모르는 키를 흘려보낸다 —
     * 엄격함은 접수하는 문([JobPayloadBinder.bindRequest])에만 둔다.
     */
    private fun parsePayload(raw: String?): JobPayload =
        JobPayloadBinder.readPersisted(raw, objectMapper) ?: SupertonicJobPayload()

    private fun parseAudioResult(raw: String?): AudioJobResult? =
        raw?.let { objectMapper.readValue(it, AudioJobResult::class.java) }

    private fun parseJobError(raw: String?): JobError? =
        raw?.let { objectMapper.readValue(it, JobError::class.java) }
}
