package com.aitts.queuetts.gateway.api.infra.db.model

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.OffsetDateTime

@Table("tts_job_generation_history")
data class TtsJobGenerationHistory(
    @Id
    @Column("job_id")
    val jobId: String,
    @Column("state")
    val state: String,
    @Column("priority")
    val priority: String = "normal",
    /** job 을 접수시킨 API Key id. 인증이 꺼진 환경에서 만들어진 job 은 null. */
    @Column("caller_id")
    val callerId: String? = null,
    /** 그 API Key 의 권한 (admin/client). */
    @Column("caller_role")
    val callerRole: String? = null,
    @Column("worker_id")
    val workerId: String? = null,
    @Column("batch_id")
    val batchId: String? = null,
    @Column("payload")
    val payload: String = "{}",
    @Column("result")
    val result: String? = null,
    @Column("error")
    val error: String? = null,
    @Column("text")
    val text: String? = null,
    @Column("voice")
    val voice: String? = null,
    @Column("lang")
    val lang: String? = null,
    @Column("speed")
    val speed: Double? = null,
    @Column("steps")
    val steps: Int? = null,
    @Column("response_format")
    val responseFormat: String? = null,
    @Column("seed")
    val seed: Int? = null,
    @Column("max_chunk_length")
    val maxChunkLength: Int? = null,
    @Column("silence_duration")
    val silenceDuration: Double? = null,
    @Column("artifact_path")
    val artifactPath: String? = null,
    @Column("artifact_name")
    val artifactName: String? = null,
    @Column("artifact_media_type")
    val artifactMediaType: String? = null,
    @Column("artifact_size")
    val artifactSize: Long? = null,
    @Column("download_url")
    val downloadUrl: String? = null,
    @Column("created_at")
    val createdAt: OffsetDateTime,
    @Column("started_at")
    val startedAt: OffsetDateTime? = null,
    @Column("finished_at")
    val finishedAt: OffsetDateTime? = null,
)
