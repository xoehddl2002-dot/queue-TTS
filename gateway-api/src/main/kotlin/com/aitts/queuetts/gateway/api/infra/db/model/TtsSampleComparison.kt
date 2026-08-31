package com.aitts.queuetts.gateway.api.infra.db.model

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.OffsetDateTime

@Table("tts_sample_comparison")
data class TtsSampleComparison(
    @Id
    @Column("sample_key")
    val sampleKey: String,
    @Column("text")
    val text: String,
    @Column("legacy_service")
    val legacyService: String? = null,
    @Column("legacy_audio_path")
    val legacyAudioPath: String? = null,
    @Column("legacy_audio_format")
    val legacyAudioFormat: String? = null,
    @Column("current_service")
    val currentService: String? = null,
    @Column("current_model")
    val currentModel: String? = null,
    @Column("current_voice")
    val currentVoice: String? = null,
    @Column("current_lang")
    val currentLang: String? = null,
    @Column("current_speed")
    val currentSpeed: Double? = null,
    @Column("current_steps")
    val currentSteps: Int? = null,
    @Column("current_response_format")
    val currentResponseFormat: String? = null,
    @Column("current_seed")
    val currentSeed: Int? = null,
    @Column("current_section_size")
    val currentSectionSize: String? = null,
    @Column("current_max_chunk_length")
    val currentMaxChunkLength: Int? = null,
    @Column("current_silence_duration")
    val currentSilenceDuration: Double? = null,
    @Column("current_audio_path")
    val currentAudioPath: String? = null,
    @Column("current_audio_format")
    val currentAudioFormat: String? = null,
    @Column("current_duration_s")
    val currentDurationS: Double? = null,
    @Column("current_sample_rate")
    val currentSampleRate: Int? = null,
    @Column("current_result_info")
    val currentResultInfo: String? = null,
    @Column("current_generated_at")
    val currentGeneratedAt: OffsetDateTime? = null,
    @Column("notes")
    val notes: String? = null,
    @Column("created_at")
    val createdAt: OffsetDateTime? = null,
    @Column("updated_at")
    val updatedAt: OffsetDateTime? = null,
)
