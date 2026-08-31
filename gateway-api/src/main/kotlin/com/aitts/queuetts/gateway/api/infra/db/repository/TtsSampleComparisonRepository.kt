package com.aitts.queuetts.gateway.api.infra.db.repository

import com.aitts.queuetts.gateway.api.dto.SampleComparisonResponse
import com.aitts.queuetts.gateway.api.utils.JdbcValueConverters
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
@ConditionalOnProperty(prefix = "queuetts.database", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class TtsSampleComparisonRepository(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun list(limit: Int): List<SampleComparisonResponse> =
        jdbcTemplate.query(
            "$SELECT_SQL ORDER BY sample_key LIMIT :limit",
            mapOf("limit" to limit.coerceIn(1, 500)),
            rowMapper,
        )

    fun findBySampleKey(sampleKey: String): SampleComparisonResponse? =
        jdbcTemplate.query(
            "$SELECT_SQL WHERE sample_key = :sampleKey",
            mapOf("sampleKey" to sampleKey),
            rowMapper,
        ).firstOrNull()

    private val rowMapper = RowMapper<SampleComparisonResponse> { rs, _ ->
        SampleComparisonResponse(
            sampleKey = rs.getString("sample_key"),
            text = rs.getString("text"),
            legacyService = rs.getString("legacy_service"),
            legacyAudioPath = rs.getString("legacy_audio_path"),
            legacyAudioFormat = rs.getString("legacy_audio_format"),
            currentService = rs.getString("current_service"),
            currentModel = rs.getString("current_model"),
            currentVoice = rs.getString("current_voice"),
            currentLang = rs.getString("current_lang"),
            currentSpeed = JdbcValueConverters.nullableDouble(rs, "current_speed"),
            currentSteps = JdbcValueConverters.nullableInt(rs, "current_steps"),
            currentResponseFormat = rs.getString("current_response_format"),
            currentSeed = JdbcValueConverters.nullableInt(rs, "current_seed"),
            currentSectionSize = rs.getString("current_section_size"),
            currentMaxChunkLength = JdbcValueConverters.nullableInt(rs, "current_max_chunk_length"),
            currentSilenceDuration = JdbcValueConverters.nullableDouble(rs, "current_silence_duration"),
            currentAudioPath = rs.getString("current_audio_path"),
            currentAudioFormat = rs.getString("current_audio_format"),
            currentDurationS = JdbcValueConverters.nullableDouble(rs, "current_duration_s"),
            currentSampleRate = JdbcValueConverters.nullableInt(rs, "current_sample_rate"),
            currentResultInfo = rs.getString("current_result_info"),
            currentGeneratedAt = JdbcValueConverters.offsetDateTime(rs, "current_generated_at"),
            comparisonStatus = rs.getString("comparison_status"),
            notes = rs.getString("notes"),
            createdAt = JdbcValueConverters.offsetDateTime(rs, "created_at"),
            updatedAt = JdbcValueConverters.offsetDateTime(rs, "updated_at"),
        )
    }

    private companion object {
        val SELECT_SQL = """
            SELECT
                sample_key, text, legacy_service, legacy_audio_path, legacy_audio_format,
                current_service, current_model, current_voice, current_lang, current_speed,
                current_steps, current_response_format, current_seed, current_section_size,
                current_max_chunk_length, current_silence_duration, current_audio_path,
                current_audio_format, current_duration_s, current_sample_rate,
                current_result_info, current_generated_at,
                CASE WHEN current_audio_path IS NULL THEN 'current_pending' ELSE 'ready_to_compare' END AS comparison_status,
                notes, created_at, updated_at
            FROM tts_sample_comparison
        """.trimIndent()
    }
}
