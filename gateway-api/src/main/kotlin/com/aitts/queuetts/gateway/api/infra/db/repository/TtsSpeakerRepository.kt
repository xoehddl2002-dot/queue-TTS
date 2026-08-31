package com.aitts.queuetts.gateway.api.infra.db.repository

import com.aitts.queuetts.gateway.api.infra.db.model.TtsSpeaker
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime

/**
 * `tts_style` 접근. 다른 저장소들과 같이 손으로 쓴 SQL 을 쓴다.
 *
 * 테이블 이름이 아직 `tts_style` 인 이유는 [TtsSpeaker] 참고 — 저장된 것은 rename 대상이 아니다.
 *
 * **모든 접근은 기본키 `(model, name)` 으로 한다.** 조회와 갱신이 같은 키를 쓰므로 "공개 이름으로
 * 찾아서 내부 id 로 쓴다" 같은 왕복이 없다. 대신 rename 은 기본키를 바꾸는 UPDATE 라
 * [updateMetadata] 가 옛 이름과 새 이름을 모두 받는다.
 *
 * 목록 조회는 **오디오 컬럼을 읽지 않는다** — 한 건에 수백 KB 라 목록에 실으면 응답과 메모리가
 * 그만큼 커진다. 오디오가 필요한 곳은 [findAudio] 로 따로 읽는다.
 */
@Repository
@ConditionalOnProperty(prefix = "queuetts.database", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class TtsSpeakerRepository(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
) {
    fun insert(speaker: TtsSpeaker) {
        jdbcTemplate.update(
            """
            INSERT INTO tts_style (
                name, model, language, description, mode, ref_text,
                audio, audio_sha256, reference_digest, audio_format, sample_rate, duration_s,
                default_params, created_by, created_at, updated_at, reference_updated_at
            ) VALUES (
                :name, :model, :language, :description, :mode, :refText,
                :audio, :audioSha256, :referenceDigest, :audioFormat, :sampleRate, :durationS,
                CAST(:defaultParams AS jsonb), :createdBy, :createdAt, :updatedAt, :referenceUpdatedAt
            )
            """.trimIndent(),
            params(speaker),
        )
    }

    /**
     * 메타만 갱신한다. 참조(오디오/ref_text/mode)는 [updateReference] 로 따로 바꾼다.
     *
     * [name] 이 [currentName] 과 다르면 이 UPDATE 가 곧 rename 이며 기본키가 바뀐다. 같은 이름이
     * 이미 있으면 기본키 위반이 `DuplicateKeyException` 으로 올라와 호출자가 409 로 바꾼다.
     * 참조 쓰기는 옛 이름을 키로 쓰므로 **반드시 이 호출보다 먼저** 나가야 한다.
     */
    fun updateMetadata(
        model: String,
        currentName: String,
        name: String,
        language: String?,
        description: String?,
        defaultParams: String,
        updatedAt: OffsetDateTime,
    ): Int = jdbcTemplate.update(
        """
        UPDATE tts_style SET
            name = :name, language = :language, description = :description,
            default_params = CAST(:defaultParams AS jsonb), updated_at = :updatedAt
        WHERE model = :model AND name = :currentName
        """.trimIndent(),
        MapSqlParameterSource()
            .addValue("model", model)
            .addValue("currentName", currentName)
            .addValue("name", name)
            .addValue("language", language)
            .addValue("description", description)
            .addValue("defaultParams", defaultParams)
            .addValue("updatedAt", updatedAt),
    )

    /** [speaker] 의 `name` 은 아직 rename 전 값이어야 한다 — WHERE 절의 키로 쓰인다. */
    fun updateReference(speaker: TtsSpeaker): Int = jdbcTemplate.update(
        """
        UPDATE tts_style SET
            mode = :mode, ref_text = :refText, audio = :audio, audio_sha256 = :audioSha256,
            reference_digest = :referenceDigest, audio_format = :audioFormat,
            sample_rate = :sampleRate, duration_s = :durationS,
            updated_at = :updatedAt, reference_updated_at = :referenceUpdatedAt
        WHERE model = :model AND name = :name
        """.trimIndent(),
        params(speaker),
    )

    /**
     * 오디오는 그대로 두고 참조의 나머지(`mode`/`ref_text`)만 바꾼다.
     *
     * [updateReference] 를 쓰면 오디오 컬럼까지 덮어쓰는데, 오디오를 보내지 않은 요청에서는
     * 넘길 바이트가 없어 빈 배열로 지워진다. 그래서 경로를 나눈다.
     */
    fun updateReferenceText(
        model: String,
        name: String,
        mode: String,
        refText: String?,
        referenceDigest: String,
        sampleRate: Int?,
        durationS: Double?,
        updatedAt: OffsetDateTime,
        referenceUpdatedAt: OffsetDateTime,
    ): Int = jdbcTemplate.update(
        """
        UPDATE tts_style SET
            mode = :mode, ref_text = :refText, reference_digest = :referenceDigest,
            sample_rate = :sampleRate, duration_s = :durationS,
            updated_at = :updatedAt, reference_updated_at = :referenceUpdatedAt
        WHERE model = :model AND name = :name
        """.trimIndent(),
        MapSqlParameterSource()
            .addValue("model", model)
            .addValue("name", name)
            .addValue("mode", mode)
            .addValue("refText", refText)
            .addValue("referenceDigest", referenceDigest)
            .addValue("sampleRate", sampleRate)
            .addValue("durationS", durationS)
            .addValue("updatedAt", updatedAt)
            .addValue("referenceUpdatedAt", referenceUpdatedAt),
    )

    fun findByName(model: String, name: String): TtsSpeaker? = jdbcTemplate.query(
        "SELECT $METADATA_COLUMNS FROM tts_style WHERE model = :model AND name = :name",
        MapSqlParameterSource().addValue("model", model).addValue("name", name),
        METADATA_MAPPER,
    ).firstOrNull()

    fun list(model: String): List<TtsSpeaker> = jdbcTemplate.query(
        """
        SELECT $METADATA_COLUMNS FROM tts_style
        WHERE model = :model
        ORDER BY created_at DESC
        """.trimIndent(),
        MapSqlParameterSource().addValue("model", model),
        METADATA_MAPPER,
    )

    /** 참조 음성 바이트. Redis blob 을 다시 채울 때만 필요하다. */
    fun findAudio(model: String, name: String): ByteArray? = jdbcTemplate.query(
        "SELECT audio FROM tts_style WHERE model = :model AND name = :name",
        MapSqlParameterSource().addValue("model", model).addValue("name", name),
    ) { rs, _ -> rs.getBytes("audio") }.firstOrNull()

    fun delete(model: String, name: String): Int = jdbcTemplate.update(
        "DELETE FROM tts_style WHERE model = :model AND name = :name",
        MapSqlParameterSource().addValue("model", model).addValue("name", name),
    )

    private fun params(speaker: TtsSpeaker) = MapSqlParameterSource()
        .addValue("name", speaker.name)
        .addValue("model", speaker.model)
        .addValue("language", speaker.language)
        .addValue("description", speaker.description)
        .addValue("mode", speaker.mode)
        .addValue("refText", speaker.refText)
        .addValue("audio", speaker.audio)
        .addValue("audioSha256", speaker.audioSha256)
        .addValue("referenceDigest", speaker.referenceDigest)
        .addValue("audioFormat", speaker.audioFormat)
        .addValue("sampleRate", speaker.sampleRate)
        .addValue("durationS", speaker.durationS)
        .addValue("defaultParams", speaker.defaultParams)
        .addValue("createdBy", speaker.createdBy)
        .addValue("createdAt", speaker.createdAt)
        .addValue("updatedAt", speaker.updatedAt)
        .addValue("referenceUpdatedAt", speaker.referenceUpdatedAt)

    private companion object {
        /** 오디오를 뺀 전 컬럼. 목록/단건 조회는 이것만 읽는다. */
        const val METADATA_COLUMNS = """
            name, model, language, description, mode, ref_text,
            audio_sha256, reference_digest, audio_format, sample_rate, duration_s,
            default_params, created_by, created_at, updated_at, reference_updated_at
        """

        val METADATA_MAPPER = RowMapper { rs, _ ->
            TtsSpeaker(
                name = rs.getString("name"),
                model = rs.getString("model"),
                language = rs.getString("language"),
                description = rs.getString("description"),
                mode = rs.getString("mode"),
                refText = rs.getString("ref_text"),
                // 오디오는 읽지 않는다. 필요하면 findAudio 로 따로 가져간다.
                audio = ByteArray(0),
                audioSha256 = rs.getString("audio_sha256"),
                referenceDigest = rs.getString("reference_digest"),
                audioFormat = rs.getString("audio_format"),
                sampleRate = rs.getObject("sample_rate") as? Int,
                durationS = rs.getObject("duration_s") as? Double,
                defaultParams = rs.getString("default_params") ?: "{}",
                createdBy = rs.getString("created_by"),
                createdAt = rs.getObject("created_at", OffsetDateTime::class.java),
                updatedAt = rs.getObject("updated_at", OffsetDateTime::class.java),
                referenceUpdatedAt = rs.getObject("reference_updated_at", OffsetDateTime::class.java),
            )
        }
    }
}
