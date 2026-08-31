package com.aitts.queuetts.gateway.api.infra.db.model

import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.OffsetDateTime

/**
 * 클론 보이스(speaker) 한 건. 원본은 Gateway 가 소유하고 worker 는 파생 캐시다.
 *
 * 버전 이력은 두지 않는다 — 참조 변경은 in-place 다. 설계와 그 대가는
 * `docs/speakers-registry.md` 참고.
 *
 * **테이블·컬럼 이름은 `tts_style` 그대로다.** speaker 로 바꾸는 것은 코드 rename 이 아니라 DDL
 * 마이그레이션이라, 이미 쌓인 행과 함께 옮겨야 한다. 어차피 매핑은 이 애노테이션이 흡수한다.
 *
 * **식별자는 `(model, name)` 하나뿐이다.** 별도의 내부 id 를 두면 공개 키(name)와 저장 키가 갈려
 * "어느 쪽으로 조회하는 경로인가"를 매번 따져야 했다. name 은 바뀔 수 있지만 job 이력은 처음부터
 * id 가 아니라 name 문자열을 남기므로 id 를 없앤다고 추적이 끊기지 않는다.
 */
@Table("tts_style")
data class TtsSpeaker(
    /** 공개 보이스 키이자 기본키의 뒷부분. 변경 가능하고 `(model, name)` 으로 유일하다. */
    @Column("name")
    val name: String,
    /** 소속 엔진 풀. 클로닝을 지원하는 풀만 들어온다. 기본키의 앞부분이다. */
    @Column("model")
    val model: String,
    /** 이 speaker 의 기본 언어. 합성 요청의 `lang` 이 우선한다. */
    @Column("language")
    val language: String? = null,
    @Column("description")
    val description: String? = null,
    /** `icl` 또는 `x_vector`. */
    @Column("mode")
    val mode: String,
    /** `mode=icl` 이면 필수. */
    @Column("ref_text")
    val refText: String? = null,
    @Column("audio")
    val audio: ByteArray,
    @Column("audio_sha256")
    val audioSha256: String,
    /**
     * `sha256(audio_sha256 + mode + ref_text)`.
     *
     * prompt 는 오디오뿐 아니라 mode·ref_text 에도 종속되므로 셋을 묶어야 캐시가 정확히 깨진다.
     */
    @Column("reference_digest")
    val referenceDigest: String,
    @Column("audio_format")
    val audioFormat: String,
    @Column("sample_rate")
    val sampleRate: Int? = null,
    @Column("duration_s")
    val durationS: Double? = null,
    /** 생성 파라미터 기본값(JSON). 요청이 주면 요청이 우선한다. */
    @Column("default_params")
    val defaultParams: String = "{}",
    /** 등록한 API Key id. 인증이 꺼진 환경에서는 null. */
    @Column("created_by")
    val createdBy: String? = null,
    @Column("created_at")
    val createdAt: OffsetDateTime,
    @Column("updated_at")
    val updatedAt: OffsetDateTime,
    /** 참조(오디오/ref_text/mode)가 마지막으로 바뀐 시각. 버전 이력이 없어 이것이 유일한 단서다. */
    @Column("reference_updated_at")
    val referenceUpdatedAt: OffsetDateTime,
) {
    // data class 가 만드는 equals/hashCode 는 ByteArray 를 참조 비교한다. 내용 비교가 필요한
    // 곳은 audioSha256 을 쓰므로, 오해를 부르는 기본 구현 대신 기본키 기준으로 고정한다.
    override fun equals(other: Any?): Boolean =
        this === other || (other is TtsSpeaker && other.model == model && other.name == name)

    override fun hashCode(): Int = 31 * model.hashCode() + name.hashCode()

    companion object {
        const val MODE_ICL = "icl"
        const val MODE_X_VECTOR = "x_vector"
    }
}
