package com.aitts.queuetts.gateway.api.service

import arrow.core.Either
import arrow.core.raise.Raise
import arrow.core.raise.either
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.config.security.CurrentCaller
import com.aitts.queuetts.gateway.api.dto.*
import com.aitts.queuetts.gateway.api.error.QwenError
import com.aitts.queuetts.gateway.api.infra.db.model.TtsSpeaker
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.SpeakerBlobStore
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.jaudiotagger.audio.AudioFileIO
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.nio.file.Files
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.logging.Level

private const val MAX_NAME_LENGTH = 64

/**
 * 클론 speaker 레지스트리. 설계는 `docs/speakers-registry.md`.
 *
 * 원본과 검증 책임은 Gateway 가 소유하고 worker 는 파생 prompt 캐시만 가진다. 등록/참조 수정은
 * worker 를 기다리지 않고 끝내며, prompt 는 합성 시 캐시가 비어 있으면 blob 에서 lazy 생성한다.
 */
@Service
@ConditionalOnProperty(prefix = "queuetts.database", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class QwenService(
    private val repository: TtsSpeakerRepository,
    private val blobStore: SpeakerBlobStore,
    private val queueClient: RedisQueueClient,
    private val streamQueueClient: RedisStreamQueueClient,
    private val properties: QueueTtsGatewayProperties,
    private val currentCaller: CurrentCaller,
    private val objectMapper: ObjectMapper,
    private val transactionTemplate: TransactionTemplate,
) {
    private val log = LoggerFactory.getLogger(QwenService::class.java)

    fun createSpeaker(request: CreateSpeakerRequest, refAudio: ByteArray): Either<QwenError, SpeakerResponse> =
        either {
            val model = cloneModel()
            val name = validName(request.name)
            if (repository.findByName(model, name) != null) {
                raise(QwenError.NameTaken(name))
            }
            val mode = cloneMode(request.xVectorOnlyMode)
            val refText = validRefText(mode, request.refText)
            val params = request.defaultParams ?: QwenSpeakerParams()
            val validatedAudio = validRefAudio(refAudio)
            val audio = validatedAudio.content

            val audioSha = sha256(audio)
            val digest = referenceDigest(audioSha, mode, refText)

            val now = OffsetDateTime.now(ZoneOffset.UTC)
            val speaker = TtsSpeaker(
                name = name,
                model = model,
                language = request.language?.trim()?.takeIf(String::isNotEmpty),
                description = request.description?.trim()?.takeIf(String::isNotEmpty),
                mode = mode,
                refText = refText,
                audio = audio,
                audioSha256 = audioSha,
                referenceDigest = digest,
                audioFormat = validatedAudio.format,
                sampleRate = validatedAudio.sampleRate,
                durationS = validatedAudio.durationS,
                defaultParams = objectMapper.writeValueAsString(params),
                createdBy = currentCaller.current()?.id,
                createdAt = now,
                updatedAt = now,
                referenceUpdatedAt = now,
            )
            try {
                repository.insert(speaker)
            } catch (exception: DuplicateKeyException) {
                raise(QwenError.NameTaken(name))
            } catch (exception: Throwable) {
                raise(QwenError.StorageError("speaker '$name' could not be saved: ${exception.message}"))
            }
            // name 키는 동시에 같은 이름을 등록하는 요청이 기존 blob을 덮어쓰지 않도록 DB의
            // 기본키 판정이 끝난 뒤에 만든다. 실패해도 DB 원본으로 첫 합성에서 복구할 수 있다.
            runCatching { blobStore.put(name, audio) }
                .onFailure { log.warn("speaker blob mirror deferred name={}: {}", name, it.message) }

            log.info("speaker.created name={} mode={} model={}", name, mode, model)
            speaker.toResponse()
        }

    fun listSpeaker(): Either<QwenError, SpeakerListResponse> =
        either {
            val model = cloneModel()
            SpeakerListResponse(
                speakers = repository.list(model).map { it.toResponse() },
                model = model,
            )
        }

    fun getSpeaker(name: String): Either<QwenError, SpeakerResponse> =
        either { load(name).toResponse() }

    /**
     * 등록해 둔 참조 음성 원본을 저장된 바이트 그대로 돌려준다.
     *
     * DB(`tts_style.audio`)에서 읽는다 — Redis 거울은 worker 배포 통로라 지워졌을 수 있고, 여기서
     * 필요한 것은 언제나 원본이다. [SpeakerResponse] 는 이 바이트를 싣지 않으므로(목록 한 번에 수
     * MB 가 실리게 된다) 재생·백업은 이 엔드포인트로 따로 받아 간다.
     */
    fun getSpeakerAudio(name: String): Either<QwenError, SpeakerAudio> =
        either {
            val speaker = load(name)
            val audio = repository.findAudio(speaker.model, speaker.name)
            // 행은 있는데 오디오가 비었다면 호출자가 고칠 수 있는 상황이 아니다 — 404 가 아니다.
            if (audio == null || audio.isEmpty()) {
                raise(QwenError.StorageError("speaker '${speaker.name}' has no stored reference audio"))
            }
            val format = speaker.audioFormat?.lowercase() ?: "wav"
            SpeakerAudio(
                content = audio,
                fileName = "${speaker.name}.$format",
                mediaType = referenceMediaType(format),
            )
        }

    /**
     * 수정. 참조가 바뀌면 digest 만 무효화하고, 새 prompt 는 다음 합성에서 lazy 생성한다.
     * 참조 필드를 보냈더라도 계산된 digest 가 그대로면 불필요한 쓰기를 하지 않는다.
     */
    fun updateSpeaker(currentName: String, request: UpdateSpeakerRequest, refAudio: ByteArray? = null): Either<QwenError, SpeakerResponse> =
        either {
            val current = load(currentName)
            val model = current.model
            val name = request.name?.let { validName(it) } ?: current.name
            if (name != current.name && repository.findByName(current.model, name) != null) {
                raise(QwenError.NameTaken(name))
            }
            val params = request.defaultParams
            val now = OffsetDateTime.now(ZoneOffset.UTC)

            var speaker = current
            // 참조 쓰기를 바로 실행하지 않고 모아 둔다. 아래에서 메타 쓰기와 한 트랜잭션으로 묶기 위함.
            var writeReference: (() -> Unit)? = null

            if (request.touchesReference() || refAudio != null) {
                val mode = request.xVectorOnlyMode?.let(::cloneMode) ?: current.mode
                // ref_text 를 명시적으로 지우는 경우는 지원하지 않는다 — icl 에서는 필수 값이고,
                // x_vector 로 바꾸면 어차피 무시된다.
                val refText = validRefText(mode, request.refText ?: current.refText)
                val validatedAudio = refAudio?.let { validRefAudio(it) }
                val audio = validatedAudio?.content
                val audioSha = audio?.let { sha256(it) } ?: current.audioSha256
                val digest = referenceDigest(audioSha, mode, refText)

                if (digest != current.referenceDigest) {
                    speaker = current.copy(
                        mode = mode,
                        refText = refText,
                        audio = audio ?: ByteArray(0),
                        audioSha256 = audioSha,
                        referenceDigest = digest,
                        audioFormat = validatedAudio?.format ?: current.audioFormat,
                        sampleRate = validatedAudio?.sampleRate ?: current.sampleRate,
                        durationS = validatedAudio?.durationS ?: current.durationS,
                        updatedAt = now,
                        referenceUpdatedAt = now,
                    )
                    // 오디오를 안 보낸 요청은 audio 컬럼을 건드리면 안 된다(빈 배열로 덮어써진다).
                    // 두 경로 모두 **옛 이름**을 키로 쓴다 — rename 은 아래 updateMetadata 가 한다.
                    val referenceStyle = speaker
                    writeReference = if (audio != null) {
                        { repository.updateReference(referenceStyle) }
                    } else {
                        {
                            repository.updateReferenceText(
                                model = model, name = current.name,
                                mode = mode, refText = refText, referenceDigest = digest,
                                sampleRate = referenceStyle.sampleRate, durationS = referenceStyle.durationS,
                                updatedAt = now, referenceUpdatedAt = now,
                            )
                        }
                    }
                }
            }

            speaker = speaker.copy(
                name = name,
                language = request.language?.trim()?.takeIf(String::isNotEmpty) ?: speaker.language,
                description = request.description?.trim() ?: speaker.description,
                defaultParams = params?.let { objectMapper.writeValueAsString(it) } ?: speaker.defaultParams,
                updatedAt = now,
            )

            val audioChanged = refAudio != null && speaker.referenceDigest != current.referenceDigest
            // 참조와 메타를 **한 트랜잭션으로** 묶는다. 나눠 쓰면 뒤가 실패했을 때 참조만 바뀌고
            // 이름/status 는 예전 값인 반쪽 상태가 남는다. 순서도 중요하다 — 참조 쓰기가 옛 이름을
            // 키로 쓰므로 기본키를 옮기는 rename 보다 먼저 나가야 한다.
            val written = speaker
            try {
                transactionTemplate.executeWithoutResult {
                    writeReference?.invoke()
                    repository.updateMetadata(
                        model = model,
                        currentName = current.name,
                        name = written.name,
                        language = written.language,
                        description = written.description,
                        defaultParams = written.defaultParams,
                        updatedAt = now,
                    )
                }
            } catch (exception: DuplicateKeyException) {
                raise(QwenError.NameTaken(written.name))
            } catch (exception: Throwable) {
                raise(QwenError.StorageError("speaker '${current.name}' could not be saved: ${exception.message}"))
            }

            // DB가 원본이다. 이름/오디오 변경 후 새 mirror가 실패하면 resolver가 첫 합성에서 다시
            // 채운다. 새 키가 준비된 뒤에만 이전 이름 키를 지워 전환 중 공백을 피한다.
            if (name != current.name || audioChanged) {
                // rename 이 이미 커밋됐으므로 행은 새 이름 아래에 있다.
                val mirrorAudio = if (audioChanged) speaker.audio else repository.findAudio(model, name)
                if (mirrorAudio == null) {
                    log.warn("speaker blob mirror deferred; DB audio missing name={}", name)
                } else {
                    runCatching { blobStore.put(name, mirrorAudio) }
                        .onSuccess {
                            if (name != current.name) runCatching { blobStore.delete(current.name) }
                        }
                        .onFailure { log.warn("speaker blob mirror deferred name={}: {}", name, it.message) }
                }
            }

            log.info("speaker.updated name={} previousName={} promptInvalidated={}", name, current.name, writeReference != null)
            speaker.toResponse()
        }

    /**
     * 삭제. 행·blob·worker 파생 캐시를 모두 지우며 **되돌릴 수 없다.**
     *
     * 예전에는 기본이 archive(`status` 를 바꿔 목록에서만 감추기)였고 `?purge=true` 라야 실제로
     * 지웠다. 삭제 하나에 동작이 둘이면 호출자가 "지웠는데 왜 남아 있나"를 매번 따져야 해서
     * 개념을 없앴다. job 이력은 speaker 행이 아니라 `voice` 문자열을 남기므로 추적은 끊기지 않고,
     * 다만 지워진 speaker 의 참조 음성은 복구할 수 없다.
     */
    fun deleteSpeaker(name: String): Either<QwenError, DeleteSpeakerResponse> =
        either {
            val speaker = load(name)

            // worker 캐시 정리는 실패해도 삭제를 막지 않는다 — prompt 는 파생물이라 남아도
            // 참조하는 speaker 이 없으면 그냥 쓰이지 않는다.
            val workerId = runCatching {
                queueClient.requestSpeakerControl(
                    type = "speaker_forget",
                    payload = mapOf("speakerName" to speaker.name),
                    source = "speaker-forget",
                    model = speaker.model,
                ).workerId
            }.getOrElse {
                log.warn("speaker_forget failed for '{}' (deleting anyway): {}", speaker.name, it.message)
                null
            }

            catchStorage(speaker.name) { repository.delete(speaker.model, speaker.name) }
            runCatching { blobStore.delete(speaker.name) }
            log.info("speaker.deleted model={} name={}", speaker.model, speaker.name)
            DeleteSpeakerResponse(name = speaker.name, workerId = workerId)
        }

    // ── 내부 ────────────────────────────────────────────────────────────────────
    private fun Raise<QwenError>.load(name: String): TtsSpeaker =
        repository.findByName(cloneModel(), validName(name)) ?: raise(QwenError.NotFound(name))

    private fun Raise<QwenError>.cloneModel(): String =
        cloneModelOrNull() ?: raise(
            QwenError.InvalidRequest(
                "clone pool '${properties.queue.voiceModel}' is not configured. " +
                        "Known models: ${streamQueueClient.modelNames.sorted().joinToString(", ")}"
            )
        )

    private fun cloneModelOrNull(): String? = streamQueueClient.canonicalModel(properties.queue.voiceModel)

    private fun Raise<QwenError>.validName(raw: String): String {
        val name = raw.trim()
        // 이름이 API path, Redis key, worker cache key를 겸하므로 path 구분자와 제어문자는 받지 않는다.
        if (name.isEmpty() || name.length > MAX_NAME_LENGTH ||
            name.any { it == '/' || it == '\\' || it.isISOControl() }
        ) {
            raise(QwenError.InvalidRequest("name must be 1..$MAX_NAME_LENGTH characters"))
        }
        return name
    }

    /** DB는 기존 mode 컬럼을 쓰지만 공개 API는 Qwen의 boolean 인자를 그대로 받는다. */
    private fun cloneMode(xVectorOnlyMode: Boolean): String =
        if (xVectorOnlyMode) TtsSpeaker.MODE_X_VECTOR else TtsSpeaker.MODE_ICL

    private fun Raise<QwenError>.validRefText(mode: String, raw: String?): String? {
        val refText = raw?.trim()?.takeIf(String::isNotEmpty)
        if (mode == TtsSpeaker.MODE_ICL && refText == null) {
            raise(
                QwenError.InvalidRequest(
                    "'ref_text' is required when 'x_vector_only_mode' is false. " +
                            "Set 'x_vector_only_mode' to true to clone without a transcript."
                )
            )
        }
        // x_vector 는 참조 텍스트를 쓰지 않는다. 받아 두면 digest 만 흔든다.
        return if (mode == TtsSpeaker.MODE_X_VECTOR) null else refText
    }

    private fun Raise<QwenError>.validRefAudio(audio: ByteArray): ValidatedReferenceAudio {
        if (audio.isEmpty()) {
            raise(QwenError.InvalidRequest("'ref_audio' file is empty"))
        }
        val limit = properties.job.speakerAudioMaxBytes
        if (audio.size > limit) {
            raise(QwenError.InvalidRequest("reference audio is ${audio.size} bytes, over the $limit byte limit"))
        }
        val metadata = try {
            ReferenceAudioInspector.inspect(audio)
        } catch (exception: Exception) {
            raise(QwenError.InvalidCloneReference("reference audio could not be decoded: ${exception.message}"))
        }
        val minSeconds = properties.job.speakerAudioMinSeconds
        val maxSeconds = properties.job.speakerAudioMaxSeconds
        if (metadata.durationS < minSeconds) {
            raise(
                QwenError.InvalidCloneReference(
                    "reference audio is too short (${String.format(Locale.US, "%.1f", metadata.durationS)}s); " +
                            "need at least ${formatLimit(minSeconds)}s",
                )
            )
        }
        if (metadata.durationS > maxSeconds) {
            raise(
                QwenError.InvalidCloneReference(
                    "reference audio is too long (${String.format(Locale.US, "%.1f", metadata.durationS)}s); " +
                            "the limit is ${formatLimit(maxSeconds)}s",
                )
            )
        }
        return ValidatedReferenceAudio(audio, metadata.format, metadata.durationS, metadata.sampleRate)
    }

    private fun <T> Raise<QwenError>.catchStorage(name: String, block: () -> T): T =
        runCatching(block).getOrElse { raise(QwenError.StorageError("speaker '$name' could not be saved: ${it.message}")) }

    private fun formatLimit(seconds: Double): String =
        if (seconds % 1.0 == 0.0) seconds.toLong().toString() else seconds.toString()

    /** [ReferenceAudioInspector.detectFormat] 가 붙이는 이름과 짝이다. mp3 만 컨테이너 이름과 다르다. */
    private fun referenceMediaType(format: String): String = when (format) {
        "mp3" -> "audio/mpeg"
        "wav", "flac", "ogg" -> "audio/$format"
        else -> "application/octet-stream"
    }

    private fun TtsSpeaker.toResponse() = SpeakerResponse(
        name = name,
        model = model,
        xVectorOnlyMode = mode == TtsSpeaker.MODE_X_VECTOR,
        language = language,
        description = description,
        refText = refText,
        durationS = durationS,
        sampleRate = sampleRate,
        audioFormat = audioFormat,
        referenceDigest = referenceDigest,
        defaultParams = readParams(defaultParams),
        createdAt = createdAt,
        updatedAt = updatedAt,
        referenceUpdatedAt = referenceUpdatedAt,
    )

    /**
     * 행에 저장된 `default_params` JSON → [QwenSpeakerParams].
     *
     * **읽을 때는 모르는 키를 흘려보낸다.** 파라미터 규격에서 빠진 키가 예전 행에 남아 있을 수 있고,
     * 그것 때문에 speaker 조회가 통째로 깨지면 곤란하다 — 엄격함은 받는 문(요청 바인딩)에만 둔다.
     */
    private fun readParams(raw: String): QwenSpeakerParams =
        runCatching {
            objectMapper.readerFor(QwenSpeakerParams::class.java)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .readValue<QwenSpeakerParams>(raw)
        }.getOrDefault(QwenSpeakerParams())

    companion object {
        /**
         * prompt 캐시 무효화 키.
         *
         * prompt 는 오디오뿐 아니라 mode 와 ref_text 에도 종속된다 — 오디오 해시만 보면
         * ref_text 오타 수정 같은 변경을 놓쳐 낡은 prompt 를 계속 쓰게 된다.
         */
        fun referenceDigest(audioSha256: String, mode: String, refText: String?): String =
            sha256("$audioSha256\n$mode\n${refText.orEmpty()}".toByteArray(Charsets.UTF_8))

        private fun sha256(bytes: ByteArray): String =
            MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    }
}

private data class ValidatedReferenceAudio(
    val content: ByteArray,
    val format: String,
    val durationS: Double,
    val sampleRate: Int,
)

internal data class ReferenceAudioMetadata(
    val format: String,
    val durationS: Double,
    val sampleRate: Int,
)

/**
 * worker를 깨우지 않고 참조 음성의 컨테이너와 메타데이터를 읽는다.
 *
 * 업로드 파일명과 Content-Type은 호출자가 임의로 붙일 수 있으므로 magic bytes로 형식을 정한 뒤,
 * 그 형식의 JVM parser로 실제 헤더를 읽는다. 원본은 DB/Redis에 저장하고 임시 파일은 즉시 지운다.
 */
internal object ReferenceAudioInspector {
    init {
        // jaudiotagger는 정상 WAV의 chunk마다 JUL INFO를 남긴다. 업로드 한 번에 여러 줄이 쌓이지 않게 한다.
        java.util.logging.Logger.getLogger("org.jaudiotagger").level = Level.WARNING
    }

    fun inspect(content: ByteArray): ReferenceAudioMetadata {
        val format = detectFormat(content)
        val temporary = Files.createTempFile("queuetts-speaker-reference-", ".$format")
        try {
            Files.write(temporary, content)
            val header = AudioFileIO.read(temporary.toFile()).audioHeader
            val durationS = header.preciseTrackLength
            val sampleRate = header.sampleRateAsNumber
            require(durationS.isFinite() && durationS > 0.0) { "audio duration is not available" }
            require(sampleRate > 0) { "audio sample rate is not available" }
            return ReferenceAudioMetadata(format, durationS, sampleRate)
        } finally {
            Files.deleteIfExists(temporary)
        }
    }

    private fun detectFormat(content: ByteArray): String = when {
        content.matchesAscii(0, "RIFF") && content.matchesAscii(8, "WAVE") -> "wav"
        content.matchesAscii(0, "RF64") && content.matchesAscii(8, "WAVE") -> "wav"
        content.matchesAscii(0, "fLaC") -> "flac"
        content.matchesAscii(0, "OggS") -> "ogg"
        content.matchesAscii(0, "ID3") || content.hasMp3FrameSync() -> "mp3"
        else -> throw IllegalArgumentException("unsupported reference audio format; use one of: flac, mp3, ogg, wav")
    }

    private fun ByteArray.matchesAscii(offset: Int, expected: String): Boolean =
        size >= offset + expected.length && expected.indices.all { this[offset + it].toInt() == expected[it].code }

    /** ID3가 없는 MP3도 있으므로 시작부에서 유효한 MPEG audio sync를 찾는다. */
    private fun ByteArray.hasMp3FrameSync(): Boolean {
        val end = minOf(size - 1, 4096)
        for (index in 0 until end) {
            val first = this[index].toInt() and 0xff
            val second = this[index + 1].toInt() and 0xff
            if (first == 0xff && second and 0xe0 == 0xe0 && second and 0x18 != 0x08) {
                return true
            }
        }
        return false
    }
}
