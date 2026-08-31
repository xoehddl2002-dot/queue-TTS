package com.aitts.queuetts.gateway.api.service

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.aitts.queuetts.gateway.api.dto.*
import com.aitts.queuetts.gateway.api.error.AdminError
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSampleComparisonRepository
import org.springframework.beans.factory.ObjectProvider
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Path

enum class SampleAudioKind { LEGACY, CURRENT }

@Service
class AdminService(
    private val jobService: JobService,
    sampleComparisonRepositoryProvider: ObjectProvider<TtsSampleComparisonRepository>,
) {
    private val sampleComparisonRepository: TtsSampleComparisonRepository? =
        sampleComparisonRepositoryProvider.getIfAvailable()

    fun overview(): JobGatewayOverviewResponse = jobService.overview()

    fun listSamples(limit: Int): SampleComparisonListResponse = SampleComparisonListResponse(
        items = sampleComparisonRepository?.list(limit).orEmpty(),
        limit = limit.coerceIn(1, 500),
    )

    fun getSample(sampleKey: String): Either<AdminError, SampleComparisonResponse> =
        either {
            ensureNotNull(sampleComparisonRepository?.findBySampleKey(sampleKey)) {
                AdminError.SampleNotFound(sampleKey)
            }
        }

    fun audio(sampleKey: String, kind: SampleAudioKind): Either<AdminError, FileDownload> =
        either {
            val record = ensureNotNull(sampleComparisonRepository?.findBySampleKey(sampleKey)) {
                AdminError.SampleNotFound(sampleKey)
            }
            val pathValue = when (kind) {
                SampleAudioKind.LEGACY -> record.legacyAudioPath
                SampleAudioKind.CURRENT -> record.currentAudioPath
            }
            val format = when (kind) {
                SampleAudioKind.LEGACY -> record.legacyAudioFormat
                SampleAudioKind.CURRENT -> record.currentAudioFormat
            }

            val notFound = AdminError.SampleAudioNotFound(
                "${kind.name.lowercase()} audio file not found for sample $sampleKey",
            )
            ensure(!pathValue.isNullOrBlank()) { notFound }
            val path = Path.of(pathValue)
            ensure(Files.isRegularFile(path)) { notFound }

            FileDownload(path = path, mediaType = mediaType(format), fileName = path.fileName.toString())
        }

    private fun mediaType(format: String?): String = when (format?.lowercase()) {
        "wav" -> "audio/wav"
        "ogg" -> "audio/ogg"
        "flac" -> "audio/flac"
        else -> MediaType.APPLICATION_OCTET_STREAM_VALUE
    }
}
