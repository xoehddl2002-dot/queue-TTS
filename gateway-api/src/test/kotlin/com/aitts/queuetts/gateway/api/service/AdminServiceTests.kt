package com.aitts.queuetts.gateway.api.service

import com.aitts.queuetts.gateway.api.dto.SampleComparisonResponse
import arrow.core.Either
import com.aitts.queuetts.gateway.api.error.AdminError
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSampleComparisonRepository
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun <T> Either<AdminError, T>.value(): T = fold(
    ifLeft = { error("expected success but got ${it::class.simpleName}: ${it.message}") },
    ifRight = { it },
)

class AdminServiceTests {

    @Test
    fun `getSample returns a not-found error when the sample key is unknown`() {
        val service = newService(mock(TtsSampleComparisonRepository::class.java))

        val error = service.getSample("missing").leftOrNull()

        assertEquals(AdminError.SampleNotFound("missing"), error)
    }

    @Test
    fun `getSample returns the record when it exists`() {
        val repository = mock(TtsSampleComparisonRepository::class.java)
        `when`(repository.findBySampleKey("sample-1")).thenReturn(sample("sample-1"))
        val service = newService(repository)

        assertEquals("sample-1", service.getSample("sample-1").value().sampleKey)
    }

    @Test
    fun `audio reports which kind was missing instead of a bare not-found`() {
        val repository = mock(TtsSampleComparisonRepository::class.java)
        `when`(repository.findBySampleKey("sample-1")).thenReturn(sample("sample-1", legacyAudioPath = null))
        val service = newService(repository)

        val error = service.audio("sample-1", SampleAudioKind.LEGACY).leftOrNull()

        assertEquals(AdminError.SampleAudioNotFound("legacy audio file not found for sample sample-1"), error)
    }

    @Test
    fun `audio reports missing file when the record points at a path that is gone`() {
        val repository = mock(TtsSampleComparisonRepository::class.java)
        `when`(repository.findBySampleKey("sample-1"))
            .thenReturn(sample("sample-1", legacyAudioPath = "D:/does/not/exist.wav"))
        val service = newService(repository)

        val error = service.audio("sample-1", SampleAudioKind.LEGACY).leftOrNull()

        assertEquals(AdminError.SampleAudioNotFound("legacy audio file not found for sample sample-1"), error)
    }

    @Test
    fun `audio resolves media type from the stored format`() {
        val audioFile = Files.createTempFile("sample", ".wav")
        val repository = mock(TtsSampleComparisonRepository::class.java)
        `when`(repository.findBySampleKey("sample-1")).thenReturn(
            sample("sample-1", legacyAudioPath = audioFile.toString(), legacyAudioFormat = "wav"),
        )
        val service = newService(repository)

        val download = service.audio("sample-1", SampleAudioKind.LEGACY).value()

        assertEquals("audio/wav", download.mediaType)
        assertTrue(download.fileName.endsWith(".wav"))
        Files.deleteIfExists(audioFile)
    }

    @Test
    fun `sample lookups fall back to an empty result when no repository is wired`() {
        val service = newService(repository = null)

        assertEquals(AdminError.SampleNotFound("sample-1"), service.getSample("sample-1").leftOrNull())
        assertEquals(0, service.listSamples(100).items.size)
    }

    private fun newService(repository: TtsSampleComparisonRepository?): AdminService {
        val beanFactory = DefaultListableBeanFactory()
        repository?.let { beanFactory.registerSingleton("ttsSampleComparisonRepository", it) }
        return AdminService(
            jobService = mock(JobService::class.java),
            sampleComparisonRepositoryProvider =
                beanFactory.getBeanProvider(TtsSampleComparisonRepository::class.java),
        )
    }

    private fun sample(
        sampleKey: String,
        legacyAudioPath: String? = "D:/audio/legacy.wav",
        legacyAudioFormat: String? = "wav",
    ) = SampleComparisonResponse(
        sampleKey = sampleKey,
        text = "hello",
        legacyService = "legacy",
        legacyAudioPath = legacyAudioPath,
        legacyAudioFormat = legacyAudioFormat,
        currentService = null,
        currentModel = null,
        currentVoice = null,
        currentLang = null,
        currentSpeed = null,
        currentSteps = null,
        currentResponseFormat = null,
        currentSeed = null,
        currentSectionSize = null,
        currentMaxChunkLength = null,
        currentSilenceDuration = null,
        currentAudioPath = null,
        currentAudioFormat = null,
        currentDurationS = null,
        currentSampleRate = null,
        currentResultInfo = null,
        currentGeneratedAt = null,
        comparisonStatus = "pending",
        notes = null,
        createdAt = null,
        updatedAt = null,
    )
}
