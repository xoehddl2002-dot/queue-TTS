package com.aitts.queuetts.gateway.api.service

import arrow.core.Either
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.config.security.CurrentCaller
import com.aitts.queuetts.gateway.api.dto.CreateSpeakerRequest
import com.aitts.queuetts.gateway.api.dto.SpeakerResponse
import com.aitts.queuetts.gateway.api.error.QwenError
import com.aitts.queuetts.gateway.api.infra.db.model.TtsSpeaker
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.SpeakerBlobStore
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`
import org.springframework.transaction.support.TransactionTemplate
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.abs
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

private fun Either<QwenError, SpeakerResponse>.value(): SpeakerResponse = fold(
    ifLeft = { error("expected success but got ${it::class.simpleName}: ${it.message}") },
    ifRight = { it },
)

class QwenServiceTests {
    @Test
    fun `reference audio inspector reads WAV metadata`() {
        val metadata = ReferenceAudioInspector.inspect(wav(seconds = 3))

        assertEquals("wav", metadata.format)
        assertEquals(24_000, metadata.sampleRate)
        assertTrue(abs(metadata.durationS - 3.0) < 0.01)
    }

    @Test
    fun `reference audio inspector reads an MP3 by content`() {
        // 실제 MP3 프레임을 읽어야 의미가 있는 검사라 픽스처를 테스트 리소스로 들고 있는다
        // (합성 결과물 디렉터리는 저장소에 없다).
        val content = checkNotNull(javaClass.getResourceAsStream("/audio/reference.mp3")) {
            "테스트 픽스처 /audio/reference.mp3 를 찾지 못했다"
        }.use { it.readBytes() }
        val metadata = ReferenceAudioInspector.inspect(content)

        assertEquals("mp3", metadata.format)
        assertTrue(metadata.sampleRate > 0)
        assertTrue(metadata.durationS > 0.0)
    }

    @Test
    fun `create validates and stores speaker without a worker roundtrip`() {
        val fixture = fixture()
        val response = fixture.service.createSpeaker(
            CreateSpeakerRequest(name = "나인애", refText = "참조 발화"),
            wav(seconds = 3),
        ).value()

        val insert = org.mockito.Mockito.mockingDetails(fixture.repository).invocations
            .single { it.method.name == "insert" }
        val speaker = insert.arguments[0] as TtsSpeaker
        val blobPut = org.mockito.Mockito.mockingDetails(fixture.blobStore).invocations
            .single { it.method.name == "put" }
        assertEquals(speaker.name, blobPut.arguments[0])
        verifyNoInteractions(fixture.queueClient)
        assertEquals("wav", response.audioFormat)
        assertEquals(24_000, response.sampleRate)
        assertTrue(abs((response.durationS ?: 0.0) - 3.0) < 0.01)
        assertEquals(false, response.promptRebuilt)
        assertNull(response.workerId)
    }

    @Test
    fun `create rejects undecodable audio before writing storage`() {
        val fixture = fixture()
        val error = fixture.service.createSpeaker(
            CreateSpeakerRequest(name = "나인애", refText = "참조 발화"),
            "not audio".toByteArray(),
        ).leftOrNull()

        assertIs<QwenError.InvalidCloneReference>(error)
        assertTrue(org.mockito.Mockito.mockingDetails(fixture.repository).invocations.none { it.method.name == "insert" })
        verifyNoInteractions(fixture.blobStore, fixture.queueClient)
    }

    @Test
    fun `create rejects audio outside the Gateway duration policy`() {
        val fixture = fixture()
        val error = fixture.service.createSpeaker(
            CreateSpeakerRequest(name = "나인애", refText = "참조 발화"),
            wav(seconds = 1),
        ).leftOrNull()

        assertIs<QwenError.InvalidCloneReference>(error)
        assertTrue(error.message.contains("too short"))
        verifyNoInteractions(fixture.blobStore, fixture.queueClient)
    }

    private fun fixture(): Fixture {
        val repository = mock(TtsSpeakerRepository::class.java)
        val blobStore = mock(SpeakerBlobStore::class.java)
        val queueClient = mock(RedisQueueClient::class.java)
        val streamQueueClient = mock(RedisStreamQueueClient::class.java)
        `when`(streamQueueClient.canonicalModel("qwen")).thenReturn("qwen")
        val properties = QueueTtsGatewayProperties(
            queue = QueueTtsGatewayProperties.Queue(voiceModel = "qwen"),
        )
        val service = QwenService(
            repository = repository,
            blobStore = blobStore,
            queueClient = queueClient,
            streamQueueClient = streamQueueClient,
            properties = properties,
            currentCaller = CurrentCaller(),
            objectMapper = jacksonObjectMapper(),
            transactionTemplate = mock(TransactionTemplate::class.java),
        )
        return Fixture(service, repository, blobStore, queueClient)
    }

    /** 외부 codec 없이 테스트가 재현되도록 24kHz mono PCM WAV를 직접 만든다. */
    private fun wav(seconds: Int, sampleRate: Int = 24_000): ByteArray {
        val dataSize = seconds * sampleRate * 2
        return ByteBuffer.allocate(44 + dataSize).order(ByteOrder.LITTLE_ENDIAN).apply {
            put("RIFF".toByteArray(Charsets.US_ASCII))
            putInt(36 + dataSize)
            put("WAVEfmt ".toByteArray(Charsets.US_ASCII))
            putInt(16)
            putShort(1.toShort())
            putShort(1.toShort())
            putInt(sampleRate)
            putInt(sampleRate * 2)
            putShort(2.toShort())
            putShort(16.toShort())
            put("data".toByteArray(Charsets.US_ASCII))
            putInt(dataSize)
            repeat(dataSize) { put(0.toByte()) }
        }.array()
    }

    private data class Fixture(
        val service: QwenService,
        val repository: TtsSpeakerRepository,
        val blobStore: SpeakerBlobStore,
        val queueClient: RedisQueueClient,
    )
}
