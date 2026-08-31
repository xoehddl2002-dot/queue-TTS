package com.aitts.queuetts.gateway.api.service

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.JobCountsResponse
import com.aitts.queuetts.gateway.api.dto.WorkerResponse
import com.aitts.queuetts.gateway.api.error.GatewayError
import com.aitts.queuetts.gateway.api.infra.db.model.TtsSpeaker
import com.aitts.queuetts.gateway.api.infra.redis.messaging.StyleCatalogResult
import com.aitts.queuetts.gateway.api.infra.redis.messaging.StylesCatalogVoice
import com.aitts.queuetts.gateway.api.infra.redis.queue.QueueStylesResult
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.ArgumentMatchers.nullable
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import java.time.OffsetDateTime

/** health/styles 가 큐 장애와 모델 분리를 어떻게 API 응답으로 옮기는지 고정한다. */
class GatewayServiceTests {
    private val streamQueueClient: RedisStreamQueueClient = mock(RedisStreamQueueClient::class.java)
    private val queueClient: RedisQueueClient = mock(RedisQueueClient::class.java)
    private val jobService: JobService = mock(JobService::class.java)
    private val properties = QueueTtsGatewayProperties()
    private val beanFactory = DefaultListableBeanFactory()
    private val service = GatewayService(
        streamQueueClient,
        queueClient,
        jobService,
        properties,
        beanFactory.getBeanProvider(TtsSpeakerRepository::class.java),
    )

    /** 기본은 supertonic 한 풀만 있는 구성. 여러 풀이 필요한 테스트는 각자 다시 스텁한다. */
    @BeforeTest
    fun setUpSingleModel() {
        stubModels("supertonic")
    }

    /** [models] 를 등록하고, [activeModels] (기본값: 전부)만 살아있는 것으로 본다. */
    private fun stubModels(vararg models: String, activeModels: Set<String> = models.toSet()) {
        `when`(streamQueueClient.modelNames).thenReturn(models.toSet())
        `when`(streamQueueClient.activeModels()).thenReturn(activeModels)
        `when`(streamQueueClient.canonicalModel(nullable(String::class.java))).thenAnswer { invocation ->
            val requested = invocation.getArgument<String?>(0) ?: models.first()
            models.firstOrNull { it.equals(requested, ignoreCase = true) }
        }
    }

    private fun catalog(vararg voices: StylesCatalogVoice, batchSize: Int? = null) = StyleCatalogResult(
        styles = voices.toList(),
        batchSize = batchSize,
    )

    @Test
    fun `health reports the queue counts and the workers seen on the streams`() {
        `when`(jobService.healthCounts()).thenReturn(counts(wait = 2, running = 3, total = 9))
        `when`(streamQueueClient.workerResponsesForHealth()).thenReturn(
            listOf(WorkerResponse(workerId = "worker-1", status = "active", pending = 0, idleMs = 10, active = true)),
        )

        val health = service.health().getOrNull()!!

        assertTrue(health.ok)
        assertEquals("ok", health.status)
        assertEquals(5, health.activeJobs)
        assertEquals(9, health.jobsTotal)
        assertEquals(listOf("worker-1"), health.workers.map { it.workerId })
    }

    @Test
    fun `health turns an unexpected failure into a worker error`() {
        `when`(jobService.healthCounts()).thenThrow(IllegalStateException("counting blew up"))

        val error = service.health().leftOrNull()

        assertEquals(GatewayError.WorkerError("counting blew up"), error)
        assertEquals("WORKER_CONNECTION_ERROR", error?.code)
    }

    @Test
    fun `styles are flattened from the worker catalog and tagged with the model`() {
        `when`(queueClient.requestStyles(anyString(), eq("supertonic"))).thenReturn(
            QueueStylesResult(
                workerId = "worker-1",
                data = catalog(
                    StylesCatalogVoice(name = "Na-in-ae", kind = "custom", path = "/voices/na.wav"),
                    StylesCatalogVoice(name = "default", kind = "builtin"),
                ),
                model = "supertonic",
            ),
        )

        val response = service.styles().getOrNull()!!

        assertEquals(listOf("Na-in-ae", "default"), response.styles.map { it.name })
        assertEquals("/voices/na.wav", response.styles.first().path)
        assertEquals(listOf("supertonic", "supertonic"), response.styles.map { it.model })
        assertNull(response.errors)
    }

    @Test
    fun `a styles timeout is reported as a worker error rather than a crash`() {
        `when`(queueClient.requestStyles(anyString(), eq("supertonic")))
            .thenThrow(IllegalStateException("styles request did not complete within 30s."))

        val error = service.styles().leftOrNull()

        assertEquals("WORKER_CONNECTION_ERROR", error?.code)
        assertEquals("supertonic: styles request did not complete within 30s.", error?.message)
    }

    @Test
    fun `styles merge every model pool when no model is given`() {
        stubModels("supertonic", "qwen")
        `when`(queueClient.requestStyles(anyString(), eq("supertonic"))).thenReturn(
            QueueStylesResult(
                workerId = "sup-1",
                data = catalog(StylesCatalogVoice(name = "Na-in-ae", kind = "custom"), batchSize = 2),
                model = "supertonic",
            ),
        )
        `when`(queueClient.requestStyles(anyString(), eq("qwen"))).thenReturn(
            QueueStylesResult(
                workerId = "qwen-1",
                data = catalog(StylesCatalogVoice(name = "Cloned-1", kind = "custom"), batchSize = 1),
                model = "qwen",
            ),
        )

        val response = service.styles().getOrNull()!!

        assertEquals(setOf("Na-in-ae", "Cloned-1"), response.styles.map { it.name }.toSet())
        assertEquals(mapOf("Na-in-ae" to "supertonic", "Cloned-1" to "qwen"), response.styles.associate { it.name to it.model })
        // 엔진마다 배치 크기가 달라 하나로 뭉뚱그리면 안 된다.
        assertEquals(mapOf("supertonic" to 2, "qwen" to 1), response.batchSizes)
        assertEquals(2, response.batchSize, "flat batchSize 는 기본 모델 값이어야 한다")
    }

    @Test
    fun `qwen styles come from the database without a worker control request`() {
        stubModels("supertonic", "qwen", activeModels = setOf("supertonic"))
        val repository = mock(TtsSpeakerRepository::class.java)
        val now = OffsetDateTime.parse("2026-08-19T00:00:00Z")
        val style = TtsSpeaker(
            name = "나인애",
            model = "qwen",
            mode = TtsSpeaker.MODE_ICL,
            refText = "참조 발화",
            audio = ByteArray(0),
            audioSha256 = "audio-sha",
            referenceDigest = "reference-digest",
            audioFormat = "wav",
            defaultParams = "{}",
            createdAt = now,
            updatedAt = now,
            referenceUpdatedAt = now,
        )
        `when`(repository.list("qwen")).thenReturn(listOf(style))
        val repositoryFactory = DefaultListableBeanFactory().also {
            it.registerSingleton("ttsStyleRepository", repository)
        }
        val qwenProperties = properties.copy(
            queue = QueueTtsGatewayProperties.Queue(
                defaultModel = "supertonic",
                voiceModel = "qwen",
                models = mapOf(
                    "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                    "qwen" to QueueTtsGatewayProperties.ModelQueue("qwen:jobs", "qwen-workers"),
                ),
            ),
        )
        val databaseService = GatewayService(
            streamQueueClient,
            queueClient,
            jobService,
            qwenProperties,
            repositoryFactory.getBeanProvider(TtsSpeakerRepository::class.java),
        )

        val response = databaseService.styles("qwen").getOrNull()!!

        assertEquals(listOf("나인애"), response.styles.map { it.name })
        assertEquals(listOf("qwen"), response.models)
        verify(queueClient, never()).requestStyles(anyString(), eq("qwen"))
    }

    @Test
    fun `styles keep the surviving pool when one pool fails`() {
        stubModels("supertonic", "qwen")
        `when`(queueClient.requestStyles(anyString(), eq("supertonic"))).thenReturn(
            QueueStylesResult(
                workerId = "sup-1",
                data = catalog(StylesCatalogVoice(name = "Na-in-ae", kind = "custom")),
                model = "supertonic",
            ),
        )
        `when`(queueClient.requestStyles(anyString(), eq("qwen")))
            .thenThrow(IllegalStateException("no qwen worker"))

        val response = service.styles().getOrNull()!!

        assertEquals(listOf("Na-in-ae"), response.styles.map { it.name })
        assertEquals(listOf("supertonic"), response.models)
        assertEquals(mapOf("qwen" to "no qwen worker"), response.errors)
    }

    @Test
    fun `styles skip a pool that has no live worker instead of waiting for its timeout`() {
        // qwen 풀은 등록돼 있지만 살아있는 worker 가 없다 — 조회 자체를 하지 않아야 한다.
        // (보내면 control-request timeout 만큼 호출자가 붙잡힌다.)
        stubModels("supertonic", "qwen", activeModels = setOf("supertonic"))
        `when`(queueClient.requestStyles(anyString(), eq("supertonic"))).thenReturn(
            QueueStylesResult(
                workerId = "sup-1",
                data = catalog(StylesCatalogVoice(name = "Na-in-ae", kind = "custom")),
                model = "supertonic",
            ),
        )

        val response = service.styles().getOrNull()!!

        assertEquals(listOf("supertonic"), response.models)
        assertNull(response.errors, "조회하지 않은 풀은 오류로 보고하지 않는다")
        verify(queueClient, never()).requestStyles(anyString(), eq("qwen"))
    }

    @Test
    fun `styles still query the default pool when nothing looks alive`() {
        // liveness 판정이 틀렸을 수도 있으므로, 아무 풀도 살아있지 않으면 조용히 빈 목록을 주는 대신
        // 기본 모델로 시도해 실패 사유를 그대로 노출한다.
        stubModels("supertonic", "qwen", activeModels = emptySet())
        `when`(queueClient.requestStyles(anyString(), eq("supertonic")))
            .thenThrow(IllegalStateException("no worker"))

        val error = service.styles().leftOrNull()

        assertEquals("WORKER_CONNECTION_ERROR", error?.code)
        assertEquals("supertonic: no worker", error?.message)
    }

    @Test
    fun `styles reject an unknown model with a bad request`() {
        val error = service.styles("nope").leftOrNull()

        assertEquals("INVALID_REQUEST", error?.code)
        assertTrue(error!!.message.contains("nope"))
    }

    private fun counts(wait: Int, running: Int, total: Int) = JobCountsResponse(
        total = total,
        byState = emptyMap(),
        byPriority = emptyMap(),
        wait = wait,
        running = running,
        terminal = total - wait - running,
    )
}
