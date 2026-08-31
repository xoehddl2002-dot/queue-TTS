package com.aitts.queuetts.gateway.api.service

import com.aitts.queuetts.gateway.api.dto.SupertonicJobPayload
import arrow.core.Either
import com.aitts.queuetts.gateway.api.error.JobError
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.config.security.CurrentCaller
import com.aitts.queuetts.gateway.api.dto.AudioJobResult
import com.aitts.queuetts.gateway.api.dto.CreateJobRequest
import com.aitts.queuetts.gateway.api.dto.JobError as JobErrorPayload
import com.aitts.queuetts.gateway.api.dto.JobArtifactResponse
import com.aitts.queuetts.gateway.api.dto.JobHistoryResponse
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.aitts.queuetts.gateway.api.dto.QwenJobPayload
import com.aitts.queuetts.gateway.api.dto.JobResponse
import com.aitts.queuetts.gateway.api.dto.PersistedGatewayJob
import com.aitts.queuetts.gateway.api.infra.db.model.TtsSpeaker
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsJobGenerationHistoryRepository
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import com.aitts.queuetts.gateway.api.infra.redis.messaging.ArtifactContent
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.queue.PendingJobResults
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.SpeakerBlobStore
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.nullable
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.nio.file.Files
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.Base64
import java.util.UUID
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/** 성공을 기대하는 테스트에서 `Either` 의 right 를 꺼낸다. left 면 그 에러를 그대로 실패 메시지에 남긴다. */
private fun <T> Either<JobError, T>.value(): T = fold(
    ifLeft = { error("expected success but got ${it::class.simpleName}: ${it.message}") },
    ifRight = { it },
)

class JobServiceTests {
    @Test
    fun `createJob rejects blank text without creating history`() {
        val service = newService()

        val error = service.createJob(jobRequest("   \t\r\n")).leftOrNull()

        assertIs<JobError.InvalidRequest>(error)
        assertEquals("Text is empty.", error.message)
        assertEquals(0, service.listJobs(null, null, 100, 0).value().total)
    }

    @Test
    fun `createJob rejects text over the limit without creating history`() {
        val service = newService()
        val limit = QueueTtsGatewayProperties().job.textMaxLength

        val error = service.createJob(jobRequest("가".repeat(limit + 1))).leftOrNull()

        assertIs<JobError.InvalidRequest>(error)
        assertEquals("Text is too long: ${limit + 1} characters (max $limit).", error.message)
        // 접수 자체가 없었어야 한다 — 발행됐다면 워커가 붙잡히고 취소도 전파되지 않는다.
        assertEquals(0, service.listJobs(null, null, 100, 0).value().total)
    }

    @Test
    fun `createJob accepts text exactly at the limit`() {
        val service = newService()
        val limit = QueueTtsGatewayProperties().job.textMaxLength

        val created = service.createJob(jobRequest("가".repeat(limit))).value()

        assertEquals("accepted", created.status)
    }

    @Test
    fun `createJob applies source priority defaults`() {
        val service = newService()
        val created = service.createJob(jobRequest("hello", source = "user_web")).value()
        val job = service.getJob(created.jobId).value()

        assertEquals("accepted", created.status)
        assertEquals("wait", created.state)
        assertEquals("high", created.priority)
        assertEquals("user_web", job.payload?.source)
    }

    @Test
    fun `createJob assigns tts type only when publishing to redis`() {
        val queueClient = redisQueueService()
        val service = newService(queueClient = queueClient)

        val created = service.createJob(jobRequest("hello")).value()

        verify(queueClient).publishJob(
            created.jobId,
            "tts",
            SupertonicJobPayload(text = "hello"),
            "normal",
            null,
        )
    }

    @Test
    fun `qwen job resolves style and merges request over style defaults`() {
        val queue = mock(RedisStreamQueueClient::class.java)
        `when`(queue.enabled).thenReturn(true)
        `when`(queue.canonicalModel("qwen")).thenReturn("qwen")
        `when`(queue.modelNames).thenReturn(setOf("supertonic", "qwen"))
        var published: JobPayload? = null
        `when`(queue.publishJob(anyString(), anyString(), any(), anyString(), nullable(String::class.java), nullable(String::class.java)))
            .thenAnswer { invocation ->
                published = invocation.getArgument(2)
                "1-0"
            }

        val repository = mock(TtsSpeakerRepository::class.java)
        val style = style(defaultParams = """{"temperature":0.85,"top_k":42,"top_p":null}""")
        `when`(repository.findByName("qwen", "나인애")).thenReturn(style)
        val referenceAudio = byteArrayOf(1, 2, 3)
        `when`(repository.findAudio(style.model, style.name)).thenReturn(referenceAudio)
        val properties = testProperties().copy(
            queue = QueueTtsGatewayProperties.Queue(
                defaultModel = "supertonic",
                voiceModel = "qwen",
                speakerBlobPrefix = "qwen-test",
                models = mapOf(
                    "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                    "qwen" to QueueTtsGatewayProperties.ModelQueue("qwen:jobs", "qwen-workers"),
                ),
            ),
        )
        val blobStore = mock(SpeakerBlobStore::class.java)
        `when`(blobStore.exists("나인애")).thenReturn(false)
        `when`(blobStore.keyFor("나인애")).thenReturn("qwen-test:style:blob:나인애")
        val service = newService(
            queueClient = queue,
            styleRepository = repository,
            speakerBlobStore = blobStore,
            properties = properties,
        )

        service.createJob(
            CreateJobRequest(
                payload = QwenJobPayload(
                    text = "안녕하세요",
                    model = "qwen",
                    voice = "나인애",
                    temperature = 0.7,
                ),
            ),
        ).value()

        val payload = assertIs<QwenJobPayload>(published)
        assertEquals("나인애", payload.voice, "이력용 voice 원문은 유지해야 한다")
        assertEquals(style.name, payload.speakerName)
        assertEquals(style.referenceDigest, payload.referenceDigest)
        assertEquals("qwen-test:style:blob:${style.name}", payload.speakerBlobKey)
        assertEquals(style.mode, payload.speakerMode)
        assertEquals(style.refText, payload.speakerRefText)
        assertEquals(style.language, payload.lang)
        assertEquals(0.7, payload.temperature, "요청값이 style 기본값보다 우선한다")
        assertEquals(42, payload.topK)
        assertNull(payload.topP, "null 기본값은 미지정으로 남아야 한다")
        verify(blobStore).put("나인애", referenceAudio)
    }

    @Test
    fun `queue completion stores successful artifact and history`() {
        val service = newService()
        val jobId = service.createJob(jobRequest("hello", "normal")).value().jobId
        val artifactBytes = "audio".toByteArray()

        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-1",
                batchId = "batch-1",
                state = "done",
                result = AudioJobResult(durationS = 1.25),
                artifact = ArtifactContent(
                    fileName = "bad:name.wav",
                    mediaType = "audio/wav",
                    contentBase64 = Base64.getEncoder().encodeToString(artifactBytes),
                ),
            ),
        )
        val succeeded = service.getJob(jobId).value()
        val artifact = requireNotNull(succeeded.artifact)
        val download = service.jobDownload(jobId).value()

        assertEquals("succeeded", succeeded.state)
        assertEquals("bad_name.wav", artifact.fileName)
        assertEquals(artifactBytes.size.toLong(), artifact.size)
        assertEquals("audio/wav", download.mediaType)
        assertEquals("bad_name.wav", download.fileName)
        assertContentEquals(artifactBytes, Files.readAllBytes(download.path))
    }

    @Test
    fun `queue completion retains worker batch id`() {
        val service = newService()
        val jobId = service.createJob(jobRequest("hello")).value().jobId

        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-1",
                batchId = "batch-42",
                state = "failed",
                error = JobErrorPayload(message = "failed"),
            ),
        )

        assertEquals("batch-42", service.getJob(jobId).value().batchId)
    }

    @Test
    fun `queue running event moves waiting job to running`() {
        val service = newService()
        val jobId = service.createJob(jobRequest("hello")).value().jobId
        val startedAt = OffsetDateTime.parse("2026-07-16T01:02:03Z")

        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-1",
                batchId = "batch-42",
                state = "running",
                startedAt = startedAt,
            ),
        )

        val job = service.getJob(jobId).value()
        val counts = service.healthCounts()
        assertEquals("running", job.state)
        assertEquals("worker-1", job.workerId)
        assertEquals("batch-42", job.batchId)
        assertEquals(startedAt, job.startedAt)
        assertEquals(0, counts.wait)
        assertEquals(1, counts.running)
    }

    @Test
    fun `terminal succeeded job ignores later failed queue result`() {
        val service = newService()
        val jobId = service.createJob(jobRequest("hello")).value().jobId
        val artifactBytes = "audio".toByteArray()

        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-1",
                batchId = "batch-success",
                state = "succeeded",
                artifact = ArtifactContent(
                    fileName = "result.wav",
                    mediaType = "audio/wav",
                    contentBase64 = Base64.getEncoder().encodeToString(artifactBytes),
                ),
            ),
        )
        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-2",
                batchId = "batch-failed",
                state = "failed",
                error = JobErrorPayload(message = "late failed duplicate"),
            ),
        )

        val job = service.getJob(jobId).value()
        assertEquals("succeeded", job.state)
        assertEquals("worker-1", job.workerId)
        assertEquals("batch-success", job.batchId)
        assertNotNull(job.artifact)
    }

    @Test
    fun `timed out waiting job fails and ignores later success`() {
        val service = newService(workerTimeoutSeconds = 0, queueClient = redisQueueService())
        val jobId = service.createJob(jobRequest("hello")).value().jobId

        val completions = service.failTimedOutJobs()

        assertEquals(listOf(jobId), completions.map { it.jobId })
        assertEquals("failed", service.getJob(jobId).value().state)

        service.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-1",
                batchId = "batch-late-success",
                state = "succeeded",
                artifact = ArtifactContent(
                    fileName = "late.wav",
                    mediaType = "audio/wav",
                    contentBase64 = Base64.getEncoder().encodeToString("late audio".toByteArray()),
                ),
            ),
        )

        val job = service.getJob(jobId).value()
        assertEquals("failed", job.state)
        assertEquals("worker_timeout", job.error?.code)
    }

    @Test
    fun `createJob returns a queue error and removes the unaccepted job when publish fails`() {
        val queueClient = mock(RedisStreamQueueClient::class.java)
        `when`(queueClient.enabled).thenReturn(true)
        doThrow(IllegalStateException("Redis is temporarily unavailable"))
            .`when`(queueClient)
            .publishJob(
                anyString(),
                anyString(),
                any(),
                anyString(),
                nullable(String::class.java),
                nullable(String::class.java),
            )
        val service = newService(queueClient = queueClient)

        val error = service.createJob(jobRequest("hello")).leftOrNull()

        assertEquals(JobError.QueueError::class, error!!::class)
        assertEquals("JOB_QUEUE_ERROR", error.code)
        assertEquals(0, service.listJobs(null, null, 100, 0).value().total)
    }

    @Test
    fun `listJobs and getJob read persisted history when runtime jobs are empty`() {
        val history = persistedHistory()
        val repository = mock(TtsJobGenerationHistoryRepository::class.java)
        `when`(repository.listJobs(null, null, null, 100, emptySet())).thenReturn(listOf(history))
        `when`(repository.countJobs(null, null, null, emptySet())).thenReturn(1)
        `when`(repository.findByJobId(history.job.jobId)).thenReturn(history)
        val service = newService(historyRepository = repository)

        val list = service.listJobs(null, null, 100, 0).value()
        val job = service.getJob(history.job.jobId).value()

        assertEquals(1, list.total)
        assertEquals(listOf(history.job.jobId), list.items.map { it.jobId })
        assertEquals("succeeded", job.state)
    }

    @Test
    fun `finished job is served from the history store, so deleting its row removes it`() {
        val repository = FakeHistoryRepository()
        val service = newService(historyRepository = repository)
        val jobId = service.createJob(jobRequest("hello")).value().jobId

        service.applyQueueEvent(
            RedisResultMessage(jobId = jobId, workerId = "worker-1", batchId = null, state = "succeeded"),
        )

        assertEquals(setOf(jobId), repository.rows.keys)
        assertEquals("succeeded", service.getJob(jobId).value().state)

        // DB 에서 행을 지운 상황. 끝난 job 을 메모리에 붙들고 있으면 여기서도 계속 조회된다.
        repository.rows.remove(jobId)

        assertEquals(JobError.NotFound::class, service.getJob(jobId).leftOrNull()!!::class)
        assertEquals(0, service.listJobs(null, null, 100, 0).value().total)
        assertEquals(0, service.healthCounts().total)
    }

    @Test
    fun `job whose history write fails stays in memory with the failure reason`() {
        val repository = FakeHistoryRepository(upsertFailure = IllegalStateException("db is down"))
        val service = newService(historyRepository = repository)
        val jobId = service.createJob(jobRequest("hello")).value().jobId

        service.applyQueueEvent(
            RedisResultMessage(jobId = jobId, workerId = "worker-1", batchId = null, state = "succeeded"),
        )

        // 기록에 실패한 job 까지 비우면 DB 에도 메모리에도 없어 흔적 없이 사라진다.
        val job = service.getJob(jobId).value()
        assertEquals("succeeded", job.state)
        assertEquals("db is down", job.historyError)
        assertEquals(1, service.listJobs(null, null, 100, 0).value().total)
    }

    @Test
    fun `listJobs filters runtime jobs by source`() {
        val service = newService()
        service.createJob(jobRequest("a", source = "tts")).value()
        service.createJob(jobRequest("b", source = "user")).value()

        val ttsOnly = service.listJobs(null, null, 100, 0, source = "tts").value()
        val all = service.listJobs(null, null, 100, 0).value()

        assertEquals(1, ttsOnly.total)
        assertEquals(listOf("tts"), ttsOnly.items.mapNotNull { it.source })
        assertEquals(2, all.total)
    }

    private fun newService(
        queueClient: RedisStreamQueueClient? = null,
        historyRepository: TtsJobGenerationHistoryRepository? = null,
        styleRepository: TtsSpeakerRepository? = null,
        speakerBlobStore: SpeakerBlobStore? = null,
        workerTimeoutSeconds: Long = 60,
        properties: QueueTtsGatewayProperties = testProperties(workerTimeoutSeconds = workerTimeoutSeconds),
    ): JobService {
        val beanFactory = DefaultListableBeanFactory()
        queueClient?.let { beanFactory.registerSingleton("redisStreamQueueClient", it) }
        historyRepository?.let { beanFactory.registerSingleton("ttsJobGenerationHistoryRepository", it) }
        styleRepository?.let { beanFactory.registerSingleton("ttsStyleRepository", it) }
        val blobStore = speakerBlobStore ?: mock(SpeakerBlobStore::class.java).also {
            `when`(it.exists(anyString())).thenReturn(true)
            `when`(it.keyFor(anyString())).thenAnswer { invocation ->
                "${properties.queue.speakerBlobPrefix}:style:blob:${invocation.getArgument<String>(0)}"
            }
        }
        return JobService(
            properties = properties,
            queueClient = mock(RedisQueueClient::class.java),
            pendingJobResults = PendingJobResults(),
            currentCaller = CurrentCaller(),
            streamQueueClientProvider = beanFactory.getBeanProvider(RedisStreamQueueClient::class.java),
            jobGenerationHistoryRepositoryProvider = beanFactory.getBeanProvider(TtsJobGenerationHistoryRepository::class.java),
            voiceResolvers = listOf(
                SpeakerRegistryVoiceResolver(
                    properties = properties,
                    speakerRepositoryProvider = beanFactory.getBeanProvider(TtsSpeakerRepository::class.java),
                    blobStore = blobStore,
                    objectMapper = ObjectMapper().findAndRegisterModules(),
                ),
            ),
        )
    }

    private fun testProperties(workerTimeoutSeconds: Long = 60): QueueTtsGatewayProperties = QueueTtsGatewayProperties(
        workerTimeoutSeconds = workerTimeoutSeconds,
        job = QueueTtsGatewayProperties.JobGateway(
            artifactDir = Path.of("build", "test-artifacts", UUID.randomUUID().toString()).toAbsolutePath(),
        ),
    )

    private fun redisQueueService(): RedisStreamQueueClient {
        val queueClient = mock(RedisStreamQueueClient::class.java)
        `when`(queueClient.enabled).thenReturn(true)
        return queueClient
    }

    private fun persistedHistory(): JobHistoryResponse {
        val createdAt = OffsetDateTime.parse("2026-07-08T00:00:00Z")
        val job = JobResponse(
            jobId = "job_persisted",
            state = "succeeded",
            priority = "normal",
            source = "user",
            caller = null,
            createdAt = createdAt,
            updatedAt = createdAt,
            startedAt = createdAt,
            finishedAt = createdAt,
            workerId = "worker-1",
            batchId = "batch-1",
            result = null,
            error = null,
            artifact = null,
            downloadUrl = null,
            statusUrl = "/api/jobs/job_persisted",
            historyError = null,
            version = 0,
            payload = SupertonicJobPayload(text = "persisted", source = "user"),
        )
        return JobHistoryResponse(
            job = job,
            artifactPath = null,
            artifactName = null,
            artifactMediaType = null,
            artifactSize = null,
        )
    }

    private fun style(defaultParams: String = "{}"): TtsSpeaker {
        val now = OffsetDateTime.parse("2026-08-19T00:00:00Z")
        return TtsSpeaker(
            name = "나인애",
            model = "qwen",
            language = "Korean",
            mode = TtsSpeaker.MODE_ICL,
            refText = "참조 발화",
            audio = ByteArray(0),
            audioSha256 = "audio-sha",
            referenceDigest = "reference-digest",
            audioFormat = "wav",
            sampleRate = 24000,
            durationS = 6.5,
            defaultParams = defaultParams,
            createdAt = now,
            updatedAt = now,
            referenceUpdatedAt = now,
        )
    }

    private fun jobRequest(
        text: String,
        priority: String? = null,
        source: String? = null,
    ): CreateJobRequest = CreateJobRequest(
        payload = SupertonicJobPayload(text = text, source = source),
        priority = priority,
    )
}

/**
 * upsert 된 행을 그대로 들고 있는 최소 이력 저장소.
 *
 * [rows] 에서 항목을 빼면 DB 에서 행을 지운 것과 같아, "지운 job 이 계속 조회되는지" 를
 * 그대로 재현할 수 있다.
 */
private class FakeHistoryRepository(
    private val upsertFailure: RuntimeException? = null,
) : TtsJobGenerationHistoryRepository(mock(NamedParameterJdbcTemplate::class.java), ObjectMapper()) {

    val rows = linkedMapOf<String, JobHistoryResponse>()

    override fun upsert(job: PersistedGatewayJob) {
        upsertFailure?.let { throw it }
        rows[job.jobId] = job.toHistoryResponse()
    }

    override fun findByJobId(jobId: String): JobHistoryResponse? = rows[jobId]

    override fun listJobs(
        state: String?,
        priority: String?,
        source: String?,
        limit: Int,
        excludeJobIds: Set<String>,
    ): List<JobHistoryResponse> = matching(state, priority, source, excludeJobIds).take(limit)

    override fun countJobs(
        state: String?,
        priority: String?,
        source: String?,
        excludeJobIds: Set<String>,
    ): Int = matching(state, priority, source, excludeJobIds).size

    override fun countByState(): Map<String, Int> = rows.values.groupingBy { it.job.state }.eachCount()

    private fun matching(
        state: String?,
        priority: String?,
        source: String?,
        excludeJobIds: Set<String>,
    ): List<JobHistoryResponse> = rows.values
        .filter { it.job.jobId !in excludeJobIds }
        .filter { state == null || it.job.state == state }
        .filter { priority == null || it.job.priority == priority }
        .filter { source == null || it.job.source?.lowercase() == source }
        .sortedByDescending { it.job.createdAt }

    /** 실제 저장소의 rowMapper 와 같은 모양으로 되돌린다. */
    private fun PersistedGatewayJob.toHistoryResponse() = JobHistoryResponse(
        job = JobResponse(
            jobId = jobId,
            state = state,
            priority = priority,
            source = payload.source,
            caller = caller,
            createdAt = createdAt,
            updatedAt = finishedAt ?: createdAt,
            startedAt = startedAt,
            finishedAt = finishedAt,
            workerId = workerId,
            batchId = batchId,
            result = result,
            error = error,
            artifact = artifactPath?.let {
                JobArtifactResponse(it, artifactName, artifactMediaType, artifactSize)
            },
            downloadUrl = downloadUrl,
            statusUrl = "/api/jobs/$jobId",
            historyError = null,
            version = 0,
            payload = payload,
        ),
        artifactPath = artifactPath,
        artifactName = artifactName,
        artifactMediaType = artifactMediaType,
        artifactSize = artifactSize,
    )
}
