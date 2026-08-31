package com.aitts.queuetts.gateway.api.scheduler

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.queue.PendingJobResults
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.service.JobService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.RecordId
import java.time.OffsetDateTime
import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * worker 가 result 스트림에 넣은 raw 필드를 잡 상태로 옮기는 지점.
 *
 * worker 구현마다 camelCase / snake_case 가 섞여 오고, 잘못된 레코드 하나가 리스너를 막으면
 * 안 되기 때문에 (ack 는 항상 해야 한다) 분기를 하나씩 고정한다.
 */
class JobResultListenerTests {
    private val objectMapper = jacksonObjectMapper()
    private val jobService: JobService = mock(JobService::class.java)
    private val streamQueueClient: RedisStreamQueueClient = mock(RedisStreamQueueClient::class.java)
    private val pendingJobResults = PendingJobResults()

    private val listener = JobResultListener(
        properties = QueueTtsGatewayProperties(),
        streamQueueClient = streamQueueClient,
        jobService = jobService,
        pendingJobResults = pendingJobResults,
        objectMapper = objectMapper,
        connectionFactory = mock(RedisConnectionFactory::class.java),
    )

    @Test
    fun `a camel case result is applied to the job`() {
        listener.handleRecord(
            record(
                "jobId" to "job_1",
                "workerId" to "worker-1",
                "batchId" to "batch-1",
                "state" to "succeeded",
                "startedAt" to "2026-07-20T00:00:00Z",
                "result" to """{"durationS":1.5}""",
            ),
        )

        val applied = appliedEvent()
        assertEquals("job_1", applied.jobId)
        assertEquals("worker-1", applied.workerId)
        assertEquals("batch-1", applied.batchId)
        assertEquals("succeeded", applied.state)
        assertEquals(OffsetDateTime.parse("2026-07-20T00:00:00Z"), applied.startedAt)
        assertEquals(1.5, applied.result?.durationS)
    }

    @Test
    fun `a snake case result from another worker implementation is read the same way`() {
        listener.handleRecord(
            record(
                "job_id" to "job_1",
                "worker_id" to "worker-1",
                "batch_id" to "batch-1",
                "status" to "succeeded",
                "started_at" to "2026-07-20T00:00:00Z",
            ),
        )

        val applied = appliedEvent()
        assertEquals("job_1", applied.jobId)
        assertEquals("worker-1", applied.workerId)
        assertEquals("batch-1", applied.batchId)
        assertEquals("succeeded", applied.state)
        assertEquals(OffsetDateTime.parse("2026-07-20T00:00:00Z"), applied.startedAt)
    }

    @Test
    fun `an artifact payload is decoded into the completion`() {
        val artifact = """{"file_name":"out.wav","media_type":"audio/wav","content_base64":"${
            Base64.getEncoder().encodeToString("audio".toByteArray())
        }"}"""

        listener.handleRecord(
            record("jobId" to "job_1", "workerId" to "worker-1", "state" to "succeeded", "artifact" to artifact),
        )

        val applied = appliedEvent()
        assertEquals("out.wav", applied.artifact?.fileName)
        assertEquals("audio/wav", applied.artifact?.mediaType)
    }

    @Test
    fun `a result without a worker id is still attributed to something`() {
        listener.handleRecord(record("jobId" to "job_1", "state" to "succeeded"))

        assertEquals("unknown-worker", appliedEvent().workerId)
    }

    @Test
    fun `a record with no job id is dropped but still acknowledged`() {
        val record = record("workerId" to "worker-1", "state" to "succeeded")

        listener.handleRecord(record)

        verify(jobService, never()).applyQueueEvent(anyCompletion())
        verify(streamQueueClient).acknowledgeResult(record)
    }

    @Test
    fun `an unparsable record does not block the listener and is acknowledged`() {
        val record = record(
            "jobId" to "job_1",
            "workerId" to "worker-1",
            "state" to "succeeded",
            "artifact" to "{not json",
        )

        listener.handleRecord(record)

        verify(jobService, never()).applyQueueEvent(anyCompletion())
        verify(streamQueueClient).acknowledgeResult(record)
    }

    @Test
    fun `a failure to acknowledge does not propagate`() {
        val record = record("jobId" to "job_1", "workerId" to "worker-1", "state" to "succeeded")
        doThrow(IllegalStateException("redis gone")).`when`(streamQueueClient).acknowledgeResult(record)

        listener.handleRecord(record)

        verify(jobService).applyQueueEvent(anyCompletion())
    }

    @Test
    fun `a control request is answered to its caller without touching job state`() {
        val waiting = pendingJobResults.register("req_abc")

        listener.handleRecord(
            record(
                "jobId" to "req_abc",
                "workerId" to "worker-1",
                "state" to "succeeded",
                "result" to """{"styles":[{"name":"Na-in-ae","kind":"custom"}]}""",
            ),
        )

        // req_ 접두사는 일회성 제어 요청이라 job 목록/이력에 남지 않아야 한다.
        verify(jobService, never()).applyQueueEvent(anyCompletion())
        val completion = waiting.get(1, TimeUnit.SECONDS)
        assertEquals(listOf("Na-in-ae"), completion.styleCatalog?.styles?.map { it.name })
        // 제어 응답은 오디오 결과로 해석하지 않는다.
        assertNull(completion.result)
    }

    @Test
    fun `a running event updates the job but keeps synchronous callers waiting`() {
        val waiting = pendingJobResults.register("job_1")

        listener.handleRecord(record("jobId" to "job_1", "workerId" to "worker-1", "state" to "running"))

        assertEquals("running", appliedEvent().state)
        assertTrue(!waiting.isDone, "running is not a terminal result")
    }

    @Test
    fun `a terminal event releases the synchronous caller`() {
        val waiting = pendingJobResults.register("job_1")

        listener.handleRecord(record("jobId" to "job_1", "workerId" to "worker-1", "state" to "failed"))

        assertNotNull(waiting.get(1, TimeUnit.SECONDS))
    }

    private fun record(vararg fields: Pair<String, String>): MapRecord<String, String, String> =
        MapRecord.create("tts:results", fields.toMap()).withId(RecordId.of("1-0"))

    private fun appliedEvent(): RedisResultMessage {
        val captor = ArgumentCaptor.forClass(RedisResultMessage::class.java)
        // capture() 는 null 을 돌려주므로 더미 값으로 받쳐준다 (여기서 matcher 를 또 쓰면 인자 수가 어긋난다).
        verify(jobService).applyQueueEvent(captor.capture() ?: RedisResultMessage(jobId = "captured"))
        return captor.value
    }

    // Mockito matcher 는 null 을 돌려주므로 Kotlin non-null 파라미터용 더미 값으로 받쳐준다.
    private fun anyCompletion(): RedisResultMessage =
        ArgumentMatchers.any(RedisResultMessage::class.java) ?: RedisResultMessage(jobId = "any")
}
