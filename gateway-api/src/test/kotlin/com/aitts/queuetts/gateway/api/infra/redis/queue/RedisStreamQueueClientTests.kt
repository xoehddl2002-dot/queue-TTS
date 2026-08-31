package com.aitts.queuetts.gateway.api.infra.redis.queue

import com.aitts.queuetts.gateway.api.dto.SupertonicJobPayload
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord

import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamInfo
import org.springframework.data.redis.core.StreamOperations
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Redis 경계의 동작을 실제 Redis 없이 고정한다: 우선순위별 스트림 선택, 장애 변환,
 * 그리고 `XINFO CONSUMERS` 를 합쳐 worker 를 세는 규칙(유령 consumer 정리 포함).
 */
class RedisStreamQueueClientTests {
    private val objectMapper = jacksonObjectMapper()

    @Suppress("UNCHECKED_CAST")
    private val streamOps: StreamOperations<String, String, String> =
        mock(StreamOperations::class.java) as StreamOperations<String, String, String>

    private val redisTemplate: StringRedisTemplate = mock(StringRedisTemplate::class.java).also {
        `when`(it.opsForStream<String, String>()).thenReturn(streamOps)
    }

    @Test
    fun `job goes to the stream of its own priority`() {
        val client = newClient()
        stubAdd()

        val recordId = client.publishJob("job_1", "tts", SupertonicJobPayload(text = "hello"), "urgent", "user_web")

        assertEquals("1-0", recordId)
        assertEquals("tts:jobs:urgent", addedStreams().single())
    }

    @Test
    fun `each priority has its own stream and unknown ones are rejected`() {
        val client = newClient()
        stubAdd()

        JobPriorities.ordered.forEach { priority ->
            client.publishJob("job_$priority", "tts", SupertonicJobPayload(text = "hello"), priority, null)
        }

        assertEquals(
            listOf("tts:jobs:urgent", "tts:jobs:high", "tts:jobs:normal", "tts:jobs:low"),
            addedStreams(),
        )
        assertFailsWith<IllegalArgumentException> {
            client.publishJob("job_x", "tts", SupertonicJobPayload(text = "hello"), "whenever", null)
        }
    }

    @Test
    fun `published stream is trimmed to the configured length`() {
        val client = newClient()
        stubAdd()

        client.publishJob("job_1", "tts", SupertonicJobPayload(text = "hello"), "normal", null)

        verify(streamOps).trim("tts:jobs:normal", 100_000L, true)
    }

    @Test
    fun `a redis outage surfaces as an unavailable error instead of a driver exception`() {
        val client = newClient()
        `when`(streamOps.add(anyMapRecord())).thenThrow(RedisConnectionFailureException("connection refused"))

        val error = assertFailsWith<IllegalStateException> {
            client.publishJob("job_1", "tts", SupertonicJobPayload(text = "hello"), "normal", null)
        }

        assertEquals("Redis is temporarily unavailable", error.message)
    }

    @Test
    fun `an XADD that returns no id is an error rather than a silent success`() {
        val client = newClient()
        `when`(streamOps.add(anyMapRecord())).thenReturn(null)

        assertFailsWith<IllegalStateException> {
            client.publishJob("job_1", "tts", SupertonicJobPayload(text = "hello"), "normal", null)
        }
    }

    @Test
    fun `a worker seen on several streams is counted once with its worst idle time`() {
        val client = newClient()
        stubConsumers(
            "tts:jobs:urgent" to listOf(consumer("worker-1", pending = 1, idleMs = 9_000)),
            "tts:jobs:high" to listOf(consumer("worker-1", pending = 2, idleMs = 500)),
            "tts:jobs:normal" to listOf(consumer("worker-2", pending = 0, idleMs = 1_000)),
        )

        val workers = client.workerSnapshots()

        assertEquals(listOf("worker-1", "worker-2"), workers.map { it.name })
        val first = workers.first()
        assertEquals(3, first.pending)
        // 여러 스트림에 걸쳐 있으면 가장 최근 활동(가장 작은 idle)을 그 worker 의 idle 로 본다.
        assertEquals(500, first.idleMs)
        assertTrue(first.active)
    }

    @Test
    fun `a consumer idling past the active window is reported as stale but kept`() {
        val client = newClient()
        stubConsumers("tts:jobs:normal" to listOf(consumer("worker-1", pending = 0, idleMs = 60_000)))

        val workers = client.workerSnapshots()

        assertEquals(1, workers.size)
        assertEquals(false, workers.first().active)
        verify(streamOps, never()).deleteConsumer(anyString(), anyConsumer())
    }

    @Test
    fun `an idle consumer with no pending work is evicted from every priority stream`() {
        val client = newClient()
        stubConsumers("tts:jobs:normal" to listOf(consumer("ghost", pending = 0, idleMs = 600_000)))

        val workers = client.workerSnapshots()

        assertTrue(workers.isEmpty())
        JobPriorities.streams("tts:jobs").values.forEach { stream ->
            verify(streamOps).deleteConsumer(stream, Consumer.from("tts-workers", "ghost"))
        }
    }

    @Test
    fun `an idle consumer still holding pending work is never evicted`() {
        val client = newClient()
        stubConsumers("tts:jobs:normal" to listOf(consumer("busy-but-quiet", pending = 3, idleMs = 600_000)))

        val workers = client.workerSnapshots()

        assertEquals(listOf("busy-but-quiet"), workers.map { it.name })
        verify(streamOps, never()).deleteConsumer(anyString(), anyConsumer())
    }

    @Test
    fun `health never fails just because redis is down`() {
        val client = newClient()
        `when`(streamOps.consumers(anyString(), anyString()))
            .thenThrow(RedisConnectionFailureException("connection refused"))

        assertTrue(client.workerResponsesForHealth().isEmpty())
    }

    @Test
    fun `a disabled queue reports no workers without touching redis`() {
        val client = newClient(enabled = false)

        assertTrue(client.workerSnapshots().isEmpty())
        verify(streamOps, never()).consumers(anyString(), anyString())
    }

    @Test
    fun `a handled result is acknowledged and dropped from the stream`() {
        val client = newClient()
        val record = MapRecord.create("tts:results", mapOf("jobId" to "job_1"))
            .withId(RecordId.of("5-0"))

        client.acknowledgeResult(record)

        verify(streamOps).acknowledge("tts:results", "gateway", record.id)
        verify(streamOps).delete("tts:results", record.id)
    }

    private fun newClient(enabled: Boolean = true): RedisStreamQueueClient = RedisStreamQueueClient(
        properties = QueueTtsGatewayProperties(
            queue = QueueTtsGatewayProperties.Queue(
                enabled = enabled,
                jobStream = "tts:jobs",
                jobGroup = "tts-workers",
                resultStream = "tts:results",
                resultGroup = "gateway",
                jobStreamMaxLength = 100_000,
                workerActiveIdleMs = 30_000,
                workerEvictIdleMs = 300_000,
            ),
        ),
        redisTemplate = redisTemplate,
        objectMapper = objectMapper,
    )

    private fun stubAdd() {
        `when`(streamOps.add(anyMapRecord())).thenReturn(RecordId.of("1-0"))
    }

    /** XADD 된 레코드들이 어느 스트림으로 갔는지 호출 순서대로 돌려준다. */
    @Suppress("UNCHECKED_CAST")
    private fun addedStreams(): List<String> {
        val captor = ArgumentCaptor.forClass(MapRecord::class.java)
            as ArgumentCaptor<MapRecord<String, String, String>>
        verify(streamOps, atLeastOnce()).add(captor.capture() ?: fallbackRecord())
        return captor.allValues.map { it.stream!! }
    }

    /** 지정하지 않은 우선순위 스트림에는 consumer 가 없는 것으로 둔다. */
    private fun stubConsumers(vararg streams: Pair<String, List<List<Any>>>) {
        val byStream = streams.toMap()
        JobPriorities.streams("tts:jobs").values.forEach { stream ->
            `when`(streamOps.consumers(stream, "tts-workers"))
                .thenReturn(StreamInfo.XInfoConsumers.fromList("tts-workers", byStream[stream].orEmpty()))
        }
    }

    /** `XINFO CONSUMERS` 가 돌려주는 한 consumer 의 원본 필드 목록. */
    private fun consumer(name: String, pending: Long, idleMs: Long): List<Any> =
        listOf("name", name, "pending", pending, "idle", idleMs)

    // 프로덕션 코드가 MapRecord 를 넘기므로 stub/verify 도 같은 오버로드를 잡아야 한다.
    // (Mockito matcher 는 null 을 돌려주므로 Kotlin non-null 파라미터용 더미 값으로 받쳐준다.)
    @Suppress("UNCHECKED_CAST")
    private fun anyMapRecord(): MapRecord<String, String, String> =
        (ArgumentMatchers.any(MapRecord::class.java) as MapRecord<String, String, String>?) ?: fallbackRecord()

    private fun anyConsumer(): Consumer =
        ArgumentMatchers.any(Consumer::class.java) ?: Consumer.from("tts-workers", "any")

    private fun fallbackRecord(): MapRecord<String, String, String> =
        MapRecord.create("tts:jobs:normal", emptyMap())
}
