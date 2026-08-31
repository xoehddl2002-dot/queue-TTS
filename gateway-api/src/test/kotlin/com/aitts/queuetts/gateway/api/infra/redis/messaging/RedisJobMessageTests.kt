package com.aitts.queuetts.gateway.api.infra.redis.messaging

import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.aitts.queuetts.gateway.api.dto.QwenJobPayload
import com.aitts.queuetts.gateway.api.dto.SupertonicJobPayload
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import java.time.OffsetDateTime

/**
 * gateway 와 TTS worker 사이의 유일한 계약인 job 스트림 레코드의 필드 형태를 고정한다.
 * 여기서 키 이름이나 직렬화 방식이 바뀌면 worker 는 조용히 job 을 못 읽게 된다.
 */
class RedisJobMessageTests {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `job record carries the fields a worker needs`() {
        val fields = message(
            payload = SupertonicJobPayload(text = "hello", voice = "na-in-ae"),
            source = "user_web",
        ).toRedisFields(objectMapper)

        assertEquals("job_1", fields["jobId"])
        assertEquals("tts", fields["type"])
        assertEquals("high", fields["priority"])
        assertEquals("user_web", fields["source"])
        // ISO-8601 문자열로 실린다 (OffsetDateTime.toString 은 0초를 생략한다).
        assertEquals(OffsetDateTime.parse("2026-07-20T00:00:00Z"), OffsetDateTime.parse(fields.getValue("enqueuedAt")))
    }

    @Test
    fun `payload is embedded as a json document`() {
        val fields = message(payload = SupertonicJobPayload(text = "hello", speed = 1.5)).toRedisFields(objectMapper)

        val payload = objectMapper.readTree(fields.getValue("payload"))
        assertEquals("hello", payload.path("text").asText())
        assertEquals(1.5, payload.path("speed").asDouble())
        // 값이 없는 합성 파라미터는 실어 보내지 않는다 (worker 가 자기 기본값을 쓴다).
        assertFalse(payload.has("voice"))
    }

    @Test
    fun `payload json keeps the snake case names the worker reads`() {
        val fields = message(
            payload = SupertonicJobPayload(responseFormat = "wav", maxChunkLength = 120, silenceDuration = 0.3),
        ).toRedisFields(objectMapper)

        val payload = objectMapper.readTree(fields.getValue("payload"))
        assertEquals("wav", payload.path("response_format").asText())
        assertEquals(120, payload.path("max_chunk_length").asInt())
        assertEquals(0.3, payload.path("silence_duration").asDouble())
    }

    @Test
    fun `qwen speaker metadata and generation params match the worker contract`() {
        val fields = message(
            payload = QwenJobPayload(
                text = "hello",
                voice = "display-name",
                speakerName = "display-name",
                referenceDigest = "digest-1",
                speakerBlobKey = "qwen:style:blob:display-name",
                speakerMode = "icl",
                speakerRefText = "reference text",
                doSample = false,
                temperature = 0.7,
                topK = 42,
            ),
        ).toRedisFields(objectMapper)

        val payload = objectMapper.readTree(fields.getValue("payload"))
        // wire 이름이 speaker 로 바뀌었다. 여기가 바뀌면 Qwen worker 의 TtsPayload 도 같이 바뀌어야 한다.
        assertEquals("display-name", payload.path("speakerName").asText())
        assertEquals("digest-1", payload.path("referenceDigest").asText())
        assertEquals("qwen:style:blob:display-name", payload.path("speakerBlobKey").asText())
        assertEquals("icl", payload.path("speakerMode").asText())
        assertEquals("reference text", payload.path("speakerRefText").asText())
        assertFalse(payload.has("speakerId"), "ID 기반 키는 더 이상 내보내지 않는다")
        assertFalse(payload.path("do_sample").asBoolean())
        assertEquals(0.7, payload.path("temperature").asDouble())
        assertEquals(42, payload.path("top_k").asInt())
        assertFalse(payload.has("top_p"), "null generation params must be omitted")
    }

    @Test
    fun `missing payload is published as an empty json object rather than null`() {
        val fields = message(payload = null).toRedisFields(objectMapper)

        assertEquals("{}", fields.getValue("payload"))
    }

    @Test
    fun `source field is omitted entirely when the job has none`() {
        val fields = message(source = null).toRedisFields(objectMapper)

        assertFalse("source" in fields)
    }

    private fun message(
        payload: JobPayload? = SupertonicJobPayload(text = "hello"),
        source: String? = "user_web",
        priority: String = "high",
    ) = RedisJobMessage(
        jobId = "job_1",
        type = "tts",
        payload = payload,
        priority = priority,
        source = source,
        enqueuedAt = OffsetDateTime.parse("2026-07-20T00:00:00Z"),
    )
}
