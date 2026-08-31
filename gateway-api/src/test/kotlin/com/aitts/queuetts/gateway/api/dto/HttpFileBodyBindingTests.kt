package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * `api-test-*.http` 에 적어 둔 요청 본문이 실제로 [CreateJobRequest] 로 바인딩되는지 고정한다.
 *
 * .http 파일은 실행해 봐야만 오타를 알 수 있어서, 필드 이름이 조용히 어긋나기 쉽다.
 * 모델별 payload 타입이 생긴 뒤로는 **그 엔진이 모르는 필드가 400** 이라, 어긋난 이름이
 * 기본값 합성으로 묻히지 않고 드러난다.
 */
class HttpFileBodyBindingTests {
    private val mapper = ObjectMapper().registerKotlinModule()

    private fun parse(json: String): CreateJobRequest =
        mapper.readValue(json, CreateJobRequest::class.java)

    @Test
    fun `supertonic 요청 본문이 그대로 바인딩된다`() {
        val request = parse(
            """
            {
              "priority": "high",
              "source": "tts",
              "payload": {
                "model": "supertonic",
                "text": "안녕하세요.",
                "voice": "Na-in-ae",
                "lang": "ko",
                "speed": 1.05,
                "steps": 12,
                "response_format": "wav",
                "seed": 0,
                "max_chunk_length": 200,
                "silence_duration": 0.3
              }
            }
            """.trimIndent(),
        )

        assertEquals("high", request.priority)
        val payload = assertIs<SupertonicJobPayload>(request.payload)
        assertEquals("supertonic", payload.model)
        assertEquals("안녕하세요.", payload.text)
        assertEquals("Na-in-ae", payload.voice)
        assertEquals("ko", payload.lang)
        assertEquals(1.05, payload.speed)
        assertEquals(12, payload.steps)
        assertEquals("wav", payload.responseFormat)
        assertEquals(0L, payload.seed)
        assertEquals(200, payload.maxChunkLength)
        assertEquals(0.3, payload.silenceDuration)
        // source 는 최상위에 적어도 payload 로 흡수된다.
        assertEquals("tts", payload.source)
    }

    @Test
    fun `qwen 요청 본문이 그대로 바인딩된다`() {
        val payload = assertIs<QwenJobPayload>(
            parse(
                """
                {
                  "priority": "high",
                  "source": "tts",
                  "payload": {
                    "model": "qwen",
                    "text": "안녕하세요.",
                    "voice": "Sohee",
                    "lang": "ko",
                    "response_format": "wav",
                    "seed": 12345,
                    "max_chunk_length": 400,
                    "silence_duration": 0.3,
                    "temperature": 0.85,
                    "top_p": 0.9
                  }
                }
                """.trimIndent(),
            ).payload,
        )

        assertEquals("qwen", payload.model)
        assertEquals("Sohee", payload.voice)
        assertEquals(400, payload.maxChunkLength)
        assertEquals(0.85, payload.temperature)
        assertEquals(0.9, payload.topP)
    }

    /**
     * 규격이 갈리기 전에는 이런 요청이 조용히 통과했고, `speed` 는 worker 에서 그냥 사라졌다 —
     * "슬라이더를 움직였는데 왜 그대로냐"를 추적할 수 없던 원인이다. 이제는 접수에서 막는다.
     */
    @Test
    fun `다른 엔진의 파라미터를 실어 보내면 400 이다`() {
        val request = parse(
            """
            {
              "priority": "high",
              "payload": { "model": "qwen", "text": "안녕하세요.", "voice": "Sohee", "speed": 1.05 }
            }
            """.trimIndent(),
        )

        assertNull(request.payload)
        val error = assertNotNull(request.payloadError)
        assertContains(error, "qwen")
        assertContains(error, "speed")
    }

    @Test
    fun `등록되지 않은 엔진 이름은 400 이다`() {
        val request = parse("""{"payload": {"model": "cosyvoice", "text": "hi"}}""")

        assertNull(request.payload)
        assertContains(assertNotNull(request.payloadError), "cosyvoice")
    }

    @Test
    fun `model 을 생략하면 기본 엔진 payload 로 바인딩된다`() {
        val payload = assertIs<SupertonicJobPayload>(
            parse(
                """
                {
                  "priority": "normal",
                  "source": "tts",
                  "payload": { "text": "기본 엔진", "voice": "F1", "lang": "ko" }
                }
                """.trimIndent(),
            ).payload,
        )

        // model 은 여전히 null 이다 — 라우팅은 queuetts.queue.default-model 이 정한다.
        assertNull(payload.model)
        assertEquals("기본 엔진", payload.text)
    }

    @Test
    fun `엔진 이름은 대소문자를 가리지 않는다`() {
        // 라우팅(canonicalModel)이 대소문자를 무시하므로 바인딩도 같은 규칙이어야 어긋나지 않는다.
        assertIs<QwenJobPayload>(parse("""{"model": "QWEN", "text": "hi"}""").payload)
    }
}
