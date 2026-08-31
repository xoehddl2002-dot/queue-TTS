package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * `/api/jobs` 요청 본문은 중첩(`{"payload":{...}}`)과 평면(`{"text":...}`) 두 형태를 모두 받는다.
 * 두 형태가 섞였을 때 어떤 값이 이기는지가 이 클래스의 핵심 규칙이다.
 */
class CreateJobRequestDeserializerTests {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `nested payload is read as is`() {
        val request = read("""{"priority":"high","payload":{"text":"hello","source":"user_web"}}""")

        assertEquals("hello", request.payload?.text)
        assertEquals("user_web", request.payload?.source)
        assertEquals("high", request.priority)
    }

    @Test
    fun `flat payload is lifted into the payload object`() {
        val request = read("""{"text":"hello","priority":"low","source":"counselor"}""")

        assertEquals("hello", request.payload?.text)
        assertEquals("counselor", request.payload?.source)
        assertEquals("low", request.priority)
    }

    @Test
    fun `nested payload without a source absorbs the top level one`() {
        val request = read("""{"source":"user_web","payload":{"text":"hello"}}""")

        assertEquals("hello", request.payload?.text)
        assertEquals("user_web", request.payload?.source)
    }

    @Test
    fun `nested source wins over the top level one`() {
        val request = read("""{"source":"top","payload":{"text":"hello","source":"nested"}}""")

        assertEquals("nested", request.payload?.source)
    }

    @Test
    fun `top level source is recognised through every alias`() {
        listOf("jobSource", "job_source", "apiType", "api_type", "clientType", "client_type", "page", "channel")
            .forEach { alias ->
                val request = read("""{"text":"hello","$alias":"user_web"}""")

                assertEquals("user_web", request.payload?.source, "alias $alias should resolve to source")
            }
    }

    @Test
    fun `synthesis parameters survive both spellings`() {
        val snake = read("""{"text":"hello","response_format":"wav","max_chunk_length":120}""")
        val camel = read("""{"text":"hello","responseFormat":"wav","maxChunkLength":120}""")

        assertEquals("wav", snake.payload?.responseFormat)
        assertEquals(120, snake.payload?.maxChunkLength)
        assertEquals(snake.payload, camel.payload)
    }

    @Test
    fun `missing priority stays null so the service can pick a default`() {
        val request = read("""{"text":"hello"}""")

        assertNull(request.priority)
    }

    @Test
    fun `explicit null priority is treated as absent`() {
        val request = read("""{"text":"hello","priority":null}""")

        assertNull(request.priority)
    }

    /**
     * 예전에는 스키마에 없는 필드를 무시했다. 그 관대함이 "다른 엔진의 파라미터를 보냈는데 아무 일도
     * 안 일어난다"를 만들었으므로, 모델별 payload 타입이 생긴 지금은 거절한다.
     */
    @Test
    fun `unknown fields are rejected instead of being silently dropped`() {
        val request = read("""{"text":"hello","totallyUnknown":{"nested":1}}""")

        assertNull(request.payload)
        assertNotNull(request.payloadError)
    }

    @Test
    fun `a nested payload is not judged by the fields left at the top level`() {
        // 중첩 형태에서 최상위는 봉투다. 거기 뭐가 있든 payload 바인딩을 깨뜨리지 않는다.
        val request = read("""{"totallyUnknown":1,"payload":{"text":"hello"}}""")

        assertEquals("hello", request.payload?.text)
        assertNull(request.payloadError)
    }

    @Test
    fun `empty body yields an empty payload`() {
        val request = read("{}")

        assertNull(request.priority)
        assertNull(request.payload?.text)
        assertNull(request.payload?.source)
    }

    @Test
    fun `a non object payload falls back to the flat reading`() {
        val request = read("""{"text":"hello","payload":"nonsense"}""")

        assertEquals("hello", request.payload?.text)
    }

    private fun read(json: String): CreateJobRequest =
        objectMapper.readValue(json, CreateJobRequest::class.java)
}
