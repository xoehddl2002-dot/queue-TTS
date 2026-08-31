package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlin.reflect.full.memberProperties
import kotlin.test.*

class QwenDtoTests {
    private val mapper = jacksonObjectMapper()

    @Test
    fun `create API uses the Qwen voice clone parameter names`() {
        val request = mapper.readValue<CreateSpeakerRequest>(
            """
            {
              "name": "나인애",
              "ref_text": "참조 발화",
              "x_vector_only_mode": false,
              "language": "ko"
            }
            """.trimIndent(),
        )

        assertEquals("참조 발화", request.refText)
        assertFalse(request.xVectorOnlyMode)
        assertEquals("ko", request.language)
    }

    @Test
    fun `create metadata defaults to ICL`() {
        val request = mapper.readValue<CreateSpeakerRequest>(
            """{"name":"voice","ref_text":"hello"}""",
        )

        assertFalse(request.xVectorOnlyMode)
    }

    @Test
    fun `patch detects each Qwen prompt input independently`() {
        assertTrue(UpdateSpeakerRequest(refText = "fixed transcript").touchesReference())
        assertTrue(UpdateSpeakerRequest(xVectorOnlyMode = true).touchesReference())
        assertFalse(UpdateSpeakerRequest(name = "renamed").touchesReference())
    }

    @Test
    fun `default params bind to named fields instead of an untyped map`() {
        val request = mapper.readValue<CreateSpeakerRequest>(
            """
            {
              "name": "나인애",
              "ref_text": "참조 발화",
              "default_params": {"temperature": 0.85, "top_k": 42, "subtalker_top_p": 0.9}
            }
            """.trimIndent(),
        )

        val params = assertNotNull(request.defaultParams)
        assertEquals(0.85, params.temperature)
        assertEquals(42, params.topK)
        assertEquals(0.9, params.subtalkerTopP)
        // 주지 않은 키는 null 로 남아 저장 JSON 에도 job payload 에도 실리지 않는다.
        assertNull(params.doSample)
    }

    @Test
    fun `unspecified default params are omitted from the stored json`() {
        val json = mapper.readTree(mapper.writeValueAsString(QwenSpeakerParams(temperature = 0.7)))

        assertEquals(0.7, json.path("temperature").asDouble())
        assertFalse(json.has("top_p"), "null 은 지정하지 않음이므로 키 자체가 없어야 한다")
    }

    /**
     * 예전에는 `Map<String, Any?>` 였고 별도 allowlist 가 걸렀다. 타입으로 바뀐 뒤에도 그 거절이
     * 유지되는지 고정한다 — Spring Boot 는 `FAIL_ON_UNKNOWN_PROPERTIES` 를 꺼 두므로 타입만으로는
     * 안 걸리고, 요청 필드에 붙인 엄격 리더가 해 준다.
     */
    @Test
    fun `unknown default params are rejected on request binding`() {
        assertFailsWith<Exception> {
            mapper.readValue<CreateSpeakerRequest>(
                """{"name":"나인애","ref_text":"참조","default_params":{"speed":1.5}}""",
            )
        }
    }

    /**
     * speaker 에 저장한 기본값은 [QwenJobPayload] 로 흘러간다([QwenJobPayload.withDefaults]).
     * 한쪽에만 knob 을 추가하면 그 기본값이 조용히 무시되므로 두 필드 집합을 묶어 둔다.
     */
    @Test
    fun `speaker params and the qwen job payload declare the same knobs`() {
        val payloadKnobs = QwenJobPayload::class.memberProperties.map { it.name }.toSet()
        val speakerKnobs = QwenSpeakerParams::class.memberProperties.map { it.name }.toSet()

        assertTrue(
            speakerKnobs.all { it in payloadKnobs },
            "QwenJobPayload 에 없는 speaker 파라미터: ${speakerKnobs - payloadKnobs}",
        )
    }

    @Test
    fun `response exposes x vector mode instead of registry internal mode`() {
        val response = SpeakerResponse(
            name = "voice",
            model = "qwen",
            xVectorOnlyMode = true,
            referenceDigest = "digest",
        )

        val json = mapper.readTree(mapper.writeValueAsString(response))

        assertTrue(json.path("x_vector_only_mode").asBoolean())
        assertFalse(json.has("mode"))
        assertNull(json.get("ref_text"))
    }
}
