package com.aitts.queuetts.gateway.api.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 모델별 큐 설정의 해석 규칙을 고정한다.
 *
 * 여기가 틀리면 잡이 엉뚱한 워커 풀로 가거나(엔진 불일치로 실패), 아무 풀에도 안 들어가
 * timeout 될 때까지 방치된다.
 */
class QueueModelPropertiesTests {

    @Test
    fun `models 를 비워두면 레거시 단일 모델 설정이 기본 모델로 승격된다`() {
        val queue = QueueTtsGatewayProperties.Queue(
            jobStream = "tts:jobs",
            jobGroup = "tts-workers",
            defaultModel = "supertonic",
        )

        assertEquals(
            mapOf("supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers")),
            queue.modelQueues,
        )
    }

    @Test
    fun `models 를 지정하면 그것만 쓴다`() {
        val queue = twoModelQueue()

        assertEquals(setOf("supertonic", "qwen"), queue.modelQueues.keys)
        assertEquals("qwen:jobs", queue.modelQueues.getValue("qwen").jobStream)
        assertEquals("qwen-workers", queue.modelQueues.getValue("qwen").jobGroup)
    }

    @Test
    fun `model 이 비어 있으면 기본 모델로 해석한다`() {
        val queue = twoModelQueue()

        assertEquals("supertonic", queue.canonicalModel(null))
        assertEquals("supertonic", queue.canonicalModel(""))
        assertEquals("supertonic", queue.canonicalModel("   "))
    }

    @Test
    fun `model 이름은 대소문자를 무시하고 정식 키로 정규화된다`() {
        val queue = twoModelQueue()

        assertEquals("qwen", queue.canonicalModel("qwen"))
        assertEquals("qwen", queue.canonicalModel("QWEN"))
        assertEquals("qwen", queue.canonicalModel(" Qwen "))
    }

    @Test
    fun `등록되지 않은 model 은 null 로 걸러진다`() {
        val queue = twoModelQueue()

        assertNull(queue.canonicalModel("supertonicx"))
        assertNull(queue.canonicalModel("gpt"))
    }

    private fun twoModelQueue() = QueueTtsGatewayProperties.Queue(
        defaultModel = "supertonic",
        models = mapOf(
            "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
            "qwen" to QueueTtsGatewayProperties.ModelQueue("qwen:jobs", "qwen-workers"),
        ),
    )
}
