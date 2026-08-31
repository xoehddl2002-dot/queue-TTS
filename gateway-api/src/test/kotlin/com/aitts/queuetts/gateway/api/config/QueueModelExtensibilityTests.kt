package com.aitts.queuetts.gateway.api.config

import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.fasterxml.jackson.databind.ObjectMapper
import org.mockito.Mockito.mock
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * **새 TTS 모델 추가는 설정만으로 끝나야 한다.**
 *
 * 이 테스트가 깨지면 "모델 하나 늘리는 데 Gateway 코드를 고쳐야 하는" 상태로 되돌아간 것이다.
 * 세 번째 엔진(`cosyvoice`)을 설정에만 넣고, 라우팅에 필요한 모든 것이 따라오는지 본다.
 *
 * 설정 검증 규칙도 함께 고정한다 — 새 모델을 넣으면서 stream/group 바꾸는 걸 잊는 실수가
 * 가장 위험하고(두 엔진이 같은 큐를 공유해 조용히 오작동), 기동을 막아야 하는 지점이다.
 */
class QueueModelExtensibilityTests {

    private fun queueOf(
        defaultModel: String = "supertonic",
        vararg models: Pair<String, QueueTtsGatewayProperties.ModelQueue>,
    ) = QueueTtsGatewayProperties.Queue(defaultModel = defaultModel, models = mapOf(*models))

    private fun clientFor(queue: QueueTtsGatewayProperties.Queue) = RedisStreamQueueClient(
        QueueTtsGatewayProperties(queue = queue),
        mock(StringRedisTemplate::class.java),
        ObjectMapper(),
    )

    private fun threeEngineQueue() = queueOf(
        models = arrayOf(
            "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
            "qwen" to QueueTtsGatewayProperties.ModelQueue("qwen:jobs", "qwen-workers"),
            "cosyvoice" to QueueTtsGatewayProperties.ModelQueue("cosy:jobs", "cosy-workers"),
        ),
    )

    @Test
    fun `설정에 모델을 한 줄 추가하면 코드 변경 없이 라우팅 대상이 된다`() {
        val client = clientFor(threeEngineQueue())

        assertTrue("cosyvoice" in client.modelNames)
        assertEquals("cosyvoice", client.canonicalModel("cosyvoice"))
        assertEquals(
            mapOf(
                "urgent" to "cosy:jobs:urgent",
                "high" to "cosy:jobs:high",
                "normal" to "cosy:jobs:normal",
                "low" to "cosy:jobs:low",
            ),
            client.priorityJobStreams("cosyvoice"),
        )
    }

    @Test
    fun `새 모델을 추가해도 기본 모델과 기존 모델은 그대로다`() {
        val client = clientFor(threeEngineQueue())

        assertEquals("supertonic", client.canonicalModel(null))
        assertEquals("tts:jobs:normal", client.priorityJobStreams()["normal"])
        assertEquals("qwen:jobs:normal", client.priorityJobStreams("qwen")["normal"])
    }

    @Test
    fun `정상 설정은 검증을 통과한다`() {
        assertEquals(emptyList(), threeEngineQueue().configurationProblems())
        clientFor(threeEngineQueue()).validateModelQueues()
    }

    @Test
    fun `두 모델이 job stream 을 공유하면 기동을 막는다`() {
        val queue = queueOf(
            models = arrayOf(
                "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                // stream 을 바꾸는 걸 잊은 전형적인 복붙 실수.
                "cosyvoice" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "cosy-workers"),
            ),
        )

        val problems = queue.configurationProblems()
        assertTrue(problems.any { it.contains("share job-stream") }, problems.toString())

        val failure = assertFailsWith<IllegalStateException> { clientFor(queue).validateModelQueues() }
        assertTrue(failure.message!!.contains("share job-stream"), failure.message!!)
    }

    @Test
    fun `두 모델이 consumer group 을 공유하면 기동을 막는다`() {
        val queue = queueOf(
            models = arrayOf(
                "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                "cosyvoice" to QueueTtsGatewayProperties.ModelQueue("cosy:jobs", "tts-workers"),
            ),
        )

        assertTrue(queue.configurationProblems().any { it.contains("share job-group") })
        assertFailsWith<IllegalStateException> { clientFor(queue).validateModelQueues() }
    }

    @Test
    fun `default-model 이 models 에 없으면 기동을 막는다`() {
        val queue = queueOf(
            defaultModel = "supertnoic", // 오타
            models = arrayOf("supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers")),
        )

        assertTrue(queue.configurationProblems().any { it.contains("default-model") })
        assertFailsWith<IllegalStateException> { clientFor(queue).validateModelQueues() }
    }

    @Test
    fun `이름이 대소문자만 다른 모델은 기동을 막는다`() {
        // canonicalModel 이 대소문자 무시로 매칭하므로 어느 쪽이 걸릴지 알 수 없다.
        val queue = queueOf(
            models = arrayOf(
                "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                "Supertonic" to QueueTtsGatewayProperties.ModelQueue("tts2:jobs", "tts2-workers"),
            ),
        )

        assertTrue(queue.configurationProblems().any { it.contains("differ only by case") })
    }

    @Test
    fun `stream 이나 group 이 비어 있으면 기동을 막는다`() {
        val queue = queueOf(
            models = arrayOf(
                "supertonic" to QueueTtsGatewayProperties.ModelQueue("tts:jobs", "tts-workers"),
                "cosyvoice" to QueueTtsGatewayProperties.ModelQueue("", ""),
            ),
        )

        val problems = queue.configurationProblems()
        assertTrue(problems.any { it.contains("blank job-stream") }, problems.toString())
        assertTrue(problems.any { it.contains("blank job-group") }, problems.toString())
    }

    @Test
    fun `큐를 끄면 설정 검증을 건너뛴다`() {
        // in-memory 모드에서는 큐 설정이 의미가 없다.
        val queue = QueueTtsGatewayProperties.Queue(
            enabled = false,
            models = mapOf("a" to QueueTtsGatewayProperties.ModelQueue("same", "same"),
                           "b" to QueueTtsGatewayProperties.ModelQueue("same", "same")),
        )

        clientFor(queue).validateModelQueues()
    }
}
