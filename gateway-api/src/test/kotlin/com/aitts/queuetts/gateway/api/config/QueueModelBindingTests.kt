package com.aitts.queuetts.gateway.api.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 실행 환경의 `application.yml` 이 모델별 큐로 실제 바인딩되는지 확인한다.
 *
 * [QueueModelPropertiesTests] 는 해석 규칙만 보므로 yml 오타(예: `models` 들여쓰기)를 잡지 못한다.
 * 여기서는 스프링이 바인딩한 실물을 본다.
 */
@SpringBootTest(
    properties = [
        "queuetts.database.enabled=false",
        "queuetts.queue.enabled=false",
        "queuetts.security.enabled=false",
    ],
)
class QueueModelBindingTests {

    @Autowired
    private lateinit var properties: QueueTtsGatewayProperties

    @Test
    fun `application yml 이 모델별 큐로 바인딩된다`() {
        val queues = properties.queue.modelQueues

        assertTrue(queues.containsKey("supertonic"), "supertonic 풀이 설정에 있어야 한다: ${queues.keys}")
        assertEquals("tts:jobs", queues.getValue("supertonic").jobStream)
        assertEquals("tts-workers", queues.getValue("supertonic").jobGroup)

        assertTrue(queues.containsKey("qwen"), "qwen 풀이 설정에 있어야 한다: ${queues.keys}")
        assertEquals("qwen:jobs", queues.getValue("qwen").jobStream)
        assertEquals("qwen-workers", queues.getValue("qwen").jobGroup)
    }

    @Test
    fun `model 없는 요청은 supertonic 으로 간다`() {
        assertEquals("supertonic", properties.queue.canonicalModel(null))
        assertEquals("supertonic", properties.queue.defaultModel)
    }

    @Test
    fun `두 풀이 서로 다른 stream 과 group 을 쓴다`() {
        // 겹치면 서로의 잡을 가져가 Unknown voice 로 실패하고, voice catalog 불일치로 자가 종료한다.
        val queues = properties.queue.modelQueues
        assertEquals(
            queues.size,
            queues.values.map { it.jobStream }.toSet().size,
            "모델마다 job stream 이 달라야 한다: $queues",
        )
        assertEquals(
            queues.size,
            queues.values.map { it.jobGroup }.toSet().size,
            "모델마다 consumer group 이 달라야 한다: $queues",
        )
    }
}
