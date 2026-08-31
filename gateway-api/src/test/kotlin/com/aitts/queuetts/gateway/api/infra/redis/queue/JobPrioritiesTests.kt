package com.aitts.queuetts.gateway.api.infra.redis.queue

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class JobPrioritiesTests {
    @Test
    fun `priority streams are ordered from urgent to low`() {
        assertEquals(
            linkedMapOf(
                "urgent" to "tts:jobs:urgent",
                "high" to "tts:jobs:high",
                "normal" to "tts:jobs:normal",
                "low" to "tts:jobs:low",
            ),
            JobPriorities.streams("tts:jobs"),
        )
    }

    @Test
    fun `unsupported priority cannot select a stream`() {
        assertFailsWith<IllegalArgumentException> {
            JobPriorities.streamName("tts:jobs", "unknown")
        }
    }
}
