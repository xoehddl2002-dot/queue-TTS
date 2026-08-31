package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class GatewayDtoTests {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `style catalog exposes only the style list`() {
        val styles = listOf(StyleInfo(name = "Na-in-ae", kind = "custom"))

        val json = objectMapper.writeValueAsString(StyleCatalogResponse(styles = styles))

        val data = objectMapper.readTree(json)

        assertEquals(1, data.path("styles").size())
        assertEquals("Na-in-ae", data.path("styles").path(0).path("name").asText())
        // 워커 동기화 표시용 필드는 제거되었다.
        assertFalse(data.has("workers"))
        assertFalse(data.has("gateway_worker"))
        assertFalse(data.has("in_sync"))
    }
}
