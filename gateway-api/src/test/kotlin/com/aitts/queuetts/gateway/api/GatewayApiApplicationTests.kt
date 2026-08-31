package com.aitts.queuetts.gateway.api

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(properties = ["queuetts.database.enabled=false", "queuetts.queue.enabled=false"])
class GatewayApiApplicationTests {
    @Test
    fun contextLoads() {
    }
}
