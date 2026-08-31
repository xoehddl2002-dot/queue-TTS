package com.aitts.queuetts.gateway.api.controller

import arrow.core.left
import arrow.core.right
import com.aitts.queuetts.gateway.api.dto.HealthResponse
import com.aitts.queuetts.gateway.api.dto.StyleCatalogResponse
import com.aitts.queuetts.gateway.api.dto.StyleInfo
import com.aitts.queuetts.gateway.api.error.GatewayError
import com.aitts.queuetts.gateway.api.service.GatewayService
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

/** gateway 엔드포인트도 job/admin 과 같은 에러 봉투와 상태 코드를 쓰는지 확인한다. */
// 컨트롤러 응답 계약만 보는 슬라이스라 보안 필터는 끈다. API Key 인증은 SecurityFilterChainTests 가 검증한다.
@WebMvcTest(GatewayController::class)
@AutoConfigureMockMvc(addFilters = false)
class GatewayControllerTests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var gatewayService: GatewayService

    @Test
    fun `health is served with the snake case counts clients read`() {
        `when`(gatewayService.health()).thenReturn(
            HealthResponse(
                ok = true,
                status = "ok",
                gateway = "QueueTts Gateway",
                activeJobs = 2,
                jobsTotal = 7,
                workers = emptyList(),
            ).right(),
        )

        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.ok").value(true))
            .andExpect(jsonPath("$.active_jobs").value(2))
            .andExpect(jsonPath("$.jobs_total").value(7))
    }

    @Test
    fun `a worker error on health is a 503 with the error body`() {
        `when`(gatewayService.health()).thenReturn(GatewayError.WorkerError("redis is down").left())

        mockMvc.perform(get("/api/health"))
            .andExpect(status().isServiceUnavailable)
            .andExpect(jsonPath("$.code").value("WORKER_CONNECTION_ERROR"))
            .andExpect(jsonPath("$.message").value("redis is down"))
    }

    @Test
    fun `styles are returned as a bare catalog`() {
        `when`(gatewayService.styles()).thenReturn(
            StyleCatalogResponse(styles = listOf(StyleInfo(name = "Na-in-ae", kind = "custom"))).right(),
        )

        mockMvc.perform(get("/api/styles"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.styles[0].name").value("Na-in-ae"))
            .andExpect(jsonPath("$.data").doesNotExist())
    }

    @Test
    fun `a worker error on styles is a 503 with the error body`() {
        `when`(gatewayService.styles())
            .thenReturn(GatewayError.WorkerError("no worker is consuming the job streams").left())

        mockMvc.perform(get("/api/styles"))
            .andExpect(status().isServiceUnavailable)
            .andExpect(jsonPath("$.code").value("WORKER_CONNECTION_ERROR"))
    }
}
