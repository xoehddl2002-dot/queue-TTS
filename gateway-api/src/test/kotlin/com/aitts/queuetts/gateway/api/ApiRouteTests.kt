package com.aitts.queuetts.gateway.api

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.util.UUID

// 라우팅 노출 여부만 보는 테스트라 인증은 끈다 (켜두면 미매핑 경로도 401 로 덮여 404 를 확인할 수 없다).
@SpringBootTest(
    properties = [
        "queuetts.database.enabled=false",
        "queuetts.queue.enabled=false",
        "queuetts.security.enabled=false",
    ],
)
@AutoConfigureMockMvc
class ApiRouteTests {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @Test
    fun `health endpoint is available`() {
        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.ok").value(true))
    }

    @Test
    fun `legacy synchronous tts endpoint is not exposed`() {
        mockMvc.perform(
            post("/api/tts")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"text":"hello"}"""),
        )
            .andExpect(status().isNotFound)
    }

    @Test
    fun `voice registry endpoints are not exposed`() {
        mockMvc.perform(get("/api/voice-registry"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `quick tts job endpoints are not exposed`() {
        mockMvc.perform(
            post("/api/tts/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"text":"hello"}"""),
        )
            .andExpect(status().isNotFound)

        mockMvc.perform(get("/api/tts/jobs/anything"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `job gateway overview exposes queue based worker info`() {
        // queue 가 비활성화된 테스트 환경에서는 redis consumer 기반 worker 목록이 비어 있어야 한다.
        mockMvc.perform(get("/api/admin/job-gateway/overview"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.workers").isArray)
            .andExpect(jsonPath("$.workerCount").value(0))
            .andExpect(jsonPath("$.activeWorkerCount").value(0))
            .andExpect(jsonPath("$.jobWorkers").isArray)
    }

    @Test
    fun `generic job can be accepted and listed`() {
        // 컨텍스트(=JobService 메모리 상태)를 공유하므로 전체 total 이 아니라
        // 이 테스트가 만든 잡만 보이도록 고유한 source 로 필터링한다.
        val source = "route_${UUID.randomUUID().toString().take(8)}"

        mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"priority":"normal","payload":{"text":"hello","source":"$source"}}"""),
        )
            .andExpect(status().isAccepted)
            .andExpect(jsonPath("$.status").value("accepted"))

        mockMvc.perform(get("/api/jobs").param("source", source))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.total").value(1))
            // 목록이 payload 를 포함해야 소비자가 행마다 상세를 다시 조회하지 않는다.
            .andExpect(jsonPath("$.items[0].payload.text").value("hello"))
    }

    @Test
    fun `flat job payload is accepted and resolves source`() {
        // 워커/로드테스트가 쓰는 평면 형태(payload 를 최상위 필드로 전송)도 계속 수락되어야 한다.
        mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"text":"hello","priority":"normal","source":"user_web"}"""),
        )
            .andExpect(status().isAccepted)
            .andExpect(jsonPath("$.status").value("accepted"))
            .andExpect(jsonPath("$.priority").value("normal"))
            .andExpect(jsonPath("$.source").value("user_web"))
    }

    @Test
    fun `worker HTTP routes are not exposed`() {
        mockMvc.perform(post("/api/workers/worker_1/claim"))
            .andExpect(status().isNotFound)
        mockMvc.perform(post("/api/workers/worker_1/heartbeat"))
            .andExpect(status().isNotFound)
        mockMvc.perform(post("/api/workers/worker_1/batches/batch_1/start"))
            .andExpect(status().isNotFound)
        mockMvc.perform(post("/api/workers/worker_1/batches/batch_1/complete"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `job history routes are folded into jobs and not exposed`() {
        // job-history 계열은 /api/jobs 로 통합되어 더 이상 노출되지 않는다 (404 회귀 검증).
        mockMvc.perform(get("/api/job-history"))
            .andExpect(status().isNotFound)
        mockMvc.perform(get("/api/job-history/anything"))
            .andExpect(status().isNotFound)
        mockMvc.perform(get("/api/job-history/anything/download"))
            .andExpect(status().isNotFound)
        mockMvc.perform(get("/api/history"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `samples routes are served under admin and the legacy path is not exposed`() {
        mockMvc.perform(get("/api/admin/samples"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.items").isArray)

        // 레거시 /api/samples 경로는 /api/admin/samples 로 이동해 더 이상 노출되지 않는다 (404 회귀 검증).
        mockMvc.perform(get("/api/samples"))
            .andExpect(status().isNotFound)
    }

    @Test
    fun `per-source create routes are folded into jobs and not exposed`() {
        // source 는 /api/jobs 요청 본문으로 전달되며 전용 라우트는 제거되었다 (404 회귀 검증).
        mockMvc.perform(
            post("/api/user/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"payload":{"text":"hello"}}"""),
        )
            .andExpect(status().isNotFound)
        mockMvc.perform(
            post("/api/counselor/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"payload":{"text":"hello"}}"""),
        )
            .andExpect(status().isNotFound)
    }
}
