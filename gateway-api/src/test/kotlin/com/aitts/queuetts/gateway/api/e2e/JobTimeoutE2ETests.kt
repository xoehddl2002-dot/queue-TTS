package com.aitts.queuetts.gateway.api.e2e

import com.aitts.queuetts.gateway.api.infra.redis.messaging.ArtifactContent
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.service.JobService
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.util.Base64

/**
 * worker 결과가 끝내 오지 않는 경우의 end-to-end 흐름.
 *
 * timeout 판정은 발행 시각(createdAt) 기준이라 `worker-timeout-seconds=0` 이면 발행 직후부터
 * 만료 대상이 된다. 다른 시나리오와 timeout 설정이 다르므로 컨텍스트를 분리했다.
 */
@SpringBootTest(
    properties = [
        "queuetts.database.enabled=false",
        "queuetts.queue.enabled=false",
        "queuetts.worker-timeout-seconds=0",
        "queuetts.job.artifact-dir=build/test-artifacts/e2e-timeout",
        // job 수명주기만 보는 시나리오라 인증은 끈다 (API Key 검증은 SecurityFilterChainTests).
        "queuetts.security.enabled=false",
    ],
)
@AutoConfigureMockMvc
class JobTimeoutE2ETests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var jobService: JobService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @MockitoBean
    private lateinit var queueClient: RedisStreamQueueClient

    @Test
    fun `published job with no worker result times out and ignores the late success`() {
        `when`(queueClient.enabled).thenReturn(true)
        val jobId = createJob("no worker will answer")

        // 조회 시점에 만료 스캔이 돌면서 timeout 실패로 확정된다.
        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("failed"))
            .andExpect(jsonPath("$.error.code").value("worker_timeout"))

        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-late",
                batchId = "batch-late",
                state = "succeeded",
                artifact = ArtifactContent(
                    fileName = "late.wav",
                    mediaType = "audio/wav",
                    contentBase64 = Base64.getEncoder().encodeToString("late".toByteArray()),
                ),
            ),
        )

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("failed"))
            .andExpect(jsonPath("$.error.code").value("worker_timeout"))
            .andExpect(jsonPath("$.artifact").doesNotExist())
    }

    @Test
    fun `job that was never published to the queue is not timed out`() {
        // 큐가 꺼져 있으면 발행 자체가 없으므로 timeout 대상이 아니다 (in-memory 모드).
        `when`(queueClient.enabled).thenReturn(false)
        val jobId = createJob("in-memory only")

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("wait"))
    }

    private fun createJob(text: String): String {
        val response = mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"payload":{"text":"$text"}}"""),
        )
            .andExpect(status().isAccepted)
            .andReturn()
            .response
            .contentAsString
        return objectMapper.readTree(response).get("jobId").asText()
    }
}
