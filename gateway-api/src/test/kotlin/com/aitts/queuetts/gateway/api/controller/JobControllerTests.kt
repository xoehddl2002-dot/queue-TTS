package com.aitts.queuetts.gateway.api.controller

import com.aitts.queuetts.gateway.api.dto.SupertonicJobPayload
import com.aitts.queuetts.gateway.api.error.*
import arrow.core.left
import arrow.core.right
import com.aitts.queuetts.gateway.api.dto.AcceptedJobResponse
import com.aitts.queuetts.gateway.api.dto.CreateJobRequest
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.aitts.queuetts.gateway.api.dto.JobResponse
import com.aitts.queuetts.gateway.api.service.JobService
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import java.time.OffsetDateTime

/**
 * 컨트롤러가 Either 를 분기해 성공/실패가 각각 올바른 상태 코드와 본문으로
 * 나가는지 확인한다.
 */
// 컨트롤러 응답 계약만 보는 슬라이스라 보안 필터는 끈다. API Key 인증은 SecurityFilterChainTests 가 검증한다.
@WebMvcTest(JobController::class)
@AutoConfigureMockMvc(addFilters = false)
class JobControllerTests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var jobService: JobService

    @Test
    fun `right is written as the success body with the declared status`() {
        `when`(jobService.createJob(CreateJobRequest(payload = SupertonicJobPayload(text = "hello")))).thenReturn(
            AcceptedJobResponse(
                status = "accepted",
                jobId = "job_abc",
                state = "wait",
                priority = "normal",
                source = null,
                statusUrl = "/api/jobs/job_abc",
            ).right(),
        )

        mockMvc.perform(post("/api/jobs").contentType("application/json").content("{\"text\":\"hello\"}"))
            .andExpect(status().isAccepted)
            .andExpect(jsonPath("$.jobId").value("job_abc"))
            .andExpect(jsonPath("$.state").value("wait"))
    }

    @Test
    fun `not found errors return 404`() {
        `when`(jobService.getJob("job_missing")).thenReturn(JobError.NotFound("job_missing").left())

        mockMvc.perform(get("/api/jobs/job_missing"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.code").value("JOB_NOT_FOUND"))
            .andExpect(jsonPath("$.message").value("job not found: job_missing"))
    }

    @Test
    fun `invalid state errors return 409`() {
        `when`(jobService.requeueJob("job_running"))
            .thenReturn(JobError.InvalidState("cannot requeue job job_running in state running").left())

        mockMvc.perform(post("/api/jobs/job_running/requeue"))
            .andExpect(status().isConflict)
            .andExpect(jsonPath("$.code").value("INVALID_JOB_STATE"))
            .andExpect(jsonPath("$.message").value("cannot requeue job job_running in state running"))
    }

    @Test
    fun `download failures return the error body with its own status`() {
        `when`(jobService.jobDownload("job_missing")).thenReturn(JobError.NotFound("job_missing").left())

        mockMvc.perform(get("/api/jobs/job_missing/download"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.code").value("JOB_NOT_FOUND"))
    }

    @Test
    fun `success body is not wrapped in an envelope`() {
        `when`(jobService.getJob("job_abc")).thenReturn(
            JobResponse(
                jobId = "job_abc",
                state = "succeeded",
                priority = "normal",
                source = null,
                caller = null,
                createdAt = OffsetDateTime.parse("2026-07-20T00:00:00Z"),
                updatedAt = OffsetDateTime.parse("2026-07-20T00:00:01Z"),
                startedAt = null,
                finishedAt = null,
                workerId = null,
                batchId = null,
                result = null,
                error = null,
                artifact = null,
                downloadUrl = null,
                statusUrl = "/api/jobs/job_abc",
                historyError = null,
                version = 1,
            ).right(),
        )

        mockMvc.perform(get("/api/jobs/job_abc"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.jobId").value("job_abc"))
            .andExpect(jsonPath("$.data").doesNotExist())
            .andExpect(jsonPath("$.error").doesNotExist())
    }
}
