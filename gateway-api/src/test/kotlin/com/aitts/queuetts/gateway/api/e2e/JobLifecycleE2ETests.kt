package com.aitts.queuetts.gateway.api.e2e

import com.aitts.queuetts.gateway.api.dto.SupertonicJobPayload
import com.aitts.queuetts.gateway.api.dto.AudioJobResult
import com.aitts.queuetts.gateway.api.dto.JobError as JobErrorPayload
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.aitts.queuetts.gateway.api.infra.redis.messaging.ArtifactContent
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.service.JobService
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.ArgumentMatchers.nullable
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.request
import java.util.Base64
import java.util.UUID
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 잡 한 건이 접수부터 다운로드까지 흐르는 동안 컨트롤러 → 서비스 → 큐 발행이 실제로
 * 이어붙어 동작하는지 검증하는 end-to-end 테스트.
 *
 * 단위 테스트가 각 계층을 따로 검증하는 것과 달리, 여기서는 HTTP 요청만으로 잡을 다루고
 * worker 응답만 [JobService.applyQueueEvent] 로 주입한다 (운영에서 `JobResultListener` 가
 * Redis result 스트림을 읽어 호출하는 지점과 동일하다). Redis 자체는 [RedisStreamQueueClient]
 * mock 으로 대체해 발행 경계만 확인한다.
 *
 * 컨텍스트를 공유하므로 각 테스트는 자기 잡만 보도록 고유한 `source` 로 목록을 필터링한다.
 */
@SpringBootTest(
    properties = [
        "queuetts.database.enabled=false",
        "queuetts.queue.enabled=false",
        "queuetts.job.artifact-dir=build/test-artifacts/e2e",
        // job 수명주기만 보는 시나리오라 인증은 끈다 (API Key 검증은 SecurityFilterChainTests).
        "queuetts.security.enabled=false",
    ],
)
@AutoConfigureMockMvc
class JobLifecycleE2ETests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var jobService: JobService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    // queuetts.queue.enabled=false 로 result 리스너/timeout sweeper 는 꺼두고,
    // 잡 발행 경계만 mock 으로 열어 실제 Redis 없이 발행 여부를 확인한다.
    @MockitoBean
    private lateinit var queueClient: RedisStreamQueueClient

    @BeforeEach
    fun enableQueuePublishing() {
        `when`(queueClient.enabled).thenReturn(true)
    }

    @Test
    fun `accepted job runs and succeeds through the queue and serves its artifact`() {
        val source = uniqueSource()
        val jobId = createJob(text = "hello e2e", source = source, priority = "high")

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("wait"))
            .andExpect(jsonPath("$.priority").value("high"))
            .andExpect(jsonPath("$.payload.text").value("hello e2e"))

        // gateway 는 worker 를 모른 채 우선순위 스트림에 발행만 한다.
        verify(queueClient).publishJob(
            jobId,
            "tts",
            SupertonicJobPayload(text = "hello e2e", source = source),
            "high",
            source,
        )

        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = "batch-e2e",
                state = "running",
            ),
        )

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("running"))
            .andExpect(jsonPath("$.workerId").value("worker-e2e"))
            .andExpect(jsonPath("$.batchId").value("batch-e2e"))

        val audio = "e2e-audio-bytes".toByteArray()
        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = "batch-e2e",
                state = "done",
                result = AudioJobResult(durationS = 2.5),
                artifact = ArtifactContent(
                    // worker 가 준 파일 이름은 저장 전에 안전한 이름으로 정규화된다.
                    fileName = "e2e:result.wav",
                    mediaType = "audio/wav",
                    contentBase64 = Base64.getEncoder().encodeToString(audio),
                ),
            ),
        )

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("succeeded"))
            .andExpect(jsonPath("$.result.durationS").value(2.5))
            .andExpect(jsonPath("$.artifact.fileName").value("e2e_result.wav"))
            .andExpect(jsonPath("$.artifact.size").value(audio.size))

        val downloaded = mockMvc.perform(get("/api/jobs/$jobId/download"))
            .andExpect(status().isOk)
            .andExpect(content().contentType(MediaType.parseMediaType("audio/wav")))
            .andExpect(header().string("Content-Disposition", org.hamcrest.Matchers.containsString("e2e_result.wav")))
            .andReturn()
            .response
            .contentAsByteArray

        assertContentEquals(audio, downloaded)
    }

    @Test
    fun `failed worker result surfaces the error and leaves nothing to download`() {
        val source = uniqueSource()
        val jobId = createJob(text = "boom", source = source)

        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = "batch-e2e",
                state = "failed",
                error = JobErrorPayload(code = "synthesis_failed", message = "model crashed"),
            ),
        )

        mockMvc.perform(get("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("failed"))
            .andExpect(jsonPath("$.error.code").value("synthesis_failed"))
            .andExpect(jsonPath("$.error.message").value("model crashed"))

        mockMvc.perform(get("/api/jobs/$jobId/download"))
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.code").value("JOB_ARTIFACT_ERROR"))
    }

    @Test
    fun `cancelled job stays cancelled even if a late worker result arrives`() {
        val source = uniqueSource()
        val jobId = createJob(text = "cancel me", source = source)

        mockMvc.perform(delete("/api/jobs/$jobId"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("cancelled"))

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
            .andExpect(jsonPath("$.state").value("cancelled"))
            .andExpect(jsonPath("$.artifact").doesNotExist())
    }

    @Test
    fun `waiting job can be requeued but a running one cannot`() {
        val source = uniqueSource()
        val jobId = createJob(text = "requeue me", source = source)

        mockMvc.perform(post("/api/jobs/$jobId/requeue"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("wait"))

        // 최초 발행 + 재발행으로 두 번 스트림에 올라가야 한다.
        // (matcher 는 null 을 돌려주므로 Kotlin non-null 파라미터용 더미 값으로 받쳐준다.)
        verify(queueClient, times(2)).publishJob(
            eq(jobId) ?: jobId,
            anyString() ?: "tts",
            nullable(JobPayload::class.java),
            anyString() ?: "normal",
            nullable(String::class.java),
            nullable(String::class.java),
        )

        jobService.applyQueueEvent(
            RedisResultMessage(jobId = jobId, workerId = "worker-e2e", batchId = null, state = "running"),
        )

        mockMvc.perform(post("/api/jobs/$jobId/requeue"))
            .andExpect(status().isConflict)
            .andExpect(jsonPath("$.code").value("INVALID_JOB_STATE"))
    }

    @Test
    fun `job list reflects each state transition for the same source`() {
        val source = uniqueSource()
        val waiting = createJob(text = "a", source = source)
        val running = createJob(text = "b", source = source)

        jobService.applyQueueEvent(
            RedisResultMessage(jobId = running, workerId = "worker-e2e", batchId = null, state = "running"),
        )

        mockMvc.perform(get("/api/jobs").param("source", source))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.total").value(2))

        mockMvc.perform(get("/api/jobs").param("source", source).param("state", "wait"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.total").value(1))
            .andExpect(jsonPath("$.items[0].jobId").value(waiting))

        mockMvc.perform(get("/api/jobs").param("source", source).param("state", "running"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.total").value(1))
            .andExpect(jsonPath("$.items[0].jobId").value(running))
    }

    @Test
    fun `health counts move with the jobs the api accepted`() {
        val before = healthActiveJobs()
        val jobId = createJob(text = "counted", source = uniqueSource())

        assertEquals(before + 1, healthActiveJobs())

        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = null,
                state = "failed",
                error = JobErrorPayload(message = "done with it"),
            ),
        )

        assertEquals(before, healthActiveJobs())
    }

    @Test
    fun `event stream replays the job history and closes once the job is done`() {
        val jobId = createJob(text = "watch me", source = uniqueSource())
        jobService.applyQueueEvent(
            RedisResultMessage(jobId = jobId, workerId = "worker-e2e", batchId = null, state = "running"),
        )
        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = null,
                state = "failed",
                error = JobErrorPayload(message = "done watching"),
            ),
        )

        // 이미 종료된 잡이라 밀린 이벤트를 모두 흘려보낸 뒤 스트림이 닫힌다.
        val body = subscribeToEvents(jobId, lastEventId = 0)

        assertTrue(body.contains("event:state"), "SSE 이벤트 이름이 실려야 한다: $body")
        assertTrue(body.contains("job accepted"), "접수 이벤트부터 재생되어야 한다: $body")
        assertTrue(body.contains("running"), "상태 전이가 실려야 한다: $body")
        assertTrue(body.contains(jobId), "이벤트 본문에 jobId 가 있어야 한다: $body")
    }

    @Test
    fun `event stream resumes after the last id the client already saw`() {
        val jobId = createJob(text = "resume me", source = uniqueSource())
        jobService.applyQueueEvent(
            RedisResultMessage(
                jobId = jobId,
                workerId = "worker-e2e",
                batchId = null,
                state = "failed",
                error = JobErrorPayload(message = "already reported"),
            ),
        )

        val fromStart = subscribeToEvents(jobId, lastEventId = 0)
        val resumed = subscribeToEvents(jobId, lastEventId = Long.MAX_VALUE)

        assertTrue(fromStart.contains("job accepted"))
        // 클라이언트가 이미 본 지점 이후로만 보내므로 재구독 시 재전송이 없다.
        assertFalse(resumed.contains("job accepted"), "이미 본 이벤트를 다시 보내면 안 된다: $resumed")
    }

    @Test
    fun `unknown job is a 404 on every job route`() {
        listOf(
            get("/api/jobs/job_missing"),
            get("/api/jobs/job_missing/download"),
            post("/api/jobs/job_missing/requeue"),
            delete("/api/jobs/job_missing"),
            get("/api/jobs/job_missing/events"),
        ).forEach { request ->
            mockMvc.perform(request).andExpect(status().isNotFound)
        }
    }

    /** POST /api/jobs 로 잡을 만들고 202 를 확인한 뒤 jobId 를 돌려준다. */
    private fun createJob(text: String, source: String, priority: String? = null): String {
        val body = buildMap {
            put("payload", mapOf("text" to text, "source" to source))
            priority?.let { put("priority", it) }
        }
        val response = mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body)),
        )
            .andExpect(status().isAccepted)
            .andExpect(jsonPath("$.status").value("accepted"))
            .andExpect(jsonPath("$.state").value("wait"))
            .andExpect(jsonPath("$.source").value(source))
            .andReturn()
            .response
            .contentAsString

        return objectMapper.readTree(response).get("jobId").asText()
    }

    /** SSE 구독을 열고 스트림이 닫힐 때까지 받은 본문을 돌려준다. */
    private fun subscribeToEvents(jobId: String, lastEventId: Long): String {
        val subscription = mockMvc.perform(
            get("/api/jobs/$jobId/events").param("lastEventId", lastEventId.toString()),
        )
            .andExpect(status().isOk)
            .andExpect(request().asyncStarted())
            .andReturn()

        return mockMvc.perform(asyncDispatch(subscription))
            .andReturn()
            .response
            .contentAsString
    }

    private fun healthActiveJobs(): Int {
        val response = mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andReturn()
            .response
            .contentAsString
        return objectMapper.readTree(response).get("active_jobs").asInt()
    }

    /** 컨텍스트를 공유하는 테스트끼리 목록이 섞이지 않도록 테스트마다 고유한 source 를 쓴다. */
    private fun uniqueSource(): String = "e2e_${UUID.randomUUID().toString().take(8)}"
}
