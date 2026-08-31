package com.aitts.queuetts.gateway.api.config.security

import com.fasterxml.jackson.databind.ObjectMapper
import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

private const val ADMIN_KEY = "test-admin-key-0123456789"
private const val CLIENT_KEY = "test-client-key-0123456789"

/**
 * API Key 인증 경계 검증.
 *
 * health 는 무인증, 나머지는 키 필요, `/api/admin` 하위는 admin 키만 —
 * 이 세 규칙이 실제 필터 체인에서 지켜지는지 확인한다.
 */
@SpringBootTest(
    properties = [
        "queuetts.database.enabled=false",
        "queuetts.queue.enabled=false",
        "queuetts.security.enabled=true",
        "queuetts.security.keys[0].id=test-admin",
        "queuetts.security.keys[0].key=$ADMIN_KEY",
        "queuetts.security.keys[0].role=admin",
        "queuetts.security.keys[1].id=test-client",
        "queuetts.security.keys[1].key=$CLIENT_KEY",
        "queuetts.security.keys[1].role=client",
    ],
)
@AutoConfigureMockMvc
class SecurityFilterChainTests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Test
    fun `health stays public so probes keep working`() {
        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.ok").value(true))
    }

    @Test
    fun `api without a key is rejected with the shared error envelope`() {
        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isUnauthorized)
            .andExpect(jsonPath("$.code").value("MISSING_API_KEY"))
            .andExpect(header().string("WWW-Authenticate", containsString("ApiKey")))
    }

    @Test
    fun `unknown key is reported separately from a missing one`() {
        mockMvc.perform(get("/api/jobs").header("X-API-Key", "not-a-registered-key-0000"))
            .andExpect(status().isUnauthorized)
            .andExpect(jsonPath("$.code").value("INVALID_API_KEY"))
    }

    @Test
    fun `client key reaches the job api`() {
        mockMvc.perform(get("/api/jobs").header("X-API-Key", CLIENT_KEY))
            .andExpect(status().isOk)
    }

    @Test
    fun `bearer header carries the key too`() {
        mockMvc.perform(get("/api/jobs").header("Authorization", "Bearer $CLIENT_KEY"))
            .andExpect(status().isOk)
    }

    @Test
    fun `client key cannot reach admin api`() {
        mockMvc.perform(get("/api/admin/job-gateway/overview").header("X-API-Key", CLIENT_KEY))
            .andExpect(status().isForbidden)
            .andExpect(jsonPath("$.code").value("FORBIDDEN"))
    }

    @Test
    fun `admin key reaches admin api`() {
        mockMvc.perform(get("/api/admin/job-gateway/overview").header("X-API-Key", ADMIN_KEY))
            .andExpect(status().isOk)
    }

    /** job 이력에 "누가/어떤 권한으로 넣었는지" 남기기 위해 접수 시점의 키를 job 에 붙인다. */
    @Test
    fun `created job records the calling key and its role`() {
        val created = mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-API-Key", CLIENT_KEY)
                .content("""{"source":"caller-test","payload":{"text":"hello","source":"caller-test"}}"""),
        )
            .andExpect(status().isAccepted)
            .andReturn()
            .response
            .contentAsString
        val jobId = ObjectMapper().readTree(created).get("jobId").asText()

        mockMvc.perform(get("/api/jobs/$jobId").header("X-API-Key", ADMIN_KEY))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.caller.id").value("test-client"))
            .andExpect(jsonPath("$.caller.role").value("CLIENT"))
    }

    @Test
    fun `write endpoints are protected as well`() {
        mockMvc.perform(
            post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"payload":{"text":"hello"}}"""),
        )
            .andExpect(status().isUnauthorized)
            .andExpect(jsonPath("$.code").value("MISSING_API_KEY"))
    }
}
