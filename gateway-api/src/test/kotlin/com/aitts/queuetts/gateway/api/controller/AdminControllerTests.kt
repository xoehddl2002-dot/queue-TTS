package com.aitts.queuetts.gateway.api.controller

import arrow.core.left
import com.aitts.queuetts.gateway.api.error.AdminError
import com.aitts.queuetts.gateway.api.service.AdminService
import com.aitts.queuetts.gateway.api.service.SampleAudioKind
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

/** admin 엔드포인트도 job 쪽과 동일한 에러 봉투로 응답하는지 확인한다. */
// 컨트롤러 응답 계약만 보는 슬라이스라 보안 필터는 끈다. API Key 인증은 SecurityFilterChainTests 가 검증한다.
@WebMvcTest(AdminController::class)
@AutoConfigureMockMvc(addFilters = false)
class AdminControllerTests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var adminService: AdminService

    @Test
    fun `unknown sample returns the error body`() {
        `when`(adminService.getSample("missing")).thenReturn(AdminError.SampleNotFound("missing").left())

        mockMvc.perform(get("/api/admin/samples/missing"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.code").value("SAMPLE_NOT_FOUND"))
            .andExpect(jsonPath("$.message").value("sample not found: missing"))
    }

    @Test
    fun `audio failures return the error body instead of a file`() {
        `when`(adminService.audio("sample-1", SampleAudioKind.LEGACY))
            .thenReturn(AdminError.SampleAudioNotFound("legacy audio file not found for sample sample-1").left())

        mockMvc.perform(get("/api/admin/samples/sample-1/legacy-audio"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.code").value("SAMPLE_AUDIO_NOT_FOUND"))
            .andExpect(jsonPath("$.message").value("legacy audio file not found for sample sample-1"))
    }
}
