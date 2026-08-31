package com.aitts.queuetts.gateway.api.controller

import arrow.core.left
import arrow.core.right
import com.aitts.queuetts.gateway.api.dto.*
import com.aitts.queuetts.gateway.api.error.QwenError
import com.aitts.queuetts.gateway.api.service.QwenService
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.mock.web.MockMultipartFile
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@WebMvcTest(QwenController::class)
@AutoConfigureMockMvc(addFilters = false)
class QwenControllerTests {
    @Autowired
    private lateinit var mockMvc: MockMvc

    @MockitoBean
    private lateinit var service: QwenService

    private fun voice(xVectorOnly: Boolean = false) = SpeakerResponse(
        name = "나인애",
        model = "qwen",
        xVectorOnlyMode = xVectorOnly,
        refText = if (xVectorOnly) null else "참조 발화",
        referenceDigest = "digest",
    )

    @Test
    fun `POST registers Qwen clone parameters and returns 201`() {
        val request = CreateSpeakerRequest(
            name = "나인애",
            refText = "참조 발화",
            xVectorOnlyMode = false,
            defaultParams = QwenSpeakerParams(temperature = 0.85),
        )
        val audioBytes = byteArrayOf(1, 2, 3)
        `when`(service.createSpeaker(request, audioBytes)).thenReturn(voice().right())
        val audio = MockMultipartFile("ref_audio", "ref.wav", "audio/wav", audioBytes)

        mockMvc.perform(
            multipart("/api/qwen/speaker")
                .file(audio)
                .param("name", "나인애")
                .param("ref_text", "참조 발화")
                .param("x_vector_only_mode", "false")
                .param("default_params", """{"temperature":0.85}"""),
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.x_vector_only_mode").value(false))
            .andExpect(jsonPath("$.ref_text").value("참조 발화"))
            .andExpect(jsonPath("$.mode").doesNotExist())
    }

    @Test
    fun `POST requires the ref audio file part`() {
        mockMvc.perform(
            multipart("/api/qwen/speaker")
                .param("name", "나인애")
                .param("ref_text", "참조 발화"),
        ).andExpect(status().isBadRequest)
    }

    @Test
    fun `multipart default params must be a JSON object`() {
        val audio = MockMultipartFile("ref_audio", "ref.wav", "audio/wav", byteArrayOf(1))

        mockMvc.perform(
            multipart("/api/qwen/speaker")
                .file(audio)
                .param("name", "나인애")
                .param("ref_text", "참조 발화")
                .param("default_params", "[1,2,3]"),
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.code").value("INVALID_SPEAKER_REQUEST"))
    }

    @Test
    fun `GET list and item expose registered clone voices`() {
        val response = voice(xVectorOnly = true)
        `when`(service.listSpeaker()).thenReturn(SpeakerListResponse(listOf(response), "qwen").right())
        `when`(service.getSpeaker(response.name)).thenReturn(response.right())

        mockMvc.perform(get("/api/qwen/speaker"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.speakers[0].x_vector_only_mode").value(true))

        mockMvc.perform(get("/api/qwen/speaker/${response.name}"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.name").value(response.name))
            // 공개 식별자는 name 하나뿐이다. 내부 id 를 되살리면 여기서 걸린다.
            .andExpect(jsonPath("$.id").doesNotExist())
    }

    /**
     * `/{name}/audio` 는 `/{name}` 조회와 경로가 겹치지 않아야 한다 — 겹치면 이름이 "나인애/audio"
     * 인 speaker 를 찾다가 404 가 난다.
     */
    @Test
    fun `GET audio returns the stored reference bytes`() {
        val name = "나인애"
        val bytes = byteArrayOf(7, 8, 9)
        `when`(service.getSpeakerAudio(name))
            .thenReturn(SpeakerAudio(bytes, "$name.wav", "audio/wav").right())

        val response = mockMvc.perform(get("/api/qwen/speaker/$name/audio"))
            .andExpect(status().isOk)
            .andExpect(header().string("Content-Type", "audio/wav"))
            .andReturn().response

        assertArrayEquals(bytes, response.contentAsByteArray)
        // 한글 이름은 RFC 5987 의 filename* 로 나가야 브라우저가 읽는다.
        assertTrue(response.getHeader("Content-Disposition")!!.contains("filename*=UTF-8''"))
    }

    @Test
    fun `GET audio reports a missing speaker as 404`() {
        `when`(service.getSpeakerAudio("없음")).thenReturn(QwenError.NotFound("없음").left())

        mockMvc.perform(get("/api/qwen/speaker/없음/audio"))
            .andExpect(status().isNotFound)
            .andExpect(jsonPath("$.code").value("SPEAKER_NOT_FOUND"))
    }

    @Test
    fun `PATCH rebuild input uses official Qwen parameter names`() {
        val name = "나인애"
        val request = UpdateSpeakerRequest(xVectorOnlyMode = true)
        `when`(service.updateSpeaker(name, request)).thenReturn(voice(xVectorOnly = true).right())

        mockMvc.perform(
            patch("/api/qwen/speaker/$name")
                .contentType("application/json")
                .content("""{"x_vector_only_mode":true}"""),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.x_vector_only_mode").value(true))
    }

    @Test
    fun `multipart PATCH uploads a replacement ref audio file`() {
        val name = "나인애"
        val request = UpdateSpeakerRequest(refText = "새 참조 발화")
        val audioBytes = byteArrayOf(4, 5, 6)
        `when`(service.updateSpeaker(name, request, audioBytes)).thenReturn(voice().right())
        val audio = MockMultipartFile("ref_audio", "replacement.wav", "audio/wav", audioBytes)

        mockMvc.perform(
            multipart("/api/qwen/speaker/$name")
                .file(audio)
                .param("ref_text", "새 참조 발화")
                .with { requestBuilder -> requestBuilder.method = "PATCH"; requestBuilder },
        ).andExpect(status().isOk)
    }

    /**
     * 삭제는 한 가지뿐이다. `?purge=` 로 동작이 갈리던 시절의 호출이 남아 있어도 무시하고 지운다 —
     * 이 테스트가 그 파라미터가 되살아나지 않는 것을 고정한다.
     */
    @Test
    fun `DELETE always removes the speaker`() {
        val name = "나인애"
        `when`(service.deleteSpeaker(name)).thenReturn(DeleteSpeakerResponse(name, workerId = "qwen-1").right())

        mockMvc.perform(delete("/api/qwen/speaker/$name"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.name").value(name))
            .andExpect(jsonPath("$.workerId").value("qwen-1"))
            .andExpect(jsonPath("$.action").doesNotExist())

        mockMvc.perform(delete("/api/qwen/speaker/$name").param("purge", "true"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.name").value(name))
    }
}
