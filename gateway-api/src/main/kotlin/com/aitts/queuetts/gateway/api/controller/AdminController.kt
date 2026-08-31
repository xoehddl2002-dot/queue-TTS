package com.aitts.queuetts.gateway.api.controller

import com.aitts.queuetts.gateway.api.dto.JobGatewayOverviewResponse
import com.aitts.queuetts.gateway.api.dto.SampleComparisonListResponse
import com.aitts.queuetts.gateway.api.service.AdminService
import com.aitts.queuetts.gateway.api.service.FileDownload
import com.aitts.queuetts.gateway.api.service.SampleAudioKind
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.core.io.FileSystemResource
import org.springframework.http.ContentDisposition
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@Tag(name = "Admin", description = "Admin and convenience API for inspecting the gateway.")
@RestController
class AdminController(
    private val adminService: AdminService,
) {
    @Operation(summary = "Gateway overview", description = "Return queue and Redis worker state for admin inspection.")
    @GetMapping("/api/admin/job-gateway/overview")
    fun overview(): JobGatewayOverviewResponse = adminService.overview()

    @Operation(summary = "샘플 비교 목록", description = "저장된 TTS 샘플 비교 항목을 조회한다. limit 는 1~500 으로 제한된다.")
    @GetMapping("/api/admin/samples")
    fun listSamples(@RequestParam(defaultValue = "500") limit: Int): SampleComparisonListResponse =
        adminService.listSamples(limit)

    @Operation(summary = "샘플 비교 상세", description = "sampleKey 로 단일 샘플 비교 항목을 조회한다.")
    @GetMapping("/api/admin/samples/{sampleKey}")
    fun getSample(@PathVariable sampleKey: String): ResponseEntity<*> {
        val result = adminService.getSample(sampleKey)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "샘플 레거시 오디오", description = "샘플의 레거시(기존) 합성 오디오 파일을 반환한다.")
    @GetMapping("/api/admin/samples/{sampleKey}/legacy-audio")
    fun getSampleLegacyAudio(@PathVariable sampleKey: String): ResponseEntity<*> {
        val result = adminService.audio(sampleKey, SampleAudioKind.LEGACY)
        val error = result.leftOrNull()
        return if (error == null) audioResponse(result.getOrNull()!!)
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "샘플 현재 오디오", description = "샘플의 현재(신규) 합성 오디오 파일을 반환한다.")
    @GetMapping("/api/admin/samples/{sampleKey}/current-audio")
    fun getSampleCurrentAudio(@PathVariable sampleKey: String): ResponseEntity<*> {
        val result = adminService.audio(sampleKey, SampleAudioKind.CURRENT)
        val error = result.leftOrNull()
        return if (error == null) audioResponse(result.getOrNull()!!)
        else ResponseEntity.status(error.status).body(error)
    }

    private fun audioResponse(download: FileDownload): ResponseEntity<FileSystemResource> {
        val headers = HttpHeaders()
        headers.contentType = runCatching { MediaType.parseMediaType(download.mediaType) }
            .getOrDefault(MediaType.APPLICATION_OCTET_STREAM)
        headers.contentDisposition = ContentDisposition.inline().filename(download.fileName).build()
        return ResponseEntity.ok().headers(headers).body(FileSystemResource(download.path))
    }
}
