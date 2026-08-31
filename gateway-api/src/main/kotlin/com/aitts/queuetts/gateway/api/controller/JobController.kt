package com.aitts.queuetts.gateway.api.controller

import com.aitts.queuetts.gateway.api.dto.CreateJobRequest
import com.aitts.queuetts.gateway.api.dto.JobResponse
import com.aitts.queuetts.gateway.api.error.JobError
import com.aitts.queuetts.gateway.api.service.FileDownload
import com.aitts.queuetts.gateway.api.service.JobService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.core.io.FileSystemResource
import org.springframework.http.ContentDisposition
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@Tag(name = "Gateway Job", description = "job API: jobs.")
@RestController
class JobController(
    private val jobService: JobService,
) {

    @Operation(summary = "Create job", description = "Create a gateway job from an arbitrary payload and return 202 with jobId.")
    @PostMapping("/api/jobs")
    fun createJob(@RequestBody(required = false) request: CreateJobRequest?): ResponseEntity<*> {
        val result = jobService.createJob(request ?: CreateJobRequest())
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.status(HttpStatus.ACCEPTED).body(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "List jobs", description = "List runtime and persisted jobs, optionally filtered by state, priority, and source.")
    @GetMapping("/api/jobs")
    fun listJobs(
        @RequestParam(required = false) state: String?,
        @RequestParam(required = false) priority: String?,
        @RequestParam(required = false) source: String?,
        @RequestParam(defaultValue = "100") limit: Int,
        @RequestParam(defaultValue = "0") offset: Int,
    ): ResponseEntity<*> {
        val result = jobService.listJobs(state, priority, limit, offset, source)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "Get job", description = "Return job state, result, artifact info, and payload for a jobId.")
    @GetMapping("/api/jobs/{jobId}")
    fun getJob(@PathVariable jobId: String): ResponseEntity<*> {
        val result = jobService.getJob(jobId)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(
        summary = "Await job",
        description = "Block until the job reaches a terminal state (event-driven, no polling) and return the final job.",
    )
    @GetMapping("/api/jobs/{jobId}/await")
    fun awaitJob(
        @PathVariable jobId: String,
        @RequestParam(required = false) timeoutSeconds: Long?,
    ): ResponseEntity<*> {
        val result = jobService.awaitJob(jobId, timeoutSeconds)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(
        summary = "Await job and download artifact",
        description = "Block until the job reaches a terminal state, then stream the artifact bytes in the same response " +
            "(merges await + download into one round-trip). Returns 409 if still processing after the wait, or the job error otherwise.",
    )
    @GetMapping("/api/jobs/{jobId}/await/download")
    fun awaitDownloadJob(
        @PathVariable jobId: String,
        @RequestParam(required = false) timeoutSeconds: Long?,
    ): ResponseEntity<*> {
        val awaited = jobService.awaitJob(jobId, timeoutSeconds)
        val job = awaited.getOrNull()
            ?: return awaited.leftOrNull()!!.let { ResponseEntity.status(it.status).body(it) }

        return when (job.state) {
            "succeeded" -> {
                val download = jobService.jobDownload(jobId)
                val downloadError = download.leftOrNull()
                if (downloadError != null) ResponseEntity.status(downloadError.status).body(downloadError)
                else audioFileResponse(download.getOrNull()!!, job)
            }
            "failed", "cancelled" -> {
                val reason = job.error?.message ?: "job ${job.state}"
                val error = JobError.InvalidState(reason)
                ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(error)
            }
            // 서버측 대기 상한 안에 안 끝남 → 호출자가 재대기하도록 409 를 준다.
            else -> {
                val error = JobError.InvalidState("job still processing (state=${job.state})")
                ResponseEntity.status(HttpStatus.CONFLICT).body(error)
            }
        }
    }

    @Operation(summary = "Cancel job", description = "Cancel a waiting or running job.")
    @DeleteMapping("/api/jobs/{jobId}")
    fun cancelJob(@PathVariable jobId: String): ResponseEntity<*> {
        val result = jobService.cancelJob(jobId)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "Requeue job", description = "Republish a waiting job.")
    @PostMapping("/api/jobs/{jobId}/requeue")
    fun requeueJob(@PathVariable jobId: String): ResponseEntity<*> {
        val result = jobService.requeueJob(jobId)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "Download job artifact", description = "Download the artifact produced by a completed job.")
    @GetMapping("/api/jobs/{jobId}/download")
    fun downloadJob(@PathVariable jobId: String): ResponseEntity<*> {
        val result = jobService.jobDownload(jobId)
        val error = result.leftOrNull()
        return if (error == null) fileResponse(result.getOrNull()!!)
        else ResponseEntity.status(error.status).body(error)
    }

    // SSE 는 본문이 열리면 상태가 확정되므로 실패는 상태 코드로만 알린다.
    @Operation(summary = "Job events", description = "Subscribe to job state changes using Server-Sent Events.")
    @GetMapping(value = ["/api/jobs/{jobId}/events"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun jobEvents(
        @PathVariable jobId: String,
        @RequestParam(defaultValue = "0") lastEventId: Long,
    ): ResponseEntity<SseEmitter> {
        val result = jobService.events(jobId, lastEventId)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).build()
    }

    private fun fileResponse(download: FileDownload): ResponseEntity<FileSystemResource> {
        val headers = HttpHeaders()
        headers.contentType = runCatching { MediaType.parseMediaType(download.mediaType) }
            .getOrDefault(MediaType.APPLICATION_OCTET_STREAM)
        headers.contentDisposition = ContentDisposition.attachment().filename(download.fileName).build()
        return ResponseEntity.ok()
            .headers(headers)
            .body(FileSystemResource(download.path))
    }

    /**
     * await/download 병합 응답: 오디오 바이트에 더해, 별도 status 왕복 없이도 소비자가 쓰도록
     * sampleRate/duration 을 헤더로 실어 보낸다. (기존 /download 는 바이트만 준다)
     */
    private fun audioFileResponse(download: FileDownload, job: JobResponse): ResponseEntity<FileSystemResource> {
        val headers = HttpHeaders()
        headers.contentType = runCatching { MediaType.parseMediaType(download.mediaType) }
            .getOrDefault(MediaType.APPLICATION_OCTET_STREAM)
        headers.contentDisposition = ContentDisposition.inline().filename(download.fileName).build()
        job.result?.sampleRate?.let { headers.set("X-Sample-Rate", it) }
        job.result?.durationS?.let { headers.set("X-Audio-Duration", it.toString()) }
        return ResponseEntity.ok()
            .headers(headers)
            .body(FileSystemResource(download.path))
    }
}
