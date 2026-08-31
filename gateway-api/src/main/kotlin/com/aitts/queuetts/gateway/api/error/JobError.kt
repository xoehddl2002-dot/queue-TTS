package com.aitts.queuetts.gateway.api.error

import org.springframework.http.HttpStatus

/**
 * JobController(`/api/jobs`) 경로의 실패.
 *
 * worker 결과에 실려 오는 [com.aitts.queuetts.gateway.api.dto.JobError] 와는 다른 개념이다.
 * 둘 다 필요한 곳에서는 dto 쪽을 alias 로 import 한다.
 */
sealed interface JobError : DomainError {
    val status: HttpStatus

    data class NotFound(val jobId: String) : JobError {
        override val code: String = "JOB_NOT_FOUND"
        override val message: String = "job not found: $jobId"
        override val status: HttpStatus = HttpStatus.NOT_FOUND
    }

    data class InvalidRequest(val reason: String = "invalid job request") : JobError {
        override val code: String = "INVALID_JOB_REQUEST"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.BAD_REQUEST
    }

    data class InvalidState(val reason: String = "job is not in a valid state") : JobError {
        override val code: String = "INVALID_JOB_STATE"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.CONFLICT
    }

    data class ArtifactError(val reason: String = "job artifact error") : JobError {
        override val code: String = "JOB_ARTIFACT_ERROR"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.BAD_REQUEST
    }

    data class QueueError(val reason: String = "job queue error") : JobError {
        override val code: String = "JOB_QUEUE_ERROR"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.SERVICE_UNAVAILABLE
    }
}
