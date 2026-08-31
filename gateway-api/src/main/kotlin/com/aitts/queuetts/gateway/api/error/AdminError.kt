package com.aitts.queuetts.gateway.api.error

import org.springframework.http.HttpStatus

/** AdminController(`/api/admin`) 경로의 실패. */
sealed interface AdminError : DomainError {
    val status: HttpStatus

    data class SampleNotFound(val sampleKey: String) : AdminError {
        override val code: String = "SAMPLE_NOT_FOUND"
        override val message: String = "sample not found: $sampleKey"
        override val status: HttpStatus = HttpStatus.NOT_FOUND
    }

    data class SampleAudioNotFound(val reason: String = "sample audio file not found") : AdminError {
        override val code: String = "SAMPLE_AUDIO_NOT_FOUND"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.NOT_FOUND
    }
}
