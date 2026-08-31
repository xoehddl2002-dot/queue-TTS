package com.aitts.queuetts.gateway.api.error

import org.springframework.http.HttpStatus

sealed interface GatewayError: DomainError {
    val status: HttpStatus

    data class WorkerError(val reason: String="Worker connection Error"): GatewayError{
        override val code: String = "WORKER_CONNECTION_ERROR"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.SERVICE_UNAVAILABLE
    }

    /** 등록되지 않은 model 을 지정하는 등, 호출자가 고칠 수 있는 잘못된 요청. */
    data class InvalidRequest(val reason: String = "invalid request"): GatewayError{
        override val code: String = "INVALID_REQUEST"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.BAD_REQUEST
    }
}
