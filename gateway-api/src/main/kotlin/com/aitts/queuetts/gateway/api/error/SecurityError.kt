package com.aitts.queuetts.gateway.api.error

import org.springframework.http.HttpStatus

/**
 * API Key 인증/인가 실패.
 *
 * 컨트롤러가 아니라 Spring Security 필터 체인에서 만들어지지만, 응답 본문 모양은 다른
 * [DomainError] 들과 동일하게 유지해 소비자가 실패 처리 코드를 하나로 쓸 수 있게 한다.
 */
sealed interface SecurityError : DomainError {
    val status: HttpStatus

    /** 키가 아예 전달되지 않음. */
    data class MissingApiKey(val headerName: String) : SecurityError {
        override val code: String = "MISSING_API_KEY"
        override val message: String = "API key required: send it in the $headerName header"
        override val status: HttpStatus = HttpStatus.UNAUTHORIZED
    }

    /** 키는 왔지만 등록되지 않은 값. 어떤 키였는지는 응답에 절대 싣지 않는다. */
    data class InvalidApiKey(val reason: String = "invalid API key") : SecurityError {
        override val code: String = "INVALID_API_KEY"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.UNAUTHORIZED
    }

    /** 유효한 키지만 해당 경로에 필요한 role 이 없음 (예: client 키로 `/api/admin` 하위 호출). */
    data class Forbidden(val path: String) : SecurityError {
        override val code: String = "FORBIDDEN"
        override val message: String = "API key is not allowed to access $path"
        override val status: HttpStatus = HttpStatus.FORBIDDEN
    }
}
