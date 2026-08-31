package com.aitts.queuetts.gateway.api.config.security

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.error.SecurityError
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.MediaType
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.core.AuthenticationException
import org.springframework.security.web.AuthenticationEntryPoint
import org.springframework.security.web.access.AccessDeniedHandler
import java.nio.charset.StandardCharsets

/**
 * 인증/인가 실패를 컨트롤러와 같은 JSON 모양(`{code, message, status, ...}`)으로 내려준다.
 *
 * 실패 응답 작성이 한 곳에 모이도록 [AuthenticationEntryPoint](401)와 [AccessDeniedHandler](403)를
 * 함께 구현한다.
 */
class SecurityErrorResponder(
    private val security: QueueTtsGatewayProperties.Security,
    private val objectMapper: ObjectMapper,
) : AuthenticationEntryPoint, AccessDeniedHandler {

    /** 미인증(익명) 요청. 키가 왔는데 틀린 경우와 아예 없는 경우를 구분해 알려준다. */
    override fun commence(
        request: HttpServletRequest,
        response: HttpServletResponse,
        authException: AuthenticationException,
    ) {
        val rejected = request.getAttribute(ApiKeyAuthenticationFilter.REJECTED_ATTRIBUTE) == true
        val error = if (rejected) SecurityError.InvalidApiKey() else SecurityError.MissingApiKey(security.headerName)
        write(response, error)
    }

    /** 인증은 됐지만 role 이 모자란 요청 (client 키로 `/api/admin` 하위 호출 등). */
    override fun handle(
        request: HttpServletRequest,
        response: HttpServletResponse,
        accessDeniedException: AccessDeniedException,
    ) {
        write(response, SecurityError.Forbidden(request.requestURI))
    }

    private fun write(response: HttpServletResponse, error: SecurityError) {
        if (response.isCommitted) return
        response.status = error.status.value()
        response.contentType = MediaType.APPLICATION_JSON_VALUE
        response.characterEncoding = StandardCharsets.UTF_8.name()
        if (error.status.value() == 401) {
            // RFC 7235: 401 은 인증 방식을 알려야 한다. 브라우저 basic-auth 팝업이 뜨지 않도록 커스텀 scheme 을 쓴다.
            response.setHeader("WWW-Authenticate", """ApiKey realm="queuetts-gateway", header="${security.headerName}"""")
        }
        response.writer.write(objectMapper.writeValueAsString(error))
        response.writer.flush()
    }
}
