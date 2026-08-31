package com.aitts.queuetts.gateway.api.config

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

// 보안 필터 체인(order -100)보다 앞에 두어야 인증 실패(401/403)로 끊긴 요청도 로그에 남는다.
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
class RequestTimingLoggingFilter : OncePerRequestFilter() {
    private val log = LoggerFactory.getLogger("com.aitts.queuetts.request")

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val requestId = request.getHeader(REQUEST_ID_HEADER)
            ?.trim()
            ?.takeIf(String::isNotBlank)
            ?: UUID.randomUUID().toString().replace("-", "").take(12)
        val method = request.method
        val path = request.requestURI
        val uri = path + request.queryString?.let { "?$it" }.orEmpty()
        val startedAt = System.nanoTime()
        val logLifecycle = shouldLogLifecycle(method, path)

        MDC.put("requestId", requestId)
        response.setHeader(REQUEST_ID_HEADER, requestId)
        if (logLifecycle) {
            log.info("request.start id={} method={} uri={}", requestId, method, uri)
        }
        try {
            filterChain.doFilter(request, response)
            logCompletion(logLifecycle, requestId, method, uri, response.status, startedAt)
        } catch (exception: Throwable) {
            log.warn(
                "request.fail id={} method={} uri={} status={} elapsedMs={} error={}: {}",
                requestId,
                method,
                uri,
                response.status,
                elapsedMs(startedAt),
                exception.javaClass.simpleName,
                exception.message,
            )
            throw exception
        } finally {
            MDC.remove("requestId")
        }
    }

    private fun elapsedMs(startedAt: Long): String =
        "%.1f".format((System.nanoTime() - startedAt) / 1_000_000.0)

    private fun logCompletion(
        logLifecycle: Boolean,
        requestId: String,
        method: String,
        uri: String,
        status: Int,
        startedAt: Long,
    ) {
        if (!logLifecycle && status < 400) {
            return
        }
        val message = "request.end id={} method={} uri={} status={} elapsedMs={}"
        if (status >= 500) {
            log.warn(message, requestId, method, uri, status, elapsedMs(startedAt))
        } else {
            log.info(message, requestId, method, uri, status, elapsedMs(startedAt))
        }
    }

    private fun shouldLogLifecycle(method: String, path: String): Boolean =
        method != "GET" || !isNoisySuccessfulGetPath(path)

    private fun isNoisySuccessfulGetPath(path: String): Boolean =
        path == "/actuator/health" ||
            path == "/api/health" ||
            GATEWAY_JOB_STATUS_REGEX.matches(path)

    private companion object {
        const val REQUEST_ID_HEADER = "X-QueueTts-Request-Id"
        val GATEWAY_JOB_STATUS_REGEX = Regex("""^/api/jobs/[^/]+$""")
    }
}
