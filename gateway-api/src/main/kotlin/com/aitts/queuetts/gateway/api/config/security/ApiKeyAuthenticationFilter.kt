package com.aitts.queuetts.gateway.api.config.security

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.security.authentication.AbstractAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.filter.OncePerRequestFilter

/**
 * 요청 헤더의 API Key 를 확인해 [SecurityContextHolder] 에 인증을 채우는 필터.
 *
 * 여기서는 실패 응답을 직접 쓰지 않는다. 인증을 채우지 않은 채 체인을 계속 태우고,
 * 접근 거부 판단과 응답 작성은 [SecurityConfig] 의 인가 규칙 + [SecurityErrorResponder] 에 맡긴다.
 * 대신 "키가 왔는데 틀렸다"와 "키가 아예 없다"를 구분하려고 [REJECTED_ATTRIBUTE] 만 남긴다.
 *
 * Spring Security 체인 안에서만 돌아야 하므로 `@Component` 로 두지 않고 [SecurityConfig] 에서 직접 만든다.
 */
class ApiKeyAuthenticationFilter(
    private val security: QueueTtsGatewayProperties.Security,
    private val registry: ApiKeyRegistry,
) : OncePerRequestFilter() {
    private val log = LoggerFactory.getLogger("com.aitts.queuetts.security")

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        if (SecurityContextHolder.getContext().authentication == null) {
            authenticate(request)
        }
        filterChain.doFilter(request, response)
    }

    private fun authenticate(request: HttpServletRequest) {
        val presented = resolveKey(request) ?: return
        val apiKey = registry.find(presented)
        if (apiKey == null) {
            request.setAttribute(REJECTED_ATTRIBUTE, true)
            // 키 값 자체는 절대 남기지 않는다. 어디서 몇 번 틀렸는지만 추적 가능하게 둔다.
            log.warn(
                "api-key.rejected method={} uri={} remoteAddr={}",
                request.method,
                request.requestURI,
                request.remoteAddr,
            )
            return
        }
        request.setAttribute(CLIENT_ID_ATTRIBUTE, apiKey.id)
        SecurityContextHolder.getContext().authentication = ApiKeyAuthentication(apiKey)
    }

    /** `X-API-Key` 헤더를 우선 보고, 허용된 경우 `Authorization: Bearer <key>` 도 받는다. */
    private fun resolveKey(request: HttpServletRequest): String? {
        request.getHeader(security.headerName)?.trim()?.takeIf(String::isNotEmpty)?.let { return it }
        if (!security.allowBearerHeader) return null
        val authorization = request.getHeader("Authorization")?.trim() ?: return null
        if (!authorization.regionMatches(0, BEARER_PREFIX, 0, BEARER_PREFIX.length, ignoreCase = true)) return null
        return authorization.substring(BEARER_PREFIX.length).trim().takeIf(String::isNotEmpty)
    }

    /**
     * 인증된 호출자. principal 은 로그/감사에 쓰는 키 id 이며 비밀값(key)은 담지 않는다.
     * ADMIN 은 CLIENT 권한도 함께 가진다.
     */
    class ApiKeyAuthentication(
        private val apiKey: QueueTtsGatewayProperties.ApiKey,
    ) : AbstractAuthenticationToken(authoritiesOf(apiKey.role)) {
        init {
            isAuthenticated = true
        }

        val clientId: String get() = apiKey.id
        val role: QueueTtsGatewayProperties.ApiKeyRole get() = apiKey.role

        override fun getPrincipal(): String = apiKey.id

        /** 자격증명(키)은 인증 이후 보관하지 않는다. */
        override fun getCredentials(): String = ""

        private companion object {
            fun authoritiesOf(role: QueueTtsGatewayProperties.ApiKeyRole): List<SimpleGrantedAuthority> =
                when (role) {
                    QueueTtsGatewayProperties.ApiKeyRole.ADMIN ->
                        listOf(SimpleGrantedAuthority("ROLE_ADMIN"), SimpleGrantedAuthority("ROLE_CLIENT"))
                    QueueTtsGatewayProperties.ApiKeyRole.CLIENT ->
                        listOf(SimpleGrantedAuthority("ROLE_CLIENT"))
                }
        }
    }

    companion object {
        /** 키가 전달됐지만 등록되지 않은 값이었음을 [SecurityErrorResponder] 에 알리는 요청 속성. */
        const val REJECTED_ATTRIBUTE = "queuetts.security.apiKeyRejected"

        /** 인증된 호출자 id. 로깅/감사에서 쓸 수 있게 남긴다. */
        const val CLIENT_ID_ATTRIBUTE = "queuetts.security.clientId"

        private const val BEARER_PREFIX = "Bearer "
    }
}
