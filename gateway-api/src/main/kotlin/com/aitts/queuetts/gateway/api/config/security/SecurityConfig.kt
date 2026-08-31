package com.aitts.queuetts.gateway.api.config.security

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

/**
 * gateway API 의 보안 경계.
 *
 * - 인증: [ApiKeyAuthenticationFilter] 가 헤더의 API Key 로 호출자를 식별한다 (세션 없음).
 * - 인가: `/api/admin` 하위는 admin 키만, health 계열([QueueTtsGatewayProperties.Security.publicPaths])은
 *   무인증, 나머지는 등록된 키가 있어야 한다.
 * - CORS: 설정에 나열된 Origin 만 허용하고, 비어 있으면 교차 출처 호출을 아예 막는다.
 *
 * 응답 보안 헤더(X-Content-Type-Options, X-Frame-Options 등)는 Spring Security 기본값을 그대로 쓴다.
 */
@Configuration
@EnableWebSecurity
open class SecurityConfig(
    private val properties: QueueTtsGatewayProperties,
    private val registry: ApiKeyRegistry,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val security get() = properties.security

    @Bean
    open fun securityFilterChain(http: HttpSecurity): SecurityFilterChain {
        val corsSource = corsConfigurationSource()
        http
            // 세션·쿠키를 쓰지 않는 헤더 인증이라 CSRF 토큰이 방어하는 대상이 없다.
            .csrf { it.disable() }
            .cors { cors -> if (corsSource == null) cors.disable() else cors.configurationSource(corsSource) }
            .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
            .httpBasic { it.disable() }
            .formLogin { it.disable() }
            .logout { it.disable() }
            .requestCache { it.disable() }

        if (!security.enabled) {
            log.warn("queuetts.security.enabled=false — 인증 없이 모든 경로를 허용합니다.")
            http.authorizeHttpRequests { it.anyRequest().permitAll() }
            return http.build()
        }

        val publicPaths = (security.publicPaths + security.additionalPublicPaths).distinct().toTypedArray()
        val responder = SecurityErrorResponder(security, objectMapper)
        http
            .authorizeHttpRequests { authorize ->
                authorize
                    .requestMatchers(*publicPaths).permitAll()
                    .requestMatchers("/api/admin/**").hasRole("ADMIN")
                    .anyRequest().authenticated()
            }
            .addFilterBefore(
                ApiKeyAuthenticationFilter(security, registry),
                UsernamePasswordAuthenticationFilter::class.java,
            )
            .exceptionHandling {
                it.authenticationEntryPoint(responder).accessDeniedHandler(responder)
            }
        log.info("보안 경계 설정 완료: public={} admin=/api/admin/** (ROLE_ADMIN)", publicPaths.toList())
        return http.build()
    }

    /** 허용 Origin 이 없으면 null 을 돌려 CORS 자체를 비활성화한다. */
    private fun corsConfigurationSource(): CorsConfigurationSource? {
        val cors = security.cors
        if (cors.allowedOrigins.isEmpty()) {
            log.info("CORS 허용 Origin 이 설정되지 않아 교차 출처 호출을 허용하지 않습니다.")
            return null
        }
        require(!(cors.allowCredentials && cors.allowedOrigins.any { it == "*" })) {
            "queuetts.security.cors: allow-credentials=true 와 allowed-origins=* 는 함께 쓸 수 없습니다."
        }
        val configuration = CorsConfiguration().apply {
            // 와일드카드가 섞인 항목은 패턴으로, 정확한 Origin 은 그대로 등록한다.
            val (patterns, exact) = cors.allowedOrigins.partition { it.contains('*') }
            if (exact.isNotEmpty()) allowedOrigins = exact
            if (patterns.isNotEmpty()) allowedOriginPatterns = patterns
            allowedMethods = cors.allowedMethods
            allowedHeaders = cors.allowedHeaders
            exposedHeaders = cors.exposedHeaders
            allowCredentials = cors.allowCredentials
            maxAge = cors.maxAgeSeconds
        }
        log.info("CORS 허용 Origin: {}", cors.allowedOrigins)
        return UrlBasedCorsConfigurationSource().apply { registerCorsConfiguration("/**", configuration) }
    }
}
