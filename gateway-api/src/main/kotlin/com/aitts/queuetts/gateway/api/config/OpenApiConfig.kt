package com.aitts.queuetts.gateway.api.config

import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * OpenAPI 문서 설정.
 *
 * 모든 API 가 API Key 를 요구하게 됐으므로 Swagger UI 의 Authorize 버튼으로 키를 넣을 수 있도록
 * apiKey security scheme 을 전역으로 선언한다. (문서 자체 노출 여부는 `springdoc.*` 설정이 정하며,
 * 운영에서는 꺼둔다.)
 */
@Configuration
open class OpenApiConfig(
    private val properties: QueueTtsGatewayProperties,
) {
    @Bean
    open fun queuettsOpenApi(): OpenAPI {
        val openApi = OpenAPI().info(
            Info()
                .title("QueueTTS Gateway API")
                .description("TTS job 큐 게이트웨이. health 계열을 제외한 모든 엔드포인트는 API Key 가 필요하다.")
                .version("v1"),
        )
        if (!properties.security.enabled) return openApi

        val scheme = SecurityScheme()
            .type(SecurityScheme.Type.APIKEY)
            .`in`(SecurityScheme.In.HEADER)
            .name(properties.security.headerName)
            .description("발급받은 API Key. `/api/admin/**` 은 admin 권한 키가 필요하다.")
        return openApi
            .components(Components().addSecuritySchemes(API_KEY_SCHEME, scheme))
            .addSecurityItem(SecurityRequirement().addList(API_KEY_SCHEME))
    }

    private companion object {
        const val API_KEY_SCHEME = "ApiKeyAuth"
    }
}
