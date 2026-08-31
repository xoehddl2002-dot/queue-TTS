package com.aitts.queuetts.gateway.api.config.security

import com.aitts.queuetts.gateway.api.dto.JobCaller
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component

/**
 * 현재 요청을 인증한 호출자(API Key) 조회.
 *
 * job 이력에 "누가/어떤 권한으로 넣은 job 인지"를 남기기 위해 쓴다. 인증이 꺼져 있거나
 * (`queuetts.security.enabled=false`) 스케줄러처럼 요청 컨텍스트가 없는 경로에서는 null 이다.
 */
@Component
class CurrentCaller {
    fun current(): JobCaller? =
        (SecurityContextHolder.getContext().authentication as? ApiKeyAuthenticationFilter.ApiKeyAuthentication)
            ?.let { JobCaller(id = it.clientId, role = it.role) }
}
