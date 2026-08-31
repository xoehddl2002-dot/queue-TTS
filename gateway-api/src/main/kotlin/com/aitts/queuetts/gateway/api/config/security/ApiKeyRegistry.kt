package com.aitts.queuetts.gateway.api.config.security

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties.ApiKey
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.security.MessageDigest
import java.util.HexFormat

/**
 * 설정(`queuetts.security.keys`)에 등록된 API Key 조회소.
 *
 * 평문 키를 메모리에 들고 비교하지 않고 SHA-256 해시로만 색인한다. 제시된 키도 같은 방식으로
 * 해시해 맞춰보므로 비교 시간이 키 내용에 따라 달라지지 않는다(타이밍 노출 방지).
 *
 * 잘못된 설정(키 없음/중복/너무 짧음)은 기동 시점에 예외로 끊는다. 보안 설정이 조용히
 * 반쯤 적용된 채 뜨는 상태가 가장 위험하기 때문이다.
 */
@Component
class ApiKeyRegistry(properties: QueueTtsGatewayProperties) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val byHash: Map<String, ApiKey>

    init {
        val security = properties.security
        byHash = if (security.enabled) {
            validate(security.keys)
            security.keys.associateBy { sha256(it.key) }
        } else {
            log.warn(
                "queuetts.security.enabled=false — 모든 API 가 인증 없이 열립니다. 로컬 디버깅 외에는 사용하지 마세요.",
            )
            emptyMap()
        }
        if (byHash.isNotEmpty()) {
            log.info(
                "API key 인증 활성화: {} 개 키 등록 ({})",
                byHash.size,
                byHash.values.joinToString { "${it.id}=${it.role}" },
            )
        }
    }

    /** 제시된 키에 대응하는 등록 키. 없으면 null. */
    fun find(presentedKey: String): ApiKey? = byHash[sha256(presentedKey)]

    private fun validate(keys: List<ApiKey>) {
        check(keys.isNotEmpty()) {
            "queuetts.security.enabled=true 인데 queuetts.security.keys 가 비어 있습니다. " +
                "키를 등록하거나 (운영이 아니라면) queuetts.security.enabled=false 로 두세요."
        }
        keys.forEachIndexed { index, apiKey ->
            check(apiKey.id.isNotBlank()) { "queuetts.security.keys[$index].id 가 비어 있습니다." }
            check(apiKey.key.isNotBlank()) { "queuetts.security.keys[$index](id=${apiKey.id}).key 가 비어 있습니다." }
            check(apiKey.key.length >= MIN_KEY_LENGTH) {
                "queuetts.security.keys[$index](id=${apiKey.id}).key 가 너무 짧습니다. 최소 ${MIN_KEY_LENGTH}자 이상이어야 합니다."
            }
        }
        val duplicatedIds = keys.groupingBy { it.id }.eachCount().filterValues { it > 1 }.keys
        check(duplicatedIds.isEmpty()) { "queuetts.security.keys 의 id 가 중복되었습니다: $duplicatedIds" }
        check(keys.map { it.key }.toSet().size == keys.size) {
            "queuetts.security.keys 에 같은 key 값이 두 번 이상 등록되었습니다 (호출자 식별이 불가능해집니다)."
        }
    }

    private fun sha256(value: String): String =
        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.toByteArray()))

    private companion object {
        /** 무작위 32자 이상을 권장하지만, 최소선만 강제한다. */
        const val MIN_KEY_LENGTH = 16
    }
}
