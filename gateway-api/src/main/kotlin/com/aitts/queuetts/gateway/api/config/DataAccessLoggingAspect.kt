package com.aitts.queuetts.gateway.api.config

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.annotation.Pointcut
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

/**
 * Logs every call into the DB repository layer (com.aitts.queuetts.gateway.api.infra.db.repository) with the
 * supplied parameters, the rows that came back and how long the call took.
 *
 * Spring's own JdbcTemplate logging (enabled in application.yml) shows the SQL text and the
 * bound parameter values; this aspect adds the missing piece — the actual query results.
 *
 * Toggle with `queuetts.logging.sql` (default true) and tune truncation with
 * `queuetts.logging.max-value-length`. Output goes to the `com.aitts.queuetts.sql` logger.
 */
@Aspect
@Component
@ConditionalOnProperty(prefix = "queuetts.logging", name = ["sql"], havingValue = "true", matchIfMissing = true)
class DataAccessLoggingAspect(
    properties: QueueTtsGatewayProperties,
) {
    private val log = LoggerFactory.getLogger("com.aitts.queuetts.sql")
    private val maxValueLength = properties.logging.maxValueLength.coerceAtLeast(20)

    @Pointcut("within(com.aitts.queuetts.gateway.api.infra.db.repository..*)")
    fun dataAccessLayer() {
    }

    @Around("dataAccessLayer()")
    fun logDataAccess(joinPoint: ProceedingJoinPoint): Any? {
        if (!log.isDebugEnabled) {
            return joinPoint.proceed()
        }
        val method = "${joinPoint.signature.declaringType.simpleName}.${joinPoint.signature.name}"
        val args = joinPoint.args.joinToString(", ", "(", ")") { summarize(it) }
        log.debug("▶ {}{}", method, args)
        val startedAt = System.nanoTime()
        try {
            val result = joinPoint.proceed()
            log.debug("◀ {} [{} ms] -> {}", method, elapsedMs(startedAt), summarizeResult(result))
            return result
        } catch (ex: Throwable) {
            log.warn("✖ {} [{} ms] failed: {}: {}", method, elapsedMs(startedAt), ex.javaClass.simpleName, ex.message)
            throw ex
        }
    }

    private fun elapsedMs(startedAt: Long): String = "%.1f".format((System.nanoTime() - startedAt) / 1_000_000.0)

    private fun summarizeResult(result: Any?): String = when (result) {
        null -> "null"
        is Collection<*> -> "${result.size} row(s): ${truncate(result.joinToString(" | ") { summarize(it) })}"
        is Pair<*, *> -> "(${summarizeResult(result.first)}, total=${summarize(result.second)})"
        else -> summarize(result)
    }

    private fun summarize(value: Any?): String = when (value) {
        null -> "null"
        is Map<*, *> -> value.entries.joinToString(", ", "{", "}") { "${it.key}=${truncate(it.value?.toString())}" }
        is Collection<*> -> "[${value.size} items]"
        else -> truncate(value.toString())
    }

    private fun truncate(text: String?): String {
        if (text == null) return "null"
        return if (text.length <= maxValueLength) text else "${text.take(maxValueLength)}…(${text.length} chars)"
    }
}
