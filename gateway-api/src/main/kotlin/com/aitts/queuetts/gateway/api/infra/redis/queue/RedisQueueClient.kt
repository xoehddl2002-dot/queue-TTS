package com.aitts.queuetts.gateway.api.infra.redis.queue

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.JobError

import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.messaging.StyleCatalogResult
import com.aitts.queuetts.gateway.api.infra.redis.messaging.SpeakerControlResult
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** job 이 아닌 일회성 제어 요청(styles 등)에 쓰는 jobId 접두사. job 상태 관리 대상이 아니다. */
const val EPHEMERAL_REQUEST_PREFIX = "req_"

/** Worker가 제어 요청을 처리했지만 payload/참조 자료를 거절했을 때의 실패. */
class WorkerReportedFailureException(
    val workerReason: String,
) : IllegalStateException(workerReason)

/** 큐를 통해 worker 가 돌려준 JSON 결과 (styles 같은 제어 요청용). */
data class QueueStylesResult(
    val workerId: String,
    val data: StyleCatalogResult,
    /** 이 결과를 돌려준 엔진(모델) 풀. */
    val model: String,
)

/** speaker 파생 캐시 삭제 제어 요청의 결과. */
data class QueueSpeakerResult(
    val workerId: String,
    val data: SpeakerControlResult,
    /** 이 결과를 돌려준 엔진(모델) 풀. */
    val model: String,
)

/**
 * 오디오가 아닌 JSON 제어 요청(styles 등)을 위한 Redis 잡 파이프라인 request/reply 브리지.
 *
 * `req_` 접두사의 일회성 id 로 job 스트림에 직접 발행하므로 gateway job 으로 영속화되지 않고
 * job 목록/이력에도 남지 않는다. worker 는 응답 JSON 을 result 필드에 담아 result 스트림으로
 * 돌려주며, 호출 스레드는 완료가 도착할 때까지 대기한다. gateway 는 worker 와 직접 통신하지
 * 않으며, 결과를 보고한 consumer 이름만 알 뿐이다.
 */
@Component
class RedisQueueClient(
    private val properties: QueueTtsGatewayProperties,
    private val streamQueueClient: RedisStreamQueueClient,
    private val pendingJobResults: PendingJobResults,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger("com.aitts.queuetts.timing")

    fun verifyAvailable() {
        // queue 비활성 모드는 기존 in-memory API 동작을 유지한다.
        if (!streamQueueClient.enabled) {
            return
        }
        streamQueueClient.verifyAvailable()
    }

    /**
     * styles 조회처럼 오디오가 아닌 JSON 응답이 필요한 제어 요청을 큐로 보낸다.
     *
     * `req_` 접두사의 일회성 id 로 job 스트림에 직접 발행하므로 gateway job 으로 영속화되지
     * 않고 job 목록/이력에도 남지 않는다. worker 는 응답 JSON 을 result 필드에 담아
     * result 스트림으로 돌려준다.
     */
    fun requestStyles(source: String = "control", model: String? = null): QueueStylesResult {
        // styles 는 인자가 없다. 대상 풀은 model 인자로만 정한다.
        val (targetModel, completion) = controlRequest("styles", payload = null, source = source, model = model)
        return QueueStylesResult(
            workerId = completion.workerId,
            data = completion.styleCatalog ?: StyleCatalogResult(),
            model = targetModel,
        )
    }

    /**
     * speaker 파생 prompt 삭제 요청(`speaker_forget`).
     *
     * styles 와 같은 일회성 request/reply 이지만 **부수효과가 있다.** timeout 이 나도 worker 가
     * 캐시를 지웠을 수 있다. prompt 는 언제든 다시 만들 수 있는 파생물이라 어느 쪽이어도 안전하다.
     */
    fun requestSpeakerControl(
        type: String,
        payload: Any?,
        source: String = "styles",
        model: String? = null,
    ): QueueSpeakerResult {
        val (targetModel, completion) = controlRequest(type, payload = payload, source = source, model = model)
        val raw = completion.rawResult
            ?: throw IllegalStateException("$type request to model '$targetModel' returned no result")
        val data = runCatching { objectMapper.readValue(raw, SpeakerControlResult::class.java) }
            .getOrElse { throw IllegalStateException("$type response from model '$targetModel' was not readable: ${it.message}") }
        return QueueSpeakerResult(workerId = completion.workerId, data = data, model = targetModel)
    }

    /**
     * 제어 요청 하나를 발행하고 응답을 기다린다.
     *
     * 응답 모양은 요청 종류마다 다르므로 여기서는 해석하지 않고 완료 메시지를 그대로 돌려준다.
     * 로그 키에 [type] 을 넣어 `queue.styles.*` / `queue.speaker_forget.*` 으로 갈라 보이게 한다.
     */
    private fun controlRequest(
        type: String,
        payload: Any?,
        source: String,
        model: String?,
    ): Pair<String, RedisResultMessage> {
        ensureQueueEnabled()
        val targetModel = streamQueueClient.canonicalModel(model)
            ?: throw UnknownQueueModelException(model.orEmpty(), streamQueueClient.modelNames)
        val started = System.nanoTime()
        val requestId = EPHEMERAL_REQUEST_PREFIX + UUID.randomUUID().toString().replace("-", "").take(12)
        // 발행 전에 등록하므로 완료를 놓치는 race 가 없다.
        val future = pendingJobResults.register(requestId)
        try {
            streamQueueClient.publishControlJob(
                jobId = requestId,
                type = type,
                payload = payload,
                priority = "urgent",
                source = source,
                model = targetModel,
            )
            log.info("queue.{}.published requestId={} source={} model={}", type, requestId, source, targetModel)
            val completion = future.get(properties.queue.controlRequestTimeoutSeconds, TimeUnit.SECONDS)
            val state = completion.state?.lowercase(Locale.US) ?: "failed"
            if (state != "succeeded") {
                throw failedJobException(requestId, completion.error)
            }
            log.info(
                "queue.{}.done requestId={} source={} model={} elapsedMs={} worker={}",
                type,
                requestId,
                source,
                targetModel,
                elapsedMs(started),
                completion.workerId,
            )
            return targetModel to completion
        } catch (exception: TimeoutException) {
            log.warn(
                "queue.{}.timeout requestId={} source={} model={} elapsedMs={} timeoutSeconds={}",
                type,
                requestId,
                source,
                targetModel,
                elapsedMs(started),
                properties.queue.controlRequestTimeoutSeconds,
            )
            throw IllegalStateException(
                "$type request for model '$targetModel' did not complete within " +
                        "${properties.queue.controlRequestTimeoutSeconds}s. " +
                        "Check that a '$targetModel' TTS worker is consuming the priority job streams.",
            )
        } catch (exception: InterruptedException) {
            Thread.currentThread().interrupt()
            log.warn(
                "queue.{}.interrupted requestId={} source={} model={} elapsedMs={}",
                type,
                requestId,
                source,
                targetModel,
                elapsedMs(started)
            )
            throw IllegalStateException("interrupted while waiting for $type result")
        } catch (exception: ExecutionException) {
            log.warn(
                "queue.{}.failed requestId={} source={} model={} elapsedMs={} error={}",
                type,
                requestId,
                source,
                targetModel,
                elapsedMs(started),
                exception.cause?.message ?: exception.message,
            )
            throw IllegalStateException(
                "$type request for model '$targetModel' failed: ${exception.cause?.message ?: exception.message}"
            )
        } finally {
            pendingJobResults.discard(requestId)
        }
    }

    private fun ensureQueueEnabled() {
        if (!streamQueueClient.enabled) {
            throw IllegalStateException("redis job queue is disabled (queuetts.queue.enabled=false)")
        }
    }

    private fun failedJobException(jobId: String, error: JobError?): IllegalStateException {
        val reason = error?.displayMessage() ?: "TTS worker reported a failure"
        log.warn("control request {} was rejected by worker: {}", jobId, reason)
        return WorkerReportedFailureException(reason)
    }

    private fun elapsedMs(started: Long): String = "%.1f".format((System.nanoTime() - started) / 1_000_000.0)
}
