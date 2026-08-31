package com.aitts.queuetts.gateway.api.scheduler

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.AudioJobResult
import com.aitts.queuetts.gateway.api.dto.JobError
import com.aitts.queuetts.gateway.api.infra.redis.messaging.ArtifactContent
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.messaging.StyleCatalogResult
import com.aitts.queuetts.gateway.api.infra.redis.queue.EPHEMERAL_REQUEST_PREFIX
import com.aitts.queuetts.gateway.api.infra.redis.queue.PendingJobResults
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.service.JobService
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.stream.StreamMessageListenerContainer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Redis result 스트림에서 TTS worker 의 진행/완료 결과를 소비해 [JobService] 에 반영한다.
 *
 * worker 에게 HTTP 로 job 을 직접 뿌리던 기존 auto processor 를 대체한다:
 * 이제 gateway 는 우선순위별 job 스트림에 발행만 하고 result 스트림에 도착하는 것에만 반응하므로
 * worker 자체에 대한 정보(URL, 갯수 등)를 전혀 알 필요가 없다.
 *
 * 결과 소비는 주기적 폴링이 아니라 blocking `XREADGROUP` 을 쓰는 [StreamMessageListenerContainer] 로
 * 처리한다. 컨테이너는 blocking read 전용 커넥션을 따로 잡으므로 공유 [RedisConnectionFactory] 커넥션
 * (publish/health 등)을 막지 않으며, 결과가 스트림에 push 되는 즉시 콜백으로 처리된다.
 */
@Component
class JobResultListener(
    private val properties: QueueTtsGatewayProperties,
    private val streamQueueClient: RedisStreamQueueClient,
    private val jobService: JobService,
    private val pendingJobResults: PendingJobResults,
    private val objectMapper: ObjectMapper,
    private val connectionFactory: RedisConnectionFactory,
) {
    private val log = LoggerFactory.getLogger(JobResultListener::class.java)
    private val running = AtomicBoolean(false)
    private val listenerHealthy = AtomicBoolean(true)
    private var bootstrapThread: Thread? = null
    @Volatile
    private var container: StreamMessageListenerContainer<String, MapRecord<String, String, String>>? = null

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (!properties.queue.enabled) {
            log.info("Job result listener is disabled (queuetts.queue.enabled=false)")
            return
        }
        if (!running.compareAndSet(false, true)) {
            return
        }
        // Redis 가 아직 안 떠 있을 수 있어 group 생성/pending 복구는 별도 스레드에서 재시도한다.
        // 준비가 끝나면 blocking 컨테이너가 이후 결과를 push 로 받아 처리한다.
        bootstrapThread = Thread(::bootstrap, "job-result-listener-bootstrap").apply {
            isDaemon = true
            start()
        }
    }

    @PreDestroy
    fun stop() {
        running.set(false)
        bootstrapThread?.interrupt()
        runCatching { container?.stop() }
        container = null
    }

    private fun bootstrap() {
        var bootstrapHealthy = true
        while (running.get()) {
            try {
                streamQueueClient.ensureGroups()
                // 재시작/crash 로 전달만 되고 unacked 인 pending 결과를 먼저 비운 뒤 컨테이너를 띄운다.
                drainPending()
                startContainer()
                log.info(
                    "Job result listener started: stream={} group={} consumer={}",
                    properties.queue.resultStream,
                    properties.queue.resultGroup,
                    properties.queue.consumerName,
                )
                return
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (exception: Exception) {
                if (bootstrapHealthy) {
                    log.warn("Job result listener bootstrap unavailable: {}", exception.message)
                    bootstrapHealthy = false
                } else {
                    log.debug("Job result listener bootstrap still unavailable: {}", exception.message)
                }
                try {
                    Thread.sleep(ERROR_BACKOFF_MS)
                } catch (interrupted: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return
                }
            }
        }
    }

    /** 이 consumer 의 pending(전달됐지만 unacked) 결과를 모두 처리한다 — 재시작/crash 복구용. */
    private fun drainPending() {
        while (running.get()) {
            val records = streamQueueClient.readPendingResults(BATCH_SIZE)
            if (records.isEmpty()) {
                return
            }
            records.forEach(::handleRecord)
        }
    }

    private fun startContainer() {
        val options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
            .builder()
            .batchSize(BATCH_SIZE.toInt())
            .pollTimeout(BLOCK_TIMEOUT)
            .errorHandler(::onContainerError)
            .build()
        val created = StreamMessageListenerContainer.create(connectionFactory, options)
        val request = StreamMessageListenerContainer.StreamReadRequest
            .builder(StreamOffset.create(properties.queue.resultStream, ReadOffset.lastConsumed()))
            .consumer(Consumer.from(properties.queue.resultGroup, properties.queue.consumerName))
            // 결과는 처리 후 직접 ack + delete 하므로 자동 ack 를 끈다.
            .autoAcknowledge(false)
            // 일시적 Redis 장애로 구독 자체가 죽지 않도록 유지한다 (아래 errorHandler 가 복구를 로깅한다).
            .cancelOnError { false }
            .build()
        created.register(request) { record -> handleRecord(record) }
        created.start()
        container = created
    }

    private fun onContainerError(error: Throwable) {
        if (listenerHealthy.compareAndSet(true, false)) {
            log.warn("Job result listener unavailable: {}", error.message)
        } else {
            log.debug("Job result listener still unavailable: {}", error.message)
        }
        // Redis 재시작 등으로 group 이 사라졌으면 재생성해 구독이 복구되도록 한다.
        if (error.message?.contains("NOGROUP") == true) {
            runCatching { streamQueueClient.ensureGroups() }
                .onFailure { log.debug("ensureGroups after NOGROUP failed: {}", it.message) }
        }
    }

    /** 컨테이너 콜백과 pending 복구가 함께 쓰는 결과 처리 지점 (테스트에서 직접 호출한다). */
    internal fun handleRecord(record: MapRecord<String, String, String>) {
        if (listenerHealthy.compareAndSet(false, true)) {
            log.info(
                "Job result listener recovered: stream={} group={}",
                properties.queue.resultStream,
                properties.queue.resultGroup,
            )
        }
        try {
            val fields = record.value
            val jobId = fields["jobId"] ?: fields["job_id"]
            if (jobId.isNullOrBlank()) {
                log.warn("Result record {} has no jobId, discarding: {}", record.id, fields.keys)
                return
            }
            val isControlRequest = jobId.startsWith(EPHEMERAL_REQUEST_PREFIX)
            val workerId = fields["workerId"] ?: fields["worker_id"] ?: "unknown-worker"
            val completion = RedisResultMessage(
                jobId = jobId,
                workerId = workerId,
                batchId = fields["batchId"] ?: fields["batch_id"],
                state = fields["state"] ?: fields["status"],
                startedAt = parseOffsetDateTime(fields["startedAt"] ?: fields["started_at"]),
                result = if (isControlRequest) null else parseAudioResult(fields["result"]),
                styleCatalog = if (isControlRequest) parseStyleCatalog(fields["result"]) else null,
                // 제어 요청은 종류마다 응답 모양이 다르므로 원문을 그대로 넘겨 호출자가 읽게 한다.
                rawResult = if (isControlRequest) fields["result"] else null,
                error = parseError(fields["error"]),
                artifact = parseArtifact(fields["artifact"]),
            )
            // req_ 접두사는 styles 같은 일회성 제어 요청이라 job 상태 관리 대상이 아니다.
            if (!isControlRequest) {
                jobService.applyQueueEvent(completion)
            }
            // 일회성 제어 요청은 terminal 결과에서만 대기자를 깨운다.
            if (!completion.state.equals("running", ignoreCase = true)) {
                pendingJobResults.complete(jobId, completion)
            }
        } catch (exception: Exception) {
            log.warn("Failed to process result record {}: {}", record.id, exception.message, exception)
        } finally {
            // 잘못된 record 가 리스너를 막아버리지 않도록 실패해도 ack 한다.
            // 실패는 로그로 남고, job 은 POST /api/jobs/{jobId}/requeue 로 재발행해 복구할 수 있다.
            runCatching { streamQueueClient.acknowledgeResult(record) }
                .onFailure { log.warn("Failed to ack result record {}: {}", record.id, it.message) }
        }
    }

    private fun parseOffsetDateTime(raw: String?): java.time.OffsetDateTime? =
        raw?.takeIf(String::isNotBlank)?.let { value ->
            runCatching { java.time.OffsetDateTime.parse(value) }
                .getOrElse { exception -> throw IllegalArgumentException("invalid startedAt", exception) }
        }

    private fun parseAudioResult(raw: String?): AudioJobResult? {
        if (raw.isNullOrBlank()) {
            return null
        }
        return runCatching {
            objectMapper.readValue(raw, AudioJobResult::class.java)
        }.getOrElse { exception ->
            throw IllegalArgumentException("invalid result JSON", exception)
        }
    }

    private fun parseStyleCatalog(raw: String?): StyleCatalogResult? {
        if (raw.isNullOrBlank()) {
            return null
        }
        return runCatching {
            objectMapper.readValue(raw, StyleCatalogResult::class.java)
        }.getOrElse { exception ->
            throw IllegalArgumentException("invalid style catalog JSON", exception)
        }
    }

    private fun parseError(raw: String?): JobError? {
        if (raw.isNullOrBlank()) {
            return null
        }
        return runCatching { objectMapper.readValue(raw, JobError::class.java) }
            .getOrElse { JobError(message = raw) }
    }

    private fun parseArtifact(raw: String?): ArtifactContent? = raw?.takeIf(String::isNotBlank)?.let {
        runCatching { objectMapper.readValue(it, ArtifactContent::class.java) }
            .getOrElse { exception -> throw IllegalArgumentException("invalid artifact JSON", exception) }
    }

    private companion object {
        const val BATCH_SIZE = 10L
        /** blocking XREADGROUP 의 BLOCK 시간. 이 시간 안에 결과가 없으면 컨테이너가 재차 blocking read 를 건다. */
        val BLOCK_TIMEOUT: Duration = Duration.ofSeconds(2)
        /** bootstrap(group 생성/pending 복구)이 실패했을 때 다음 시도까지 쉬는 backoff(ms). tight-spin 방지용. */
        const val ERROR_BACKOFF_MS = 3000L
    }
}
