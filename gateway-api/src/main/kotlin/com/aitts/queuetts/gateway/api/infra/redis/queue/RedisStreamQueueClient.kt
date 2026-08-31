package com.aitts.queuetts.gateway.api.infra.redis.queue


import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.JobPayload
import com.aitts.queuetts.gateway.api.dto.QueueModelOverviewResponse
import com.aitts.queuetts.gateway.api.dto.QueueStreamResponse
import com.aitts.queuetts.gateway.api.dto.RedisQueueOverviewResponse
import com.aitts.queuetts.gateway.api.dto.WorkerResponse
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisJobMessage
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.RedisSystemException
import org.springframework.data.redis.connection.stream.*
import org.springframework.data.redis.core.RedisCallback
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

object JobPriorities {
    val ordered: List<String> = listOf("urgent", "high", "normal", "low")

    fun streamName(baseStream: String, priority: String): String {
        require(priority in ordered) { "unsupported job priority: $priority" }
        return "$baseStream:$priority"
    }

    fun streams(baseStream: String): Map<String, String> =
        ordered.associateWith { priority -> streamName(baseStream, priority) }
}

/** 우선순위별 Redis job 스트림의 consumer 로 관측된 TTS worker (`XINFO CONSUMERS` 기반). */
data class QueueWorkerSnapshot(
    val name: String,
    val pending: Long,
    val idleMs: Long,
    val active: Boolean,
    /** 이 worker 가 속한 합성 엔진(모델) 풀. */
    val model: String,
)

/** 등록되지 않은 model 로 발행을 시도했을 때. 호출부가 400 으로 바꾼다. */
class UnknownQueueModelException(val model: String, knownModels: Collection<String>) :
    IllegalArgumentException("unknown model '$model'. Known models: ${knownModels.sorted().joinToString(", ")}")

/**
 * 분리된 잡 파이프라인의 Redis Streams 게이트웨이.
 *
 * gateway 는 TTS worker 와 직접 통신하지 않는다. 접수된 job 은 우선순위별 job 스트림에
 * `XADD` 되고 worker 들이 높은 우선순위부터 consumer group 으로 직접 가져간다. 진행/완료 이벤트는
 * result 스트림으로 들어오며 gateway 가 자체 consumer group 으로 소비한다. worker 존재 여부는
 * 네 job 스트림의 consumer group 정보를 합쳐 파악하므로 gateway 에는 worker 설정이 전혀 필요 없다.
 */
@Component
class RedisStreamQueueClient(
    private val properties: QueueTtsGatewayProperties,
    private val redisTemplate: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(RedisStreamQueueClient::class.java)
    private val redisAvailable = AtomicReference<Boolean?>(null)
    private val queue get() = properties.queue
    private val streamOps get() = redisTemplate.opsForStream<String, String>()

    val enabled: Boolean get() = queue.enabled

    /** 설정된 합성 엔진(모델) 이름들. */
    val modelNames: Set<String> get() = queue.modelQueues.keys

    /**
     * 모델 큐 설정을 기동 시점에 검증한다.
     *
     * 잘못된 설정으로 뜨면 잡이 엉뚱한 풀로 가거나 두 엔진이 같은 큐를 공유해 **조용히**
     * 오작동한다. 로그로 흘리지 않고 기동을 막는다.
     */
    @PostConstruct
    fun validateModelQueues() {
        if (!queue.enabled) {
            return
        }
        val problems = queue.configurationProblems()
        check(problems.isEmpty()) {
            "invalid queuetts.queue model configuration:" + problems.joinToString("") { "\n  - $it" }
        }
        log.info(
            "redis.queue.models defaultModel={} models={}",
            queue.defaultModel,
            queue.modelQueues.entries.joinToString(", ") { (model, q) ->
                "$model[${q.jobStream} / ${q.jobGroup}]"
            },
        )
    }

    /** 요청의 model 을 정식 키로 바꾼다. 등록되지 않은 이름이면 null (호출부가 400 으로 처리). */
    fun canonicalModel(model: String?): String? = queue.canonicalModel(model)

    private fun modelQueue(model: String?): QueueTtsGatewayProperties.ModelQueue {
        val canonical = queue.canonicalModel(model)
            ?: throw UnknownQueueModelException(model.orEmpty(), queue.modelQueues.keys)
        return queue.modelQueues.getValue(canonical)
    }

    /** 한 모델의 우선순위별 job stream. */
    fun priorityJobStreams(model: String? = null): Map<String, String> =
        JobPriorities.streams(modelQueue(model).jobStream)

    /** 기본 모델의 우선순위별 job stream (기존 호출부 호환). */
    val priorityJobStreams: Map<String, String> get() = priorityJobStreams(null)

    /** 모든 모델의 우선순위별 job group 과 result group 을 MKSTREAM 으로 생성한다. */
    fun ensureGroups() {
        queue.modelQueues.forEach { (_, modelQueue) ->
            JobPriorities.streams(modelQueue.jobStream).values.forEach { stream ->
                createGroup(stream, modelQueue.jobGroup)
            }
        }
        createGroup(queue.resultStream, queue.resultGroup)
    }


    fun publishJob(
        jobId: String,
        type: String,
        payload: JobPayload?,
        priority: String,
        source: String?,
        model: String? = null,
    ): String {
        val message = RedisJobMessage(
            jobId = jobId,
            type = type,
            payload = payload,
            priority = priority,
            source = source,
            enqueuedAt = OffsetDateTime.now(ZoneOffset.UTC),
        )
        // 발행 대상은 payload 의 model 이 정한다. 인자로 준 model 이 우선(제어 요청용).
        return publishMessage(message, model ?: payload?.model, priority)
    }

    /**
     * 인자가 있는 제어 요청(`speaker_forget` 등)을 발행한다.
     *
     * [publishJob] 과 달리 payload 가 [JobPayload] 가 아니므로 라우팅에 쓸 model 을 payload 에서
     * 꺼낼 수 없다 — 대상 풀을 인자로 받는다.
     */
    fun publishControlJob(
        jobId: String,
        type: String,
        payload: Any?,
        priority: String,
        source: String?,
        model: String,
    ): String = publishMessage(
        RedisJobMessage(
            jobId = jobId,
            type = type,
            payload = payload,
            priority = priority,
            source = source,
            enqueuedAt = OffsetDateTime.now(ZoneOffset.UTC),
        ),
        model,
        priority,
    )

    private fun publishMessage(message: RedisJobMessage, model: String?, priority: String): String {
        val fields = message.toRedisFields(objectMapper)
        val targetStream = JobPriorities.streamName(modelQueue(model).jobStream, priority)
        val recordId = try {
            streamOps.add(StreamRecords.newRecord().ofMap(fields).withStreamKey(targetStream))
                ?: throw IllegalStateException("XADD to $targetStream returned no record id")
        } catch (exception: RedisConnectionFailureException) {
            markRedisUnavailable("publish-job", exception)
            throw IllegalStateException("Redis is temporarily unavailable", exception)
        } catch (exception: RedisSystemException) {
            markRedisUnavailable("publish-job", exception)
            throw IllegalStateException("Redis is temporarily unavailable", exception)
        }
        markRedisAvailable("publish-job")
        runCatching { streamOps.trim(targetStream, queue.jobStreamMaxLength, true) }
        return recordId.value
    }

    /** API 요청을 수락하기 전에 Redis 연결과 인증을 빠르게 확인한다. */
    fun verifyAvailable() {
        if (!enabled) {
            return
        }
        try {
            redisTemplate.execute(RedisCallback<String> { connection -> connection.ping() })
            markRedisAvailable("verify")
        } catch (exception: RedisConnectionFailureException) {
            markRedisUnavailable("verify", exception)
            throw IllegalStateException("Redis is temporarily unavailable", exception)
        } catch (exception: RedisSystemException) {
            markRedisUnavailable("verify", exception)
            throw IllegalStateException("Redis is temporarily unavailable", exception)
        }
    }

    /**
     * 이 consumer 의 pending(전달됐지만 unacked) 결과를 읽는다 — 재시작/crash 복구용.
     * 새 결과의 실시간 소비는 [com.aitts.queuetts.gateway.api.scheduler.JobResultListener] 의
     * blocking StreamMessageListenerContainer 가 담당한다.
     */
    fun readPendingResults(count: Long): List<MapRecord<String, String, String>> =
        readResultsAt(ReadOffset.from("0"), count)

    /** 결과를 ack 하고 스트림에서 삭제한다 (결과에는 큰 base64 오디오가 실려 있어 바로 지운다). */
    fun acknowledgeResult(record: MapRecord<String, String, String>) {
        streamOps.acknowledge(queue.resultStream, queue.resultGroup, record.id)
        runCatching { streamOps.delete(queue.resultStream, record.id) }
    }

    /**
     * 우선순위별 job 스트림 group 의 `XINFO CONSUMERS` 를 합쳐 TTS worker 목록을 뽑는다.
     * pending 이 없고 [QueueTtsGatewayProperties.Queue.workerEvictIdleMs] 를 넘겨 idle 인 consumer 는
     * 그룹에서 제거해 죽은 worker 가 갯수를 부풀리지 않게 한다.
     */
    fun workerSnapshots(): List<QueueWorkerSnapshot> {
        if (!enabled) {
            return emptyList()
        }
        return listWorkerConsumers()
    }

    /**
     * 살아있는 worker 가 하나라도 있는 모델들.
     *
     * 제어 요청(styles)을 보내기 전에 쓴다 — worker 가 없는 풀에 보내면 응답이 없어
     * control-request timeout(기본 30초)만큼 호출자가 붙잡힌다.
     */
    fun activeModels(): Set<String> =
        workerSnapshots().filter(QueueWorkerSnapshot::active).map(QueueWorkerSnapshot::model).toSet()

    /** Health API는 Redis 장애 때문에 Gateway 자체의 응답까지 실패시키지 않는다. */
    fun workerResponsesForHealth(): List<WorkerResponse> = try {
        workerResponses()
    } catch (exception: Exception) {
        markRedisUnavailable("health.worker-consumers", exception)
        emptyList()
    }

    /**
     * 모든 모델 풀의 consumer 를 합쳐 worker 목록을 만든다.
     *
     * 모델별로 stream/group 이 다르므로 풀마다 따로 조회해 `model` 로 태깅한다. 한 풀만 보면
     * 다른 엔진의 worker 가 전부 죽어도 health 가 정상으로 보이므로 반드시 전부 순회해야 한다.
     */
    private fun listWorkerConsumers(): List<QueueWorkerSnapshot> =
        queue.modelQueues.flatMap { (model, modelQueue) -> listWorkerConsumers(model, modelQueue) }
            .sortedWith(compareBy(QueueWorkerSnapshot::model, QueueWorkerSnapshot::name))

    private fun listWorkerConsumers(
        model: String,
        modelQueue: QueueTtsGatewayProperties.ModelQueue,
    ): List<QueueWorkerSnapshot> {
        data class Aggregate(var pending: Long = 0, var idleMs: Long = Long.MAX_VALUE)

        val streams = JobPriorities.streams(modelQueue.jobStream).values
        val aggregates = linkedMapOf<String, Aggregate>()
        streams.forEach { stream ->
            streamOps.consumers(stream, modelQueue.jobGroup).forEach { consumer ->
                val aggregate = aggregates.getOrPut(consumer.consumerName()) { Aggregate() }
                aggregate.pending += consumer.pendingCount()
                aggregate.idleMs = minOf(aggregate.idleMs, consumer.idleTimeMs())
            }
        }
        return aggregates.mapNotNull { (name, aggregate) ->
            if (aggregate.pending == 0L && aggregate.idleMs > queue.workerEvictIdleMs) {
                streams.forEach { stream ->
                    runCatching { streamOps.deleteConsumer(stream, Consumer.from(modelQueue.jobGroup, name)) }
                        .onFailure {
                            log.debug(
                                "Failed to evict stale consumer {} from {}: {}",
                                name,
                                stream,
                                it.message
                            )
                        }
                }
                return@mapNotNull null
            }
            QueueWorkerSnapshot(
                name = name,
                pending = aggregate.pending,
                idleMs = aggregate.idleMs,
                active = aggregate.idleMs <= queue.workerActiveIdleMs,
                model = model,
            )
        }
    }

    fun overviewInfo(): RedisQueueOverviewResponse {
        val workers = workerSnapshots()
        val workersByModel = workers.groupBy(QueueWorkerSnapshot::model)

        val models = queue.modelQueues.mapValues { (model, modelQueue) ->
            val streams = JobPriorities.streams(modelQueue.jobStream)
            val lengths = streams.mapValues { (_, stream) -> streamLength(stream) }
            val modelWorkers = workersByModel[model].orEmpty()
            QueueModelOverviewResponse(
                jobStreamPrefix = modelQueue.jobStream,
                jobGroup = modelQueue.jobGroup,
                jobStreams = streams.mapValues { (priority, stream) ->
                    QueueStreamResponse(stream = stream, length = lengths.getValue(priority))
                },
                jobStreamLength = lengths.values.sum(),
                workerCount = modelWorkers.size,
                activeWorkerCount = modelWorkers.count(QueueWorkerSnapshot::active),
            )
        }

        // 최상위 jobStream* 필드는 기본 모델 기준으로 채워 기존 소비자를 깨뜨리지 않는다.
        val default = models[queue.defaultModel] ?: models.values.first()
        return RedisQueueOverviewResponse(
            jobStream = default.jobStreamPrefix,
            jobStreamPrefix = default.jobStreamPrefix,
            jobStreams = default.jobStreams,
            jobStreamLength = models.values.sumOf(QueueModelOverviewResponse::jobStreamLength),
            resultStream = queue.resultStream,
            resultStreamLength = streamLength(queue.resultStream),
            workers = workers.map(::workerResponse),
            workerCount = workers.size,
            activeWorkerCount = workers.count(QueueWorkerSnapshot::active),
            models = models,
            defaultModel = queue.defaultModel,
        )
    }

    fun workerResponses(): List<WorkerResponse> = workerSnapshots().map(::workerResponse)

    private fun workerResponse(worker: QueueWorkerSnapshot): WorkerResponse = WorkerResponse(
        workerId = worker.name,
        status = if (worker.active) "active" else "stale",
        pending = worker.pending,
        idleMs = worker.idleMs,
        active = worker.active,
        model = worker.model,
    )

    private fun streamLength(stream: String): Long = streamOps.size(stream) ?: 0L

    private fun readResultsAt(offset: ReadOffset, count: Long): List<MapRecord<String, String, String>> =
        try {
            streamOps.read(
                Consumer.from(queue.resultGroup, queue.consumerName),
                StreamReadOptions.empty().count(count),
                StreamOffset.create(queue.resultStream, offset),
            ) ?: emptyList()
        } catch (exception: RedisConnectionFailureException) {
            markRedisUnavailable("read-results", exception)
            throw exception
        } catch (exception: RedisSystemException) {
            markRedisUnavailable("read-results", exception)
            throw exception
        }

    private fun createGroup(stream: String, group: String) {
        try {
            redisTemplate.execute(
                RedisCallback { connection ->
                    connection.streamCommands().xGroupCreate(
                        stream.toByteArray(Charsets.UTF_8),
                        group,
                        ReadOffset.from("0"),
                        true,
                    )
                },
            )
        } catch (exception: Exception) {
            if (exception.message?.contains("BUSYGROUP") == true || exception.cause?.message?.contains("BUSYGROUP") == true) {
                return
            }
            markRedisUnavailable("create-group", exception)
            throw exception
        }
    }

    private fun markRedisAvailable(action: String) {
        if (redisAvailable.getAndSet(true) != true) {
            log.info(
                "redis.queue.available action={} jobStreamPrefix={} resultStream={}",
                action,
                queue.jobStream,
                queue.resultStream,
            )
        }
    }

    private fun markRedisUnavailable(action: String, exception: Throwable) {
        if (redisAvailable.getAndSet(false) != false) {
            log.warn(
                "redis.queue.unavailable action={} jobStreamPrefix={} resultStream={} error={}: {}",
                action,
                queue.jobStream,
                queue.resultStream,
                exception.javaClass.simpleName,
                exception.message,
            )
        }
    }

}

/**
 * 큐에 넣은 job 의 완료를 기다리는 동기 호출자들의 대기 목록.
 * worker 의 결과가 result 스트림에 도착하면 result 리스너가 해당 future 를 완료시킨다.
 */
@Component
class PendingJobResults {
    private val futures = ConcurrentHashMap<String, CompletableFuture<RedisResultMessage>>()

    fun register(jobId: String): CompletableFuture<RedisResultMessage> =
        futures.computeIfAbsent(jobId) { CompletableFuture() }

    fun complete(jobId: String, completion: RedisResultMessage) {
        futures.remove(jobId)?.complete(completion)
    }

    fun discard(jobId: String) {
        futures.remove(jobId)
    }
}
