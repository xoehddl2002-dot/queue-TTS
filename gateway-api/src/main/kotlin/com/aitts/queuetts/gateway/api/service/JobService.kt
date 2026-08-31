package com.aitts.queuetts.gateway.api.service

import arrow.core.Either
import com.aitts.queuetts.gateway.api.error.JobError
import arrow.core.left
import arrow.core.raise.Raise
import arrow.core.raise.catch
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.raise.recover
import arrow.core.right
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.config.security.CurrentCaller
import com.aitts.queuetts.gateway.api.dto.*
import com.aitts.queuetts.gateway.api.dto.JobError as JobErrorPayload
import com.aitts.queuetts.gateway.api.infra.redis.messaging.ArtifactContent
import com.aitts.queuetts.gateway.api.infra.redis.messaging.RedisResultMessage
import com.aitts.queuetts.gateway.api.infra.redis.queue.JobPriorities
import com.aitts.queuetts.gateway.api.infra.redis.queue.PendingJobResults
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.SpeakerBlobStore
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsJobGenerationHistoryRepository
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.io.path.absolute
import kotlin.io.path.createDirectories
import kotlin.io.path.name
import kotlin.io.path.notExists

data class FileDownload(
    val path: Path,
    val mediaType: String,
    val fileName: String,
)

@Service
class JobService(
    private val properties: QueueTtsGatewayProperties,
    private val queueClient: RedisQueueClient,
    private val pendingJobResults: PendingJobResults,
    private val currentCaller: CurrentCaller,
    streamQueueClientProvider: ObjectProvider<RedisStreamQueueClient>,
    jobGenerationHistoryRepositoryProvider: ObjectProvider<TtsJobGenerationHistoryRepository>,
    /** 엔진별 voice 해석기. 맡는 것이 없으면 요청의 `voice` 가 그대로 worker 로 간다. */
    private val voiceResolvers: List<VoiceResolver> = emptyList(),
) {
    private val log = LoggerFactory.getLogger(JobService::class.java)
    private val streamQueueClient: RedisStreamQueueClient? = streamQueueClientProvider.getIfAvailable()
    private val jobGenerationHistoryRepository: TtsJobGenerationHistoryRepository? =
        jobGenerationHistoryRepositoryProvider.getIfAvailable()

    private data class GatewayJob(
        val jobId: String,
        var payload: JobPayload,
        var priority: String,
        val source: String?,
        /** 접수 시점의 API Key 호출자. 인증이 꺼진 환경에서는 null. */
        val caller: JobCaller?,
        val createdAt: OffsetDateTime,
        var updatedAt: OffsetDateTime,
        val sequence: Long,
        var state: String = WAIT_STATE,
        var batchId: String? = null,
        var workerId: String? = null,
        var startedAt: OffsetDateTime? = null,
        var finishedAt: OffsetDateTime? = null,
        var result: AudioJobResult? = null,
        var error: JobErrorPayload? = null,
        var artifactPath: String? = null,
        var artifactName: String? = null,
        var artifactMediaType: String? = null,
        var artifactSize: Long? = null,
        var historyError: String? = null,
        var version: Long = 0,
        /** Redis job 스트림에 발행된 job 이면 true (큐가 소유한 job — HTTP claim 대상에서 제외). */
        var enqueuedToRedis: Boolean = false,
        val events: MutableList<JobEventResponse> = mutableListOf(),
    )

    private val lock = Object()

    /**
     * **진행 중(wait/running) job 만** 담는 working set.
     *
     * terminal 이 되는 순간 [persistJobHistory] 가 이력 저장소로 넘기고 여기서 지운다. 끝난 job 을
     * 계속 들고 있으면 DB 가 아니라 이 맵이 사실상의 조회 소스가 되어(=DB 에서 행을 지워도 계속
     * 조회된다) 프로세스 수명만큼 메모리도 함께 커진다. 기록에 실패한 job 만 예외적으로 남는다.
     */
    private val jobs = linkedMapOf<String, GatewayJob>()

    /** DB 가 없는 in-memory 모드에서만 쓰는 이력 저장소. DB 가 있으면 채우지 않는다. */
    private val jobHistory = linkedMapOf<String, JobHistoryResponse>()

    /**
     * 최근에 끝난 job 의 상태 전이 이벤트. `GET /api/jobs/{id}/events` 의 재생용이다.
     *
     * 이벤트는 DB 에 저장하지 않으므로(잡 행에 컬럼이 없다) job 을 working set 에서 비우면 함께
     * 사라진다. 스트림을 보는 쪽은 방금 끝난 job 을 보므로, **개수를 못 박은** 이 버퍼에만 남긴다 —
     * job 레코드 자체는 여전히 매번 DB 에서 읽고, 여기서 밀려난 job 은 최종 상태 이벤트 하나로
     * 대체된다([finishedStateEvent]).
     */
    private val recentJobEvents = object : LinkedHashMap<String, List<JobEventResponse>>(16, 0.75f, false) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, List<JobEventResponse>>): Boolean =
            size > RECENT_JOB_EVENTS_CAPACITY
    }
    private val sseExecutor = Executors.newCachedThreadPool()
    private var nextSequence = 0L

    // overview 표시용 worker batchSize 캐시. batchSize 는 worker 소유값이라 styles 응답에만 실려 오므로,
    // overview 에서 styles 왕복을 throttle 로 대신 조회해 캐시한다(active worker 가 없으면 조회하지 않는다).
    // **모델별로 따로 캐시한다** — 엔진마다 배치 크기가 달라(예: supertonic 2, qwen 1) 하나로 뭉치면
    // 어느 풀이 마지막에 응답했느냐에 따라 값이 덮어써진다.
    private val cachedBatchSizes = ConcurrentHashMap<String, Int>()
    @Volatile
    private var batchSizeLastAttemptMs: Long = 0

    fun createJob(request: CreateJobRequest): Either<JobError, AcceptedJobResponse> = createJobLocked(request)

    fun failTimedOutJobs(): List<RedisResultMessage> = synchronized(lock) {
        expireQueueTimeouts(now())
    }

    private fun createJobLocked(
        request: CreateJobRequest,
    ): Either<JobError, AcceptedJobResponse> = synchronized(lock) {
        either {
            // 모델별 payload 로 바인딩하지 못한 요청(모르는 model, 다른 엔진의 파라미터)은 여기서 400.
            request.payloadError?.let { raise(JobError.InvalidRequest(it)) }

            // 참조 음성처럼 Gateway 가 채우는 필드는 요청분을 버린다.
            val rawPayload = (request.payload ?: SupertonicJobPayload()).sanitized()
            // 빈 text 를 worker 에 넘기면 worker 가 실패를 돌려줄 때까지 이미 접수된 job 이
            // 이력에 남는다. 접수/sequence 증가/큐 발행보다 먼저 거절해 생성되지 않은 요청이
            // history 로 보이는 일을 막는다. 공백만 있는 text 도 동일하게 빈 입력이다.
            ensure(rawPayload.text?.isNotBlank() == true) {
                JobError.InvalidRequest("Text is empty.")
            }
            // 길이 상한도 같은 자리에서 본다 — 접수/sequence 증가/큐 발행보다 먼저 거절해야
            // 워커가 붙잡히지 않는다. 워커는 취소를 받지 못하므로 한 번 발행되면 끝까지 합성한다.
            val textLength = rawPayload.text?.length ?: 0
            val textMaxLength = properties.job.textMaxLength
            ensure(textLength <= textMaxLength) {
                JobError.InvalidRequest("Text is too long: $textLength characters (max $textMaxLength).")
            }
            val source = rawPayload.source?.trim()?.takeIf(String::isNotBlank)
            val priority = request.priority?.let { parsePriority(it) } ?: defaultPriorityForSource(source)
            val sourcedPayload = if (source != null) rawPayload.withSource(source) else rawPayload

            // 등록되지 않은 model 은 접수 단계에서 거절한다. 발행 시점까지 미루면 어느 스트림에도
            // 안 들어간 job 이 wait 로 남아 timeout 될 때까지 아무도 처리하지 않는다.
            ensureKnownModel(sourcedPayload.model)
            val payload = resolveVoice(sourcedPayload)

            nextSequence += 1
            val created = now()
            val job = GatewayJob(
                jobId = newId("job"),
                payload = payload,
                priority = priority,
                source = source,
                caller = currentCaller.current(),
                createdAt = created,
                updatedAt = created,
                sequence = nextSequence,
            )
            recordEvent(job, "job accepted")
            jobs[job.jobId] = job
            log.info(
                "gateway.job.accepted jobId={} priority={} source={} caller={} callerRole={} textChars={}",
                job.jobId,
                job.priority,
                job.source,
                job.caller?.id,
                job.caller?.role,
                job.payload.text?.length,
            )
            // 발행에 실패하면, 요청자에게 전달되지 않은 유령 job 이 메모리에 남지 않게 생성 자체를 취소한다.
            recover({ publishToQueue(job) }) { error ->
                jobs.remove(job.jobId)
                raise(error)
            }
            acceptedResponse(job)
        }
    }

    fun listJobs(
        state: String?,
        priority: String?,
        limit: Int,
        offset: Int,
        source: String? = null,
    ): Either<JobError, JobListResponse> =
        synchronized(lock) {
            either {
            expireQueueTimeouts(now())
            val parsedState = state?.trim()?.lowercase()?.takeIf(String::isNotBlank)
            val parsedPriority = priority?.let { parsePriority(it) }
            val parsedSource = source?.trim()?.lowercase()?.takeIf(String::isNotBlank)
            val safeLimit = limit.coerceIn(1, 500)
            val safeOffset = offset.coerceAtLeast(0)
            // working set = 진행 중(wait/running) job. 끝난 job 은 전부 이력 저장소에서 온다.
            val runtimeJobIds = jobs.keys.toSet()
            val runtimeJobs = jobs.values
                .asSequence()
                .filter { parsedState == null || it.state == parsedState }
                .filter { parsedPriority == null || it.priority == parsedPriority }
                .filter { parsedSource == null || it.source?.lowercase() == parsedSource }
                .map { jobResponse(it, includePayload = true) }
                .toList()
            val historyFetchLimit = (safeOffset + safeLimit + runtimeJobs.size).coerceAtLeast(safeLimit)
            val fallbackHistory = if (jobGenerationHistoryRepository == null) {
                jobHistory.values
                    .asSequence()
                    .filter { it.job.jobId !in runtimeJobIds }
                    .filter { parsedState == null || it.job.state == parsedState }
                    .filter { parsedPriority == null || it.job.priority == parsedPriority }
                    .filter { parsedSource == null || it.job.source?.lowercase() == parsedSource }
                    .toList()
            } else {
                null
            }
            // payload 는 이미 조회된 값이므로 그대로 내려준다. 목록에서 제거하면 소비자가
            // 행마다 GET /api/jobs/{jobId} 를 다시 호출해야 해서 N+1 이 된다.
            val persistedJobs = jobGenerationHistoryRepository
                ?.listJobs(parsedState, parsedPriority, parsedSource, historyFetchLimit, runtimeJobIds)
                ?.map { it.job }
                ?: fallbackHistory.orEmpty()
                    .sortedByDescending { it.job.createdAt }
                    .take(historyFetchLimit)
                    .map { it.job }
            val persistedTotal = jobGenerationHistoryRepository
                ?.countJobs(parsedState, parsedPriority, parsedSource, runtimeJobIds)
                ?: fallbackHistory.orEmpty().size
            val filtered = (runtimeJobs + persistedJobs)
                .sortedWith(compareByDescending<JobResponse> { it.createdAt }.thenByDescending { it.updatedAt })
            JobListResponse(
                items = filtered.drop(safeOffset).take(safeLimit),
                total = runtimeJobs.size + persistedTotal,
                limit = safeLimit,
                offset = safeOffset,
                // 상태 요약은 현재 필터와 무관한 전체 기준으로 함께 내려준다(별도 overview 왕복 제거).
                counts = jobCounts(),
            )
            }
        }

    fun getJob(jobId: String): Either<JobError, JobResponse> = synchronized(lock) {
        expireQueueTimeouts(now())
        // 메모리에는 진행 중(wait/running) job 만 있다. 끝난 job 은 매번 이력 저장소에서 읽는다.
        jobs[jobId]?.let { return@synchronized jobResponse(it, includePayload = true).right() }
        val persisted = persistedRecord(jobId)?.job
            ?: return@synchronized JobError.NotFound(jobId).left()
        persisted.right()
    }

    /**
     * job 이 terminal(succeeded/failed/cancelled) 이 될 때까지 **event-driven** 으로 블로킹 대기한 뒤
     * 최종 [JobResponse] 를 돌려준다. 폴링/SSE 500ms 간격 없이, worker 결과가 Redis result 스트림에
     * 도착하는 순간 [JobResultListener] 가 완료시키는 [PendingJobResults] future 로 깨어난다.
     *
     * 레이스 방지 순서: future 등록 → 현재 상태 스냅샷 확인. 이미 terminal 이면 즉시 반환하고,
     * 아니면 future.get 으로 대기한다. (등록을 먼저 하므로 등록 직후 도착하는 완료를 놓치지 않는다.)
     */
    fun awaitJob(jobId: String, timeoutSeconds: Long? = null): Either<JobError, JobResponse> {
        // 큐 비활성(in-memory 동기) 모드에서는 대기 대상 future 가 없으므로 현재 상태를 그대로 반환한다.
        if (streamQueueClient == null || !properties.queue.enabled) {
            return getJob(jobId)
        }

        val future = pendingJobResults.register(jobId)
        val snapshot = getJob(jobId)
        val current = snapshot.getOrNull()
        if (current == null) {
            pendingJobResults.discard(jobId)
            return snapshot // Left(NotFound)
        }
        if (current.state in TERMINAL_STATES) {
            pendingJobResults.discard(jobId)
            return current.right()
        }

        val waitSeconds = (timeoutSeconds ?: properties.workerTimeoutSeconds).coerceAtLeast(1)
        return try {
            future.get(waitSeconds, TimeUnit.SECONDS)
            // 리스너는 future 완료 직전에 applyQueueEvent 로 in-memory 상태를 terminal 로 갱신해 둔다.
            getJob(jobId)
        } catch (exception: TimeoutException) {
            // 타임아웃이면 현재 상태(대개 running/queued)를 그대로 돌려준다. 호출자가 재대기/에러 처리한다.
            getJob(jobId)
        } catch (exception: InterruptedException) {
            Thread.currentThread().interrupt()
            getJob(jobId)
        } catch (exception: ExecutionException) {
            getJob(jobId)
        } finally {
            pendingJobResults.discard(jobId)
        }
    }

    fun cancelJob(jobId: String): Either<JobError, JobResponse> = synchronized(lock) {
        expireQueueTimeouts(now())
        val job = jobs[jobId]
            // 이미 끝난 job 은 메모리에 없다. 취소할 것이 없으므로 기록된 최종 상태를 그대로 돌려준다.
            ?: return@synchronized (persistedRecord(jobId)?.job?.right() ?: JobError.NotFound(jobId).left())
        if (job.state !in TERMINAL_STATES) {
            job.batchId = null
            job.workerId = null
            setJobState(job, "cancelled", "job cancelled")
        }
        jobResponse(job).right()
    }

    /**
     * Redis result 스트림으로 들어온 TTS worker 의 진행/완료 이벤트를 반영한다.
     * running 이벤트에서는 worker/batch/startedAt 을 기록하고, terminal 이벤트에서는
     * result/error/artifact 적용 후 job history 를 영속화한다.
     * worker 는 오직 큐 consumer 이름으로만 식별된다.
     */
    fun applyQueueEvent(completion: RedisResultMessage): Unit = synchronized(lock) {
        val jobId = completion.jobId
        val workerId = completion.workerId
        val job = jobs[jobId]
        if (job == null) {
            // 끝난 job 은 working set 에서 비워지므로 늦게 도착한 중복 결과가 여기로 온다.
            // 정말 모르는 job 과 구분해서 로그를 남긴다(전자는 정상, 후자는 조사 대상).
            if (persistedRecord(jobId) != null) {
                log.debug("Ignoring late queue result for finished job {} from worker {}", jobId, workerId)
            } else {
                log.warn("Received queue result for unknown job {} from worker {}", jobId, workerId)
            }
            return@synchronized
        }
        if (job.state == "cancelled" || job.state in TERMINAL_STATES) {
            return@synchronized
        }
        var state = normalizeCompletionState(completion.state, completion.error)
        if (state == RUNNING_STATE) {
            if (job.state == RUNNING_STATE) {
                return@synchronized
            }
            job.workerId = workerId
            job.batchId = completion.batchId
            job.startedAt = completion.startedAt ?: now()
            setJobState(job, RUNNING_STATE, "started via queue by $workerId")
            return@synchronized
        }
        job.workerId = workerId
        job.batchId = completion.batchId
        job.startedAt = job.startedAt ?: completion.startedAt ?: now()
        job.result = completion.result
        job.error = completion.error
        if (state == "succeeded") {
            either { artifactFromContent(completion.artifact) }
                .onRight { artifact -> artifact?.let { storeArtifact(job, it) } }
                .onLeft { error ->
                    state = "failed"
                    job.error = JobErrorPayload(code = error.code, message = error.message)
                }
        }
        setJobState(job, state, "completed via queue by $workerId")
        if (job.state in TERMINAL_STATES) {
            persistJobHistory(job)
        }
    }

    /** wait 상태의 job 을 Redis job 스트림에 재발행한다 (메시지 유실 복구용). */
    fun requeueJob(jobId: String): Either<JobError, JobResponse> = synchronized(lock) {
        either {
            // 끝난 job 은 메모리에 없다. 재발행 대상이 아니라는 사실을 NotFound 가 아니라
            // 기록된 상태로 알려 준다.
            val job = jobs[jobId] ?: run {
                val finished = persistedRecord(jobId)?.job ?: raise(JobError.NotFound(jobId))
                raise(JobError.InvalidState("cannot requeue job $jobId in state ${finished.state}"))
            }
            if (job.state != WAIT_STATE) {
                raise(JobError.InvalidState("cannot requeue job $jobId in state ${job.state}"))
            }
            // style 은 버전 이력이 없으므로 requeue 시점의 현재 참조로 다시 해석한다.
            job.payload = resolveVoice(job.payload)
            publishToQueue(job)
            jobResponse(job)
        }
    }

    /**
     * 등록되지 않은 model 이면 400.
     *
     * model 을 **명시한 요청만** 검사한다 — 생략한 요청은 기본 모델로 흐르므로 검사할 것이 없고,
     * 기존 호출자 전부가 이 경로를 탄다. 큐가 꺼져 있으면(in-memory 모드) 검사하지 않는다.
     */
    private fun Raise<JobError>.ensureKnownModel(model: String?) {
        val requested = model?.trim()?.takeIf(String::isNotEmpty) ?: return
        val queueClient = streamQueueClient?.takeIf(RedisStreamQueueClient::enabled) ?: return
        ensure(queueClient.canonicalModel(requested) != null) {
            JobError.InvalidRequest(
                "unknown model '$requested'. Known models: " +
                        queueClient.modelNames.sorted().joinToString(", ")
            )
        }
    }

    /**
     * 공개 `voice` 이름을 그 엔진의 등록소에서 해석한다.
     *
     * 맡는 [VoiceResolver] 가 없으면 해석할 것이 없다는 뜻이라 payload 를 그대로 흘려보낸다
     * (supertonic 처럼 voice 카탈로그를 worker 가 소유하는 엔진).
     */
    private fun Raise<JobError>.resolveVoice(payload: JobPayload): JobPayload {
        val targetModel = properties.queue.canonicalModel(payload.model) ?: return payload
        val resolver = voiceResolvers.firstOrNull { it.handles(targetModel) } ?: return payload
        return resolver.resolve(payload, targetModel).bind()
    }

    /** 큐가 꺼져 있으면 아무 것도 하지 않는다 (in-memory 모드). */
    private fun Raise<JobError>.publishToQueue(job: GatewayJob) {
        val queueClient = streamQueueClient?.takeIf(RedisStreamQueueClient::enabled) ?: return
        catch({
            // 발행 대상 stream 은 payload 의 model 이 정한다 (없으면 queuetts.queue.default-model).
            // requeue 도 저장된 payload 를 그대로 넘기므로 재발행이 같은 풀로 간다.
            queueClient.publishJob(
                jobId = job.jobId,
                type = TTS_JOB_TYPE,
                payload = job.payload,
                priority = job.priority,
                source = job.source,
            )
        }) { e: Exception ->
            log.warn("Failed to publish job {} to redis stream: {}", job.jobId, e.message, e)
            recordEvent(job, "redis publish failed: ${e.message}")
            raise(JobError.QueueError(e.message ?: "failed to publish job ${job.jobId} to redis stream"))
        }
        job.enqueuedToRedis = true
        recordEvent(job, "job published to redis stream")
        log.info(
            "gateway.job.published jobId={} priority={} source={} type={} model={} elapsedMs={}",
            job.jobId,
            job.priority,
            job.source,
            TTS_JOB_TYPE,
            job.payload.model ?: properties.queue.defaultModel,
            elapsedMsSince(job.createdAt),
        )
    }

    fun jobDownload(jobId: String): Either<JobError, FileDownload> = synchronized(lock) {
        either {
            jobs[jobId]?.let { job ->
                return@either downloadFrom(job.artifactPath, job.artifactMediaType, job.artifactName, "job result")
            }
            // 다운로드 대상은 언제나 끝난 job 이라 사실상 항상 이 경로를 탄다.
            val record = persistedRecord(jobId) ?: raise(JobError.NotFound(jobId))
            downloadFrom(record.artifactPath, record.artifactMediaType, record.artifactName, "job result")
        }
    }

    fun overview(): JobGatewayOverviewResponse {
        // worker 정보는 설정된 URL 이 아니라 Redis job 스트림의 consumer group 에서 가져온다.
        val queueInfo = streamQueueClient?.takeIf(RedisStreamQueueClient::enabled)?.overviewInfo()
        // batchSize 는 worker 가 styles 응답에 실어 보내는 값이라, worker 가 있으면 throttle 로 조회해 노출한다.
        val activeModels = queueInfo?.workers.orEmpty()
            .filter { it.active == true }
            .mapNotNull(WorkerResponse::model)
            .distinct()
        val batchSizes = workerBatchSizes(activeModels)
        return JobGatewayOverviewResponse(
            workers = queueInfo?.workers.orEmpty(),
            workerCount = queueInfo?.workerCount ?: 0,
            activeWorkerCount = queueInfo?.activeWorkerCount ?: 0,
            redisQueue = queueInfo,
            jobWorkers = emptyList(),
            batchSize = batchSizes[properties.queue.defaultModel] ?: batchSizes.values.firstOrNull(),
            batchSizes = batchSizes.ifEmpty { null },
        )
    }

    /** GET /api/styles 등 styles 응답을 이미 받은 곳에서 overview 캐시를 갱신한다(중복 왕복 감소). */
    fun updateWorkerBatchSize(model: String, batchSize: Int?) {
        if (batchSize != null) {
            cachedBatchSizes[model] = batchSize
            batchSizeLastAttemptMs = System.currentTimeMillis()
        }
    }

    /**
     * overview 표시용 모델별 batchSize. throttle 이 지났고 active worker 가 있는 모델만
     * styles 왕복으로 갱신한다.
     */
    private fun workerBatchSizes(activeModels: List<String>): Map<String, Int> {
        val now = System.currentTimeMillis()
        if (activeModels.isNotEmpty() && now - batchSizeLastAttemptMs > WORKER_BATCH_SIZE_TTL_MS) {
            batchSizeLastAttemptMs = now
            activeModels.forEach { model ->
                runCatching { queueClient.requestStyles(source = "overview", model = model).data.batchSize }
                    .onSuccess { if (it != null) cachedBatchSizes[model] = it }
                    .onFailure { log.debug("overview batchSize refresh failed for {}: {}", model, it.message) }
            }
        }
        return cachedBatchSizes.toMap()
    }

    fun events(jobId: String, lastEventId: Long): Either<JobError, SseEmitter> {
        val nextEventId = lastEventId.coerceAtLeast(0)
        val running = synchronized(lock) {
            if (jobs.containsKey(jobId)) {
                true
            } else {
                // 끝난 job. 이력에도 없으면 404.
                if (!recentJobEvents.containsKey(jobId) && persistedRecord(jobId) == null) {
                    return JobError.NotFound(jobId).left()
                }
                false
            }
        }
        val emitter = SseEmitter(30_000)
        if (!running) {
            // 이미 끝난 job 은 더 올 이벤트가 없다. 백그라운드 스레드를 띄우지 않고 그 자리에서
            // 남은 이벤트를 흘려보내고 닫는다 (응답을 두 스레드가 함께 건드리지 않는다).
            try {
                for (event in finishedJobEvents(jobId, nextEventId)) {
                    emitter.send(SseEmitter.event().id(event.id.toString()).name("state").data(event))
                }
                emitter.complete()
            } catch (exception: Exception) {
                emitter.completeWithError(exception)
            }
            return emitter.right()
        }
        sseExecutor.submit {
            var sentEventId = nextEventId
            try {
                while (true) {
                    // job 이 끝나면 working set 에서 비워지므로 snapshot 이 null 이 된다.
                    // 남은 이벤트는 이력 쪽에서 마저 흘려보내고 스트림을 닫는다.
                    val snapshot = synchronized(lock) {
                        jobs[jobId]?.let { job ->
                            job.events.filter { it.id > sentEventId } to (job.state in TERMINAL_STATES)
                        }
                    } ?: (finishedJobEvents(jobId, sentEventId) to true)
                    for (event in snapshot.first) {
                        sentEventId = event.id
                        emitter.send(SseEmitter.event().id(event.id.toString()).name("state").data(event))
                    }
                    if (snapshot.second) {
                        break
                    }
                    Thread.sleep(500)
                }
                emitter.complete()
            } catch (exception: Exception) {
                emitter.completeWithError(exception)
            }
        }
        return emitter.right()
    }

    /**
     * 끝난 job 의, 클라이언트가 아직 못 본 이벤트.
     *
     * 재생 버퍼에 남아 있으면 상태 전이를 그대로 돌려주고, 밀려났으면 이력의 최종 상태를 이벤트
     * 하나로 만들어 준다(이벤트는 DB 에 저장하지 않는다).
     */
    private fun finishedJobEvents(jobId: String, lastEventId: Long): List<JobEventResponse> = synchronized(lock) {
        recentJobEvents[jobId]?.let { events -> return events.filter { it.id > lastEventId } }
        val job = persistedRecord(jobId)?.job ?: return emptyList()
        listOf(
            JobEventResponse(
                // 클라이언트가 보낸 lastEventId 가 그대로 더해지므로 오버플로를 막는다.
                id = lastEventId.coerceAtMost(Long.MAX_VALUE - 1) + 1,
                at = job.finishedAt ?: job.updatedAt,
                state = job.state,
                message = "job finished",
                jobId = jobId,
            ),
        )
    }

    @PreDestroy
    fun shutdown() {
        sseExecutor.shutdownNow()
    }

    /**
     * terminal job 을 이력 저장소로 넘기고 **working set 에서 비운다.**
     *
     * 끝난 job 의 소유자는 DB 다. 메모리에 남겨 두면 (1) `listJobs` 가 그 job 을 DB 조회에서
     * 제외해 버려 DB 에서 행을 지워도 계속 조회되고, (2) 프로세스가 살아 있는 동안 접수한 job 이
     * 하나도 해제되지 않아 working set 이 무한히 커진다. 그래서 기록이 끝나는 즉시 지운다.
     *
     * 기록에 실패하면 지우지 않는다 — DB 에도 메모리에도 없는 job 이 되어 흔적 없이 사라진다.
     * 대신 사유를 [GatewayJob.historyError] 로 남겨 응답에 실어 보낸다.
     */
    private fun persistJobHistory(job: GatewayJob) {
        val repository = jobGenerationHistoryRepository
        if (repository == null) {
            // DB 가 없는 in-memory 모드(테스트/로컬)에서는 이 맵이 유일한 이력 저장소다.
            jobHistory[job.jobId] = jobHistoryRecord(job)
            releaseFinishedJob(job)
            return
        }
        runCatching { repository.upsert(job.toPersisted()) }
            .onSuccess {
                job.historyError = null
                releaseFinishedJob(job)
            }
            .onFailure { exception ->
                // 예외를 밖으로 던지면 결과 리스너가 대기자를 깨우는 단계까지 못 가서 호출자가
                // timeout 까지 붙잡힌다. 여기서 삼키고 job 은 메모리에 남겨 목록에 계속 보이게 한다.
                job.historyError = exception.message ?: exception.javaClass.simpleName
                log.error(
                    "gateway.job.history.persist_failed jobId={} state={}: {}",
                    job.jobId,
                    job.state,
                    exception.message,
                    exception,
                )
            }
    }

    /** 이력에 넘긴 job 을 working set 에서 놓아준다. 이벤트만 SSE 재생용 버퍼로 옮긴다. */
    private fun releaseFinishedJob(job: GatewayJob) {
        recentJobEvents[job.jobId] = job.events.toList()
        jobs.remove(job.jobId)
    }

    /** 메모리에서 비워진 terminal job 의 기록. DB 가 있으면 DB, 없으면 in-memory 이력 맵. */
    private fun persistedRecord(jobId: String): JobHistoryResponse? =
        jobGenerationHistoryRepository?.findByJobId(jobId) ?: jobHistory[jobId]

    private fun GatewayJob.toPersisted(): PersistedGatewayJob = PersistedGatewayJob(
        jobId = jobId,
        state = state,
        priority = priority,
        caller = caller,
        workerId = workerId,
        batchId = batchId,
        payload = payload,
        result = result,
        error = error,
        artifactPath = artifactPath,
        artifactName = artifactName,
        artifactMediaType = artifactMediaType,
        artifactSize = artifactSize,
        downloadUrl = artifactPath?.let { "/api/jobs/$jobId/download" },
        sequence = sequence,
        leaseUntil = null,
        batchingToken = null,
        batchingUntil = null,
        createdAt = createdAt,
        startedAt = startedAt,
        finishedAt = finishedAt,
        version = version,
        events = events.toList(),
    )

    private fun jobResponse(job: GatewayJob, includePayload: Boolean = false): JobResponse = JobResponse(
        jobId = job.jobId,
        state = job.state,
        priority = job.priority,
        source = job.source,
        caller = job.caller,
        createdAt = job.createdAt,
        updatedAt = job.updatedAt,
        startedAt = job.startedAt,
        finishedAt = job.finishedAt,
        workerId = job.workerId,
        batchId = job.batchId,
        result = job.result,
        error = job.error,
        artifact = artifactResponse(job),
        downloadUrl = job.artifactPath?.let { "/api/jobs/${job.jobId}/download" },
        statusUrl = "/api/jobs/${job.jobId}",
        historyError = job.historyError,
        version = job.version,
        payload = job.payload.takeIf { includePayload },
    )

    private fun artifactResponse(job: GatewayJob): JobArtifactResponse? =
        job.artifactPath?.let {
            JobArtifactResponse(
                path = it,
                fileName = job.artifactName,
                mediaType = job.artifactMediaType,
                size = job.artifactSize,
            )
        }

    private fun jobHistoryRecord(job: GatewayJob): JobHistoryResponse = JobHistoryResponse(
        job = jobResponse(job, includePayload = true),
        artifactPath = job.artifactPath,
        artifactName = job.artifactName,
        artifactMediaType = job.artifactMediaType,
        artifactSize = job.artifactSize,
    )

    /** health 프로브용 경량 카운트. Redis 를 조회하지 않고 working set 과 이력 집계만 읽는다. */
    fun healthCounts(): JobCountsResponse = jobCounts()

    // lock 은 재진입 가능하므로 이미 lock 을 쥔 listJobs 에서 불러도 안전하다. health 프로브처럼
    // lock 밖에서 들어오는 호출이 집계 중인 맵을 다른 스레드가 바꾸는 것을 막는다.
    private fun jobCounts(): JobCountsResponse = synchronized(lock) { countJobsLocked() }

    private fun countJobsLocked(): JobCountsResponse {
        val liveByState = jobs.values.groupingBy { it.state }.eachCount()
        val wait = liveByState[WAIT_STATE] ?: 0
        val running = liveByState[RUNNING_STATE] ?: 0
        // wait/running 은 아직 DB 에 없는 진행 중 상태라 인메모리 working set 에서 센다.
        // terminal(succeeded/failed/cancelled)은 프로세스 재시작/멀티 인스턴스와 무관하게
        // 유지되도록 DB 누적에서 집계한다. DB 가 없으면(in-memory 모드) 이력 맵으로 폴백한다 —
        // terminal job 은 working set 에서 비워지므로 liveByState 에는 남아 있지 않다.
        val terminalByState = jobGenerationHistoryRepository?.countByState()
            ?.filterKeys { it in TERMINAL_STATES }
            ?: jobHistory.values
                .groupingBy { it.job.state }
                .eachCount()
                .filterKeys { it in TERMINAL_STATES }
        val byState = buildMap {
            if (wait > 0) put(WAIT_STATE, wait)
            if (running > 0) put(RUNNING_STATE, running)
            terminalByState.forEach { (state, count) -> if (count > 0) put(state, count) }
        }
        val terminal = terminalByState.values.sum()
        return JobCountsResponse(
            total = wait + running + terminal,
            byState = byState,
            // priority 분포는 진행 중(wait/running) 큐 부하 파악용이라 live working set 기준.
            byPriority = jobs.values
                .filter { it.state == WAIT_STATE || it.state == RUNNING_STATE }
                .groupingBy { it.priority }
                .eachCount(),
            wait = wait,
            running = running,
            terminal = terminal,
        )
    }

    private fun setJobState(job: GatewayJob, state: String, message: String) {
        val current = now()
        job.state = state
        job.updatedAt = current
        job.version += 1
        if (state in TERMINAL_STATES) {
            job.finishedAt = job.finishedAt ?: current
        }
        recordEvent(job, message)
        log.info(
            "gateway.job.state jobId={} state={} priority={} source={} workerId={} batchId={} elapsedMs={} message={}",
            job.jobId,
            job.state,
            job.priority,
            job.source,
            job.workerId,
            job.batchId,
            elapsedMsSince(job.createdAt),
            message,
        )
    }

    private fun recordEvent(job: GatewayJob, message: String) {
        job.events += JobEventResponse(
            id = (job.events.size + 1).toLong(),
            at = now(),
            state = job.state,
            message = message,
            jobId = job.jobId,
        )
    }

    private fun expireQueueTimeouts(current: OffsetDateTime): List<RedisResultMessage> {
        val timeoutSeconds = properties.workerTimeoutSeconds.coerceAtLeast(0)
        val timedOutAt = current.minus(Duration.ofSeconds(timeoutSeconds))
        return jobs.values
            // 데드라인은 createdAt(발행 시각) 기준의 절대 시간이다. updatedAt(마지막 상태 변경)이 아니므로
            // 중간에 상태가 바뀌든 worker 가 처리 중이든 상관없이, 발행 후 timeoutSeconds 안에 결과 스트림으로
            // 완료가 도착하지 않으면 실패시킨다. worker 존재/상태는 여기서 보지 않는다.
            .filter { it.enqueuedToRedis && it.state !in TERMINAL_STATES && !it.createdAt.isAfter(timedOutAt) }
            .map(::failQueueTimeout)
    }

    private fun failQueueTimeout(job: GatewayJob): RedisResultMessage {
        val timeoutSeconds = properties.workerTimeoutSeconds.coerceAtLeast(0)
        val message = "TTS production failed because no worker result arrived within ${timeoutSeconds}s."
        job.result = null
        job.error = JobErrorPayload(code = "worker_timeout", message = message)
        setJobState(job, "failed", "queue timeout after ${timeoutSeconds}s")
        persistJobHistory(job)
        return RedisResultMessage(
            jobId = job.jobId,
            workerId = job.workerId ?: "gateway-timeout",
            batchId = job.batchId,
            state = "failed",
            error = job.error,
        )
    }

    private fun Raise<JobError>.artifactFromContent(artifactContent: ArtifactContent?): Artifact? {
        val contentBase64 = artifactContent?.contentBase64 ?: return null
        val content = catch({ Base64.getDecoder().decode(contentBase64) }) { _: IllegalArgumentException ->
            raise(JobError.ArtifactError("invalid artifact contentBase64"))
        }
        val maxBytes = properties.job.artifactMaxBytes
        ensure(content.size <= maxBytes) {
            JobError.ArtifactError("artifact is too large: ${content.size} bytes (max $maxBytes)")
        }
        return Artifact(
            fileName = safeFileName(artifactContent.fileName, "result.bin"),
            mediaType = artifactContent.mediaType ?: "application/octet-stream",
            content = content,
        )
    }

    private fun storeArtifact(job: GatewayJob, artifact: Artifact) {
        val targetDir = properties.job.artifactDir.resolve(job.jobId).absolute()
        if (targetDir.notExists()) {
            targetDir.createDirectories()
        }
        val target = targetDir.resolve(artifact.fileName)
        Files.write(target, artifact.content)
        job.artifactPath = target.toString()
        job.artifactName = artifact.fileName
        job.artifactMediaType = artifact.mediaType
        job.artifactSize = artifact.content.size.toLong()
    }

    private fun Raise<JobError>.downloadFrom(
        artifactPath: String?,
        mediaType: String?,
        artifactName: String?,
        label: String
    ): FileDownload {
        ensure(!artifactPath.isNullOrBlank()) { JobError.ArtifactError("$label file is not available") }
        val path = Path.of(artifactPath)
        ensure(Files.isRegularFile(path)) { JobError.ArtifactError("$label file is missing") }
        return FileDownload(
            path = path,
            mediaType = mediaType ?: "application/octet-stream",
            fileName = artifactName ?: path.name,
        )
    }

    private fun normalizeCompletionState(value: String?, error: JobErrorPayload?): String {
        val raw = value?.lowercase() ?: "succeeded"
        val normalized = when (raw) {
            "ok", "done", "completed", "success" -> "succeeded"
            else -> raw
        }
        return when {
            normalized in setOf(RUNNING_STATE, "succeeded", "failed") -> normalized
            error != null -> "failed"
            else -> "succeeded"
        }
    }

    private fun Raise<JobError>.parsePriority(value: String): String {
        val priority = value.trim().lowercase()
        ensure(priority in PRIORITIES) { JobError.InvalidRequest("priority must be one of ${PRIORITIES.joinToString(", ")}") }
        return priority
    }

    private fun defaultPriorityForSource(source: String?): String {
        val clean = source?.lowercase().orEmpty()
        return when {
            HIGH_PRIORITY_SOURCE_TOKENS.any(clean::contains) -> "high"
            LOW_PRIORITY_SOURCE_TOKENS.any(clean::contains) -> "low"
            else -> "normal"
        }
    }

    private fun acceptedResponse(job: GatewayJob): AcceptedJobResponse = AcceptedJobResponse(
        status = "accepted",
        jobId = job.jobId,
        state = job.state,
        priority = job.priority,
        source = job.source,
        statusUrl = "/api/jobs/${job.jobId}",
    )

    private fun jobError(message: String): JobErrorPayload = JobErrorPayload(message = message)

    private fun safeFileName(value: String?, default: String): String {
        val clean = value?.trim()?.replace(Regex("""[\\/:*?"<>|]"""), "_")?.takeIf(String::isNotBlank)
        return clean ?: default
    }

    private fun newId(prefix: String): String = "${prefix}_${UUID.randomUUID().toString().replace("-", "").take(12)}"

    private fun elapsedMsSince(startedAt: OffsetDateTime): Long =
        Duration.between(startedAt, now()).toMillis().coerceAtLeast(0)

    private data class Artifact(
        val fileName: String,
        val mediaType: String,
        val content: ByteArray,
    )

    private companion object {
        val PRIORITIES = JobPriorities.ordered
        const val TTS_JOB_TYPE = "tts"
        /** overview 의 worker batchSize 조회(styles 왕복) 최소 간격. */
        const val WORKER_BATCH_SIZE_TTL_MS = 30_000L
        /** SSE 재생용으로 남겨 두는, 최근에 끝난 job 의 수. */
        const val RECENT_JOB_EVENTS_CAPACITY = 500
        const val WAIT_STATE = "wait"
        const val RUNNING_STATE = "running"
        val TERMINAL_STATES = setOf("succeeded", "failed", "cancelled")
        val HIGH_PRIORITY_SOURCE_TOKENS = listOf("user", "customer", "member", "public", "front", "end_user", "client")
        val LOW_PRIORITY_SOURCE_TOKENS = listOf("counselor", "counsellor", "agent", "consult", "staff", "admin")

        fun now(): OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
    }
}

/**
 * 요청의 공개 `voice` 이름을 worker 가 쓸 참조로 바꾼다.
 *
 * **voice 를 어디서 찾는지가 엔진마다 다르다** — supertonic 은 worker 가 Redis 에 올려 둔 카탈로그를
 * 그대로 쓰므로 Gateway 가 해석할 것이 없고, qwen 은 Gateway 소유 speaker registry(Postgres + Redis
 * blob)를 거쳐야 한다. 생성 파라미터 차이를 모델별 payload 타입이 담당한다면, 이건 나머지 절반의
 * 차이다.
 *
 * 새 엔진이 자기 등록소를 들고 오면 이 인터페이스 구현을 하나 추가한다. 맡는 resolver 가 없으면
 * 요청의 `voice` 가 그대로 worker 로 간다.
 */
interface VoiceResolver {
    /** 이 resolver 가 맡는 모델인지. 인자는 설정에 있는 정식 모델 키다. */
    fun handles(canonicalModel: String): Boolean

    /** 해석 결과를 채운 payload. 해석에 실패하면 400 계열 [JobError] 를 왼쪽으로 돌려준다. */
    fun resolve(payload: JobPayload, canonicalModel: String): Either<JobError, JobPayload>
}

/**
 * Gateway 소유 speaker registry 를 쓰는 엔진(기본값 qwen)의 resolver.
 *
 * `queuetts.queue.voice-model` 이 가리키는 모델에만 걸린다. 참조 오디오는 Redis blob 에 있고 worker 가
 * read-through 하므로, 여기서는 **어디를 읽으라는 지시**(name·digest·blob 키)와 캐시 미스 때 prompt 를
 * 다시 만들 재료(mode·ref_text)만 실어 보낸다.
 *
 * DB 테이블과 Redis namespace의 `style` 문자열은 저장 호환성 때문에 남아 있지만 식별자는 registry
 * 기본키와 같은 `(model, name)`이다. **blob 키 문자열은 `SpeakerBlobStore`가 쓰는 형식과 반드시
 * 같아야 한다.**
 */
@Component
class SpeakerRegistryVoiceResolver(
    private val properties: QueueTtsGatewayProperties,
    speakerRepositoryProvider: ObjectProvider<TtsSpeakerRepository>,
    private val blobStore: SpeakerBlobStore,
    private val objectMapper: ObjectMapper,
) : VoiceResolver {
    private val speakerRepository: TtsSpeakerRepository? = speakerRepositoryProvider.getIfAvailable()

    override fun handles(canonicalModel: String): Boolean =
        canonicalModel == properties.queue.canonicalModel(properties.queue.voiceModel)

    override fun resolve(payload: JobPayload, canonicalModel: String): Either<JobError, JobPayload> = either {
        // 이 resolver 가 걸리는 모델은 곧 qwen payload 로 바인딩되는 모델이다. 설정에서 voice-model
        // 을 payload 타입이 없는 엔진으로 바꾸면 여기서 걸린다.
        val qwenPayload = payload as? QwenJobPayload
            ?: raise(
                JobError.InvalidRequest(
                    "model '$canonicalModel' is configured as the clone-voice model but its payload " +
                            "type does not support a speaker registry"
                )
            )
        val repository = speakerRepository
            ?: raise(JobError.InvalidRequest("speaker registry is unavailable because the database is disabled"))
        val requestedVoice = qwenPayload.voice?.trim()?.takeIf(String::isNotEmpty)
            ?: raise(JobError.InvalidRequest("voice is required for model '$canonicalModel'"))
        val speaker = repository.findByName(canonicalModel, requestedVoice)
            ?: raise(JobError.InvalidRequest("unknown speaker '$requestedVoice' for model '$canonicalModel'"))

        val defaults = runCatching { readDefaults(speaker.defaultParams) }
            .getOrElse {
                raise(JobError.InvalidRequest("speaker '$requestedVoice' has invalid default_params: ${it.message}"))
            }

        // blob 은 파생 거울이라 Redis 를 비웠거나 등록 직후 mirror 가 실패했을 수 있다. DB 오디오가
        // 원본이므로 첫 합성에서 보충한다.
        if (!blobStore.exists(speaker.name)) {
            val audio = repository.findAudio(canonicalModel, speaker.name)
                ?: raise(JobError.InvalidRequest("reference audio for speaker '${speaker.name}' is missing"))
            runCatching { blobStore.put(speaker.name, audio) }
                .getOrElse {
                    raise(JobError.InvalidRequest("reference audio for speaker '${speaker.name}' could not be mirrored: ${it.message}"))
                }
        }

        qwenPayload.withDefaults(defaults).copy(
            // 요청도 worker 캐시도 등록된 이름 하나만 식별자로 쓴다.
            voice = speaker.name,
            lang = qwenPayload.lang ?: speaker.language,
            speakerName = speaker.name,
            referenceDigest = speaker.referenceDigest,
            speakerBlobKey = blobStore.keyFor(speaker.name),
            speakerMode = speaker.mode,
            speakerRefText = speaker.refText,
        )
    }

    /**
     * 행에 저장된 `default_params` JSON → [QwenSpeakerParams].
     *
     * 허용 키는 등록 시점에 이미 검사했지만, 그 뒤 규격에서 빠진 키가 행에 남아 있을 수 있어
     * **읽을 때는 관대하다** — 오래된 기본값 하나 때문에 합성 요청이 실패하면 곤란하다.
     */
    private fun readDefaults(raw: String?): QwenSpeakerParams =
        raw?.takeIf(String::isNotBlank)
            ?.let {
                objectMapper.readerFor(QwenSpeakerParams::class.java)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .readValue<QwenSpeakerParams>(it)
            }
            ?: QwenSpeakerParams()
}
