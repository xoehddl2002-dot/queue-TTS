package com.aitts.queuetts.gateway.api.service

import arrow.core.Either
import arrow.core.raise.catch
import arrow.core.raise.either
import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.dto.HealthResponse
import com.aitts.queuetts.gateway.api.dto.StyleCatalogResponse
import com.aitts.queuetts.gateway.api.dto.StyleInfo
import com.aitts.queuetts.gateway.api.error.GatewayError
import com.aitts.queuetts.gateway.api.infra.db.repository.TtsSpeakerRepository
import com.aitts.queuetts.gateway.api.infra.redis.queue.QueueStylesResult
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisQueueClient
import com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

@Service
class GatewayService(
    private val streamQueueClient: RedisStreamQueueClient,
    private val queueClient: RedisQueueClient,
    private val jobService: JobService,
    private val properties: QueueTtsGatewayProperties,
    speakerRepositoryProvider: ObjectProvider<TtsSpeakerRepository>,
) {
    private val log = LoggerFactory.getLogger(GatewayService::class.java)
    private val speakerRepository: TtsSpeakerRepository? = speakerRepositoryProvider.getIfAvailable()

    fun health(): Either<GatewayError, HealthResponse> =
        either{
            catch({
                val counts = jobService.healthCounts()
                HealthResponse(
                    ok = true,
                    status = "ok",
                    gateway = "QueueTts Gateway",
                    activeJobs = counts.wait + counts.running,
                    jobsTotal = counts.total,
                    // 모든 엔진 풀의 worker 를 합쳐 보고한다 (각 항목이 model 로 구분된다).
                    // 한 풀만 보면 다른 엔진이 전부 죽어도 ok 가 나가므로 반드시 전부 포함해야 한다.
                    workers = streamQueueClient.workerResponsesForHealth(),
                )
            }){e->
                raise(GatewayError.WorkerError(e.message ?: "worker connection error"))
            }
        }

    /**
     * 사용 가능한 보이스 목록.
     *
     * [model] 을 주면 그 엔진 풀만, 생략하면 모든 풀을 합친다. Qwen clone speaker 는 Gateway 가
     * 소유하므로 DB 에서 읽고, 나머지 풀만 worker 에 병렬로 묻는다. 순차로 돌면 죽은 풀 하나마다
     * control-request timeout(기본 30초)만큼 응답이 밀리기 때문이다.
     *
     * 일부 풀이 실패해도 나머지 보이스는 그대로 돌려주고, 실패 사실은 `errors` 에 남긴다.
     * 전부 실패했을 때만 오류로 처리한다.
     */
    fun styles(model: String? = null): Either<GatewayError, StyleCatalogResponse> =
        either {
            val requestedModel = model?.let { requested ->
                streamQueueClient.canonicalModel(requested)
                    ?: raise(
                        GatewayError.InvalidRequest(
                            "unknown model '$requested'. Known models: " +
                                    streamQueueClient.modelNames.sorted().joinToString(", ")
                        )
                    )
            }
            val cloneModel = streamQueueClient.canonicalModel(properties.queue.voiceModel)
            val readsCloneRegistry = speakerRepository != null && cloneModel != null &&
                    (requestedModel == null || requestedModel == cloneModel)

            val targets = if (requestedModel == null) {
                // worker 가 없는 풀에 제어 요청을 보내면 응답이 없어 control-request timeout(기본 30초)
                // 만큼 호출자가 붙잡힌다. 살아있는 풀만 고르되 Qwen 은 DB 에서 읽으므로 제외한다.
                val active = runCatching { streamQueueClient.activeModels() }.getOrDefault(emptySet())
                streamQueueClient.modelNames.filter { it in active && (!readsCloneRegistry || it != cloneModel) }
                    // 아무 풀도 살아있지 않으면 기본 모델로 시도해 실패 사유를 그대로 노출한다
                    // (liveness 판정이 틀렸을 때 조용히 빈 목록을 주는 것보다 낫다).
                    .ifEmpty {
                        if (readsCloneRegistry) emptyList()
                        else listOfNotNull(streamQueueClient.canonicalModel(null))
                    }
            } else {
                if (readsCloneRegistry) emptyList() else listOf(requestedModel)
            }

            val outcomes = requestStylesFor(targets)
            val succeeded = outcomes.mapNotNull { (name, outcome) -> outcome.getOrNull()?.let { name to it } }
            val errors = outcomes.mapNotNull { (name, outcome) ->
                outcome.exceptionOrNull()?.let { name to (it.message ?: it.javaClass.simpleName) }
            }.toMap()

            val cloneSpeakers = if (readsCloneRegistry) {
                speakerRepository.list(cloneModel).map { speaker ->
                    StyleInfo(name = speaker.name, kind = "custom", model = cloneModel)
                }
            } else {
                emptyList()
            }

            if (succeeded.isEmpty() && !readsCloneRegistry) {
                raise(
                    GatewayError.WorkerError(
                        errors.entries.joinToString("; ") { "${it.key}: ${it.value}" }
                            .ifEmpty { "no TTS worker responded" }
                    )
                )
            }

            val batchSizes = succeeded.mapNotNull { (name, result) ->
                result.data.batchSize?.let { name to it }
            }.toMap()
            batchSizes.forEach { (name, size) -> jobService.updateWorkerBatchSize(name, size) }

            StyleCatalogResponse(
                styles = succeeded.flatMap { (name, result) ->
                    result.data.styles.map { style ->
                        StyleInfo(name = style.name, kind = style.kind, path = style.path, model = name)
                    }
                } + cloneSpeakers,
                batchSize = batchSizes[streamQueueClient.canonicalModel(null)] ?: batchSizes.values.firstOrNull(),
                batchSizes = batchSizes.ifEmpty { null },
                models = (succeeded.map { it.first } + listOfNotNull(cloneModel.takeIf { readsCloneRegistry })).distinct(),
                errors = errors.ifEmpty { null },
            )
        }

    /** 모델별 styles 요청을 병렬로 보내고 결과/실패를 그대로 모은다. */
    private fun requestStylesFor(models: List<String>): List<Pair<String, Result<QueueStylesResult>>> {
        if (models.size == 1) {
            val name = models.first()
            return listOf(name to runCatching { queueClient.requestStyles(source = "styles", model = name) })
        }
        return models.map { name ->
            name to CompletableFuture.supplyAsync {
                runCatching { queueClient.requestStyles(source = "styles", model = name) }
            }
        }.map { (name, future) ->
            val outcome = runCatching { future.join() }.getOrElse { Result.failure(it) }
            outcome.onFailure { log.warn("styles request failed for model {}: {}", name, it.message) }
            name to outcome
        }
    }
}
