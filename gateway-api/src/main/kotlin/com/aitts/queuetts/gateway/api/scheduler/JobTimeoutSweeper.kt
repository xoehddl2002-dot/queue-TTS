package com.aitts.queuetts.gateway.api.scheduler

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import com.aitts.queuetts.gateway.api.infra.redis.queue.PendingJobResults
import com.aitts.queuetts.gateway.api.service.JobService
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 발행됐지만 결과가 오지 않는 job 을 timeout 실패시키는 주기적 sweeper.
 *
 * result 스트림 소비([JobResultListener])와 달리 이건 push 로 대체할 수 없다:
 * timeout 은 "이벤트의 부재"(worker 가 죽었거나 응답하지 않아 결과 스트림에 아무것도 도착하지 않음)라서
 * 반응할 메시지 자체가 없고, 시간 경과로만 감지할 수 있다. 그래서 주기적 스캔이 본질적으로 필요하다.
 *
 * 판단 기준은 job 의 createdAt(발행 시각) 경과 시간뿐이며, worker(consumer) 의 수·생존·idle 상태는 보지 않는다.
 * worker 상태 조회는 [com.aitts.queuetts.gateway.api.infra.redis.queue.RedisStreamQueueClient] 의 XINFO CONSUMERS 쪽에 있다.
 */
@Component
class JobTimeoutSweeper(
    private val properties: QueueTtsGatewayProperties,
    private val jobService: JobService,
    private val pendingJobResults: PendingJobResults,
) {
    private val log = LoggerFactory.getLogger(JobTimeoutSweeper::class.java)
    private val running = AtomicBoolean(false)
    private var thread: Thread? = null

    private companion object {
        /** timeout 된 job 을 스캔하는 주기(ms). 필요하면 여기서 조정한다. */
        const val SWEEP_INTERVAL_MS = 1000L

        /**
         * sweep 루프가 예외(예: Redis 장애)를 만났을 때 다음 루프까지 쉬는 backoff(ms).
         * 요청 단위 재시도가 아니라 데몬 루프가 tight-spin 하지 않도록 하는 liveness 장치일 뿐이다.
         */
        const val ERROR_BACKOFF_MS = 3000L
    }

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        if (!properties.queue.enabled) {
            log.info("Job timeout sweeper is disabled (queuetts.queue.enabled=false)")
            return
        }
        if (!running.compareAndSet(false, true)) {
            return
        }
        thread = Thread(::loop, "job-timeout-sweeper").apply {
            isDaemon = true
            start()
        }
        log.info(
            "Job timeout sweeper started: timeoutSeconds={} intervalMs={}",
            properties.workerTimeoutSeconds,
            SWEEP_INTERVAL_MS,
        )
    }

    @PreDestroy
    fun stop() {
        running.set(false)
        thread?.interrupt()
    }

    private fun loop() {
        var sweeperHealthy = true
        while (running.get()) {
            try {
                val completions = jobService.failTimedOutJobs()
                completions.forEach { completion -> pendingJobResults.complete(completion.jobId, completion) }
                if (completions.isNotEmpty()) {
                    log.warn(
                        "Timed out {} unfinished TTS jobs after {}s: {}",
                        completions.size,
                        properties.workerTimeoutSeconds,
                        completions.joinToString(", ") { it.jobId },
                    )
                }
                if (!sweeperHealthy) {
                    log.info("Job timeout sweeper recovered")
                    sweeperHealthy = true
                }
                Thread.sleep(SWEEP_INTERVAL_MS)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (exception: Exception) {
                if (sweeperHealthy) {
                    log.warn("Job timeout sweeper failed: {}", exception.message, exception)
                    sweeperHealthy = false
                } else {
                    log.debug("Job timeout sweeper still failing: {}", exception.message)
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


}
