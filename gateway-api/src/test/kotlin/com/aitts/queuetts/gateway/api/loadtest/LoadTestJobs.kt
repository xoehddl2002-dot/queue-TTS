package com.aitts.queuetts.gateway.api.loadtest

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * 게이트웨이 잡 큐(스케줄러)에 잡을 잔뜩 쌓아두고, 종료될 때까지 폴링하면서
 * 큐 동작/실행 순서/동시 실행 수를 검증하는 부하 테스트 러너.
 *
 * 운영 jar 에 포함되지 않도록 테스트 소스셋에 둔다. 별도 서비스 의존성 없이
 * JDK HttpClient + Jackson 만 사용한다.
 *
 * 실행 (Gradle 태스크):
 *   ./gradlew loadTestJobs -Penv=local
 *   ./gradlew loadTestJobs -Penv=dev --args="--count 50 --submit-concurrency 4 --strict-order"
 *
 * 환경(local/dev) 별 base URL 은 변수로 바꿀 수 있다 (우선순위 높은 순):
 *   1) --base-url <url>           (직접 지정)
 *   2) 환경변수 QUEUETTS_GATEWAY_LOCAL_URL / QUEUETTS_GATEWAY_DEV_URL
 *   3) Gradle 속성 -PlocalUrl=... / -PdevUrl=... (태스크가 위 환경변수로 전달)
 *   4) Env enum 의 기본값 (현재 둘 다 http://127.0.0.1:8080)
 */

private val MAPPER = jacksonObjectMapper()

private val TERMINAL_STATUSES = setOf("done", "succeeded", "completed", "error", "failed", "cancelled")
private val SUCCESS_STATUSES = setOf("done", "succeeded", "completed")
private val RUNNING_STATUSES = setOf("processing", "running", "claimed")
private val ORDERED_PRIORITIES = listOf("urgent", "high", "normal", "low")
private val PRIORITY_RANK = ORDERED_PRIORITIES.withIndex().associate { (index, priority) -> priority to (ORDERED_PRIORITIES.size - index) }
private const val UNKNOWN_PRIORITY_LABEL = "unknown"

/** 부하 테스트 대상 환경. base URL 은 환경변수/인자로 덮어쓸 수 있다. */
enum class Env(val envVar: String, val fallbackBaseUrl: String) {
    LOCAL("QUEUETTS_GATEWAY_LOCAL_URL", "http://127.0.0.1:8080"),
    DEV("QUEUETTS_GATEWAY_DEV_URL", "http://localhost:8080"),
    PROD("QUEUETTS_GATEWAY_PROD_URL", "http://211.43.12.67:8080")
    ;

    fun resolveBaseUrl(): String = System.getenv(envVar)?.takeIf { it.isNotBlank() } ?: fallbackBaseUrl

    companion object {
        fun from(value: String): Env = when (value.trim().lowercase()) {
            "local" -> LOCAL
            "dev", "test", "stage", "staging" -> DEV
            "prod"->PROD
            else -> error("알 수 없는 환경: '$value' (사용 가능: local, dev)")
        }
    }
}

class Options {
    var env: Env = Env.LOCAL
    var baseUrl: String? = null
    var submitPath: String = "/api/jobs"
    var statusPathTemplate: String = "/api/jobs/{jobId}"
    var count: Int = 20
    var submitConcurrency: Int = 1
    var submitDelayMs: Long = 0
    var pollIntervalMs: Long = 1000
    var timeoutSeconds: Double = 600.0
    var requestTimeoutSeconds: Double = 30.0
    var expectedMaxRunning: Int? = null
    var strictOrder: Boolean = false
    var verbose: Boolean = false

    var textPrefix: String = "게이트웨이 부하 테스트 작업"
    var voice: String = "Na-in-ae"
    var lang: String = "ko"
    var speed: Double = 1.05
    var steps: Int = 12
    var responseFormat: String = "wav"
    var seed: Long = 0
    var maxChunkLength: Int = 200
    var silenceDuration: Double = 0.3
    var priority: String? = null
    var source: String? = null

    fun resolvedBaseUrl(): String = (baseUrl ?: env.resolveBaseUrl()).trimEnd('/')
}

class JobProbe(
    val index: Int,
    val jobId: String,
    val submittedAt: Long,
    var priority: String = UNKNOWN_PRIORITY_LABEL,
) {
    var lastStatus: String = "unknown"
    var startedAt: String? = null
    var finishedAt: String? = null
    var workerUrl: String? = null
    var error: String? = null
    val transitions: MutableList<Pair<Long, String>> = mutableListOf()
}

private fun utcNow(): String = OffsetDateTime.now(ZoneOffset.UTC).toString()

private fun Map<String, Any?>.pick(vararg names: String): Any? {
    for (name in names) if (containsKey(name)) return this[name]
    return null
}

private class GatewayClient(private val baseUrl: String, requestTimeoutSeconds: Double) {
    private val http: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()
    private val requestTimeout: Duration = Duration.ofMillis((requestTimeoutSeconds * 1000).toLong())

    fun requestJson(method: String, path: String, body: Map<String, Any?>? = null): Map<String, Any?> {
        val url = baseUrl + path
        val builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(requestTimeout)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
        val publisher = if (body == null) {
            HttpRequest.BodyPublishers.noBody()
        } else {
            HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body))
        }
        builder.method(method, publisher)

        val response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        val payload = response.body() ?: ""
        if (response.statusCode() >= 400) {
            throw RuntimeException("$method $url failed with HTTP ${response.statusCode()}: $payload")
        }
        if (payload.isBlank()) return emptyMap()

        @Suppress("UNCHECKED_CAST")
        return MAPPER.readValue(payload, Map::class.java) as Map<String, Any?>
    }
}

private fun parsePriority(value: String, flag: String): String {
    val priority = value.trim().lowercase()
    require(priority in ORDERED_PRIORITIES) { "$flag must be one of ${ORDERED_PRIORITIES.joinToString(", ")}" }
    return priority
}

private fun randomPriority(): String =
    ORDERED_PRIORITIES[ThreadLocalRandom.current().nextInt(ORDERED_PRIORITIES.size)]

private fun priorityForJob(opts: Options): String =
    opts.priority ?: randomPriority()

private fun priorityRank(priority: String): Int = PRIORITY_RANK[priority] ?: 0

private fun formatPriorityCounts(counts: Map<String, Int>): String =
    counts.entries
        .sortedWith(compareByDescending<Map.Entry<String, Int>> { priorityRank(it.key) }.thenBy { it.key })
        .joinToString(", ") { "${it.key}=${it.value}" }

private fun priorityPlanSummary(opts: Options): String {
    return opts.priority ?: "random(${ORDERED_PRIORITIES.joinToString(",")})"
}

private fun buildPayload(opts: Options, index: Int): Map<String, Any?> {
    val payload = linkedMapOf<String, Any?>(
        "text" to "${opts.textPrefix} ${index + 1}/${opts.count} - ${utcNow()},${opts.textPrefix} - ${utcNow()},${opts.textPrefix} - ${utcNow()},${opts.textPrefix} - ${utcNow()}",
        "voice" to opts.voice,
        "lang" to opts.lang,
        "speed" to opts.speed,
        "steps" to opts.steps,
        "response_format" to opts.responseFormat,
        "seed" to if (opts.seed >= 0) opts.seed + index else -1,
        "max_chunk_length" to opts.maxChunkLength,
        "silence_duration" to opts.silenceDuration,
    )
    payload["priority"] = priorityForJob(opts)
    opts.source?.let { payload["source"] = it }
    return payload
}

private fun submitOne(client: GatewayClient, opts: Options, index: Int): JobProbe {
    val submittedAt = System.currentTimeMillis()
    val payload = buildPayload(opts, index)
    val submittedPriority = payload["priority"]?.toString()
    val data = client.requestJson("POST", opts.submitPath, payload)
    val jobId = data.pick("job_id", "jobId", "id")
        ?: throw RuntimeException("submit response did not include a job id: $data")
    val status = (data.pick("state", "status") ?: "unknown").toString()
    val priority = (data.pick("priority") ?: submittedPriority ?: UNKNOWN_PRIORITY_LABEL).toString()
    val probe = JobProbe(index = index, jobId = jobId.toString(), submittedAt = submittedAt, priority = priority)
    probe.lastStatus = status
    probe.transitions.add(submittedAt to status)
    return probe
}

private fun fetchJob(client: GatewayClient, opts: Options, jobId: String): Map<String, Any?> {
    val path = opts.statusPathTemplate.replace("{jobId}", jobId).replace("{job_id}", jobId)
    return client.requestJson("GET", path)
}

private fun updateProbe(probe: JobProbe, data: Map<String, Any?>) {
    val now = System.currentTimeMillis()
    val status = (data.pick("state", "status") ?: "unknown").toString()
    if (status != probe.lastStatus) {
        probe.transitions.add(now to status)
        probe.lastStatus = status
    }
    (data.pick("started_at", "startedAt"))?.let { probe.startedAt = it.toString() }
    (data.pick("finished_at", "finishedAt"))?.let { probe.finishedAt = it.toString() }
    (data.pick("worker_url", "workerUrl", "worker"))?.let { probe.workerUrl = it.toString() }
    (data.pick("error", "message"))?.let { probe.error = it.toString() }
    (data.pick("priority"))?.let { probe.priority = it.toString() }
}

private fun submitJobs(client: GatewayClient, opts: Options): List<JobProbe> {
    println(
        "[submit] sending ${opts.count} jobs to ${opts.resolvedBaseUrl()}${opts.submitPath} " +
            "(env=${opts.env.name.lowercase()}, priorities=${priorityPlanSummary(opts)})",
    )
    val probes = mutableListOf<JobProbe>()

    if (opts.submitConcurrency <= 1) {
        for (index in 0 until opts.count) {
            val probe = submitOne(client, opts, index)
            probes.add(probe)
            println("  #${"%03d".format(probe.index + 1)} -> ${probe.jobId} (${probe.lastStatus}, priority=${probe.priority})")
            if (opts.submitDelayMs > 0) Thread.sleep(opts.submitDelayMs)
        }
        return probes
    }

    val executor = Executors.newFixedThreadPool(opts.submitConcurrency)
    try {
        val futures = (0 until opts.count).map { index ->
            executor.submit<JobProbe> { submitOne(client, opts, index) }
        }
        for (future in futures) {
            val probe = future.get()
            probes.add(probe)
            println("  #${"%03d".format(probe.index + 1)} -> ${probe.jobId} (${probe.lastStatus}, priority=${probe.priority})")
        }
    } finally {
        executor.shutdown()
        executor.awaitTermination(1, TimeUnit.MINUTES)
    }
    probes.sortBy { it.index }
    return probes
}

private fun pollUntilDone(client: GatewayClient, opts: Options, probes: List<JobProbe>): Int {
    val deadline = System.currentTimeMillis() + (opts.timeoutSeconds * 1000).toLong()
    var maxRunning = 0
    var lastSummary = ""

    while (System.currentTimeMillis() < deadline) {
        val counts = sortedMapOf<String, Int>()
        for (probe in probes) {
            if (probe.lastStatus !in TERMINAL_STATUSES) {
                runCatching { fetchJob(client, opts, probe.jobId) }
                    .onSuccess { updateProbe(probe, it) }
                    .onFailure { probe.error = it.message }
            }
            counts[probe.lastStatus] = (counts[probe.lastStatus] ?: 0) + 1
        }

        val running = probes.count { it.lastStatus in RUNNING_STATUSES }
        maxRunning = maxOf(maxRunning, running)

        val summary = counts.entries.joinToString(", ") { "${it.key}=${it.value}" }
        if (summary != lastSummary) {
            println("[poll] $summary | running_now=$running max_running=$maxRunning")
            lastSummary = summary
        }

        if (probes.all { it.lastStatus in TERMINAL_STATUSES }) return maxRunning

        Thread.sleep(opts.pollIntervalMs)
    }

    val notDone = probes.filter { it.lastStatus !in TERMINAL_STATUSES }.map { it.jobId }
    throw RuntimeException("timed out after ${opts.timeoutSeconds}s; unfinished jobs: ${notDone.take(10)}")
}

private fun transitionTime(probe: JobProbe, statuses: Set<String>): Long? =
    probe.transitions.firstOrNull { it.second in statuses }?.first

private fun observedStartTime(probe: JobProbe): Long? =
    probe.startedAt
        ?.let { raw -> runCatching { OffsetDateTime.parse(raw).toInstant().toEpochMilli() }.getOrNull() }
        ?: transitionTime(probe, RUNNING_STATUSES)

private data class PriorityOrderViolation(
    val position: Int,
    val lowerStarted: JobProbe,
    val higherSubmitted: JobProbe,
)

private fun priorityOrderViolations(startedOrder: List<JobProbe>): List<PriorityOrderViolation> {
    val violations = mutableListOf<PriorityOrderViolation>()
    for ((position, lowerStarted) in startedOrder.withIndex()) {
        val lowerRank = priorityRank(lowerStarted.priority)
        val lowerStartTime = observedStartTime(lowerStarted) ?: continue
        if (lowerRank == 0) continue

        val higherSubmitted = startedOrder
            .drop(position + 1)
            .filter { candidate ->
                priorityRank(candidate.priority) > lowerRank && candidate.submittedAt <= lowerStartTime
            }
            .maxWithOrNull(compareBy<JobProbe> { priorityRank(it.priority) }.thenByDescending { it.submittedAt })

        if (higherSubmitted != null) {
            violations += PriorityOrderViolation(position + 1, lowerStarted, higherSubmitted)
        }
    }
    return violations
}

private fun printReport(opts: Options, probes: List<JobProbe>, maxRunning: Int): Int {
    val successCount = probes.count { it.lastStatus in SUCCESS_STATUSES }
    val failureCount = probes.size - successCount
    val workers = probes.mapNotNull { it.workerUrl }.toSortedSet()
    val priorityCounts = probes.groupingBy { it.priority }.eachCount()

    println("\n[result]")
    println("  total=${probes.size} success=$successCount failed_or_cancelled=$failureCount")
    println("  priority_counts=${formatPriorityCounts(priorityCounts)}")
    println("  max_observed_running=$maxRunning")
    println("  workers_used=${if (workers.isEmpty()) "unknown" else workers.joinToString(", ")}")

    val startedOrder = probes
        .filter { observedStartTime(it) != null }
        .sortedWith(
            compareBy(
                { observedStartTime(it) ?: Long.MAX_VALUE },
                { it.index },
            ),
        )
    val startedIndexes = startedOrder.map { it.index }
    val expectedIndexes = startedIndexes.sorted()
    val orderOk = startedIndexes == expectedIndexes

    println("  started_order_ok=$orderOk")
    if (!orderOk) {
        println("  first_order_mismatches:")
        var shown = 0
        for ((pos, pair) in startedIndexes.zip(expectedIndexes).withIndex()) {
            val (actual, expected) = pair
            if (actual != expected) {
                println("    position=${pos + 1} expected_job=#${"%03d".format(expected + 1)} actual_job=#${"%03d".format(actual + 1)}")
                if (++shown >= 10) break
            }
        }
    }

    val priorityViolations = priorityOrderViolations(startedOrder)
    val priorityOrderOk = priorityViolations.isEmpty()
    println("  priority_order_ok=$priorityOrderOk")
    if (!priorityOrderOk) {
        println("  first_priority_mismatches:")
        priorityViolations.take(10).forEach { violation ->
            println(
                "    position=${violation.position} lower_started=#${"%03d".format(violation.lowerStarted.index + 1)}" +
                    "(${violation.lowerStarted.priority}) before available higher=#${"%03d".format(violation.higherSubmitted.index + 1)}" +
                    "(${violation.higherSubmitted.priority})",
            )
        }
    }

    val runningOk = opts.expectedMaxRunning?.let { expected ->
        val ok = maxRunning <= expected
        println("  expected_max_running=$expected ok=$ok")
        ok
    } ?: true

    if (opts.verbose) {
        println("\n[jobs]")
        for (probe in probes) {
            val transitions = probe.transitions.joinToString(" -> ") { it.second }
            println(
                "  #${"%03d".format(probe.index + 1)} ${probe.jobId} priority=${probe.priority} status=${probe.lastStatus} " +
                    "worker=${probe.workerUrl ?: "-"} transitions=$transitions",
            )
            probe.error?.let { println("      error=$it") }
        }
    }

    var exitCode = 0
    if (failureCount > 0) exitCode = 1
    if (opts.strictOrder && !orderOk) exitCode = 1
    if (!runningOk) exitCode = 1
    return exitCode
}

private fun parseArgs(args: Array<String>): Options {
    val opts = Options()

    // Gradle 태스크가 -Penv 를 QUEUETTS_GATEWAY_ENV 로 전달한다. --env 인자가 있으면 아래에서 덮어쓴다.
    System.getenv("QUEUETTS_GATEWAY_ENV")?.takeIf { it.isNotBlank() }?.let { opts.env = Env.from(it) }

    // --key=value 와 --key value 두 형태를 모두 지원하도록 평탄화한다.
    val tokens = mutableListOf<String>()
    for (arg in args) {
        if (arg.startsWith("--") && arg.contains('=')) {
            val idx = arg.indexOf('=')
            tokens.add(arg.substring(0, idx))
            tokens.add(arg.substring(idx + 1))
        } else {
            tokens.add(arg)
        }
    }

    var i = 0
    fun next(flag: String): String {
        if (i >= tokens.size) error("$flag 에 값이 필요합니다")
        return tokens[i++]
    }

    while (i < tokens.size) {
        when (val flag = tokens[i++]) {
            "--env" -> opts.env = Env.from(next(flag))
            "--base-url" -> opts.baseUrl = next(flag)
            "--submit-path" -> opts.submitPath = next(flag)
            "--status-path-template" -> opts.statusPathTemplate = next(flag)
            "--count" -> opts.count = next(flag).toInt()
            "--submit-concurrency" -> opts.submitConcurrency = next(flag).toInt()
            "--submit-delay-ms" -> opts.submitDelayMs = next(flag).toLong()
            "--poll-interval" -> opts.pollIntervalMs = (next(flag).toDouble() * 1000).toLong()
            "--poll-interval-ms" -> opts.pollIntervalMs = next(flag).toLong()
            "--timeout" -> opts.timeoutSeconds = next(flag).toDouble()
            "--request-timeout" -> opts.requestTimeoutSeconds = next(flag).toDouble()
            "--expected-max-running" -> opts.expectedMaxRunning = next(flag).toInt()
            "--strict-order" -> opts.strictOrder = true
            "--verbose" -> opts.verbose = true
            "--text-prefix" -> opts.textPrefix = next(flag)
            "--voice" -> opts.voice = next(flag)
            "--lang" -> opts.lang = next(flag)
            "--speed" -> opts.speed = next(flag).toDouble()
            "--steps" -> opts.steps = next(flag).toInt()
            "--response-format" -> opts.responseFormat = next(flag)
            "--seed" -> opts.seed = next(flag).toLong()
            "--max-chunk-length" -> opts.maxChunkLength = next(flag).toInt()
            "--silence-duration" -> opts.silenceDuration = next(flag).toDouble()
            "--priority" -> opts.priority = parsePriority(next(flag), flag)
            "--source" -> opts.source = next(flag)
            "-h", "--help" -> {
                printHelp()
                exitProcess(0)
            }
            else -> error("알 수 없는 인자: $flag")
        }
    }

    require(opts.count >= 1) { "--count 는 1 이상이어야 합니다" }
    require(opts.submitConcurrency >= 1) { "--submit-concurrency 는 1 이상이어야 합니다" }
    return opts
}

private fun printHelp() {
    println(
        """
        게이트웨이 잡 큐 부하 테스트 러너

        사용 예:
          ./gradlew loadTestJobs -Penv=local
          ./gradlew loadTestJobs -Penv=dev --args="--count 50 --submit-concurrency 4 --strict-order --verbose"

        주요 옵션:
          --env <local|dev>             대상 환경 (기본 local)
          --base-url <url>              base URL 직접 지정 (환경 기본값보다 우선)
          --count <n>                   제출할 잡 개수 (기본 20)
          --submit-concurrency <n>      동시 제출 스레드 수 (기본 1)
          --submit-delay-ms <ms>        순차 제출 시 잡 사이 지연
          --poll-interval <sec>         상태 폴링 간격 (기본 1.0초)
          --timeout <sec>               전체 완료 대기 한도 (기본 600초)
          --expected-max-running <n>    동시 실행 수 상한 검증 (초과 시 실패)
          --strict-order                제출 순서대로 시작되지 않으면 실패 처리
          --priority <urgent|high|normal|low>  fixed priority; omitted means random per job
          --source <user|counselor|...>
          --verbose                     잡별 상세 전이 출력
        """.trimIndent(),
    )
}

fun main(args: Array<String>) {
    val opts = try {
        parseArgs(args)
    } catch (e: Exception) {
        System.err.println("[error] ${e.message}")
        printHelp()
        exitProcess(2)
    }

    val client = GatewayClient(opts.resolvedBaseUrl(), opts.requestTimeoutSeconds)
    val exitCode = try {
        val probes = submitJobs(client, opts)
        val maxRunning = pollUntilDone(client, opts, probes)
        printReport(opts, probes, maxRunning)
    } catch (e: InterruptedException) {
        System.err.println("\ninterrupted")
        130
    } catch (e: Exception) {
        System.err.println("\n[error] ${e.message}")
        1
    }
    exitProcess(exitCode)
}
