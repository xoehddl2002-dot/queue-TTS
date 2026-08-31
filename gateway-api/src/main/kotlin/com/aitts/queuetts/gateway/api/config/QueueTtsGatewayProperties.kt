package com.aitts.queuetts.gateway.api.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.nio.file.Path
import kotlin.io.path.Path

@ConfigurationProperties(prefix = "queuetts")
data class QueueTtsGatewayProperties(
    /** 동기 TTS 요청이 큐 결과를 기다리는 최대 시간. */
    val workerTimeoutSeconds: Long = 60,
    val database: Database = Database(),
    val job: JobGateway = JobGateway(),
    val queue: Queue = Queue(),
    val logging: Logging = Logging(),
    val security: Security = Security(),
) {
    data class Database(
        val enabled: Boolean = true,
    )

    data class Logging(
        val sql: Boolean = true,
        val maxValueLength: Int = 500,
    )

    data class JobGateway(
        val artifactDir: Path = Path("job_artifacts"),
        val artifactMaxBytes: Long = 100L * 1024L * 1024L,
        /**
         * 합성 문장 길이 상한 (글자 수).
         *
         * 워커는 긴 텍스트를 청크로 쪼개 **순차 합성**하므로 입력 길이가 곧 GPU 점유 시간이다.
         * 상한이 없으면 요청 하나가 워커 풀을 수십 분~수 시간 붙잡아 뒤에 줄 선 job 이 전부 밀린다
         * (batch 로 함께 잡힌 job 까지 같이 막힌다). 게다가 gateway 가 [workerTimeoutSeconds] 로
         * job 을 실패시켜도 **워커는 계속 합성한다** — 취소가 전파되지 않으므로 실패 처리만으로는
         * GPU 가 풀리지 않는다. 그래서 접수 경계에서 막는다.
         *
         * 클라이언트도 같은 값으로 입력을 제한하지만 그쪽은 실수 방지일 뿐이고
         * (API 를 직접 부르면 그만이다) 실제 보호는 여기다.
         */
        val textMaxLength: Int = 200,
        /**
         * 클론 speaker 참조 음성의 상한 (base64 디코딩 후 기준).
         *
         * 권장 길이가 5~10초라 훨씬 작지만, 무손실 포맷과 고샘플레이트를 감안해 여유를 둔다.
         */
        val speakerAudioMaxBytes: Long = 16L * 1024L * 1024L,
        /** 너무 짧은 참조는 화자 특성을 안정적으로 얻기 어렵다. worker가 아니라 등록 경계에서 막는다. */
        val speakerAudioMinSeconds: Double = 2.0,
        /** ICL prefill 비용과 비정상 업로드를 제한하는 참조 음성 길이 상한. */
        val speakerAudioMaxSeconds: Double = 30.0,
    )

    /**
     * Redis Streams 기반 잡 파이프라인 설정.
     *
     * gateway 는 TTS worker 를 전혀 모른 채 접수한 job 을 모델별 job stream prefix 기반의
     * 우선순위별 stream (`{jobStream}:urgent` 등)에 발행한다. worker 들은 해당 모델의
     * consumer group 으로 높은 우선순위 stream 부터 job 을 가져가 running 이벤트와 완료 결과를
     * [resultStream] 에 넣는다. gateway 는 해당 이벤트를 [resultGroup] 으로 소비한다.
     * worker 존재/갯수는 우선순위별 stream 의 `XINFO CONSUMERS` 정보를 합쳐 파악한다.
     *
     * **결과 스트림은 모델과 무관하게 하나**다 — 결과는 jobId 로 식별되므로 모델별로 나눌 이유가 없다.
     */
    data class Queue(
        val enabled: Boolean = true,
        /**
         * (레거시) 단일 모델 구성의 job stream prefix. [models] 가 비어 있을 때
         * [defaultModel] 의 설정으로 승격된다. 새 설정은 [models] 를 쓴다.
         */
        val jobStream: String = "tts:jobs",
        /** (레거시) 단일 모델 구성의 consumer group. [models] 가 비어 있을 때만 쓰인다. */
        val jobGroup: String = "tts-workers",
        val resultStream: String = "tts:results",
        val resultGroup: String = "gateway",
        /** gateway 재시작 후 미처리(unacked) 결과를 복구할 수 있도록 고정된 consumer 이름을 사용한다. */
        val consumerName: String = "gateway-1",
        /** job 발행 시 각 우선순위 stream 에 적용하는 근사(approximate) XTRIM MAXLEN 값. */
        val jobStreamMaxLength: Long = 100_000,
        /** idle 이 이 값 이하인 consumer 만 active worker 로 집계한다. */
        val workerActiveIdleMs: Long = 30_000,
        /** pending 0 인 채 idle 이 이 값을 넘는 consumer 는 그룹에서 삭제한다 (유령 worker 정리). */
        val workerEvictIdleMs: Long = 300_000,
        /** styles 같은 제어성 request/reply 요청의 응답 대기 timeout (초). */
        val controlRequestTimeoutSeconds: Long = 30,
        /** job payload 에 `model` 이 없을 때 사용할 모델. 기존 호출자의 하위 호환을 담당한다. */
        val defaultModel: String = "supertonic",
        /**
         * clone speaker 합성/캐시 삭제(`/api/qwen/speaker`)를 처리할 모델.
         *
         * 보이스 클로닝은 엔진마다 되는 것이 아니라서 [defaultModel] 로 흘려보내면 그 풀의 worker 가
         * `unsupported job type` 으로 거절한다. 그래서 대상 풀을 따로 둔다. 요청이 `model` 을
         * 명시하면 그쪽이 우선한다.
         */
        val voiceModel: String = "qwen",
        /**
         * 참조 음성 blob 키의 prefix (`{prefix}:style:blob:{speakerName}`).
         *
         * worker 는 이 규칙을 알 필요가 없다 — Gateway 가 만든 키를 payload 로 알려준다.
         * 그래서 양쪽이 맞춰야 하는 설정이 아니고, Redis 네임스페이스 정리용으로만 쓴다.
         */
        val speakerBlobPrefix: String = "qwen",
        /**
         * 합성 엔진(모델)별 큐. 엔진마다 voice 목록과 파라미터 규격이 달라 워커 풀을 분리하며,
         * 풀이 섞이면 서로의 잡을 가져가 실패하므로 stream 과 group 이 모두 달라야 한다.
         *
         * 비워 두면 [jobStream]/[jobGroup] 으로 [defaultModel] 항목 하나를 만들어 기존 설정이
         * 그대로 동작한다.
         */
        val models: Map<String, ModelQueue> = emptyMap(),
    ) {
        /** 실제로 사용할 모델별 큐. [models] 가 비면 레거시 단일 모델 설정으로 대체한다. */
        val modelQueues: Map<String, ModelQueue>
            get() = models.ifEmpty { mapOf(defaultModel to ModelQueue(jobStream, jobGroup)) }

        /**
         * 요청의 model 문자열을 설정에 있는 정식 모델 키로 바꾼다. 대소문자는 무시한다.
         * 비어 있으면 [defaultModel], 등록되지 않은 이름이면 null.
         */
        fun canonicalModel(model: String?): String? {
            val requested = model?.trim()?.takeIf(String::isNotEmpty) ?: defaultModel
            return modelQueues.keys.firstOrNull { it.equals(requested, ignoreCase = true) }
        }

        /**
         * 모델 큐 설정의 문제를 사람이 읽을 수 있는 문장으로 모은다. 비어 있으면 정상.
         *
         * 새 모델을 추가할 때 stream/group 을 바꾸는 걸 잊는 실수가 가장 위험하다 — 두 엔진이
         * 같은 큐를 공유하면 서로의 잡을 가져가 `Unknown voice` 로 실패하고, voice catalog
         * 불일치로 나중에 뜬 워커가 자가 종료한다. 조용히 오작동하느니 기동을 막는 게 낫다.
         */
        fun configurationProblems(): List<String> {
            val problems = mutableListOf<String>()
            val queues = modelQueues

            if (queues.isEmpty()) {
                problems += "no models configured (set queuetts.queue.models or the legacy job-stream/job-group)"
                return problems
            }
            if (canonicalModel(null) == null) {
                problems += "default-model '$defaultModel' is not one of ${queues.keys.sorted()}"
            }
            // 모델 이름은 대소문자 무시로 매칭하므로, 케이스만 다른 이름은 어느 쪽이 걸릴지 알 수 없다.
            queues.keys.groupBy { it.lowercase() }
                .filterValues { it.size > 1 }
                .forEach { (_, names) -> problems += "model names ${names.sorted()} differ only by case" }

            queues.forEach { (model, modelQueue) ->
                if (modelQueue.jobStream.isBlank()) problems += "model '$model' has a blank job-stream"
                if (modelQueue.jobGroup.isBlank()) problems += "model '$model' has a blank job-group"
            }
            queues.entries.groupBy { it.value.jobStream }
                .filterValues { it.size > 1 }
                .forEach { (stream, shared) ->
                    problems += "models ${shared.map { it.key }.sorted()} share job-stream '$stream'; " +
                            "each engine needs its own stream"
                }
            queues.entries.groupBy { it.value.jobGroup }
                .filterValues { it.size > 1 }
                .forEach { (group, shared) ->
                    problems += "models ${shared.map { it.key }.sorted()} share job-group '$group'; " +
                            "each engine needs its own consumer group"
                }
            return problems
        }
    }

    /**
     * 한 합성 엔진(모델)이 소비하는 큐.
     *
     * 예) supertonic → `tts:jobs` / `tts-workers`, qwen → `qwen:jobs` / `qwen-workers`.
     * 각 워커 프로젝트의 `QUEUETTS_JOB_STREAM` / `QUEUETTS_JOB_GROUP` 과 값이 일치해야 한다.
     */
    data class ModelQueue(
        /** 실제 stream 은 이 값을 prefix 로 사용해 `:urgent`, `:high`, `:normal`, `:low`가 붙는다. */
        val jobStream: String = "tts:jobs",
        val jobGroup: String = "tts-workers",
    )

    /**
     * API Key 기반 인증/CORS 설정.
     *
     * gateway 는 사용자 개념이 없는 서버-투-서버 API 이므로 세션 없이 헤더([headerName])로 전달된
     * API Key 하나로 호출자를 식별한다. `/api/admin` 하위는 [ApiKeyRole.ADMIN] 키만 통과하고,
     * 그 외 API 는 등록된 아무 키나 통과한다. [publicPaths] 와 [additionalPublicPaths] 만 무인증이다.
     */
    data class Security(
        /** false 면 모든 경로가 무인증으로 열린다. 로컬 디버깅용 탈출구이며 dev/prod 에서 끄지 말 것. */
        val enabled: Boolean = true,
        val headerName: String = "X-API-Key",
        /** `Authorization: Bearer <key>` 형태의 전달도 허용할지. */
        val allowBearerHeader: Boolean = true,
        val keys: List<ApiKey> = emptyList(),
        /** 인증 없이 열어두는 경로. 로드밸런서/컨테이너 probe 가 쓰는 health 계열이다. */
        val publicPaths: List<String> = listOf(
            "/api/health",
            "/actuator/health",
            "/actuator/health/**",
            "/actuator/info",
        ),
        /** 환경별로 더 열어둘 경로 (예: local/dev 의 swagger-ui). prod 는 비워둔다. */
        val additionalPublicPaths: List<String> = emptyList(),
        val cors: Cors = Cors(),
    )

    /**
     * 호출자 한 명분의 API Key.
     *
     * [key] 는 비밀값이므로 toString 에서 마스킹한다 (설정 바인딩 실패 로그 등에 평문이 찍히지 않도록).
     */
    data class ApiKey(
        /** 로그·감사에 남는 호출자 식별자. 비밀이 아니다. */
        val id: String = "",
        val key: String = "",
        val role: ApiKeyRole = ApiKeyRole.CLIENT,
    ) {
        override fun toString(): String = "ApiKey(id=$id, role=$role, key=***)"
    }

    enum class ApiKeyRole {
        /** `/api/admin` 하위를 포함한 모든 API 호출 가능. */
        ADMIN,

        /** admin 을 제외한 API 호출 가능. */
        CLIENT,
    }

    /**
     * CORS 화이트리스트. [allowedOrigins] 가 비어 있으면 CORS 헤더를 전혀 내려주지 않는다
     * (= 브라우저 교차 출처 호출 차단).
     */
    data class Cors(
        val allowedOrigins: List<String> = emptyList(),
        val allowedMethods: List<String> = listOf("GET", "POST", "PATCH", "DELETE", "OPTIONS"),
        val allowedHeaders: List<String> = listOf("Content-Type", "Accept", "X-API-Key", "Authorization", "Last-Event-ID"),
        /** 브라우저 JS 가 읽을 수 있게 노출할 응답 헤더 (await/download 의 오디오 메타 포함). */
        val exposedHeaders: List<String> = listOf("X-QueueTts-Request-Id", "X-Sample-Rate", "X-Audio-Duration"),
        val allowCredentials: Boolean = false,
        val maxAgeSeconds: Long = 3600,
    )
}
