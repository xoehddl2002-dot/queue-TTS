package com.aitts.queuetts.gateway.api.infra.redis.queue

import com.aitts.queuetts.gateway.api.config.QueueTtsGatewayProperties
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.RedisCallback
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

/**
 * 참조 음성 원본을 worker 가 읽어갈 수 있게 Redis 에 두는 거울.
 *
 * DB(`tts_style.audio`)가 원본이고 이 키는 배포 통로다. worker 는 prompt 캐시가 비었을 때
 * (새 worker 투입, 캐시 삭제, 재시작) 이 키를 읽어 스스로 복구한다.
 *
 * **TTL 을 두지 않는다.** 캐시 미스 시점을 예측할 수 없으므로 만료된 blob 은 복구 불가능한
 * 실패가 된다. speaker 가 바뀌면 덮어쓰고 purge 때 지운다.
 *
 * **키 이름은 Gateway 가 정해 payload 에 실어 보낸다.** worker 가 같은 규칙으로 키를 조립하게
 * 하면 양쪽이 맞아야 하는 설정이 하나 더 생긴다 — 그냥 알려주는 편이 낫다.
 *
 * 값은 base64 가 아니라 raw 바이트다. 33% 오버헤드가 없고, 스트림 엔트리가 아니라 일반 키라
 * 정확히 갱신·삭제할 수 있다(스트림은 개수 기준으로만 trim 된다).
 */
@Component
class SpeakerBlobStore(
    private val redisTemplate: StringRedisTemplate,
    private val properties: QueueTtsGatewayProperties,
) {
    private val log = LoggerFactory.getLogger(SpeakerBlobStore::class.java)

    /**
     * 키 형식의 `style` 조각은 기존 namespace 호환을 위해 두되 마지막 식별자는 공개 name이다.
     * 과거 id 기반 blob만 있는 행은 합성 resolver가 DB 원본을 읽어 name 키로 lazy backfill한다.
     *
     * Gateway 가 만든 키를 job payload(`speakerBlobKey`)로 알려주므로 worker 는 이 규칙을 모른다 —
     * 즉 이 문자열을 바꿀 때 맞춰야 하는 상대는 워커가 아니라 **이미 쌓인 데이터**다.
     */
    fun keyFor(speakerName: String): String = "${properties.queue.speakerBlobPrefix}:style:blob:$speakerName"

    fun put(speakerName: String, audio: ByteArray) {
        val key = keyFor(speakerName)
        // StringRedisTemplate 의 직렬화기는 바이너리를 그대로 두지 않으므로 연결 명령을 직접 쓴다.
        redisTemplate.execute(
            RedisCallback { connection ->
                connection.stringCommands().set(key.toByteArray(Charsets.UTF_8), audio)
            },
        )
        log.info("speaker.blob.put key={} bytes={}", key, audio.size)
    }

    fun exists(speakerName: String): Boolean {
        val key = keyFor(speakerName)
        return redisTemplate.execute(
            RedisCallback { connection -> connection.keyCommands().exists(key.toByteArray(Charsets.UTF_8)) },
        ) ?: false
    }

    fun delete(speakerName: String) {
        val key = keyFor(speakerName)
        redisTemplate.execute(
            RedisCallback { connection -> connection.keyCommands().del(key.toByteArray(Charsets.UTF_8)) },
        )
        log.info("speaker.blob.delete key={}", key)
    }
}
