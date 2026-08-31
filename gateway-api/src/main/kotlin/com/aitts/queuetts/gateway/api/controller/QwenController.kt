package com.aitts.queuetts.gateway.api.controller

import com.aitts.queuetts.gateway.api.dto.CreateSpeakerRequest
import com.aitts.queuetts.gateway.api.dto.QwenSpeakerParams
import com.aitts.queuetts.gateway.api.dto.UpdateSpeakerRequest
import com.aitts.queuetts.gateway.api.error.QwenError
import com.aitts.queuetts.gateway.api.service.QwenService
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.http.ContentDisposition
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile

/**
 * PATCH 는 JSON 핸들러와 multipart 핸들러 둘로 나뉘어 있지만 **엔드포인트는 하나다.** 두 `@Operation` 이
 * 한 Swagger operation 을 공유하므로 설명도 하나로 둔다.
 */
private const val UPDATE_SPEAKER_DESCRIPTION =
    "Update metadata or invalidate the lazy clone prompt when its reference changes. " +
        "Send `application/json` for metadata-only edits, or `multipart/form-data` to also replace `ref_audio`."

@Tag(name = "Qwen Clone Voices", description = "Reusable Qwen Base voice-clone prompt registry API.")
@RestController
@ConditionalOnProperty(prefix = "queuetts.database", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class QwenController(
    private val qwenService: QwenService,
    private val objectMapper: ObjectMapper,
) {
    @Operation(
        summary = "Register a Qwen clone voice",
        description = "Validate and store ref_audio. The worker builds the clone prompt lazily on first synthesis.",
    )
    @PostMapping("/api/qwen/speaker", consumes = [MediaType.MULTIPART_FORM_DATA_VALUE])
    fun createSpeaker(
        @RequestPart("ref_audio") refAudio: MultipartFile,
        @RequestParam name: String,
        @RequestParam(name = "ref_text", required = false) refText: String?,
        @RequestParam(name = "x_vector_only_mode", defaultValue = "false") xVectorOnlyMode: Boolean,
        @RequestParam(required = false) language: String?,
        @RequestParam(required = false) description: String?,
        @RequestParam(name = "default_params", required = false) defaultParamsJson: String?,
    ): ResponseEntity<*> {
        val request = try {
            CreateSpeakerRequest(
                name = name,
                refText = refText,
                xVectorOnlyMode = xVectorOnlyMode,
                language = language,
                description = description,
                defaultParams = parseDefaultParams(defaultParamsJson),
            )
        } catch (exception: IllegalArgumentException) {
            return invalidDefaultParams(exception)
        }
        val result = qwenService.createSpeaker(request, refAudio.bytes)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.status(201).body(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "List Qwen clone voices", description = "List reusable clone voices stored in the Gateway registry.")
    @GetMapping("/api/qwen/speaker")
    fun listSpeaker(): ResponseEntity<*> {
        val result = qwenService.listSpeaker()
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(summary = "Get a Qwen clone voice", description = "Return one registered clone voice without its raw reference audio.")
    @GetMapping("/api/qwen/speaker/{name}")
    fun getSpeaker(@PathVariable name: String): ResponseEntity<*> {
        val result = qwenService.getSpeaker(name)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    /**
     * 참조 음성 원본 다운로드.
     *
     * 경로가 `/{name}` 조회와 겹치지 않게 `/audio` 를 붙였다 — `name` 은 [QwenService.validName] 이
     * `/` 를 막으므로 세그먼트가 갈릴 일이 없다.
     */
    @Operation(
        summary = "Download a Qwen clone voice's reference audio",
        description = "Return the stored reference audio bytes as-is, for playback or backup.",
    )
    @GetMapping("/api/qwen/speaker/{name}/audio")
    fun getSpeakerAudio(@PathVariable name: String): ResponseEntity<*> {
        val result = qwenService.getSpeakerAudio(name)
        val error = result.leftOrNull()
        if (error != null) return ResponseEntity.status(error.status).body(error)
        val audio = result.getOrNull() ?: return ResponseEntity.status(500).body(QwenError.StorageError())
        return ResponseEntity.ok()
            .contentType(MediaType.parseMediaType(audio.mediaType))
            .header(
                HttpHeaders.CONTENT_DISPOSITION,
                // inline 이라 브라우저가 곧바로 재생한다. 한글 이름은 filename* 로 인코딩된다.
                ContentDisposition.inline().filename(audio.fileName, Charsets.UTF_8).build().toString(),
            )
            .body(audio.content)
    }

    @Operation(
        summary = "Update a Qwen clone voice",
        description = UPDATE_SPEAKER_DESCRIPTION,
    )
    @PatchMapping("/api/qwen/speaker/{name}", consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun updateSpeaker(
        @PathVariable name: String,
        @RequestBody request: UpdateSpeakerRequest,
    ): ResponseEntity<*> {
        val result = qwenService.updateSpeaker(name, request)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    /**
     * 참조 음성 교체가 포함된 수정. 파일이 없으면 JSON PATCH를 사용한다.
     *
     * 경로는 JSON PATCH 와 **같은 `{name}`** 이다 — 달라지면 springdoc 이 경로 문자열을 키로 Paths 를
     * 만들기 때문에 Swagger 에 PATCH 가 두 개로 갈라진다. 같으면 하나의 operation 으로 합쳐지고
     * requestBody 에 content-type 두 개가 들어간다. 그때 `@Operation` 도 같은 객체에 쓰여 나중에 처리된
     * 쪽이 이기므로 **위 JSON 핸들러와 문구를 맞춰 둔다.**
     *
     * 폼의 `name` 은 *새* 이름이라 경로 변수와 이름이 겹친다. 그래서 `@PathVariable("name")` 으로 명시해
     * 바인딩하고 코틀린 쪽 식별자만 `speakerName` 으로 둔다.
     */
    @Operation(
        summary = "Update a Qwen clone voice",
        description = UPDATE_SPEAKER_DESCRIPTION,
    )
    @PatchMapping("/api/qwen/speaker/{name}", consumes = [MediaType.MULTIPART_FORM_DATA_VALUE])
    fun updateSpeakerWithAudio(
        @PathVariable("name") speakerName: String,
        @RequestPart("ref_audio", required = false) refAudio: MultipartFile?,
        @RequestParam(required = false) name: String?,
        @RequestParam(name = "ref_text", required = false) refText: String?,
        @RequestParam(name = "x_vector_only_mode", required = false) xVectorOnlyMode: Boolean?,
        @RequestParam(required = false) language: String?,
        @RequestParam(required = false) description: String?,
        @RequestParam(name = "default_params", required = false) defaultParamsJson: String?,
    ): ResponseEntity<*> {
        val request = try {
            UpdateSpeakerRequest(
                name = name,
                language = language,
                description = description,
                xVectorOnlyMode = xVectorOnlyMode,
                refText = refText,
                defaultParams = parseDefaultParams(defaultParamsJson),
            )
        } catch (exception: IllegalArgumentException) {
            return invalidDefaultParams(exception)
        }
        val result = qwenService.updateSpeaker(speakerName, request, refAudio?.bytes)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    @Operation(
        summary = "Delete a Qwen clone voice",
        description = "Permanently delete the speaker row, its reference audio, and the worker's cached prompt. This cannot be undone.",
    )
    @DeleteMapping("/api/qwen/speaker/{name}")
    fun deleteSpeaker(@PathVariable name: String): ResponseEntity<*> {
        val result = qwenService.deleteSpeaker(name)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

    /**
     * multipart 폼의 `default_params` 문자열을 [QwenSpeakerParams] 로 읽는다.
     *
     * 모르는 키는 여기서 400 이다 — Spring Boot 가 `FAIL_ON_UNKNOWN_PROPERTIES` 를 꺼 두므로 읽는
     * 지점에서 켠다. 예전에는 Map 으로 받아 서비스에서 allowlist 로 걸렀는데, 이제 타입이 그 일을
     * 한다. JSON PATCH 경로는 `StrictQwenSpeakerParamsDeserializer` 가 같은 일을 한다.
     */
    private fun parseDefaultParams(raw: String?): QwenSpeakerParams? {
        if (raw == null) return null
        return runCatching {
            objectMapper.readerFor(QwenSpeakerParams::class.java)
                .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .readValue<QwenSpeakerParams>(raw)
        }.getOrElse {
            throw IllegalArgumentException(
                "'default_params' must be a JSON object of Qwen generation parameters: ${it.message}",
                it,
            )
        }
    }

    private fun invalidDefaultParams(exception: IllegalArgumentException): ResponseEntity<QwenError> {
        val error = QwenError.InvalidRequest(exception.message ?: "invalid 'default_params'")
        return ResponseEntity.status(error.status).body(error)
    }
}
