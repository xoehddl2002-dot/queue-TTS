package com.aitts.queuetts.gateway.api.error

import org.springframework.http.HttpStatus

/**
 * QwenController(`/api/qwen/speaker`) 경로의 실패.
 *
 * 설계는 `docs/speakers-registry.md` 참고.
 */
sealed interface QwenError : DomainError {
    val status: HttpStatus

    data class NotFound(val idOrName: String) : QwenError {
        override val code: String = "SPEAKER_NOT_FOUND"
        override val message: String = "speaker not found: $idOrName"
        override val status: HttpStatus = HttpStatus.NOT_FOUND
    }

    data class InvalidRequest(val reason: String = "invalid speaker request") : QwenError {
        override val code: String = "INVALID_SPEAKER_REQUEST"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.BAD_REQUEST
    }

    /** Gateway가 참조 음성을 디코딩하지 못했거나 형식·길이 정책에 맞지 않는다. */
    data class InvalidCloneReference(val reason: String) : QwenError {
        override val code: String = "INVALID_CLONE_REFERENCE"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.BAD_REQUEST
    }

    /** 같은 풀에 같은 이름이 이미 있다. 이름은 `(model, name)` 으로 유일하다. */
    data class NameTaken(val name: String) : QwenError {
        override val code: String = "SPEAKER_NAME_TAKEN"
        override val message: String = "a speaker named '$name' already exists"
        override val status: HttpStatus = HttpStatus.CONFLICT
    }

    /** 저장소(DB/Redis) 문제. 호출자가 고칠 수 있는 것이 아니다. */
    data class StorageError(val reason: String = "speaker storage error") : QwenError {
        override val code: String = "SPEAKER_STORAGE_ERROR"
        override val message: String = reason
        override val status: HttpStatus = HttpStatus.INTERNAL_SERVER_ERROR
    }
}
