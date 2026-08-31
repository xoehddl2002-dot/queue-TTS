"""Typed Redis Stream messages shared with the QueueTts Gateway contract.

The gateway serializes nested payload/result values as JSON strings while the
outer message is a flat Redis Stream field map.  Models in this module keep
that wire format at the Redis boundary and expose typed Python objects to the
worker.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol, TypeAlias


JsonObject: TypeAlias = dict[str, Any]


class RedisModelError(ValueError):
    """Raised when a Redis message does not satisfy the gateway contract."""


class JsonModel(Protocol):
    def to_dict(self) -> JsonObject: ...


class JobType(StrEnum):
    TTS = "tts"
    STYLES = "styles"


class ResultState(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _json_object(value: Any, field_name: str) -> JsonObject:
    if value is None or value == "":
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RedisModelError(f"'{field_name}' is not valid JSON: {exc.msg}") from exc
    if not isinstance(value, Mapping):
        raise RedisModelError(f"'{field_name}' must be a JSON object")
    return dict(value)


def _required_text(data: Mapping[str, Any], name: str) -> str:
    value = data.get(name)
    if value is None:
        raise RedisModelError(f"missing '{name}' in payload")
    return str(value)


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


@dataclass(frozen=True, slots=True)
class TtsPayload:
    text: str
    voice: str | None = None
    lang: str | None = None
    speed: float | None = None
    steps: int | None = None
    response_format: str | None = None
    seed: int | None = None
    max_chunk_length: int | None = None
    silence_duration: float | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TtsPayload":
        return cls(
            text=_required_text(data, "text"),
            voice=_optional_text(data.get("voice")),
            lang=_optional_text(data.get("lang")),
            speed=_optional_float(data.get("speed")),
            steps=_optional_int(data.get("steps")),
            response_format=_optional_text(data.get("response_format")),
            seed=_optional_int(data.get("seed")),
            max_chunk_length=_optional_int(data.get("max_chunk_length")),
            silence_duration=_optional_float(data.get("silence_duration")),
        )


@dataclass(frozen=True, slots=True)
class EmptyPayload:
    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EmptyPayload":
        return cls()


JobPayload: TypeAlias = TtsPayload | EmptyPayload


def parse_job_payload(job_type: JobType, data: Mapping[str, Any]) -> JobPayload:
    model = {
        JobType.TTS: TtsPayload,
        JobType.STYLES: EmptyPayload,
    }[job_type]
    return model.from_dict(data)


@dataclass(frozen=True, slots=True)
class RedisJobMessage:
    job_id: str
    job_type: JobType
    payload: JobPayload

    @classmethod
    def from_redis_fields(cls, fields: Mapping[str, Any]) -> "RedisJobMessage":
        job_id = str(fields.get("jobId") or "").strip()
        if not job_id:
            raise RedisModelError("Redis job message has no jobId")
        type_value = str(fields.get("type") or JobType.TTS)
        try:
            job_type = JobType(type_value)
        except ValueError as exc:
            raise RedisModelError(f"unsupported job type: {type_value}") from exc
        payload_data = _json_object(fields.get("payload"), "payload")
        return cls(
            job_id=job_id,
            job_type=job_type,
            payload=parse_job_payload(job_type, payload_data),
        )


@dataclass(frozen=True, slots=True)
class AudioResult:
    duration_s: float
    sample_rate: int
    media_type: str
    audio_format: str
    processed_text: str

    def to_dict(self) -> JsonObject:
        return {
            "durationS": self.duration_s,
            "sampleRate": str(self.sample_rate),
            "mediaType": self.media_type,
            "audioFormat": self.audio_format,
            "processedText": self.processed_text,
        }


@dataclass(frozen=True, slots=True)
class ArtifactContent:
    file_name: str
    media_type: str
    content_base64: str

    def to_dict(self) -> JsonObject:
        return {
            "fileName": self.file_name,
            "mediaType": self.media_type,
            "contentBase64": self.content_base64,
        }


@dataclass(frozen=True, slots=True)
class StyleInfo:
    name: str
    kind: str
    path: str | None = None

    def to_dict(self) -> JsonObject:
        return _without_none({
            "name": self.name,
            "kind": self.kind,
            "path": self.path,
        })


@dataclass(frozen=True, slots=True)
class StylesResult:
    styles: list[StyleInfo]
    worker_id: str
    batch_size: int

    def to_dict(self) -> JsonObject:
        return {
            "styles": [style.to_dict() for style in self.styles],
            "worker_id": self.worker_id,
            "batch_size": self.batch_size,
        }


@dataclass(frozen=True, slots=True)
class ErrorResult:
    message: str
    code: str | None = None
    detail: str | None = None
    reason: str | None = None

    def to_dict(self) -> JsonObject:
        return _without_none({
            "code": self.code,
            "message": self.message,
            "detail": self.detail,
            "reason": self.reason,
        })


ResultPayload: TypeAlias = AudioResult | StylesResult


@dataclass(frozen=True, slots=True)
class RedisResultMessage:
    job_id: str
    worker_id: str
    batch_id: str
    state: ResultState
    result: ResultPayload | None = None
    error: ErrorResult | None = None
    artifact: ArtifactContent | None = None
    started_at: datetime | None = None

    @classmethod
    def running(
        cls,
        *,
        job_id: str,
        worker_id: str,
        batch_id: str,
        started_at: datetime | None = None,
    ) -> "RedisResultMessage":
        return cls(
            job_id,
            worker_id,
            batch_id,
            ResultState.RUNNING,
            started_at=started_at or datetime.now(timezone.utc),
        )

    @classmethod
    def succeeded(
        cls,
        *,
        job_id: str,
        worker_id: str,
        batch_id: str,
        result: ResultPayload,
        artifact: ArtifactContent | None = None,
    ) -> "RedisResultMessage":
        return cls(job_id, worker_id, batch_id, ResultState.SUCCEEDED, result=result, artifact=artifact)

    @classmethod
    def failed(
        cls,
        *,
        job_id: str,
        worker_id: str,
        batch_id: str,
        message: str,
    ) -> "RedisResultMessage":
        return cls(job_id, worker_id, batch_id, ResultState.FAILED, error=ErrorResult(message))

    def to_redis_fields(self) -> dict[str, str]:
        fields = {
            "jobId": self.job_id,
            "workerId": self.worker_id,
            "batchId": self.batch_id,
            "state": self.state.value,
        }
        if self.result is not None:
            fields["result"] = _json_dump(self.result)
        if self.error is not None:
            fields["error"] = _json_dump(self.error)
        if self.artifact is not None:
            fields["artifact"] = _json_dump(self.artifact)
        if self.started_at is not None:
            fields["startedAt"] = self.started_at.isoformat()
        return fields


def _without_none(data: Mapping[str, Any]) -> JsonObject:
    return {key: value for key, value in data.items() if value is not None}


def _json_dump(value: JsonModel | Mapping[str, Any]) -> str:
    data = value.to_dict() if hasattr(value, "to_dict") else dict(value)
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
