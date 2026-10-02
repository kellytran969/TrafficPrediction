"""Stream bus abstraction: Redis Streams in production, in-memory queue locally.

The simulator publishes SensorReading dicts; the ingestion service consumes them.
Both backends expose the same interface so the whole pipeline runs with zero
infrastructure (STREAM_BACKEND=memory) or against real Redis Streams.
"""

from __future__ import annotations

import json
import queue
import threading
import uuid
from typing import Any

from .config import get_settings


class StreamBus:
    def publish(self, payload: dict[str, Any]) -> str:  # pragma: no cover
        raise NotImplementedError

    def read_batch(self, batch_size: int = 100, block_ms: int = 1000) -> list[dict[str, Any]]:
        """Return up to batch_size messages; each message is the payload dict."""
        raise NotImplementedError  # pragma: no cover


class MemoryStreamBus(StreamBus):
    """Process-local FIFO used for local runs and tests."""

    def __init__(self) -> None:
        self._q: queue.Queue[dict[str, Any]] = queue.Queue()
        self._lock = threading.Lock()

    def publish(self, payload: dict[str, Any]) -> str:
        msg_id = uuid.uuid4().hex
        self._q.put({"_id": msg_id, **payload})
        return msg_id

    def read_batch(self, batch_size: int = 100, block_ms: int = 1000) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        try:
            out.append(self._q.get(timeout=block_ms / 1000))
        except queue.Empty:
            return out
        while len(out) < batch_size:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out


class RedisStreamBus(StreamBus):
    """Redis Streams with a consumer group: at-least-once delivery, replayable."""

    def __init__(self, url: str, stream: str, group: str, consumer: str | None = None) -> None:
        import redis

        self._client = redis.Redis.from_url(url, decode_responses=True)
        self._stream = stream
        self._group = group
        self._consumer = consumer or f"ingest-{uuid.uuid4().hex[:8]}"
        try:
            self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    def publish(self, payload: dict[str, Any]) -> str:
        fields = {k: json.dumps(v) if not isinstance(v, (str, int, float)) else v
                  for k, v in payload.items()}
        return self._client.xadd(self._stream, fields)

    def read_batch(self, batch_size: int = 100, block_ms: int = 1000) -> list[dict[str, Any]]:
        resp = self._client.xreadgroup(
            self._group, self._consumer, {self._stream: ">"},
            count=batch_size, block=block_ms,
        )
        messages: list[dict[str, Any]] = []
        for _stream, entries in resp or []:
            for msg_id, fields in entries:
                payload = {k: _coerce(v) for k, v in fields.items()}
                payload["_id"] = msg_id
                messages.append(payload)
                self._client.xack(self._stream, self._group, msg_id)
        return messages


def _coerce(value: str) -> Any:
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return value


def get_bus(backend: str | None = None) -> StreamBus:
    settings = get_settings()
    which = (backend or settings.stream_backend).lower()
    if which == "redis":
        try:
            bus = RedisStreamBus(settings.redis_url, settings.stream_name,
                                 settings.consumer_group)
            bus._client.ping()
            return bus
        except Exception:
            pass  # fall through to memory bus
    return MemoryStreamBus()
