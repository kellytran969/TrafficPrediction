"""Cache abstraction: Redis in production, in-memory dict for local dev/tests."""

from __future__ import annotations

import json
import time
from typing import Any

from .config import get_settings


class Cache:
    def get(self, key: str) -> Any | None:  # pragma: no cover - interface
        raise NotImplementedError

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:  # pragma: no cover
        raise NotImplementedError

    def delete(self, key: str) -> None:  # pragma: no cover
        raise NotImplementedError


class MemoryCache(Cache):
    """Tiny TTL cache used when Redis is unavailable (local runs, tests)."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, str]] = {}

    def get(self, key: str) -> Any | None:
        item = self._store.get(key)
        if item is None:
            return None
        expires_at, payload = item
        if expires_at < time.time():
            del self._store[key]
            return None
        return json.loads(payload)

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        self._store[key] = (time.time() + ttl_seconds, json.dumps(value))

    def delete(self, key: str) -> None:
        self._store.pop(key, None)


class RedisCache(Cache):
    def __init__(self, url: str) -> None:
        import redis

        self._client = redis.Redis.from_url(url, decode_responses=True)

    def get(self, key: str) -> Any | None:
        raw = self._client.get(key)
        return json.loads(raw) if raw is not None else None

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        self._client.setex(key, ttl_seconds, json.dumps(value))

    def delete(self, key: str) -> None:
        self._client.delete(key)


def get_cache(backend: str | None = None) -> Cache:
    settings = get_settings()
    which = (backend or settings.cache_backend).lower()
    if which == "redis":
        try:
            cache = RedisCache(settings.redis_url)
            cache._client.ping()
            return cache
        except Exception:
            pass  # fall through to memory cache
    return MemoryCache()
