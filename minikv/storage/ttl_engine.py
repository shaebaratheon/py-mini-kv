import time
from typing import Optional, Dict, Any

class ExpiringStorageEngine:
    """Key-value storage engine supporting millisecond-precision Time-To-Live (TTL)."""

    def __init__(self):
        self._data: Dict[str, bytes] = {}
        self._expires: Dict[str, float] = {}

    def set(self, key: str, value: bytes, ttl_seconds: Optional[float] = None) -> None:
        self._data[key] = value
        if ttl_seconds is not None:
            self._expires[key] = time.time() + ttl_seconds
        elif key in self._expires:
            del self._expires[key]

    def get(self, key: str) -> Optional[bytes]:
        if key not in self._data:
            return None
        expire_at = self._expires.get(key)
        if expire_at is not None and time.time() >= expire_at:
            del self._data[key]
            del self._expires[key]
            return None
        return self._data[key]

    def ttl(self, key: str) -> Optional[float]:
        if key not in self._data:
            return None
        expire_at = self._expires.get(key)
        if expire_at is None:
            return -1.0  # Key exists with no expiration
        remaining = expire_at - time.time()
        if remaining <= 0:
            del self._data[key]
            del self._expires[key]
            return None
        return remaining

    def purge_expired(self) -> int:
        now = time.time()
        expired_keys = [k for k, exp in self._expires.items() if now >= exp]
        for k in expired_keys:
            del self._data[k]
            del self._expires[k]
        return len(expired_keys)
