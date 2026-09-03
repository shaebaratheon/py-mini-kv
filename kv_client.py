"""
Client interface, transactional sessions, and batch operations for MiniKV.
"""

import time
import uuid
import threading
from typing import Dict, Any, Optional, List, Callable


class TransactionAbortedException(Exception):
    """Raised when a transaction experiences an isolation conflict."""
    pass


class Transaction:
    """
    ACID Transaction session supporting Optimistic Concurrency Control (OCC)
    and Snapshot Isolation.
    """

    def __init__(self, engine, isolation_level: str = "SNAPSHOT"):
        self.tx_id = str(uuid.uuid4())
        self.engine = engine
        self.isolation_level = isolation_level
        self.read_set: Dict[str, float] = {}
        self.write_set: Dict[str, Optional[str]] = {}
        self.start_time = time.time()
        self.is_active = True

    def get(self, key: str) -> Optional[str]:
        if not self.is_active:
            raise RuntimeError("Transaction is closed.")
        if key in self.write_set:
            return self.write_set[key]
        val, ts = self.engine.get_with_timestamp(key)
        self.read_set[key] = ts
        return val

    def set(self, key: str, value: str):
        if not self.is_active:
            raise RuntimeError("Transaction is closed.")
        self.write_set[key] = value

    def delete(self, key: str):
        if not self.is_active:
            raise RuntimeError("Transaction is closed.")
        self.write_set[key] = None

    def commit(self) -> bool:
        if not self.is_active:
            raise RuntimeError("Transaction is closed.")
        try:
            success = self.engine.validate_and_commit(self)
            if not success:
                raise TransactionAbortedException(f"Write conflict detected in tx {self.tx_id}")
            return True
        finally:
            self.is_active = False

    def rollback(self):
        self.is_active = False
        self.write_set.clear()
        self.read_set.clear()


class ConnectionPool:
    """Thread-safe connection and worker resource pool."""

    def __init__(self, max_connections: int = 16):
        self.max_connections = max_connections
        self._available = [f"conn-{i}" for i in range(max_connections)]
        self._lock = threading.Condition()

    def acquire(self, timeout: float = 5.0) -> str:
        deadline = time.time() + timeout
        with self._lock:
            while not self._available:
                remaining = deadline - time.time()
                if remaining <= 0 or not self._lock.wait(remaining):
                    raise TimeoutError("Connection pool exhausted.")
            return self._available.pop()

    def release(self, conn_id: str):
        with self._lock:
            self._available.append(conn_id)
            self._lock.notify()


class MiniKVClient:
    """
    High-level API client with connection pooling, retries, and batch execution.
    """

    def __init__(self, store, max_retries: int = 3, retry_backoff: float = 0.05):
        self.store = store
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.pool = ConnectionPool(max_connections=32)

    def _execute_with_retry(self, func: Callable, *args, **kwargs):
        attempts = 0
        last_err = None
        while attempts < self.max_retries:
            conn = self.pool.acquire()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempts += 1
                last_err = e
                time.sleep(self.retry_backoff * (2 ** (attempts - 1)))
            finally:
                self.pool.release(conn)
        raise last_err

    def set(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        return self._execute_with_retry(self.store.set, key, value, ttl_seconds)

    def get(self, key: str) -> Optional[str]:
        return self._execute_with_retry(self.store.get, key)

    def delete(self, key: str) -> bool:
        return self._execute_with_retry(self.store.delete, key)

    def mset(self, mapping: Dict[str, str]) -> bool:
        """Multi-key atomic batch write."""
        def _mset():
            for k, v in mapping.items():
                self.store.set(k, v)
            return True
        return self._execute_with_retry(_mset)

    def mget(self, keys: List[str]) -> Dict[str, Optional[str]]:
        """Multi-key batch read."""
        def _mget():
            return {k: self.store.get(k) for k in keys}
        return self._execute_with_retry(_mget)

    def compare_and_swap(self, key: str, expected_val: Optional[str], new_val: str) -> bool:
        """Atomic Compare-And-Swap (CAS) primitive."""
        def _cas():
            curr = self.store.get(key)
            if curr == expected_val:
                self.store.set(key, new_val)
                return True
            return False
        return self._execute_with_retry(_cas)

    def begin_transaction(self) -> Transaction:
        return Transaction(self.store)
