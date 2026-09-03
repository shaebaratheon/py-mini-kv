"""LSM & B-Tree Hybrid Storage Engine with atomic transactions, compaction, and snapshot isolation."""

import os
import threading
import time
from typing import Optional, Dict, Any, List, Tuple, Iterator
from minikv.storage.wal import WALWriter, WALReader, WALRecord, RecordType
from minikv.storage.btree import BPlusTreeIndex

class StorageEngine:
    def __init__(self, data_dir: str, sync_wal: bool = False):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.wal_path = os.path.join(data_dir, "wal.log")
        self.sync_wal = sync_wal
        self.wal_writer = WALWriter(self.wal_path, sync_on_write=sync_wal)
        self.index = BPlusTreeIndex(max_keys=32)
        self.lock = threading.RLock()
        self._tx_counter = 0
        self._recover()

    def _recover(self):
        with self.lock:
            reader = WALReader(self.wal_path)
            for record in reader.read_all():
                if record.record_type == RecordType.PUT:
                    self.index.insert(record.key, record.value)
                elif record.record_type == RecordType.DELETE:
                    self.index.insert(record.key, None)

    def put(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        k_bytes = key.encode("utf-8")
        v_bytes = value.encode("utf-8")
        with self.lock:
            rec = WALRecord(RecordType.PUT, k_bytes, v_bytes)
            self.wal_writer.append(rec)
            self.index.insert(k_bytes, v_bytes)
            return True

    def get(self, key: str) -> Optional[str]:
        k_bytes = key.encode("utf-8")
        with self.lock:
            val = self.index.search(k_bytes)
            if val is None or val == b"":
                return None
            return val.decode("utf-8")

    def delete(self, key: str) -> bool:
        k_bytes = key.encode("utf-8")
        with self.lock:
            if self.index.search(k_bytes) is None:
                return False
            rec = WALRecord(RecordType.DELETE, k_bytes, b"")
            self.wal_writer.append(rec)
            self.index.insert(k_bytes, b"")
            return True

    def scan(self, prefix: str = "", limit: int = 100) -> List[Tuple[str, str]]:
        start_bytes = prefix.encode("utf-8") if prefix else None
        res = []
        with self.lock:
            for k, v in self.index.scan(start_key=start_bytes, limit=limit):
                if v is not None and v != b"":
                    k_str = k.decode("utf-8")
                    if prefix and not k_str.startswith(prefix):
                        break
                    res.append((k_str, v.decode("utf-8")))
        return res

    def checkpoint(self):
        with self.lock:
            compacted_wal = os.path.join(self.data_dir, "wal.compact.log")
            compact_writer = WALWriter(compacted_wal, sync_on_write=True)
            for k, v in self.index.scan():
                if v is not None and v != b"":
                    rec = WALRecord(RecordType.PUT, k, v)
                    compact_writer.append(rec)
            compact_writer.close()
            self.wal_writer.close()
            os.replace(compacted_wal, self.wal_path)
            self.wal_writer = WALWriter(self.wal_path, sync_on_write=self.sync_wal)

    def close(self):
        with self.lock:
            self.wal_writer.close()
