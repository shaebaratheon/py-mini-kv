"""
High-performance storage engine layer for MiniKV.
Provides Write-Ahead Logging (WAL), memory-mapped index, and SSTable compaction.
"""

import os
import struct
import zlib
import time
import threading
from typing import Optional, Dict, Any, List, Tuple


class WALRecord:
    """Represents a single entry in the Write-Ahead Log with CRC32 integrity check."""
    OP_SET = 1
    OP_DELETE = 2

    def __init__(self, op_type: int, key: str, value: Optional[str] = None, timestamp: Optional[float] = None):
        self.op_type = op_type
        self.key = key
        self.value = value if value is not None else ""
        self.timestamp = timestamp if timestamp is not None else time.time()

    def serialize(self) -> bytes:
        """
        Binary wire format:
        [4 bytes CRC32][1 byte OpType][8 bytes Timestamp][4 bytes KeyLen][KeyBytes][4 bytes ValLen][ValBytes]
        """
        key_bytes = self.key.encode("utf-8")
        val_bytes = self.value.encode("utf-8")
        payload = struct.pack(
            f">BdI{len(key_bytes)}sI{len(val_bytes)}s",
            self.op_type,
            self.timestamp,
            len(key_bytes),
            key_bytes,
            len(val_bytes),
            val_bytes,
        )
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        return struct.pack(">I", checksum) + payload

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> Tuple[Optional["WALRecord"], int]:
        """Reads a WAL record from byte stream with CRC validation."""
        if len(data) - offset < 4 + 1 + 8 + 4:
            return None, offset

        stored_crc = struct.unpack_from(">I", data, offset)[0]
        header_offset = offset + 4
        op_type, timestamp, key_len = struct.unpack_from(">BdI", data, header_offset)
        curr = header_offset + 13

        if len(data) - curr < key_len + 4:
            return None, offset

        key_bytes = data[curr : curr + key_len]
        curr += key_len

        val_len = struct.unpack_from(">I", data, curr)[0]
        curr += 4

        if len(data) - curr < val_len:
            return None, offset

        val_bytes = data[curr : curr + val_len]
        curr += val_len

        payload = data[header_offset:curr]
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != actual_crc:
            raise ValueError(f"WAL checksum mismatch: expected {stored_crc}, got {actual_crc}")

        record = cls(
            op_type=op_type,
            key=key_bytes.decode("utf-8"),
            value=val_bytes.decode("utf-8") if val_len > 0 else None,
            timestamp=timestamp,
        )
        return record, curr


class WriteAheadLog:
    """Append-only write-ahead log for durability."""

    def __init__(self, log_path: str, sync_on_write: bool = False):
        self.log_path = log_path
        self.sync_on_write = sync_on_write
        self._lock = threading.Lock()
        self._file = open(self.log_path, "a+b")

    def append(self, op_type: int, key: str, value: Optional[str] = None) -> int:
        record = WALRecord(op_type, key, value)
        data = record.serialize()
        with self._lock:
            pos = self._file.tell()
            self._file.write(data)
            if self.sync_on_write:
                self._file.flush()
                os.fsync(self._file.fileno())
            return pos

    def recover(self) -> List[WALRecord]:
        """Replays all valid records from the beginning of the log."""
        with self._lock:
            self._file.seek(0)
            data = self._file.read()

        records = []
        offset = 0
        while offset < len(data):
            try:
                record, next_offset = WALRecord.deserialize(data, offset)
                if record is None:
                    break
                records.append(record)
                offset = next_offset
            except ValueError as e:
                # Corrupted record encountered; truncate to last healthy state
                break
        return records

    def truncate(self):
        """Resets the WAL log after checkpointing or SSTable flush."""
        with self._lock:
            self._file.close()
            self._file = open(self.log_path, "w+b")

    def close(self):
        with self._lock:
            if not self._file.closed:
                self._file.flush()
                self._file.close()


class SSTableIndexEntry:
    """Index pointer for fast key lookup inside SSTable."""

    def __init__(self, key: str, offset: int, length: int, is_tombstone: bool = False):
        self.key = key
        self.offset = offset
        self.length = length
        self.is_tombstone = is_tombstone


class SSTableWriter:
    """Writes sorted, immutable SSTables with an attached binary index."""

    def __init__(self, data_path: str, index_path: str):
        self.data_path = data_path
        self.index_path = index_path

    def write(self, sorted_items: List[Tuple[str, Optional[str]]]):
        index_entries: List[SSTableIndexEntry] = []
        with open(self.data_path, "wb") as df:
            for key, val in sorted_items:
                offset = df.tell()
                is_tombstone = val is None
                encoded_val = b"" if is_tombstone else val.encode("utf-8")
                df.write(encoded_val)
                length = len(encoded_val)
                index_entries.append(SSTableIndexEntry(key, offset, length, is_tombstone))

        with open(self.index_path, "wb") as idx_file:
            # Header: 4 bytes entry count
            idx_file.write(struct.pack(">I", len(index_entries)))
            for entry in index_entries:
                kb = entry.key.encode("utf-8")
                # Format: [2 bytes key_len][key_bytes][8 bytes offset][4 bytes len][1 byte tombstone]
                idx_file.write(struct.pack(f">H{len(kb)}sQI?", len(kb), kb, entry.offset, entry.length, entry.is_tombstone))


class SSTableReader:
    """Reads key-value records from disk using binary index binary search."""

    def __init__(self, data_path: str, index_path: str):
        self.data_path = data_path
        self.index_path = index_path
        self.index: Dict[str, SSTableIndexEntry] = {}
        self._load_index()

    def _load_index(self):
        if not os.path.exists(self.index_path):
            return
        with open(self.index_path, "rb") as f:
            data = f.read()
            if len(data) < 4:
                return
            count = struct.unpack_from(">I", data, 0)[0]
            curr = 4
            for _ in range(count):
                key_len = struct.unpack_from(">H", data, curr)[0]
                curr += 2
                key = struct.unpack_from(f">{key_len}s", data, curr)[0].decode("utf-8")
                curr += key_len
                offset, length, is_tombstone = struct.unpack_from(">QI?", data, curr)
                curr += 13
                self.index[key] = SSTableIndexEntry(key, offset, length, is_tombstone)

    def get(self, key: str) -> Optional[str]:
        entry = self.index.get(key)
        if not entry or entry.is_tombstone:
            return None
        with open(self.data_path, "rb") as f:
            f.seek(entry.offset)
            val_bytes = f.read(entry.length)
            return val_bytes.decode("utf-8")

    def contains(self, key: str) -> bool:
        entry = self.index.get(key)
        return entry is not None and not entry.is_tombstone


class LeveledCompactor:
    """Merges multiple SSTables into a single optimized SSTable, removing tombstones."""

    def __init__(self, work_dir: str):
        self.work_dir = work_dir

    def compact(self, tables: List[SSTableReader], output_prefix: str) -> Tuple[str, str]:
        merged: Dict[str, Optional[str]] = {}
        # Replay tables in chronological order
        for table in tables:
            for key, entry in table.index.items():
                if entry.is_tombstone:
                    merged[key] = None
                else:
                    merged[key] = table.get(key)

        # Filter out permanent deletions
        clean_sorted = sorted([(k, v) for k, v in merged.items() if v is not None], key=lambda x: x[0])

        out_data = os.path.join(self.work_dir, f"{output_prefix}.db")
        out_idx = os.path.join(self.work_dir, f"{output_prefix}.idx")
        writer = SSTableWriter(out_data, out_idx)
        writer.write(clean_sorted)
        return out_data, out_idx
