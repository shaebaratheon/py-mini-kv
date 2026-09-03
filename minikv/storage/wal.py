"""Write-Ahead Logging (WAL) subsystem with binary encoding, checksum validation, and crash recovery."""

import struct
import zlib
import os
import time
from enum import IntEnum
from typing import Iterator, Tuple, Optional, BinaryIO

class RecordType(IntEnum):
    PUT = 1
    DELETE = 2
    TXN_BEGIN = 3
    TXN_COMMIT = 4
    TXN_ROLLBACK = 5
    CHECKPOINT = 6

HEADER_FORMAT = "<BIIQ"  # type (1B), checksum (4B), payload_length (4B), timestamp_ms (8B)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

class WALRecord:
    def __init__(self, record_type: RecordType, key: bytes, value: Optional[bytes] = None, txn_id: int = 0, timestamp_ms: Optional[int] = None):
        self.record_type = record_type
        self.key = key
        self.value = value or b""
        self.txn_id = txn_id
        self.timestamp_ms = timestamp_ms or int(time.time() * 1000)

    def encode(self) -> bytes:
        payload = struct.pack("<QII", self.txn_id, len(self.key), len(self.value)) + self.key + self.value
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        header = struct.pack(HEADER_FORMAT, int(self.record_type), checksum, len(payload), self.timestamp_ms)
        return header + payload

    @classmethod
    def decode(cls, header_bytes: bytes, payload_bytes: bytes) -> "WALRecord":
        rec_type_raw, checksum, payload_len, ts = struct.unpack(HEADER_FORMAT, header_bytes)
        actual_checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF
        if checksum != actual_checksum:
            raise ValueError(f"WAL record checksum mismatch: expected {checksum:#x}, got {actual_checksum:#x}")
        txn_id, key_len, val_len = struct.unpack("<QII", payload_bytes[:16])
        key = payload_bytes[16:16 + key_len]
        val = payload_bytes[16 + key_len:16 + key_len + val_len]
        return cls(RecordType(rec_type_raw), key, val, txn_id, ts)

class WALWriter:
    def __init__(self, filepath: str, sync_on_write: bool = False):
        self.filepath = filepath
        self.sync_on_write = sync_on_write
        self._file: Optional[BinaryIO] = None
        self._open()

    def _open(self):
        self._file = open(self.filepath, "a+b")

    def append(self, record: WALRecord) -> int:
        data = record.encode()
        pos = self._file.tell()
        self._file.write(data)
        if self.sync_on_write:
            self._file.flush()
            os.fsync(self._file.fileno())
        return pos

    def flush(self):
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self):
        if self._file and not self._file.closed:
            self.flush()
            self._file.close()

class WALReader:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_all(self) -> Iterator[WALRecord]:
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, "rb") as f:
            while True:
                header = f.read(HEADER_SIZE)
                if not header or len(header) < HEADER_SIZE:
                    break
                rec_type, checksum, payload_len, ts = struct.unpack(HEADER_FORMAT, header)
                payload = f.read(payload_len)
                if len(payload) < payload_len:
                    raise IOError("Unexpected EOF while reading WAL payload")
                yield WALRecord.decode(header, payload)
