import zlib
import struct
from typing import Dict

MAGIC = b"MKV1"

class SnapshotEngine:
    @staticmethod
    def serialize(data: Dict[str, str]) -> bytes:
        payload = bytearray()
        payload.extend(MAGIC)
        payload.extend(struct.pack(">I", len(data)))
        for k, v in data.items():
            kb = k.encode("utf-8")
            vb = v.encode("utf-8")
            payload.extend(struct.pack(">H", len(kb)))
            payload.extend(kb)
            payload.extend(struct.pack(">I", len(vb)))
            payload.extend(vb)
        crc = zlib.crc32(payload) & 0xffffffff
        payload.extend(struct.pack(">I", crc))
        return bytes(payload)

    @staticmethod
    def deserialize(raw: bytes) -> Dict[str, str]:
        if len(raw) < 8:
            raise ValueError("Corrupted snapshot: payload too short")
        if raw[:4] != MAGIC:
            raise ValueError("Invalid magic header")
        payload = raw[:-4]
        expected_crc = struct.unpack(">I", raw[-4:])[0]
        if (zlib.crc32(payload) & 0xffffffff) != expected_crc:
            raise ValueError("CRC32 integrity check failed")
        
        count = struct.unpack(">I", payload[4:8])[0]
        offset = 8
        result = {}
        for _ in range(count):
            k_len = struct.unpack(">H", payload[offset:offset+2])[0]
            offset += 2
            k = payload[offset:offset+k_len].decode("utf-8")
            offset += k_len
            v_len = struct.unpack(">I", payload[offset:offset+4])[0]
            offset += 4
            v = payload[offset:offset+v_len].decode("utf-8")
            offset += v_len
            result[k] = v
        return result
