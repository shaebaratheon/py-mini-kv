import unittest
from minikv.snapshot import SnapshotEngine

class TestSnapshotEngine(unittest.TestCase):
    def test_roundtrip(self):
        data = {"user:1": "Alice", "config": "timeout=30", "empty": ""}
        serialized = SnapshotEngine.serialize(data)
        self.assertTrue(serialized.startswith(b"MKV1"))
        restored = SnapshotEngine.deserialize(serialized)
        self.assertEqual(restored, data)

    def test_checksum_corruption(self):
        data = {"a": "1"}
        raw = bytearray(SnapshotEngine.serialize(data))
        raw[6] ^= 0xFF
        with self.assertRaises(ValueError):
            SnapshotEngine.deserialize(bytes(raw))

    def test_empty_store_snapshot(self):
        serialized = SnapshotEngine.serialize({})
        restored = SnapshotEngine.deserialize(serialized)
        self.assertEqual(restored, {})

if __name__ == "__main__":
    unittest.main()
