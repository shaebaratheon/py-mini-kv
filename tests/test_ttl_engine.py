import time
import unittest
from minikv.storage.ttl_engine import ExpiringStorageEngine

class TestExpiringStorageEngine(unittest.TestCase):

    def setUp(self):
        self.engine = ExpiringStorageEngine()

    def test_key_retrieval_before_and_after_expiration(self):
        self.engine.set("session:1", b"active_user", ttl_seconds=0.1)
        self.assertEqual(self.engine.get("session:1"), b"active_user")
        time.sleep(0.15)
        self.assertIsNone(self.engine.get("session:1"))

    def test_ttl_inquiry(self):
        self.engine.set("permanent", b"data")
        self.assertEqual(self.engine.ttl("permanent"), -1.0)
        self.engine.set("temporary", b"temp", ttl_seconds=5.0)
        remaining = self.engine.ttl("temporary")
        self.assertIsNotNone(remaining)
        self.assertGreater(remaining, 0.0)
        self.assertLessEqual(remaining, 5.0)

    def test_purge_expired_keys_batch(self):
        self.engine.set("k1", b"v1", ttl_seconds=0.05)
        self.engine.set("k2", b"v2", ttl_seconds=0.05)
        self.engine.set("k3", b"v3", ttl_seconds=10.0)
        time.sleep(0.08)
        purged = self.engine.purge_expired()
        self.assertEqual(purged, 2)
        self.assertIsNone(self.engine.get("k1"))
        self.assertIsNone(self.engine.get("k2"))
        self.assertEqual(self.engine.get("k3"), b"v3")

if __name__ == '__main__':
    unittest.main()
