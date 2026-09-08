import unittest
from minikv.lru_policy import LRUCachePolicy

class TestLRUCachePolicy(unittest.TestCase):
    def test_eviction_order(self):
        cache = LRUCachePolicy(2)
        cache.put("a", 1)
        cache.put("b", 2)
        self.assertEqual(cache.get("a"), 1)
        evicted = cache.put("c", 3)
        self.assertEqual(evicted, "b")
        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("a"), 1)
        self.assertEqual(cache.get("c"), 3)

    def test_update_existing_key(self):
        cache = LRUCachePolicy(2)
        cache.put("k1", 100)
        cache.put("k2", 200)
        cache.put("k1", 150)
        self.assertEqual(cache.get("k1"), 150)
        evicted = cache.put("k3", 300)
        self.assertEqual(evicted, "k2")
        self.assertEqual(cache.size(), 2)

    def test_invalid_capacity(self):
        with self.assertRaises(ValueError):
            LRUCachePolicy(0)

if __name__ == "__main__":
    unittest.main()
