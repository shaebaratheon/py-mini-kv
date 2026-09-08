import unittest
from minikv.lru_policy import LRUCachePolicy
from minikv.counter import AtomicCounterStore

class TestAtomicCounter(unittest.TestCase):
    def setUp(self):
        self.cache = LRUCachePolicy(10)
        self.counters = AtomicCounterStore(self.cache)

    def test_incr_default(self):
        res = self.counters.incr("cnt")
        self.assertEqual(res, 1)
        res = self.counters.incr("cnt")
        self.assertEqual(res, 2)

    def test_incr_by_and_decr(self):
        self.counters.incr("views", 10)
        self.assertEqual(int(self.cache.get("views")), 10)
        res = self.counters.decr("views", 3)
        self.assertEqual(res, 7)

    def test_non_integer_type_error(self):
        self.cache.put("str_key", "hello")
        with self.assertRaises(TypeError):
            self.counters.incr("str_key")

if __name__ == "__main__":
    unittest.main()
