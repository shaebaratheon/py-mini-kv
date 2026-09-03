"""High-load Stress and Latency Benchmarking Matrix for Distributed MiniKV."""

import time
import random
import threading
import unittest
from minikv.storage.engine import StorageEngine
from minikv.consensus.raft import RaftNode, NodeRole, LogEntry

class StressBenchmarkTest(unittest.TestCase):
    def test_concurrent_writer_throughput_under_lock(self):
        engine = StorageEngine("/tmp/test_stress_bench")
        num_threads = 8
        ops_per_thread = 500
        threads = []
        errors = []

        def worker(tid: int):
            try:
                for i in range(ops_per_thread):
                    k = f"key_{tid}_{i:04d}"
                    v = f"val_{random.randint(1000, 9999)}_payload_data_string_block_{i}"
                    engine.put(k, v)
                    if i % 10 == 0:
                        got = engine.get(k)
                        if got != v:
                            errors.append(f"Mismatch at {k}")
            except Exception as e:
                errors.append(str(e))

        t0 = time.time()
        for t_idx in range(num_threads):
            t = threading.Thread(target=worker, args=(t_idx,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        t1 = time.time()

        self.assertEqual(len(errors), 0, f"Errors encountered: {errors[:5]}")
        total_ops = num_threads * ops_per_thread
        elapsed = t1 - t0
        ops_sec = total_ops / max(elapsed, 0.0001)
        print(f"Throughput: {ops_sec:.2f} ops/sec across {total_ops} transactions")

    def test_range_query_bloom_filter_simulation(self):
        engine = StorageEngine("/tmp/test_stress_bench_range")
        for i in range(1000):
            engine.put(f"user:account:{i:05d}:profile", f"profile_data_blob_{i*7}")

        res = engine.scan(prefix="user:account:005", limit=100)
        self.assertGreaterEqual(len(res), 10)
