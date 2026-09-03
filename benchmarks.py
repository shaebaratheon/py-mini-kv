"""
Throughput, latency distribution, and stress benchmark harness for MiniKV.
"""

import time
import random
import statistics
import threading
from typing import List
from kv_client import MiniKVClient


class BenchmarkHarness:
    def __init__(self, client: MiniKVClient, total_operations: int = 10000, num_workers: int = 8):
        self.client = client
        self.total_operations = total_operations
        self.num_workers = num_workers
        self.latencies: List[float] = []
        self._lock = threading.Lock()

    def worker_loop(self, ops_per_worker: int):
        local_latencies = []
        for i in range(ops_per_worker):
            key = f"bench_key_{random.randint(1, 1000)}"
            val = f"payload_data_{random.randint(1000, 9999)}"
            t0 = time.perf_counter()
            if random.random() < 0.7:
                self.client.set(key, val)
            else:
                self.client.get(key)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            local_latencies.append(elapsed_ms)

        with self._lock:
            self.latencies.extend(local_latencies)

    def run(self):
        print(f"Starting benchmark: {self.total_operations} ops across {self.num_workers} threads...")
        ops_per_worker = self.total_operations // self.num_workers
        threads = []
        start_time = time.perf_counter()

        for _ in range(self.num_workers):
            t = threading.Thread(target=self.worker_loop, args=(ops_per_worker,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        total_duration = time.perf_counter() - start_time
        throughput = len(self.latencies) / total_duration

        self.latencies.sort()
        p50 = statistics.median(self.latencies)
        p90 = self.latencies[int(len(self.latencies) * 0.90)]
        p99 = self.latencies[int(len(self.latencies) * 0.99)]

        print(f"Benchmark finished in {total_duration:.2f}s")
        print(f"Throughput: {throughput:.1f} ops/sec")
        print(f"Latency P50: {p50:.3f} ms")
        print(f"Latency P90: {p90:.3f} ms")
        print(f"Latency P99: {p99:.3f} ms")


if __name__ == "__main__":
    class SimpleStore:
        def __init__(self):
            self.data = {}
            self.lock = threading.Lock()
        def set(self, k, v, ttl=None):
            with self.lock: self.data[k] = v; return True
        def get(self, k):
            with self.lock: return self.data.get(k)
        def delete(self, k):
            with self.lock: return self.data.pop(k, None) is not None

    harness = BenchmarkHarness(MiniKVClient(SimpleStore()), total_operations=5000, num_workers=4)
    harness.run()
