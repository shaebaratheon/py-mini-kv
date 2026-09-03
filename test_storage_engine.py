"""
Comprehensive unit and integration test suite for MiniKV.
"""

import os
import shutil
import tempfile
import threading
import unittest
from storage_engine import WALRecord, WriteAheadLog, SSTableWriter, SSTableReader, LeveledCompactor
from kv_client import MiniKVClient, TransactionAbortedException


class MockEngine:
    def __init__(self):
        self.store = {}
        self.timestamps = {}
        self.lock = threading.Lock()

    def set(self, key, value, ttl_seconds=None):
        with self.lock:
            self.store[key] = value
            self.timestamps[key] = self.timestamps.get(key, 0.0) + 1.0
            return True

    def get(self, key):
        with self.lock:
            return self.store.get(key)

    def delete(self, key):
        with self.lock:
            if key in self.store:
                del self.store[key]
                self.timestamps[key] = 2.0
                return True
            return False

    def get_with_timestamp(self, key):
        with self.lock:
            return self.store.get(key), self.timestamps.get(key, 0.0)

    def validate_and_commit(self, tx):
        with self.lock:
            for k, read_ts in tx.read_set.items():
                if self.timestamps.get(k, 0.0) > read_ts:
                    return False
            for k, v in tx.write_set.items():
                if v is None:
                    self.store.pop(k, None)
                else:
                    self.store[k] = v
                self.timestamps[k] = self.timestamps.get(k, 0.0) + 1.0
            return True


class TestStorageEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_wal_record_crc_integrity(self):
        rec = WALRecord(WALRecord.OP_SET, "cluster:alpha", "active_state")
        raw = rec.serialize()
        decoded, offset = WALRecord.deserialize(raw)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.key, "cluster:alpha")
        self.assertEqual(decoded.value, "active_state")

        # Intentionally corrupt the payload
        corrupted = bytearray(raw)
        corrupted[-1] ^= 0xFF
        with self.assertRaises(ValueError):
            WALRecord.deserialize(bytes(corrupted))

    def test_wal_append_and_recover(self):
        wal_path = os.path.join(self.test_dir, "test.wal")
        wal = WriteAheadLog(wal_path, sync_on_write=True)
        wal.append(WALRecord.OP_SET, "k1", "v1")
        wal.append(WALRecord.OP_SET, "k2", "v2")
        wal.append(WALRecord.OP_DELETE, "k1", None)
        wal.close()

        reader = WriteAheadLog(wal_path)
        records = reader.recover()
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].key, "k1")
        self.assertEqual(records[1].key, "k2")
        self.assertEqual(records[2].op_type, WALRecord.OP_DELETE)
        reader.close()

    def test_sstable_write_and_binary_search(self):
        data_path = os.path.join(self.test_dir, "segment.db")
        idx_path = os.path.join(self.test_dir, "segment.idx")

        data = [("app:01", "val1"), ("app:02", "val2"), ("user:99", "admin")]
        writer = SSTableWriter(data_path, idx_path)
        writer.write(data)

        reader = SSTableReader(data_path, idx_path)
        self.assertTrue(reader.contains("app:01"))
        self.assertEqual(reader.get("app:01"), "val1")
        self.assertEqual(reader.get("user:99"), "admin")
        self.assertIsNone(reader.get("nonexistent"))

    def test_leveled_compaction(self):
        t1_data = os.path.join(self.test_dir, "t1.db")
        t1_idx = os.path.join(self.test_dir, "t1.idx")
        SSTableWriter(t1_data, t1_idx).write([("key1", "old_val"), ("key2", "retain_val")])

        t2_data = os.path.join(self.test_dir, "t2.db")
        t2_idx = os.path.join(self.test_dir, "t2.idx")
        SSTableWriter(t2_data, t2_idx).write([("key1", "updated_val"), ("key2", None)])

        compactor = LeveledCompactor(self.test_dir)
        r1 = SSTableReader(t1_data, t1_idx)
        r2 = SSTableReader(t2_data, t2_idx)

        out_data, out_idx = compactor.compact([r1, r2], "compacted_run")
        compacted_reader = SSTableReader(out_data, out_idx)

        self.assertEqual(compacted_reader.get("key1"), "updated_val")
        self.assertFalse(compacted_reader.contains("key2"))


class TestClientAndTransactions(unittest.TestCase):
    def setUp(self):
        self.engine = MockEngine()
        self.client = MiniKVClient(self.engine)

    def test_batch_mset_and_mget(self):
        items = {f"k{i}": f"val{i}" for i in range(50)}
        self.client.mset(items)
        fetched = self.client.mget(list(items.keys()))
        self.assertEqual(items, fetched)

    def test_compare_and_swap(self):
        self.client.set("version_key", "1.0.0")
        success = self.client.compare_and_swap("version_key", "1.0.0", "1.1.0")
        self.assertTrue(success)
        self.assertEqual(self.client.get("version_key"), "1.1.0")

        # Stale CAS attempt
        failure = self.client.compare_and_swap("version_key", "1.0.0", "2.0.0")
        self.assertFalse(failure)
        self.assertEqual(self.client.get("version_key"), "1.1.0")

    def test_transaction_commit_and_abort(self):
        self.client.set("account:A", "100")
        self.client.set("account:B", "50")

        tx1 = self.client.begin_transaction()
        tx1.set("account:A", "80")
        tx1.set("account:B", "70")
        tx1.commit()

        self.assertEqual(self.client.get("account:A"), "80")
        self.assertEqual(self.client.get("account:B"), "70")

        # Simulate concurrent write conflict
        tx2 = self.client.begin_transaction()
        tx2.get("account:A")

        # External change
        self.client.set("account:A", "999")

        tx2.set("account:A", "500")
        with self.assertRaises(TransactionAbortedException):
            tx2.commit()


if __name__ == "__main__":
    unittest.main()
