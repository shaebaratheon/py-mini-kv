"""Unit test suite for WAL, B+Tree, StorageEngine, MVCC, and Server."""

import unittest
import tempfile
import shutil
import os
from minikv.storage.wal import WALWriter, WALReader, WALRecord, RecordType
from minikv.storage.btree import BPlusTreeIndex
from minikv.storage.engine import StorageEngine
from minikv.txn.mvcc import MVCCManager

class TestStorageEngineSuite(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_wal_append_and_recovery(self):
        wal_path = os.path.join(self.test_dir, "test.wal")
        writer = WALWriter(wal_path, sync_on_write=True)
        for i in range(100):
            writer.append(WALRecord(RecordType.PUT, f"key_{i}".encode(), f"val_{i}".encode(), txn_id=i))
        writer.close()

        reader = WALReader(wal_path)
        records = list(reader.read_all())
        self.assertEqual(len(records), 100)
        self.assertEqual(records[0].key, b"key_0")
        self.assertEqual(records[99].value, b"val_99")

    def test_btree_ordered_scan(self):
        tree = BPlusTreeIndex(max_keys=4)
        for i in range(50):
            tree.insert(f"k_{i:03d}".encode(), f"v_{i}".encode())
        self.assertEqual(len(tree), 50)
        self.assertEqual(tree.search(b"k_025"), b"v_25")
        
        scanned = list(tree.scan(start_key=b"k_010", end_key=b"k_020"))
        self.assertEqual(len(scanned), 11)
        self.assertEqual(scanned[0][0], b"k_010")
        self.assertEqual(scanned[-1][0], b"k_020")

    def test_storage_engine_crud(self):
        engine = StorageEngine(self.test_dir)
        engine.put("user:100", "Alice")
        engine.put("user:200", "Bob")
        self.assertEqual(engine.get("user:100"), "Alice")
        self.assertEqual(engine.get("user:200"), "Bob")
        self.assertIsNone(engine.get("user:999"))

        # Test scan
        results = engine.scan(prefix="user:")
        self.assertEqual(len(results), 2)

        # Test delete
        self.assertTrue(engine.delete("user:100"))
        self.assertIsNone(engine.get("user:100"))
        engine.close()

        # Test persistence
        engine2 = StorageEngine(self.test_dir)
        self.assertIsNone(engine2.get("user:100"))
        self.assertEqual(engine2.get("user:200"), "Bob")
        engine2.close()

    def test_mvcc_isolation_and_conflict(self):
        manager = MVCCManager()
        tx1 = manager.begin()
        tx2 = manager.begin()

        tx1.put("balance", "100")
        self.assertTrue(tx1.commit())

        # Tx2 was created with snapshot before tx1 commit
        tx3 = manager.begin()
        self.assertEqual(tx3.get("balance"), "100")

        # Conflict test
        tx_a = manager.begin()
        tx_b = manager.begin()
        tx_a.put("counter", "1")
        tx_b.put("counter", "2")
        self.assertTrue(tx_a.commit())
        self.assertFalse(tx_b.commit())  # Write conflict aborts

if __name__ == "__main__":
    unittest.main()
