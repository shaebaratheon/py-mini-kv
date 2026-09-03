"""Multi-Version Concurrency Control (MVCC) and Transaction Manager."""

from typing import Dict, List, Optional, Set, Tuple
import threading
import time
from enum import Enum

class TxnState(Enum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3

class MVCCVersion:
    def __init__(self, value: Optional[str], create_tx: int, expire_tx: int = float("inf")):
        self.value = value
        self.create_tx = create_tx
        self.expire_tx = expire_tx

class Transaction:
    def __init__(self, txn_id: int, snapshot_tx: int, manager: "MVCCManager"):
        self.txn_id = txn_id
        self.snapshot_tx = snapshot_tx
        self.manager = manager
        self.state = TxnState.ACTIVE
        self.write_set: Dict[str, Optional[str]] = {}
        self.read_set: Set[str] = set()

    def get(self, key: str) -> Optional[str]:
        if key in self.write_set:
            return self.write_set[key]
        self.read_set.add(key)
        return self.manager.read_version(key, self.snapshot_tx)

    def put(self, key: str, value: str):
        self.write_set[key] = value

    def delete(self, key: str):
        self.write_set[key] = None

    def commit(self) -> bool:
        return self.manager.commit_txn(self)

    def abort(self):
        self.manager.abort_txn(self)

class MVCCManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._tx_counter = 0
        self._versions: Dict[str, List[MVCCVersion]] = {}
        self._active_txns: Dict[int, Transaction] = {}

    def begin(self) -> Transaction:
        with self._lock:
            self._tx_counter += 1
            txn_id = self._tx_counter
            txn = Transaction(txn_id=txn_id, snapshot_tx=txn_id, manager=self)
            self._active_txns[txn_id] = txn
            return txn

    def read_version(self, key: str, snapshot_tx: int) -> Optional[str]:
        with self._lock:
            versions = self._versions.get(key, [])
            for ver in reversed(versions):
                if ver.create_tx <= snapshot_tx and snapshot_tx < ver.expire_tx:
                    return ver.value
            return None

    def commit_txn(self, txn: Transaction) -> bool:
        with self._lock:
            if txn.state != TxnState.ACTIVE:
                return False
            # Check write-write conflicts
            for key in txn.write_set:
                versions = self._versions.get(key, [])
                if versions and versions[-1].create_tx > txn.snapshot_tx:
                    txn.state = TxnState.ABORTED
                    return False
            # Apply writes
            for key, val in txn.write_set.items():
                if key not in self._versions:
                    self._versions[key] = []
                if self._versions[key]:
                    self._versions[key][-1].expire_tx = txn.txn_id
                self._versions[key].append(MVCCVersion(val, txn.txn_id))
            txn.state = TxnState.COMMITTED
            self._active_txns.pop(txn.txn_id, None)
            return True

    def abort_txn(self, txn: Transaction):
        with self._lock:
            txn.state = TxnState.ABORTED
            self._active_txns.pop(txn.txn_id, None)
