"""
Distributed cluster replication and consensus module for MiniKV.
Implements Raft-inspired log replication, heartbeat monitoring, and gossip membership.
"""

import time
import enum
import queue
import random
import threading
from typing import Dict, List, Optional, Tuple, Set, Any


class NodeRole(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class LogEntry:
    def __init__(self, term: int, index: int, command: str, key: str, value: Optional[str] = None):
        self.term = term
        self.index = index
        self.command = command
        self.key = key
        self.value = value

    def to_dict(self) -> Dict:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "key": self.key,
            "value": self.value,
        }


class ConsensusNode:
    """Represents a single consensus node in a MiniKV cluster."""

    def __init__(self, node_id: str, peers: List[str], state_machine):
        self.node_id = node_id
        self.peers = peers
        self.state_machine = state_machine
        self.role = NodeRole.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0

        # Leader tracking state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.election_timeout = random.uniform(0.15, 0.30)
        self.last_heartbeat = time.time()
        self.is_running = True
        self._lock = threading.RLock()
        self._worker_thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self):
        self._worker_thread.start()

    def stop(self):
        self.is_running = False
        if self._worker_thread.is_alive():
            self._worker_thread.join(timeout=0.5)

    def _run_loop(self):
        while self.is_running:
            with self._lock:
                now = time.time()
                if self.role != NodeRole.LEADER and (now - self.last_heartbeat > self.election_timeout):
                    self._start_election()
                elif self.role == NodeRole.LEADER and (now - self.last_heartbeat > 0.05):
                    self._send_heartbeats()
            time.sleep(0.02)

    def _start_election(self):
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(0.15, 0.30)
        votes = 1

        last_log_idx = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0

        for peer in self.peers:
            if self._request_vote(peer, self.current_term, self.node_id, last_log_idx, last_log_term):
                votes += 1

        if votes > (len(self.peers) + 1) // 2:
            self._become_leader()

    def _become_leader(self):
        self.role = NodeRole.LEADER
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        self._send_heartbeats()

    def _send_heartbeats(self):
        self.last_heartbeat = time.time()
        for peer in self.peers:
            self._append_entries(peer, self.current_term, self.node_id, len(self.log), 0, [], self.commit_index)

    def _request_vote(self, peer: str, term: int, candidate_id: str, last_log_idx: int, last_log_term: int) -> bool:
        # Simulated in-memory RPC
        return True

    def _append_entries(self, peer: str, term: int, leader_id: str, prev_log_idx: int, prev_log_term: int, entries: List[LogEntry], leader_commit: int) -> bool:
        # Simulated in-memory RPC
        return True

    def propose_command(self, command: str, key: str, value: Optional[str]) -> bool:
        with self._lock:
            if self.role != NodeRole.LEADER:
                return False
            entry = LogEntry(self.current_term, len(self.log) + 1, command, key, value)
            self.log.append(entry)
            self._apply_committed_entries()
            return True

    def _apply_committed_entries(self):
        while self.commit_index < len(self.log):
            self.commit_index += 1
            entry = self.log[self.commit_index - 1]
            if entry.command == "SET":
                self.state_machine.set(entry.key, entry.value)
            elif entry.command == "DELETE":
                self.state_machine.delete(entry.key)
            self.last_applied = self.commit_index


class GossipNode:
    """Decentralized failure detection via SWAN-like gossip protocol."""

    def __init__(self, node_id: str, cluster_members: Set[str]):
        self.node_id = node_id
        self.members: Dict[str, Dict[str, Any]] = {
            m: {"status": "ALIVE", "heartbeat": 0, "last_seen": time.time()} for m in cluster_members
        }
        self._lock = threading.Lock()

    def heartbeat(self):
        with self._lock:
            self.members[self.node_id]["heartbeat"] += 1
            self.members[self.node_id]["last_seen"] = time.time()

    def merge_gossip(self, incoming_state: Dict[str, Dict[str, Any]]):
        with self._lock:
            now = time.time()
            for node, state in incoming_state.items():
                if node not in self.members:
                    self.members[node] = state
                else:
                    if state["heartbeat"] > self.members[node]["heartbeat"]:
                        self.members[node]["heartbeat"] = state["heartbeat"]
                        self.members[node]["last_seen"] = now
                        self.members[node]["status"] = "ALIVE"

    def detect_failures(self, timeout_seconds: float = 2.0) -> List[str]:
        failed = []
        with self._lock:
            now = time.time()
            for node, state in self.members.items():
                if node != self.node_id and state["status"] == "ALIVE":
                    if now - state["last_seen"] > timeout_seconds:
                        state["status"] = "DEAD"
                        failed.append(node)
        return failed
