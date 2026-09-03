"""Raft Consensus Algorithm Implementation for MiniKV Distributed Clustering."""

import asyncio
import enum
import random
import time
from typing import Dict, List, Optional, Tuple, Set

class NodeRole(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class LogEntry:
    def __init__(self, term: int, index: int, command: str, key: str, value: Optional[str]):
        self.term = term
        self.index = index
        self.command = command
        self.key = key
        self.value = value

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command, "key": self.key, "value": self.value}

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], election_timeout_range: Tuple[float, float] = (0.15, 0.30)):
        self.node_id = node_id
        self.peers = peers
        self.election_timeout_range = election_timeout_range
        self.role = NodeRole.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: Dict[str, int] = {p: 1 for p in peers}
        self.match_index: Dict[str, int] = {p: 0 for p in peers}
        self.state_machine: Dict[str, str] = {}
        self.votes_received: Set[str] = set()
        self.heartbeat_interval = 0.05

    def request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> Tuple[int, bool]:
        if term > self.current_term:
            self.current_term = term
            self.role = NodeRole.FOLLOWER
            self.voted_for = None

        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log)

        log_ok = (last_log_term > my_last_term) or (last_log_term == my_last_term and last_log_index >= my_last_index)
        vote_granted = False
        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            vote_granted = True
            self.voted_for = candidate_id

        return self.current_term, vote_granted

    def append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int, entries: List[LogEntry], leader_commit: int) -> Tuple[int, bool]:
        if term < self.current_term:
            return self.current_term, False

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

        self.role = NodeRole.FOLLOWER

        if prev_log_index > 0:
            if len(self.log) < prev_log_index:
                return self.current_term, False
            if self.log[prev_log_index - 1].term != prev_log_term:
                self.log = self.log[:prev_log_index - 1]
                return self.current_term, False

        self.log = self.log[:prev_log_index] + entries

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self._apply_entries()

        return self.current_term, True

    def _apply_entries(self):
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            if entry.command == "SET":
                self.state_machine[entry.key] = entry.value
            elif entry.command == "DEL":
                self.state_machine.pop(entry.key, None)
            self.last_applied += 1
