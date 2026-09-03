"""Comprehensive Distributed Cluster Simulator with Network Partition & Chaos Testing."""

import asyncio
import random
import time
from typing import Dict, List, Optional, Set, Tuple
from minikv.consensus.raft import RaftNode, NodeRole, LogEntry

class NetworkMessage:
    def __init__(self, sender: str, receiver: str, msg_type: str, payload: dict):
        self.sender = sender
        self.receiver = receiver
        self.msg_type = msg_type
        self.payload = payload
        self.delayed_until = 0.0

class SimulatedNetwork:
    def __init__(self, latency_ms_min: float = 1.0, latency_ms_max: float = 10.0, drop_rate: float = 0.0):
        self.latency_ms_min = latency_ms_min
        self.latency_ms_max = latency_ms_max
        self.drop_rate = drop_rate
        self.partitions: Set[Tuple[str, str]] = set()
        self.message_queue: List[NetworkMessage] = []

    def isolate_node(self, node_id: str, all_nodes: List[str]):
        for n in all_nodes:
            if n != node_id:
                self.partitions.add((node_id, n))
                self.partitions.add((n, node_id))

    def heal_partitions(self):
        self.partitions.clear()

    def send(self, sender: str, receiver: str, msg_type: str, payload: dict):
        if (sender, receiver) in self.partitions:
            return  # Dropped by partition
        if random.random() < self.drop_rate:
            return  # Dropped by random loss

        msg = NetworkMessage(sender, receiver, msg_type, payload)
        delay = random.uniform(self.latency_ms_min, self.latency_ms_max) / 1000.0
        msg.delayed_until = time.time() + delay
        self.message_queue.append(msg)

    def deliver_ready(self) -> List[NetworkMessage]:
        now = time.time()
        ready = [m for m in self.message_queue if m.delayed_until <= now]
        self.message_queue = [m for m in self.message_queue if m.delayed_until > now]
        return ready

class RaftCluster:
    def __init__(self, node_ids: List[str]):
        self.node_ids = node_ids
        self.network = SimulatedNetwork()
        self.nodes: Dict[str, RaftNode] = {
            nid: RaftNode(nid, [p for p in node_ids if p != nid])
            for nid in node_ids
        }

    def step(self):
        # 1. Deliver network messages
        for msg in self.network.deliver_ready():
            receiver_node = self.nodes.get(msg.receiver)
            if not receiver_node:
                continue

            if msg.msg_type == "REQUEST_VOTE":
                term, granted = receiver_node.request_vote(
                    msg.payload["term"], msg.payload["candidate_id"],
                    msg.payload["last_log_index"], msg.payload["last_log_term"]
                )
                self.network.send(msg.receiver, msg.sender, "REQUEST_VOTE_RESP", {
                    "term": term, "granted": granted, "from": msg.receiver
                })

            elif msg.msg_type == "APPEND_ENTRIES":
                entries = [LogEntry(**e) for e in msg.payload["entries"]]
                term, success = receiver_node.append_entries(
                    msg.payload["term"], msg.payload["leader_id"],
                    msg.payload["prev_log_index"], msg.payload["prev_log_term"],
                    entries, msg.payload["leader_commit"]
                )
                self.network.send(msg.receiver, msg.sender, "APPEND_ENTRIES_RESP", {
                    "term": term, "success": success, "match_index": len(receiver_node.log)
                })

    def find_leader(self) -> Optional[str]:
        leaders = [nid for nid, node in self.nodes.items() if node.role == NodeRole.LEADER]
        return leaders[0] if leaders else None
