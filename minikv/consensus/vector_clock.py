"""Vector Clocks and Causality Tracking for Distributed Multi-Master MiniKV."""

from typing import Dict, List, Optional, Any
import copy

class VectorClock:
    def __init__(self, node_id: str, initial_clocks: Optional[Dict[str, int]] = None):
        self.node_id = node_id
        self.clocks: Dict[str, int] = initial_clocks.copy() if initial_clocks else {node_id: 0}

    def increment(self):
        self.clocks[self.node_id] = self.clocks.get(self.node_id, 0) + 1

    def update(self, other: "VectorClock"):
        for node, timestamp in other.clocks.items():
            self.clocks[node] = max(self.clocks.get(node, 0), timestamp)
        self.increment()

    def compare(self, other: "VectorClock") -> str:
        """Returns: 'EQUAL', 'BEFORE', 'AFTER', or 'CONCURRENT'"""
        self_greater = False
        other_greater = False

        all_nodes = set(self.clocks.keys()).union(set(other.clocks.keys()))
        for node in all_nodes:
            c1 = self.clocks.get(node, 0)
            c2 = other.clocks.get(node, 0)
            if c1 > c2:
                self_greater = True
            elif c1 < c2:
                other_greater = True

        if self_greater and not other_greater:
            return "AFTER"
        elif other_greater and not self_greater:
            return "BEFORE"
        elif not self_greater and not other_greater:
            return "EQUAL"
        else:
            return "CONCURRENT"

    def to_dict(self) -> Dict[str, int]:
        return copy.deepcopy(self.clocks)
