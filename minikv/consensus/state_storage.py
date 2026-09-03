"""Raft Hard State & Snapshot Persistence Layer."""

import json
import os
from typing import Optional, Dict, Any

class RaftHardState:
    def __init__(self, current_term: int, voted_for: Optional[str]):
        self.current_term = current_term
        self.voted_for = voted_for

    def to_dict(self) -> dict:
        return {"current_term": self.current_term, "voted_for": self.voted_for}

    @classmethod
    def from_dict(cls, data: dict) -> "RaftHardState":
        return cls(data["current_term"], data.get("voted_for"))

class RaftStorageManager:
    def __init__(self, base_dir: str, node_id: str):
        self.base_dir = base_dir
        self.node_id = node_id
        os.makedirs(base_dir, exist_ok=True)
        self.state_file = os.path.join(base_dir, f"raft_state_{node_id}.json")
        self.snapshot_file = os.path.join(base_dir, f"raft_snapshot_{node_id}.json")

    def save_hard_state(self, state: RaftHardState):
        tmp = self.state_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state.to_dict(), f)
        os.replace(tmp, self.state_file)

    def load_hard_state(self) -> Optional[RaftHardState]:
        if not os.path.exists(self.state_file):
            return None
        with open(self.state_file, "r") as f:
            return RaftHardState.from_dict(json.load(f))

    def save_snapshot(self, last_included_index: int, last_included_term: int, data: Dict[str, str]):
        tmp = self.snapshot_file + ".tmp"
        payload = {
            "last_index": last_included_index,
            "last_term": last_included_term,
            "data": data
        }
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, self.snapshot_file)

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.snapshot_file):
            return None
        with open(self.snapshot_file, "r") as f:
            return json.load(f)
