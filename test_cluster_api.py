"""
Unit test suite for cluster sync and consensus mechanisms in MiniKV.
"""

import unittest
import time
from cluster_sync import ConsensusNode, GossipNode, NodeRole


class DummyStateMachine:
    def __init__(self):
        self.state = {}
    def set(self, k, v):
        self.state[k] = v
    def delete(self, k):
        self.state.pop(k, None)


class TestClusterSync(unittest.TestCase):
    def test_gossip_heartbeat_and_failure_detection(self):
        gossip_a = GossipNode("node-a", {"node-a", "node-b", "node-c"})
        gossip_b = GossipNode("node-b", {"node-a", "node-b", "node-c"})

        gossip_a.heartbeat()
        gossip_b.merge_gossip(gossip_a.members)

        self.assertEqual(gossip_b.members["node-a"]["heartbeat"], 1)
        self.assertEqual(gossip_b.members["node-a"]["status"], "ALIVE")

        # Advance time artificially to verify failure detection
        gossip_b.members["node-c"]["last_seen"] = time.time() - 10.0
        failures = gossip_b.detect_failures(timeout_seconds=2.0)
        self.assertIn("node-c", failures)
        self.assertEqual(gossip_b.members["node-c"]["status"], "DEAD")

    def test_consensus_leader_election(self):
        sm = DummyStateMachine()
        node = ConsensusNode("node-1", ["node-2", "node-3"], sm)
        self.assertEqual(node.role, NodeRole.FOLLOWER)

        # Trigger election directly
        node._start_election()
        self.assertEqual(node.role, NodeRole.LEADER)
        self.assertEqual(node.voted_for, "node-1")

        # Propose state replication
        success = node.propose_command("SET", "cluster:key", "cluster:value")
        self.assertTrue(success)
        self.assertEqual(sm.state.get("cluster:key"), "cluster:value")

        # Propose deletion
        node.propose_command("DELETE", "cluster:key", None)
        self.assertNotIn("cluster:key", sm.state)


if __name__ == "__main__":
    unittest.main()
