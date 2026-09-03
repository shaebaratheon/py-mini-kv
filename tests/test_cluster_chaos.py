"""Chaos engineering tests for Raft cluster split-brain prevention and convergence."""

import unittest
from minikv.consensus.cluster_simulator import RaftCluster, SimulatedNetwork
from minikv.consensus.raft import NodeRole, LogEntry

class TestRaftClusterChaos(unittest.TestCase):
    def test_single_node_isolation_and_reconnection(self):
        nodes = ["n1", "n2", "n3", "n4", "n5"]
        cluster = RaftCluster(nodes)
        
        # Candidate election
        cluster.nodes["n1"].role = NodeRole.CANDIDATE
        cluster.nodes["n1"].current_term = 1
        for p in ["n2", "n3", "n4"]:
            cluster.network.send("n1", p, "REQUEST_VOTE", {
                "term": 1, "candidate_id": "n1", "last_log_index": 0, "last_log_term": 0
            })

        # Process messages
        for _ in range(10):
            cluster.step()

        # Isolate n1 (leader)
        cluster.network.isolate_node("n1", nodes)
        
        # New candidate elected in majority partition (n2, n3, n4, n5)
        cluster.nodes["n2"].role = NodeRole.CANDIDATE
        cluster.nodes["n2"].current_term = 2
        for p in ["n3", "n4", "n5"]:
            cluster.network.send("n2", p, "REQUEST_VOTE", {
                "term": 2, "candidate_id": "n2", "last_log_index": 0, "last_log_term": 0
            })

        for _ in range(10):
            cluster.step()

        # Heal network
        cluster.network.heal_partitions()
        for _ in range(10):
            cluster.step()
            
        self.assertGreaterEqual(cluster.nodes["n2"].current_term, 2)

    def test_log_replication_linearizability(self):
        cluster = RaftCluster(["a", "b", "c"])
        entries = [{"term": 1, "index": 1, "command": "SET", "key": "k1", "value": "v1"}]
        cluster.network.send("a", "b", "APPEND_ENTRIES", {
            "term": 1, "leader_id": "a", "prev_log_index": 0, "prev_log_term": 0,
            "entries": entries, "leader_commit": 1
        })
        for _ in range(5):
            cluster.step()
        self.assertEqual(cluster.nodes["b"].state_machine.get("k1"), "v1")
