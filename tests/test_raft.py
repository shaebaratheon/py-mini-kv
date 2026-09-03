"""Unit test suite for Raft distributed consensus node."""

import unittest
from minikv.consensus.raft import RaftNode, NodeRole, LogEntry

class TestRaftConsensus(unittest.TestCase):
    def test_vote_request_granted(self):
        node = RaftNode("node1", ["node2", "node3"])
        term, granted = node.request_vote(1, "node2", 0, 0)
        self.assertEqual(term, 1)
        self.assertTrue(granted)
        self.assertEqual(node.voted_for, "node2")

    def test_reject_lower_term_vote(self):
        node = RaftNode("node1", ["node2", "node3"])
        node.current_term = 5
        term, granted = node.request_vote(3, "node2", 0, 0)
        self.assertFalse(granted)
        self.assertEqual(term, 5)

    def test_append_entries_success(self):
        node = RaftNode("node1", ["node2", "node3"])
        entries = [LogEntry(1, 1, "SET", "name", "Alice"), LogEntry(1, 2, "SET", "age", "30")]
        term, success = node.append_entries(1, "node2", 0, 0, entries, 2)
        self.assertTrue(success)
        self.assertEqual(len(node.log), 2)
        self.assertEqual(node.state_machine.get("name"), "Alice")
        self.assertEqual(node.state_machine.get("age"), "30")

if __name__ == "__main__":
    unittest.main()
