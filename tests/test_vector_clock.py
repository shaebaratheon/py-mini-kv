import unittest
from minikv.consensus.vector_clock import VectorClock

class TestVectorClock(unittest.TestCase):
    def test_causal_ordering(self):
        vc_a = VectorClock("nodeA")
        vc_b = VectorClock("nodeB")

        vc_a.increment() # A:1
        self.assertEqual(vc_a.compare(vc_b), "AFTER")
        self.assertEqual(vc_b.compare(vc_a), "BEFORE")

        vc_b.update(vc_a) # B: A:1, B:1
        self.assertEqual(vc_b.compare(vc_a), "AFTER")

        vc_a.increment() # A: A:2, B:0
        self.assertEqual(vc_a.compare(vc_b), "CONCURRENT")
