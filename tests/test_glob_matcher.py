import unittest
from minikv.glob_matcher import GlobMatcher

class TestGlobMatcher(unittest.TestCase):
    def setUp(self):
        self.keys = ["user:1:name", "user:2:name", "user:1:age", "config:timeout", "temp"]

    def test_wildcard_suffix(self):
        res = GlobMatcher.match(self.keys, "user:*:name")
        self.assertEqual(res, ["user:1:name", "user:2:name"])

    def test_question_mark_single_char(self):
        res = GlobMatcher.match(self.keys, "user:?:age")
        self.assertEqual(res, ["user:1:age"])

    def test_no_match(self):
        res = GlobMatcher.match(self.keys, "nonexistent:*")
        self.assertEqual(res, [])

    def test_all_match(self):
        res = GlobMatcher.match(self.keys, "*")
        self.assertEqual(len(res), 5)

if __name__ == "__main__":
    unittest.main()
