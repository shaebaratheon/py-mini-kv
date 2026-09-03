"""B+ Tree in-memory and disk index structure supporting point lookups and range scans."""

from typing import List, Tuple, Optional, Any, Iterator
import bisect

class BPlusTreeNode:
    def __init__(self, is_leaf: bool = False, max_keys: int = 16):
        self.is_leaf = is_leaf
        self.max_keys = max_keys
        self.keys: List[bytes] = []
        self.values: List[Any] = []  # For leaf nodes: actual values / offsets; For internal nodes: child pointers
        self.next: Optional["BPlusTreeNode"] = None
        self.prev: Optional["BPlusTreeNode"] = None

    def is_full(self) -> bool:
        return len(self.keys) >= self.max_keys

    def split(self) -> Tuple[bytes, "BPlusTreeNode"]:
        mid = len(self.keys) // 2
        sibling = BPlusTreeNode(is_leaf=self.is_leaf, max_keys=self.max_keys)
        
        if self.is_leaf:
            promoted_key = self.keys[mid]
            sibling.keys = self.keys[mid:]
            sibling.values = self.values[mid:]
            self.keys = self.keys[:mid]
            self.values = self.values[:mid]
            
            sibling.next = self.next
            sibling.prev = self
            if self.next:
                self.next.prev = sibling
            self.next = sibling
            return promoted_key, sibling
        else:
            promoted_key = self.keys[mid]
            sibling.keys = self.keys[mid + 1:]
            sibling.values = self.values[mid + 1:]
            self.keys = self.keys[:mid]
            self.values = self.values[:mid + 1]
            return promoted_key, sibling

class BPlusTreeIndex:
    def __init__(self, max_keys: int = 16):
        self.root = BPlusTreeNode(is_leaf=True, max_keys=max_keys)
        self.max_keys = max_keys
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def insert(self, key: bytes, value: Any):
        split_res = self._insert_recursive(self.root, key, value)
        if split_res:
            promoted_key, new_sibling = split_res
            new_root = BPlusTreeNode(is_leaf=False, max_keys=self.max_keys)
            new_root.keys = [promoted_key]
            new_root.values = [self.root, new_sibling]
            self.root = new_root
        self._size += 1

    def _insert_recursive(self, node: BPlusTreeNode, key: bytes, value: Any) -> Optional[Tuple[bytes, BPlusTreeNode]]:
        if node.is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            if idx < len(node.keys) and node.keys[idx] == key:
                node.values[idx] = value
                self._size -= 1  # Updated existing key
                return None
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
            if node.is_full():
                return node.split()
            return None
        else:
            idx = bisect.bisect_right(node.keys, key)
            child = node.values[idx]
            split_res = self._insert_recursive(child, key, value)
            if split_res:
                promoted_key, new_child = split_res
                idx = bisect.bisect_left(node.keys, promoted_key)
                node.keys.insert(idx, promoted_key)
                node.values.insert(idx + 1, new_child)
                if node.is_full():
                    return node.split()
            return None

    def search(self, key: bytes) -> Optional[Any]:
        curr = self.root
        while not curr.is_leaf:
            idx = bisect.bisect_right(curr.keys, key)
            curr = curr.values[idx]
        idx = bisect.bisect_left(curr.keys, key)
        if idx < len(curr.keys) and curr.keys[idx] == key:
            return curr.values[idx]
        return None

    def scan(self, start_key: Optional[bytes] = None, end_key: Optional[bytes] = None, limit: Optional[int] = None) -> Iterator[Tuple[bytes, Any]]:
        curr = self.root
        while not curr.is_leaf:
            if start_key is not None:
                idx = bisect.bisect_right(curr.keys, start_key)
            else:
                idx = 0
            curr = curr.values[idx]
            
        count = 0
        while curr:
            start_idx = 0
            if start_key is not None:
                start_idx = bisect.bisect_left(curr.keys, start_key)
                start_key = None  # Only apply for first leaf
            for i in range(start_idx, len(curr.keys)):
                k = curr.keys[i]
                if end_key is not None and k > end_key:
                    return
                yield k, curr.values[i]
                count += 1
                if limit and count >= limit:
                    return
            curr = curr.next
