import fnmatch
from typing import List, Iterable

class GlobMatcher:
    @staticmethod
    def match(keys: Iterable[str], pattern: str) -> List[str]:
        if not pattern:
            return []
        matched = [k for k in keys if fnmatch.fnmatchcase(k, pattern)]
        matched.sort()
        return matched
