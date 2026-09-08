import threading

class AtomicCounterStore:
    def __init__(self, store):
        self.store = store
        self.lock = threading.Lock()

    def incr(self, key: str, amount: int = 1) -> int:
        with self.lock:
            val = self.store.get(key)
            if val is None:
                new_val = amount
            else:
                try:
                    new_val = int(val) + amount
                except ValueError:
                    raise TypeError(f"Value at {key} is not an integer")
            self.store.put(key, str(new_val))
            return new_val

    def decr(self, key: str, amount: int = 1) -> int:
        return self.incr(key, -amount)
