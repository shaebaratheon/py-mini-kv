import json
import os
import time
from datetime import datetime, timedelta

class MiniKV:
    """A simplistic in-memory Key-Value store with JSON persistence."""
    def __init__(self, filename="storage.json"):
        self.filename = filename
        self.data = {}
        self.load()

    def set(self, key, value):
        """Set a key to a value."""
        self.data[key] = {
            "value": value,
            "updated_at": datetime.now().isoformat()
        }
        return True

    def get(self, key):
        """Get a value by key."""
        if key in self.data:
            return self.data[key]["value"]
        return None

    def delete(self, key):
        """Delete a key."""
        if key in self.data:
            del self.data[key]
            return True
        return False

    def persist(self):
        """Persist data to disk."""
        with open(self.filename, 'w') as f:
            # Strip internal metadata before saving if needed, but keeping it simple for now
            json.dump(self.data, f, indent=4)
        return True

    def load(self):
        """Load data from disk."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.data = json.load(f)
            except json.JSONDecodeError:
                self.data = {}
        else:
            self.data = {}

def main():
    db = MiniKV()
    print("MiniKV Initialized.")
    db.set("user:1000", " Alice")
    db.set("config:theme", "dark")
    
    print(f"User: {db.get('user:1000')}")
    print(f"Theme: {db.get('config:theme')}")
    
    db.persist()
    print("Data persisted.")

if __name__ == "__main__":
    main()
