import json
import os
import time
from datetime import datetime, timedelta

class MiniKV:
    """A simplistic in-memory Key-Value store with JSON persistence and TTL."""
    def __init__(self, filename="storage.json"):
        self.filename = filename
        self.data = {}
        self.load()

    def set(self, key, value, ttl_seconds=None):
        """Set a key to a value with optional Time-To-Live."""
        now = time.time()
        expires_at = now + ttl_seconds if ttl_seconds else None
        
        self.data[key] = {
            "value": value,
            "updated_at": datetime.now().isoformat(),
            "expires_at": expires_at
        }
        return True

    def get(self, key):
        """Get a value by key, returning None if expired."""
        if key in self.data:
            item = self.data[key]
            expires_at = item.get("expires_at")
            
            # Check for expiration
            if expires_at and time.time() > expires_at:
                print(f"Key '{key}' has expired.")
                del self.data[key] # Lazy deletion
                return None
                
            return item["value"]
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
    print("MiniKV Initialized with TTL support.")
    
    # Set with TTL
    db.set("temp_token", "secret123", ttl_seconds=2)
    db.set("user:1000", "Alice")
    
    print(f"Temp Token (Initial): {db.get('temp_token')}")
    print("Waiting 3 seconds for TTL to expire...")
    time.sleep(3)
    print(f"Temp Token (After 3s): {db.get('temp_token')}")
    
    print(f"User: {db.get('user:1000')}")
    
    db.persist()
    print("Data persisted.")

if __name__ == "__main__":
    main()
