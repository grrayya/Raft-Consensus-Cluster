import json
from typing import Callable, Awaitable, Optional, Dict, Any, List

class StateMachine:
    def __init__(self):
        # The distributed log and current commit tracker
        self.log = []
        self.commit_index = 0
        # The actual database state
        self.kv_store = {}

    def append_entry(self, term: int, command: str, key: str, value: str):
        entry = {"term": term, "cmd": command, "key": key, "val": value}
        self.log.append(entry)
        return len(self.log) - 1

    def apply_commits(self, new_commit_index: int):
        while self.commit_index < new_commit_index:
            self.commit_index += 1
            entry = self.log[self.commit_index - 1]
            
            if entry["cmd"] == "SET":
                self.kv_store[entry["key"]] = entry["val"]
                
        print(f"[State] Machine updated. Current state: {self.kv_store}")
