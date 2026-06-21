import json
import asyncio
from typing import Callable, Awaitable, Optional, Dict, Any, List

class StateMachine:
    def __init__(self):
        self.log = []
        self.commit_index = 0
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

class RaftRPC:
    """Constructs the standard RPC payloads defined by the Raft Consensus Algorithm."""

    @staticmethod
    def request_vote(term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> str:
        payload = {
            "rpc_type": "RequestVote",
            "term": term,
            "candidate_id": candidate_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
        }
        return json.dumps(payload)

    @staticmethod
    def append_entries(term: int, leader_id: int, prev_log_index: int, 
                       prev_log_term: int, entries: List[Dict], leader_commit: int) -> str:
        payload = {
            "rpc_type": "AppendEntries",
            "term": term,
            "leader_id": leader_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": leader_commit
        }
        return json.dumps(payload)
