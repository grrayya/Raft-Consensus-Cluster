import asyncio
import random
import time
from network import TCPTransport, StateMachine, RaftRPC

class RaftNode:
    def __init__(self, node_id: int, peers: list[int]):
        # Identity and cluster layout
        self.node_id = node_id
        self.peers = peers
        
        # Raft state variables
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        
        # Randomized election timeout (crucial to prevent split votes)
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()
        
        # Network and storage wiring
        self.port = 5000 + node_id  # Node 1 gets port 5001, etc.
        self.transport = TCPTransport('127.0.0.1', self.port)
        self.state_machine = StateMachine()

    async def start(self):
        """Boots the TCP server and starts the background Raft daemon."""
        await self.transport.start_listening(self.handle_incoming_rpc)
        await self.listen_for_heartbeats()
