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
        
    async def handle_incoming_rpc(self, request: dict) -> dict:
        """Processes incoming network requests from other nodes."""
        rpc_type = request.get("rpc_type")
        
        if rpc_type == "RequestVote":
            # If the candidate has a higher term, vote for them and step down
            if request["term"] > self.current_term:
                self.current_term = request["term"]
                self.state = "follower"
                self.voted_for = request["candidate_id"]
                return {"vote_granted": True, "term": self.current_term}
            
            # Otherwise, reject the vote
            return {"vote_granted": False, "term": self.current_term}
            
        elif rpc_type == "AppendEntries":
            # We received a heartbeat from the leader, reset our death timer!
            self.last_heartbeat = time.time() 
            
            if request["term"] >= self.current_term:
                self.state = "follower"
                self.current_term = request["term"]
                
            return {"success": True, "term": self.current_term}
            
        return {"error": "Unknown RPC"}
    
    async def listen_for_heartbeats(self):
        """Background daemon that monitors leader health and triggers elections."""
        while True:
            await asyncio.sleep(0.1)
            
            # If we are the leader, our job is to send heartbeats, not listen for them
            if self.state == "leader":
                await self.broadcast_heartbeats()
                continue

            # Check if our randomized countdown timer has expired
            elapsed = time.time() - self.last_heartbeat
            if elapsed > self.election_timeout:
                print(f"[Node {self.node_id}] Leader timeout. Initiating election.")
                await self.start_election()

    async def start_election(self):
        """Transitions to candidate, increments term, and requests votes over TCP."""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id  # Vote for ourselves
        votes_received = 1
        self.last_heartbeat = time.time()

        print(f"[Node {self.node_id}] Term {self.current_term}: Requesting votes...")
        
        # 1. Fire concurrent network requests to all peers
        tasks = []
        for peer_id in self.peers:
            target_port = 5000 + peer_id
            payload = RaftRPC.request_vote(self.current_term, self.node_id, len(self.state_machine.log), 0)
            tasks.append(self.transport.send_rpc('127.0.0.1', target_port, payload))
            
        # 2. Wait for all peers to reply (or timeout)
        results = await asyncio.gather(*tasks)
        
        # 3. Tally the incoming votes
        for res in results:
            if res and res.get("vote_granted"):
                votes_received += 1

        # 4. Check if we achieved a majority consensus
        majority = (len(self.peers) + 1) // 2 + 1
        if votes_received >= majority:
            print(f"[Node {self.node_id}] Achieved consensus ({votes_received} votes)! Becoming leader.")
            self.state = "leader"
            await self.broadcast_heartbeats()

    async def broadcast_heartbeats(self):
        """Sends AppendEntries RPCs to all peers to maintain leadership authority."""
        print(f"[Node {self.node_id}] Broadcasting authority pulses...")
        self.last_heartbeat = time.time()
        
        # Fire heartbeat RPCs to every other node in the cluster
        tasks = []
        for peer_id in self.peers:
            target_port = 5000 + peer_id
            
            # Sending an empty 'entries' list acts as a pure heartbeat
            payload = RaftRPC.append_entries(
                term=self.current_term, 
                leader_id=self.node_id, 
                prev_log_index=len(self.state_machine.log), 
                prev_log_term=0, 
                entries=[], 
                leader_commit=self.state_machine.commit_index
            )
            tasks.append(self.transport.send_rpc('127.0.0.1', target_port, payload))
            
        # Send them all concurrently
        await asyncio.gather(*tasks)
