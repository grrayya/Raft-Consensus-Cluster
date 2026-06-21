import sys
import asyncio
from node import RaftNode

async def boot_node(node_id: int):
    print(f"\n--- Nexus Cluster Booting: Node {node_id} ---")
    
    # Define the 5-node cluster topology
    cluster_peers = [1, 2, 3, 4, 5]
    if node_id in cluster_peers:
        cluster_peers.remove(node_id) # A node shouldn't connect to itself

    # Initialize and start the Raft daemon
    node = RaftNode(node_id, cluster_peers)
    
    try:
        await node.start()
    except asyncio.CancelledError:
        print(f"\n[Node {node_id}] Shutting down safely.")

if __name__ == "__main__":
    # Ensure the user provided a node ID in the terminal
    if len(sys.argv) < 2:
        print("Error: Node ID required.")
        print("Usage: python main.py <node_id> (e.g., python main.py 1)")
        sys.exit(1)

    target_id = int(sys.argv[1])
    
    # Run the asynchronous event loop
    try:
        asyncio.run(boot_node(target_id))
    except KeyboardInterrupt:
        print(f"\nNode {target_id} forcefully terminated by user.")

# Custom Raft Consensus Cluster (Nexus)

A from-scratch, asynchronous Python implementation of the Raft distributed consensus algorithm. This project demonstrates distributed state machines, leader election, and resilient networking using raw TCP sockets.

## Architecture
- **State Machine:** Key-Value store with distributed logging.
- **RPC Layer:** Serialized `RequestVote` and `AppendEntries` payloads.
- **Transport:** Non-blocking `asyncio` TCP server/client architecture.
- **Daemon:** Background election timers and authority heartbeats.

## How to Test Locally
To simulate a 5-node cluster, you will need to open 5 separate terminal windows.

1. Open Terminal 1 and run: `python main.py 1`
2. Open Terminal 2 and run: `python main.py 2`
3. Open Terminal 3 and run: `python main.py 3`
4. Open Terminal 4 and run: `python main.py 4`
5. Open Terminal 5 and run: `python main.py 5`

Watch the terminals as they hit their randomized timeouts, hold an election, and crown a leader. If you press `Ctrl+C` on the leader's terminal to simulate a server crash, the remaining 4 nodes will automatically hold a new election!
