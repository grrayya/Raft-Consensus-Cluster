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
