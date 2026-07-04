import time
import threading
import os
from kvstore import DistributedKVStore

def run_node(node_id, cluster_ports):
    print(f"Starting node {node_id}...")
    kv = DistributedKVStore(node_id, cluster_ports)
    return kv

if __name__ == "__main__":
    cluster_ports = {
        "node-1": 8001,
        "node-2": 8002,
        "node-3": 8003
    }
    
    # Clean up old raft files
    for f in os.listdir("."):
        if f.startswith("raft_node_") and f.endswith(".json"):
            os.remove(f)

    nodes = {}
    for node_id in cluster_ports:
        nodes[node_id] = run_node(node_id, cluster_ports)
    
    print("Waiting for leader election (max 15s)...")
    leader = None
    for _ in range(30):
        for node_id, kv in nodes.items():
            if kv.raft_node.state.name == "LEADER":
                leader = kv
                leader_id = node_id
                print(f"Node {node_id} is LEADER")
                break
        if leader:
            break
        time.sleep(0.5)
    
    if not leader:
        print("No leader elected!")
    else:
        print(f"Submitting SET operation to {leader_id}...")
        success, _ = leader.put("hello", "world", timeout=10.0)
        print(f"Put success: {success}")
        
        if success:
            print("Verifying data via GET on all nodes...")
            time.sleep(2) # Give it a moment to replicate
            for node_id, kv in nodes.items():
                # Linearizable GET (only on leader)
                if kv.raft_node.state.name == "LEADER":
                    ok, val, _ = kv.get("hello")
                    print(f"Node {node_id} (LEADER) GET 'hello': {val} (Success: {ok})")
                else:
                    # Direct DB read for verification of replication
                    with kv.db_lock:
                        val = kv.db.get("hello")
                    print(f"Node {node_id} (FOLLOWER) Local DB 'hello': {val}")
        else:
            print("SET operation failed, skipping verification.")

    # Shutdown
    print("Shutting down cluster...")
    for kv in nodes.values():
        kv.shutdown()
    print("Done.")
