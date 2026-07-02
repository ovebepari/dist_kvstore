import time
import threading
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
    import os
    for f in os.listdir("."):
        if f.startswith("raft_node_") and f.endswith(".json"):
            os.remove(f)

    nodes = {}
    for node_id in cluster_ports:
        nodes[node_id] = run_node(node_id, cluster_ports)
    
    print("Waiting for leader election...")
    time.sleep(5)
    
    leader = None
    for node_id, kv in nodes.items():
        if kv.raft_node.state.name == "LEADER":
            leader = kv
            print(f"Node {node_id} is LEADER")
            break
    
    if not leader:
        print("No leader elected!")
    else:
        print("Submitting SET operation...")
        success, _ = leader.put("hello", "world")
        print(f"Put success: {success}")
        
        time.sleep(2)
        
        print("Verifying data on all nodes...")
        for node_id, kv in nodes.items():
            # Force a read even if not leader for testing replication
            with kv.db_lock:
                val = kv.db.get("hello")
                print(f"Node {node_id} value for 'hello': {val}")

    # Shutdown
    for kv in nodes.values():
        kv.shutdown()
