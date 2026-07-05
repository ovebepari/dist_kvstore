import time
import threading
import json
import logging
from queue import Queue

from raft.core import RaftNode, RaftState
from raft.storage import Storage
from raft.transport import Transport, TransportServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class DistributedKVStore:
    """
    A Replicated State Machine built on the local 'raft' package.
    """
    def __init__(self, node_id: str, cluster_ports: dict[str, int]):
        # 1. Initialize Raft components
        self.storage = Storage(node_id)
        self.transport = Transport(cluster_ports)
        
        peers = [pid for pid in cluster_ports if pid != node_id]
        self.raft_node = RaftNode(
            node_id=node_id,
            peers=peers,
            transport=self.transport,
            storage=self.storage
        )
        
        # 2. Start the Raft RPC server
        self.server = TransportServer(self.raft_node, cluster_ports[node_id])
        self.server.start()

        # The core internal application state
        self.db: dict[str, str] = {}
        self.last_applied = -1
        self.db_lock = threading.Lock()
        
        # Mapping index -> threading.Event() 
        self.pending_commits: dict[int, threading.Event] = {}
        self.commits_lock = threading.Lock()
        
        # Start the background worker thread that monitors your Raft commit pipeline
        self.running = True
        self.apply_worker = threading.Thread(target=self._run_apply_loop, daemon=True)
        self.apply_worker.start()

    def _run_apply_loop(self):
        """
        The core Replicated State Machine worker. It pulls committed entries from 
        the Raft layer's commit queue and applies them to the DB.
        """
        logging.info("State Machine apply loop running...")
        
        commit_pipeline = self.raft_node.commit_queue 

        while self.running:
            try:
                # Blocks waiting for Raft to signal a committed entry
                entry = commit_pipeline.get(timeout=1.0)
                
                index = entry['index']
                command_bytes = entry['command']
                
                if index <= self.last_applied:
                    continue
                
                # 1. Parse application bytes
                cmd_data = json.loads(command_bytes.decode('utf-8'))
                action = cmd_data.get("action")
                key = cmd_data.get("key")
                val = cmd_data.get("value")
                
                # 2. Execute state transitions
                with self.db_lock:
                    if action == "SET":
                        self.db[key] = val
                    elif action == "DELETE":
                        self.db.pop(key, None)
                        
                self.last_applied = index
                logging.info(f"Applied index {index}: {action} {key}")
                
                # 3. Wake up the specific client thread waiting on this write execution
                with self.commits_lock:
                    if index in self.pending_commits:
                        self.pending_commits[index].set()
                        
            except Exception:
                continue

    def put(self, key: str, value: str, timeout: float = 5.0) -> tuple[bool, str | None]:
        """
        WRITE path: Submits a SET operation to the distributed log.
        Returns: (success_boolean, leader_id_to_redirect_to)
        """
        cmd_payload = json.dumps({"action": "SET", "key": key, "value": value}).encode('utf-8')
        
        # Propose the write to your real Raft layer
        is_leader, index, term = self.raft_node.propose(cmd_payload)
        
        if not is_leader:
            # In this implementation, we don't have an explicit 'leader_id' attribute,
            # but we could potentially find it from heartbeat traffic.
            # For now, return None for leader redirect.
            return False, None
            
        # Create a synchronization fence for this specific index
        commit_event = threading.Event()
        with self.commits_lock:
            self.pending_commits[index] = commit_event
            
        # Block the client thread until the entry is replicated
        logging.info(f"Write assigned index {index}. Waiting for consensus...")
        achieved = commit_event.wait(timeout=timeout)
        
        # Clean up memory mapping
        with self.commits_lock:
            self.pending_commits.pop(index, None)
            
        if not achieved:
            logging.warning(f"⏳ Write timeout on index {index}.")
            return False, None
            
        return True, None

    def delete(self, key: str, timeout: float = 5.0) -> tuple[bool, str | None]:
        """
        DELETE path: Submits a DELETE operation to the distributed log.
        Returns: (success_boolean, leader_id_to_redirect_to)
        """
        cmd_payload = json.dumps({"action": "DELETE", "key": key}).encode('utf-8')
        
        # Propose the delete to your real Raft layer
        is_leader, index, term = self.raft_node.propose(cmd_payload)
        
        if not is_leader:
            return False, None
            
        # Create a synchronization fence for this specific index
        commit_event = threading.Event()
        with self.commits_lock:
            self.pending_commits[index] = commit_event
            
        # Block the client thread until the entry is replicated
        logging.info(f"Delete assigned index {index}. Waiting for consensus...")
        achieved = commit_event.wait(timeout=timeout)
        
        # Clean up memory mapping
        with self.commits_lock:
            self.pending_commits.pop(index, None)
            
        if not achieved:
            logging.warning(f"⏳ Delete timeout on index {index}.")
            return False, None
            
        return True, None

    def get(self, key: str) -> tuple[bool, str | None, str | None]:
        """
        READ path: Implements linearizable lookups.
        Returns: (success_boolean, value, leader_id_to_redirect_to)
        """
        if self.raft_node.state != RaftState.LEADER:
            return False, None, None
            
        with self.db_lock:
            return True, self.db.get(key, None), None

    def shutdown(self):
        self.running = False
        self.raft_node.running = False
        self.server.stop()
        self.apply_worker.join()

if __name__ == "__main__":
    # Example configurations for starting this individual node
    import sys
    if len(sys.argv) < 2:
        print("Usage: python kvstore.py <node_id>")
        sys.exit(1)
        
    my_id = sys.argv[1]
    cluster_ports = {
        "node-1": 8001,
        "node-2": 8002,
        "node-3": 8003
    }
    
    kv_engine = DistributedKVStore(
        node_id=my_id, 
        cluster_ports=cluster_ports
    )
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        kv_engine.shutdown()
