# Distributed KV Store

A fault-tolerant, linearizable Distributed Key-Value Store built from scratch using the Raft Consensus Algorithm.

## Architecture

The system follows the **Replicated State Machine** (RSM) architecture. Each node consists of three main layers:

1.  **Application Layer (`kvstore.py`)**: Implements the KV logic (SET, GET, DELETE) and maintains the in-memory database.
2.  **Consensus Layer (`raft/`)**: Implements the Raft algorithm (Leader Election, Log Replication, Safety).
3.  **Infrastructure Layer**: 
    - **Transport**: Handles TCP-based RPC communication between nodes.
    - **Storage**: Provides persistence for the Raft log and hard state (term, votedFor).

```text
       +---------------------------------------------------+
       |                   Cluster Node                    |
       |                                                   |
       |   +-------------------------------------------+   |
       |   |            Application Layer              |   |
       |   |       (DistributedKVStore.py)             |   |
       |   +----------^-------------------|------------+   |
       |              |                   |                |
       |      (Commit)|           (Propose)                |
       |              |                   |                |
       |   +----------|-------------------v------------+   |
       |   |            Consensus Layer                |   |
       |   |              (RaftNode)                   |   |
       |   +---|-------------------|-----------^-------+   |
       |       |                   |           |           |
       |  (Persist)            (Replicate) (Incoming)      |
       |       |                   |           |           |
       |   +---v---+        +------v-----------|---+       |
       |   |Storage|        |   Transport Layer    |       |
       |   +-------+        +----------|-------^---+       |
       |                               |       |           |
       +-------------------------------|-------|-----------+
                                       |       |
                                  <----|-------|---->
                                   (To Peer Nodes)
```

## Features

- **Leader Election**: Automated failover if the leader becomes unreachable.
- **Log Replication**: Ensures all nodes converge on the same sequence of operations.
- **Linearizability**: Provides strong consistency guarantees for client operations.
- **Persistence**: Recovers state (term, votes, log) after crashes to prevent voting twice in the same term.
- **Synchronous Client API**: `put()` blocks until the cluster reaches consensus.

## Project Structure

- `kvstore/kvstore.py`: Main entry point and application logic.
- `kvstore/raft/`:
    - `core.py`: The Raft Consensus Module.
    - `transport.py`: TCP socket-based RPC implementation.
    - `storage.py`: JSON/Base64 persistent state management.
- `kvstore/test_cluster.py`: Integration test for local cluster simulation.

## Usage

### Prerequisites
- Python 3.10+

### Running a Cluster
You can start individual nodes by specifying their ID. Ensure the `cluster_ports` configuration in `kvstore.py` matches your setup.

```bash
python3 kvstore.py node-1
python3 kvstore.py node-2
python3 kvstore.py node-3
```

### Local Simulation
To test the cluster locally with 3 nodes in a single process:

```bash
python3 test_cluster.py
```

## Client API

The `DistributedKVStore` provides a synchronous API that ensures linearizability by interacting with the Raft leader.

```python
from kvstore import DistributedKVStore

# 1. Initialize
# Connect to the cluster by providing the local node ID and the full cluster map.
cluster = {"node-1": 8001, "node-2": 8002, "node-3": 8003}
kv = DistributedKVStore("node-1", cluster)

# 2. Write Data
# blocks until the operation is replicated to a majority of nodes.
# Returns: (success: bool, leader_id: str | None)
success, leader = kv.put("user:123", "Alice")

# 3. Read Data
# Returns the value from the state machine. Must be called on the leader.
# Returns: (success: bool, value: str | None, leader_id: str | None)
success, value, leader = kv.get("user:123")

# 4. Delete Data
# blocks until the deletion is committed.
# Returns: (success: bool, leader_id: str | None)
success, leader = kv.delete("user:123")

# 5. Shutdown
# Gracefully stops the Raft node and the RPC server.
kv.shutdown()
```

### Note on Linearizability
- **Writes (`put`, `delete`)**: These operations are proposed to the Raft log. The call blocks until the leader has successfully replicated the entry to a majority of the cluster and applied it to the local state machine.
- **Reads (`get`)**: Currently, reads are serviced by the leader's local state machine. In a production environment, this would involve a "read index" or "lease" mechanism to ensure the leader is still current.
- **Redirection**: If an operation is called on a follower, it returns `success=False`. The client is responsible for retrying against the leader (future updates will include automated leader discovery).
