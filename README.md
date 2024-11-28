# PyKVRaft

PyKVRaft is a Python implementation of a distributed, fault-tolerant Key-Value store based on the Raft consensus protocol. It ensures data consistency and reliability across multiple nodes in a distributed system, handling leader election, log replication, and fault tolerance seamlessly.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Starting Nodes](#starting-nodes)
  - [Interacting with the Cluster](#interacting-with-the-cluster)
- [File Descriptions](#file-descriptions)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Raft Consensus Algorithm**: Ensures consistency and fault tolerance across distributed nodes.
- **Distributed Key-Value Store**: Supports basic `SET` and `GET` operations.
- **Log Compaction and Snapshotting**: Optimizes storage by compacting logs into snapshots.
- **RPC Mechanism**: Facilitates communication between nodes using Remote Procedure Calls.
- **Command-Line Interface**: Interact with the nodes using simple CLI commands.

## Installation

### Prerequisites

- **Python 3.8** or higher

## Usage

### Starting Nodes

To simulate a cluster, you need to start multiple instances of the server, each representing a Raft node. Each node requires a unique `node_id`, a `port`, and a list of peer ports.

**Example: Starting a 3-Node Cluster**

1. **Start Node 1**

   ```bash
   python src/server.py --node-id n1 --port 5000 --peers 5001 5002
   ```

2. **Start Node 2**

   ```bash
   python src/server.py --node-id n2 --port 5001 --peers 5000 5002
   ```

3. **Start Node 3**

   ```bash
   python src/server.py --node-id n3 --port 5002 --peers 5000 5001
   ```

Each command initializes a Raft node with a unique ID and port, specifying the ports of its peers in the cluster.

### Interacting with the Cluster

Once the nodes are running, you can interact with them using the command-line interface provided by each node. Below are the available commands:

- **Set a Key-Value Pair**

  ```bash
  set <key> <value>
  ```

  *Example:*

  ```bash
  set username alice
  ```

- **Get the Value of a Key**

  ```bash
  get <key>
  ```

  *Example:*

  ```bash
  get username
  ```

- **Send a Custom RPC Message**

  ```bash
  send <target_port> to <endpoint> <message>
  ```

  *Example:*

  ```bash
  send 5001 to Hello bob
  ```

- **Force a Node to Become Leader (For Testing)**

  ```bash
  make-leader
  ```

- **Start or Stop Raft Protocol**

  ```bash
  start raft
  stop
  ```

- **Save Current State to a File**

  ```bash
  save <filename>
  ```

  *Example:*

  ```bash
  save n1.save
  ```

- **Exit the Node**

  Press `Ctrl+C` or use the `stop` command.

**Sample Interaction:**

```bash
Node n1> set user_id 12345
SET user_id = 12345 succeeded.

Node n1> get user_id
user_id = 12345
```

## File Descriptions

- **corpus.py**

  - **Location:** `./corpus.py`
  - **Description:** Utility script that generates a `corpus.txt` file containing the content of all Python files in a specified directory and its subdirectories. Useful for analysis or testing purposes.

- **src/node.py**

  - **Location:** `./src/node.py`
  - **Description:** Defines the `Node` class, representing a Raft node. Handles HTTP requests, message routing, and communication between nodes. Implements a singleton pattern to ensure only one instance per node.

- **src/raft_server.py**

  - **Location:** `./src/raft_server.py`
  - **Description:** Implements the core Raft consensus algorithm. Manages server states (Leader, Follower, Candidate), handles leader election, log replication, commit indexing, and snapshot creation. Integrates with the `Node` class for communication.

- **src/rpc.py**

  - **Location:** `./src/rpc.py`
  - **Description:** Provides an RPC (Remote Procedure Call) mechanism using decorators. Facilitates the registration and handling of RPC endpoints, serialization, and deserialization of messages using `msgpack` via `mashumaro`.

- **src/server.py**

  - **Location:** `./src/server.py`
  - **Description:** The main entry point to start a Raft node. Parses command-line arguments to configure the node ID, port, peers, and state persistence options. Initializes the `RaftServer` and provides a command-line interface for interacting with the node.

- **src/verify_snapshot.py**

  - **Location:** `./src/verify_snapshot.py`
  - **Description:** Utility script to verify the integrity and correctness of snapshot files created during log compaction. Checks for proper log truncation and snapshot data consistency.

## Testing

### Verifying Snapshots

Use the `verify_snapshot.py` script to verify the snapshots created by Raft's log compaction.

**Usage:**

```bash
python src/verify_snapshot.py --save-file <path_to_save_file>
```

*Example:*

```bash
python src/verify_snapshot.py --save-file n1.save
```

This will output the verification details of the specified save file, including current term, voted-for candidate, log entries, and snapshot existence.
