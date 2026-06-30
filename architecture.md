# Architecture and Solution Specification

## System Architecture
The system implements a Distributed Key-Value store using the Raft Consensus Algorithm. 
- **Servers:** 4 Python nodes communicating exclusively via gRPC.
- **Persistence:** SQLite database per node (isolated via Docker volumes).
- **Client:** A Node.js TypeScript CLI application.
- **Infrastructure:** Docker Compose orchestrating the 4 server nodes and their respective volume mounts.

## File Structure

```text
.
├── docker-compose.yml
├── proto/
│   └── raft.proto
├── server/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server.py
│   ├── raft_node.py
│   └── database.py
├── client/
│   ├── package.json
│   ├── tsconfig.json
│   └── index.ts
└── README.md
```

### Expected File Contents

#### `proto/raft.proto`

Defines the gRPC services and message structures. Must include `RequestVote`, `AppendEntries`, `PublishData`, and `ConsumeData` RPCs, along with their respective Request/Response messages.

#### `server/` (Python)

* **`Dockerfile`**: Container definition for a Python 3.11+ environment, installing requirements and running the server.
* **`requirements.txt`**: Must include `grpcio`, `grpcio-tools`.
* **`server.py`**: The entry point. Initializes the gRPC server, binds the port, instantiates the Raft node, and starts the gRPC event loop.
* **`raft_node.py`**: Contains the core Raft logic (`State`, `Term`, `Leader Election`, `Heartbeats`, `Log Replication`). Inherits from the generated gRPC servicer class.
* **`database.py`**: SQLite interface. Handles `INSERT` for logs and `UPDATE` for volatile state (`term`, `voted_for`, `commit_index`). Ensures synchronous writes to disk.

#### `client/` (TypeScript)

* **`index.ts`**: Implements the CLI loop. Handles connection to a random node, leader discovery (parsing `leader_id` from failed responses), and redirection logic for `PublishData` and `ConsumeData` requests.

## Coding Style Guidelines

* **Python**: Adhere to PEP 8. Use standard type hinting (`typing` module) for all function arguments and return types. Classes and methods must contain docstrings detailing their purpose and parameters.
* **TypeScript**: Use strict mode (`"strict": true` in `tsconfig.json`). Avoid `any`; use specific interfaces for gRPC payloads. Use ES6+ features (async/await, destructuring).
* **General**: No commented-out code. Error handling must explicitly catch specific exceptions (e.g., `grpc.RpcError`) rather than generic `Exception`.
