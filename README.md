
## Raft Consensus gRPC Implementation

A distributed key-value store implementing the Raft consensus protocol, featuring gRPC interoperability, automatic leader election, log replication, and SQLite-backed persistence. Developed for the Distributed Systems course.

### Prerequisites

* Docker & Docker Compose
* Node.js (v18+)
* Linux environment (e.g., Ubuntu/WSL)

### How to Run

1. **Start the Raft Cluster:**
Open your terminal and build the containers:
```bash
docker-compose up --build

```


This will spin up 4 isolated nodes on ports `50051`, `50052`, `50053`, and `50054`.
2. **Setup the Client:**
In a new terminal window, navigate to the client directory and install dependencies:
```bash
cd client
npm install

```


3. **Run the Client:**
Start the interactive CLI to publish and consume data:
```bash
npm start

```



### Demonstration Scenarios Supported

1. Normal operation (automatic election and log replication).
2. Leader failure and re-election.
3. Node shutdown, restart, and state recovery via SQLite.
4. Replica recovery and targeted log synchronization.
5. Consistent reads (only returning committed data).


