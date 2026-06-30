import sys
import time

import grpc

import raft_internal_pb2_grpc
import raft_client_pb2_grpc
from raft_node import RaftNode
from concurrent import futures

NODE_HOSTS = {
    "node1": "node1:50051",
    "node2": "node2:50052",
    "node3": "node3:50053",
    "node4": "node4:50054",
}


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python server.py <node_id> <port>")
        sys.exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2])

    peer_stubs = {}
    for peer_id, peer_host in NODE_HOSTS.items():
        if peer_id == node_id:
            continue
        channel = grpc.insecure_channel(peer_host)
        stub = raft_internal_pb2_grpc.RaftInternalStub(channel)
        peer_stubs[peer_id] = stub

    db_path = f"/app/data/{node_id}.db"

    node = RaftNode(node_id, db_path, peer_stubs)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_internal_pb2_grpc.add_RaftInternalServicer_to_server(node, server)
    raft_client_pb2_grpc.add_RaftClientAPIServicer_to_server(node, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()

    print(f"[{node_id}] Raft node listening on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
