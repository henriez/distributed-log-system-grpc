import hashlib
import random
import threading
import time
from typing import Optional

import grpc

import raft_internal_pb2
import raft_internal_pb2_grpc
import raft_client_pb2
import raft_client_pb2_grpc
from database import RaftDatabase

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

ELECTION_TIMEOUT_MIN = 0.300
ELECTION_TIMEOUT_MAX = 0.600
HEARTBEAT_INTERVAL = 0.100


class RaftNode(
    raft_internal_pb2_grpc.RaftInternalServicer,
    raft_client_pb2_grpc.RaftClientAPIServicer):
    def __init__(self, node_id: str, db_path: str,
                 peer_stubs: dict[str, raft_internal_pb2_grpc.RaftInternalStub]):
        self.node_id = node_id
        self.db = RaftDatabase(db_path)
        self.peer_stubs = peer_stubs

        state = self.db.get_state()
        self.current_term = state["current_term"]
        self.voted_for = state["voted_for"]
        self.commit_index = state["commit_index"]

        logs = self.db.get_logs_from(0)
        if logs:
            self.last_log_index = logs[-1]["log_index"]
            self.last_log_term = logs[-1]["term"]
        else:
            self.last_log_index = 0
            self.last_log_term = 0

        self.state = FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received = 0

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.pending_commits: dict[int, threading.Event] = {}

        self.lock = threading.Lock()
        self.election_timer: Optional[threading.Timer] = None
        self._reset_election_timer()

    def _random_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _reset_election_timer(self) -> None:
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(
            self._random_timeout(), self._start_election
        )
        self.election_timer.daemon = True
        self.election_timer.start()

    def _start_election(self) -> None:
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.votes_received = 1
            print(f"[{self.node_id}] Starting election for term {self.current_term}...", flush=True)
            self.db.update_state(self.current_term, self.voted_for, self.commit_index)
            self._reset_election_timer()

            term = self.current_term
            last_log_index = self.last_log_index
            last_log_term = self.last_log_term

        votes_granted = 1
        votes_needed = 3
        votes_lock = threading.Lock()

        def request_vote(stub: raft_internal_pb2_grpc.RaftInternalStub) -> None:
            nonlocal votes_granted
            try:
                req = raft_internal_pb2.VoteRequest(
                    term=term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term,
                )
                resp = stub.RequestVote(req, timeout=0.05)
                with votes_lock:
                    if resp.term > term:
                        with self.lock:
                            if resp.term > self.current_term:
                                self._become_follower(resp.term)
                    elif resp.vote_granted:
                        votes_granted += 1
            except grpc.RpcError:
                pass

        threads = [
            threading.Thread(target=request_vote, args=(stub,))
            for stub in self.peer_stubs.values()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with votes_lock:
            if votes_granted >= votes_needed:
                with self.lock:
                    if self.state == CANDIDATE:
                        self._become_leader()

    def _become_follower(self, term: int) -> None:
        if self.state != FOLLOWER or self.current_term != term:
            print(f"[{self.node_id}] is now FOLLOWER at term {term}.", flush=True)
        self.state = FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.votes_received = 0
        self.leader_id = None
        self.db.update_state(self.current_term, self.voted_for, self.commit_index)
        self._reset_election_timer()

    def _become_leader(self) -> None:
        self.state = LEADER
        self.leader_id = self.node_id

        if self.election_timer:
            self.election_timer.cancel()

        print(f"[{self.node_id}] *** is now LEADER at term {self.current_term} ***", flush=True)
        
        next_idx = self.last_log_index + 1
        self.next_index = {peer_id: next_idx for peer_id in self.peer_stubs}
        self.match_index = {peer_id: 0 for peer_id in self.peer_stubs}
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _heartbeat_loop(self) -> None:
        while True:
            with self.lock:
                if self.state != LEADER:
                    return
            for peer_id in self.peer_stubs:
                self._send_append_entries(peer_id)
            time.sleep(HEARTBEAT_INTERVAL)

    def _send_append_entries(self, peer_id: str) -> None:
        with self.lock:
            if self.state != LEADER:
                return
            peer = self.peer_stubs[peer_id]
            next_idx = self.next_index[peer_id]
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index > 0:
                entry = self.db.get_log_entry(prev_log_index)
                if entry:
                    prev_log_term = entry["term"]
            entries = self.db.get_logs_from(next_idx)
            entries_pb = [
                raft_internal_pb2.LogEntry(
                    log_index=e["log_index"],
                    term=e["term"],
                    data=e["data"],
                    hash=e["hash"],
                )
                for e in entries
            ]
            req = raft_internal_pb2.AppendRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_pb,
                leader_commit=self.commit_index,
            )

        try:
            resp = peer.AppendEntries(req, timeout=0.05)
        except grpc.RpcError:
            return

        with self.lock:
            if self.state != LEADER:
                return

            if resp.term > self.current_term:
                self._become_follower(resp.term)
                return

            if resp.success:
                if entries_pb:
                    last_new_index = entries_pb[-1].log_index
                    if self.match_index[peer_id] < last_new_index:
                        self.match_index[peer_id] = last_new_index
                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                    self._advance_commit_index()
            else:
                if resp.conflict_term != 0:
                    leader_log = self.db.get_logs_from(0)
                    last_in_conflict_term = None
                    for e in reversed(leader_log):
                        if e["term"] == resp.conflict_term:
                            last_in_conflict_term = e["log_index"]
                            break
                    if last_in_conflict_term is not None:
                        self.next_index[peer_id] = last_in_conflict_term
                    else:
                        self.next_index[peer_id] = resp.conflict_index
                else:
                    self.next_index[peer_id] = resp.conflict_index
                if self.next_index[peer_id] < 1:
                    self.next_index[peer_id] = 1

    def _advance_commit_index(self) -> None:
        if self.state != LEADER:
            return
        match_indices = [self.last_log_index]
        for peer_id in self.peer_stubs:
            match_indices.append(self.match_index.get(peer_id, 0))
        match_indices.sort(reverse=True)
        majority_index = match_indices[2]

        old_commit = self.commit_index
        for n in range(self.commit_index + 1, majority_index + 1):
            entry = self.db.get_log_entry(n)
            if entry and entry["term"] == self.current_term:
                self.commit_index = n
        self.db.update_state(self.current_term, self.voted_for, self.commit_index)

        for n in range(old_commit + 1, self.commit_index + 1):
            print(f"[{self.node_id}] Entry {n} COMMITTED", flush=True)
            event = self.pending_commits.pop(n, None)
            if event:
                event.set()

    def RequestVote(self, request: raft_internal_pb2.VoteRequest,
                    context) -> raft_internal_pb2.VoteResponse:
        with self.lock:
            if request.term < self.current_term:
                return raft_internal_pb2.VoteResponse(
                    term=self.current_term, vote_granted=False
                )

            if request.term > self.current_term:
                self._become_follower(request.term)

            if self.voted_for is not None and self.voted_for != request.candidate_id:
                return raft_internal_pb2.VoteResponse(
                    term=self.current_term, vote_granted=False
                )

            log_ok = (
                request.last_log_term > self.last_log_term
                or (
                    request.last_log_term == self.last_log_term
                    and request.last_log_index >= self.last_log_index
                )
            )
            if not log_ok:
                return raft_internal_pb2.VoteResponse(
                    term=self.current_term, vote_granted=False
                )

            self.voted_for = request.candidate_id
            self.db.update_state(self.current_term, self.voted_for, self.commit_index)
            self._reset_election_timer()
            return raft_internal_pb2.VoteResponse(
                term=self.current_term, vote_granted=True
            )

    def AppendEntries(self, request: raft_internal_pb2.AppendRequest,
                      context) -> raft_internal_pb2.AppendResponse:
        with self.lock:
            if request.term < self.current_term:
                return raft_internal_pb2.AppendResponse(
                    term=self.current_term, success=False,
                    conflict_index=0, conflict_term=0,
                )

            if request.term > self.current_term:
                self._become_follower(request.term)

            self.leader_id = request.leader_id
            self._reset_election_timer()

            if request.prev_log_index > 0:
                entry = self.db.get_log_entry(request.prev_log_index)
                if entry is None:
                    return raft_internal_pb2.AppendResponse(
                        term=self.current_term, success=False,
                        conflict_index=self.last_log_index + 1,
                        conflict_term=0,
                    )
                if entry["term"] != request.prev_log_term:
                    conflict_term = entry["term"]
                    conflict_index = request.prev_log_index
                    all_logs = self.db.get_logs_from(0)
                    for e in all_logs:
                        if e["term"] == conflict_term:
                            conflict_index = e["log_index"]
                            break
                    return raft_internal_pb2.AppendResponse(
                        term=self.current_term, success=False,
                        conflict_index=conflict_index,
                        conflict_term=conflict_term,
                    )

            for entry_pb in request.entries:
                existing = self.db.get_log_entry(entry_pb.log_index)
                if existing is not None and existing["term"] != entry_pb.term:
                    self.db.delete_logs_from(entry_pb.log_index)
                    break

            for entry_pb in request.entries:
                existing = self.db.get_log_entry(entry_pb.log_index)
                if existing is None:
                    prev_hash = "0"
                    if entry_pb.log_index > 1:
                        prev_entry = self.db.get_log_entry(entry_pb.log_index - 1)
                        if prev_entry:
                            prev_hash = prev_entry["hash"]
                    content = str(prev_hash) + str(entry_pb.data)
                    expected_hash = hashlib.sha256(content.encode()).hexdigest()

                    if expected_hash != entry_pb.hash:
                        return raft_internal_pb2.AppendResponse(
                            term=self.current_term, success=False,
                            conflict_index=entry_pb.log_index,
                            conflict_term=0,
                        )

                    self.db.append_log(
                        entry_pb.log_index, entry_pb.term,
                        entry_pb.data, entry_pb.hash,
                    )

            logs = self.db.get_logs_from(0)
            if logs:
                self.last_log_index = logs[-1]["log_index"]
                self.last_log_term = logs[-1]["term"]
            else:
                self.last_log_index = 0
                self.last_log_term = 0

            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, self.last_log_index)
                self.db.update_state(
                    self.current_term, self.voted_for, self.commit_index
                )

            return raft_internal_pb2.AppendResponse(
                term=self.current_term, success=True,
                conflict_index=0, conflict_term=0,
            )

    def PublishData(self, request: raft_client_pb2.PublishRequest,
                    context) -> raft_client_pb2.PublishResponse:
        with self.lock:
            if self.state != LEADER:
                return raft_client_pb2.PublishResponse(
                    success=False, leader_id=self.leader_id or "",
                    message=f"Not leader, redirect to {self.leader_id or 'unknown'}",
                )

            self.last_log_index += 1
            log_index = self.last_log_index
            data = request.data

            prev_hash = "0"
            if log_index > 1:
                prev_entry = self.db.get_log_entry(log_index-1)
                if prev_entry:
                    prev_hash = prev_entry["hash"]

            content = str(prev_hash) + str(data)
            hash = hashlib.sha256(content.encode()).hexdigest()
            self.db.append_log(log_index, self.current_term, data, hash)

            event = threading.Event()
            self.pending_commits[log_index] = event

        committed = event.wait(timeout=2.0)

        with self.lock:
            if self.state != LEADER:
                return raft_client_pb2.PublishResponse(
                    success=False, leader_id=self.leader_id or "",
                    message="No longer leader during replication",
                )
            if not committed:
                self.pending_commits.pop(log_index, None)
                return raft_client_pb2.PublishResponse(
                    success=False, leader_id=self.node_id,
                    message="Replication timeout, entry not committed",
                )
            return raft_client_pb2.PublishResponse(
                success=True, leader_id=self.node_id,
                message="Data published and committed",
            )

    def ConsumeData(self, request: raft_client_pb2.Empty,
                    context) -> raft_client_pb2.ConsumeResponse:
        with self.lock:
            logs = self.db.get_logs_from(1)
            committed = [
                log["data"] for log in logs
                if log["log_index"] <= self.commit_index
            ]
            return raft_client_pb2.ConsumeResponse(
                success=True, leader_id=self.leader_id or "",
                commited_data=committed,
            )
