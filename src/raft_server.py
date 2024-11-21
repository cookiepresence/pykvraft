import enum
import dataclasses
from typing import Optional, List, Dict, Any, Tuple
import threading
import time
import random
import logging
import json
import concurrent.futures

import rpc

class ServerStatus(enum.Enum):
    """
    Represents the current state of a Raft server.

    Each server in the Raft cluster can be in one of three states:
    - Leader:    Responsible for managing the cluster and handling client
                 requests.
    - Follower:  Passive state that responds to requests from leaders and
                 candidates.
    - Candidate: State during which a server attempts to become the leader by
                 soliciting votes.

    Attributes:
        Follower (str):  Indicates the server is a follower.
        Candidate (str): Indicates the server is a candidate.
        Leader (str):    Indicates the server is the leader.
    """

    Follower = "Follower"
    Candidate = "Candidate"
    Leader = "Leader"


@dataclasses.dataclass
class PersistantServerState:
    """
    Stores the persistent state of a Raft server that must be maintained across
    server restarts.

    Attributes:
        current_term (int): The latest term the server has seen. Initialized to
                            0 and increases monotonically.
        voted_for (Optional[str]): The candidate ID that received the server's
                                   vote in the current term. Set to None if no
                                   vote has been cast in the current term.
        log (List[Dict[str, Any]]): The log entries. Each entry is a dictionary
                                    containing a command for the state machine
                                    and the term when the entry was received by
                                    the leader. The first index is atleast 1.
    """

    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[Dict[str, Any]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class VolatileLeaderState:
    """
    Stores the volatile state of the leader that can be recreated after each
    election.

    Attributes:
        next_index (Dict[int, int]): For each follower, the index of the next
                                     log entry to send. Initialized to
                                     leader's last log index + 1.
        match_index (Dict[int, int]): For each follower, the index of the
                                      highest log entry known to be replicated.
                                      Initialized to 0 and increases
                                      monotonically.
    """

    next_index: Dict[int, int]
    match_index: Dict[int, int]


@dataclasses.dataclass
class ServerState:
    """
    Combines both persistent and volatile state of a Raft server.

    Attributes:
        persistant_state (PersistantServerState): The persistent state that is
                                                  stored on durable storage.
        leader_state (Optional[VolatileLeaderState]): The volatile state
                                                      specific to the leader.
                                                      Set to None if the server
                                                      is not a leader.
        commit_index (int): The index of the highest log entry known to be
                            committed. Initialized to 0 and increases
                            monotonically.
        last_applied (int): The index of the highest log entry applied to the
                            state machine. Initialized to 0 and increases
                            monotonically.
    """

    persistant_state: PersistantServerState
    leader_state: Optional[VolatileLeaderState] = None
    commit_index: int = 0
    last_applied: int = 0


class KeyValueStore:
    """
    A thread-safe singleton key-value store acting as the state machine for the
    Raft consensus algorithm.

    This store maintains the key-value pairs that represent the state of the
    distributed system. It ensures that all modifications to the state are
    thread-safe and consistent across different nodes.

    Attributes:
        _instance (Optional[KeyValueStore]): The singleton instance of the
                                             KeyValueStore.
        _lock (threading.Lock): A class-level lock to ensure thread-safe
                                singleton instantiation.
        store (Dict[str, str]): The dictionary storing key-value pairs.
        lock (threading.Lock): An instance-level lock to ensure thread-safe
                               access to the store.
    """

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.store: Dict[str, str] = {}
        self.lock = threading.Lock()

    @classmethod
    def instance(cls):
        """
        Retrieves the singleton instance of the KeyValueStore.
        If the instance does not exist, it creates one in a thread-safe manner.

        Returns:
            KeyValueStore: The singleton instance of the KeyValueStore.
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def set(self, key: str, value: str):
        """
        Sets the value for a given key in the key-value store.
        This method is thread-safe and logs the operation for auditing purposes.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.
        """
        with self.lock:
            self.store[key] = value
            logging.info(f"KV Store: Set {key} = {value}")

    def get(self, key: str) -> Optional[str]:
        """
        Retrieves the value associated with a given key from the key-value store.
        This method is thread-safe and logs the operation for auditing purposes.

        Args:
            key (str): The key whose value is to be retrieved.

        Returns:
            Optional[str]: The value associated with the key, or None if the
                           key does not exist.
        """
        with self.lock:
            value = self.store.get(key)
            logging.info(f"KV Store: Get {key} = {value}")
            return value


class RaftServer:
    """
    Implements the Raft consensus algorithm to manage a distributed key-value
    store.

    This server handles leader election, log replication, and state machine
    application to ensure consistency across a cluster of nodes. It interacts
    with other nodes via RPCs and maintains both persistent and volatile states
    as defined by the Raft protocol.

    Attributes:
        node (Node): The singleton instance representing the current node in
                     the cluster.
        node_id (str): The unique identifier for this Raft server.
        peers (List[int]): A list of peer ports representing other nodes in the
                           cluster.
        status (ServerStatus): The current status of the server
                               (Leader, Follower, or Candidate).
        state (ServerState): The combined persistent and volatile state of the
                             server.
        leader_id (Optional[str]): The ID of the current leader, if any.
        election_timeout (float): The timestamp at which the server should
                                  start a new election if no heartbeats are
                                  received.
        heartbeat_interval (float): The interval (in seconds) at which the
                                    leader sends heartbeats to followers.
        lock (threading.Lock): A lock to ensure thread-safe operations within
                               the server.
        election_timer (threading.Thread): The thread responsible for
                                           monitoring election timeouts.
        heartbeat_timer (threading.Thread): The thread responsible for sending
                                            heartbeats when the server is the
                                            leader.
    """

    def __init__(self, node_id: str, peers: List[int], min_election_timeout=1.0, max_election_timeout=2.0):
        """
        Initializes the RaftServer with the given node ID and peer ports.

        Sets the server's initial state to Follower and starts the election and
        heartbeat timers. Registers RPC endpoints for handling incoming
        RequestVote and AppendEntries messages.

        Args:
            node_id (str): The unique identifier for this Raft server.
            peers (List[int]): A list of peer ports representing other nodes
                               in the cluster.
            min_election_timeout (float): The minimum time (in seconds) for a
                                          term to last without receiving any
                                          messages, after which an election
                                          would start. (default: 1.0 seconds)
            max_election_timeout (float): The maximum time (in seconds) for a
                                          term to last without receiving any
                                          messages, after which an election
                                          would start. (default: 2.0 seconds)
        """
        # TODO: separate it out into a separate class
        self.node_id: str = node_id
        self.peers: List[int] = peers
        self.running = True

        self.status: ServerStatus = ServerStatus.Follower
        self.state: ServerState = ServerState(persistant_state=PersistantServerState())

        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout

        self.leader_id: Optional[str] = None
        self.election_timeout: float = self.reset_election_timeout()
        self.heartbeat_interval: float = 0.1

        self.lock = threading.RLock()

        self.election_timer = threading.Thread(
            target=self.run_election_timer, daemon=True
        )
        self.election_timer.start()

        # TODO: Do not start the heartbeat thread whenever, it should
        # be only when the leader is elected
        # self.heartbeat_timer = threading.Thread(
        #     target=self.run_heartbeat_timer, daemon=True
        # )
        # self.heartbeat_timer.start()

        # requrired for the RPC setup
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if callable(attr) and hasattr(attr, "rpc_bind_handler"):
                attr.rpc_bind_handler(self)

        # self.node.register_endpoint("RequestVote", self.handle_request_vote)
        # self.node.register_endpoint("AppendEntries", self.handle_append_entries)

    def reset_election_timeout(self) -> float:
        """
        Resets the election timeout by setting it to a future timestamp.

        The election timeout is randomized to reduce the likelihood of split
        votes during leader elections.

        Returns:
            float: The new election timeout timestamp.
        """
        # TODO: Ideally, do not rely on the system clock. time.time() is not
        # monotonic or precise
        return time.time() + random.uniform(self.min_election_timeout, self.max_election_timeout)

    def run_election_timer(self):
        """
        Monitors the election timeout and initiates a new election if the
        timeout is reached.

        This method runs in a separate thread and continuously checks whether
        the current time has exceeded the election timeout. If so, it triggers
        the election process.
        """
        logging.info("starting election timer!")
        while self.running:
            # NOTE: likely bug! the election blocks the running of the timer, killing it
            # in the process. ideally, we would want the election to be on a separate
            # thread. The election thread can run independently, and only change states
            # by acquiring locks
            with self.lock:
                # TODO: change to monotonic timers
                if time.time() >= self.election_timeout:
                    self.start_election()
            time.sleep(0.05)

    def run_heartbeat_timer(self):
        """
        Sends periodic heartbeats to all followers if the server is the leader.

        This method runs in a separate thread and continuously checks the
        server's status. If the server is the leader, it sends heartbeats at
        the defined heartbeat interval.
        """
        while self.running:
            with self.lock:
                if self.status == ServerStatus.Leader:
                    self.send_heartbeats()
            time.sleep(self.heartbeat_interval)

    def is_incoming_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Checks if incoming log is atleast as up-to-date as the local log.

        [ref: Raft paper, section 5.4.1, pg 8]
        Raft determines which of two logs is more up-to-date
        by comparing the index and term of the last entries in the
        logs. If the logs have last entries with different terms, then
        the log with the later term is more up-to-date. If the logs
        end with the same term, then whichever log is longer is
        more up-to-date.

        Args:
             last_log_index (int): Last log index of the incoming log.
             last_log_term (int):  Term of the last log index of the
                                   incoming node.
        """
        with self.lock:
            self_last_log_term = (
                self.state.persistant_state.log[-1]["term"]
                if self.state.persistant_state.log
                else 0
            )
            self_last_log_index = (
                len(self.state.persistant_state.log)
                if self.state.persistant_state.log
                else 0
            )
            # If the logs have last entries with different terms, then
            # the log with the later term is more up-to-date.
            if self_last_log_term < last_log_term:
                logging.debug(
                    "incoming log is up-to-date with more terms "
                    f"[{last_log_term} terms vs {self_last_log_term} terms]"
                )
                return True
            # If the logs end with the same term, then whichever log is longer
            # is more up-to-date.
            if (
                    self_last_log_term == last_log_term and
                    self_last_log_index <= last_log_index
            ):
                logging.debug(
                    "incoming log is more up-to-date with more entries "
                    f"[{last_log_index} entries vs {self_last_log_index} entries]"
                )
                return True
            # Since none of the earlier conditions match, our own log is more up
            # to date.
            logging.debug(
                "incoming log is out-of-date with less terms and less entries "
                f"[{last_log_term} terms vs {self_last_log_term} terms] "
                f"[{last_log_index} entries vs {self_last_log_index} entries]"
            )
            return False

    @rpc.rpc_call(is_class_method=True)
    def RequestVote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> Tuple[int, bool]:
        with self.lock:
            current_term = self.state.persistant_state.current_term
            voted_for = self.state.persistant_state.voted_for

            # (§5.1) Reply false if term < current term
            if term < current_term:
                logging.debug(
                    f"Vote rejected for {candidate_id} in term {term} "
                    f"(current term {current_term})"
                )
                return (current_term, False)

            # (§5.1) [All servers] If RPC request or response contains
            # term T > currentTerm:
            # set currentTerm = T, convert to follower
            if term > current_term:
                logging.debug(
                    "Updating"
                    f" term {current_term} -> {term};"
                    f" state {self.status} -> {ServerStatus.Follower}"
                )
                self.state.persistant_state.current_term = term
                self.state.persistant_state.voted_for = None
                self.status = ServerStatus.Follower
                current_term = term
                voted_for = None

            # (§5.2, §5.4) If votedFor is null or candidateId,...
            if (voted_for is not None and voted_for != candidate_id):
                logging.info(
                    f"Vote rejected for {candidate_id} "
                    f"(Already voted for {voted_for} in {current_term})")
                return (current_term, False)
            # ...and candidate's log is atleast as up-to-date as receiver's
            # log: grant vote
            if ((voted_for is None
                 or voted_for == candidate_id) and
                self.is_incoming_log_up_to_date(last_log_index, last_log_term)):
                logging.info(f"Vote granted to {candidate_id}")
                self.reset_election_timeout()
                return (current_term, True)
            else:
                logging.info(
                    f"Vote rejected for {candidate_id} "
                    f"(incoming log out of date wrt our own log)"
                )
                return (current_term, False)


    def start_election(self):
        """
        Initiates a new election by transitioning the server to the Candidate
        state.

        The server increments its current term, votes for itself, and solicits
        votes from peers. If a majority of votes is obtained, it becomes the
        leader; otherwise, it reverts to Follower.

        ref: Raft paper, Section 5.2, pg 5
        """
        #  If a follower receives no communication over a period of time
        # called the election timeout, then it assumes there is no vi-
        # able leader and begins an election to choose a new leader.
        self.reset_election_timeout()

        # To begin an election, a follower increments its current term...
        self.state.persistant_state.current_term += 1
        current_term = self.state.persistant_state.current_term
        # ...and transitions to candidate state
        self.status = ServerStatus.Candidate
        # It then votes for itself...
        self.state.persistant_state.voted_for = self.node_id
        # note: implicit vote counted for self.
        votes_granted = 0

        logging.info(
            f"Node {self.node_id} is now a Candidate for term "
            f"{self.state.persistant_state.current_term}"
        )

        num_peers = len(self.peers)
        last_log_index = len(self.state.persistant_state.log)
        last_log_term = (
            self.state.persistant_state.log[-1]["term"]
            if last_log_index > 0
            else 0
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for peer, vote in zip(self.peers, executor.map(
                    self.RequestVote,
                    self.peers,
                    [self.state.persistant_state.current_term] * num_peers,
                    [self.node_id] * num_peers,
                    [last_log_index] * num_peers,
                    [last_log_term] * num_peers
            )):
                if vote is not None:
                    (term, vote) = vote
                    # (§5.1) [All servers] If RPC request or response contains
                    # term T > currentTerm:
                    # set currentTerm = T, convert to follower
                    if term > current_term:
                        # TODO: Check if the state has not drifted [acquire lock]...
                        self.status = ServerStatus.Follower
                        logging.debug(
                            f"breaking out of the election and returning to Follower "
                            f"(received {term},"
                            f" greater than {self.state.persistant_state.current_term})"
                        )
                    logging.info(f"vote recieved from {peer}")
                    votes_granted += 1 if vote else 0
                    # If at any point we have enough votes for a majority, we
                    # can proceed with the leader election
                    if votes_granted > len(self.peers) // 2:
                        break
                else:
                    # Erorred out mid-way, ignore the result
                    pass

        # A candidate wins an election if it receives votes from
        # a majority of the servers in the full cluster for the same term
        if votes_granted > len(self.peers) // 2:
            # TODO: acquire lock!
            self.status = ServerStatus.Leader
            self.leader_id = self.node_id
            logging.info(
                f"elected Leader for term {self.state.persistant_state.current_term}"
            )
            self.state.leader_state = VolatileLeaderState(
                next_index={
                    peer: len(self.state.persistant_state.log) + 1
                    for peer in self.peers
                },
                match_index={peer: 0 for peer in self.peers},
            )
            # TODO: Turn this back on!!
            # self.send_heartbeats()
        else:
            # election failed due to lack of majority (either because of split vote,
            # or because of the election of another leader)
            # TODO: make sure that we are acquiring the lock
            self.status = ServerStatus.Follower
            logging.info(
                f"failed to attain majority in term {self.state.persistant_state.current_term}"
            )
            self.reset_election_timeout()

    def send_heartbeats(self):
        """
        Sends heartbeat messages (empty AppendEntries RPCs) to all followers.

        Heartbeats are used by the leader to maintain authority and prevent
        followers from initiating new elections. This method spawns separate
        threads to send heartbeats concurrently to each peer.
        """
        logging.debug(f"Node {self.node_id} sending heartbeats to peers")
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()

    def send_append_entries(self, peer_port):
        """
        Sends an AppendEntries RPC to a specified follower to replicate the
        leader's log.

        This method constructs the payload for the AppendEntries RPC, including
        the current term, leader ID, previous log index and term, any new
        entries, and the leader's commit index. It then sends the RPC to the
        designated peer and processes the response.

        Args:
            peer_port (int): The port number of the follower to send the
                             AppendEntries RPC to.
        """
        if self.status != ServerStatus.Leader:
            return
        prev_log_index = self.state.leader_state.next_index.get(peer_port, 1) - 1
        prev_log_term = (
            self.state.persistant_state.log[prev_log_index - 1]["term"]
            if prev_log_index > 0
            else 0
        )
        entries = []
        payload = {
            "term": self.state.persistant_state.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.state.commit_index,
        }
        response = self.node.send_message(
            peer_port, "AppendEntries", json.dumps(payload).encode()
        )
        if response:
            resp = json.loads(response.decode())
            if resp.get("success"):
                self.state.leader_state.match_index[peer_port] = prev_log_index
                self.state.leader_state.next_index[peer_port] = prev_log_index + 1
                logging.info(
                    f"Node {self.node_id} successfully replicated to peer {peer_port}"
                )
            else:
                if resp.get("term", 0) > self.state.persistant_state.current_term:
                    with self.lock:
                        self.state.persistant_state.current_term = resp["term"]
                        self.status = ServerStatus.Follower
                        self.state.persistant_state.voted_for = None
                        logging.info(
                            f"Node {self.node_id} found higher term {resp['term']} from peer {peer_port}, reverting to Follower"
                        )

    def handle_append_entries(self, msg: bytes) -> bytes:
        """
        Handles incoming AppendEntries RPCs from the leader.

        This method processes the AppendEntries RPC by verifying the leader's
        term and ensuring log consistency. If the entries are valid, it appends
        them to the local log and updates the commit index accordingly.

        Args:
            msg (bytes): The serialized JSON payload of the AppendEntries RPC.

        Returns:
            bytes: The serialized JSON response indicating whether the append
                   was successful.
        """
        data = json.loads(msg.decode())
        term = data["term"]
        leader_id = data["leader_id"]
        prev_log_index = data["prev_log_index"]
        prev_log_term = data["prev_log_term"]
        entries = data["entries"]
        leader_commit = data["leader_commit"]

        success = False

        with self.lock:
            if term < self.state.persistant_state.current_term:
                success = False
                logging.debug(
                    f"Node {self.node_id} rejects AppendEntries from {leader_id} for term {term} (current term {self.state.persistant_state.current_term})"
                )
            else:
                if term > self.state.persistant_state.current_term:
                    self.state.persistant_state.current_term = term
                    self.state.persistant_state.voted_for = None
                self.leader_id = leader_id
                self.status = ServerStatus.Follower
                self.reset_election_timeout()
                logging.debug(
                    f"Node {self.node_id} received AppendEntries from Leader {leader_id} for term {term}"
                )

                if prev_log_index == 0 or (
                    len(self.state.persistant_state.log) >= prev_log_index
                    and self.state.persistant_state.log[prev_log_index - 1]["term"]
                    == prev_log_term
                ):
                    success = True
                    for entry in entries:
                        index = entry["index"]
                        if len(self.state.persistant_state.log) >= index:
                            if (
                                self.state.persistant_state.log[index - 1]["term"]
                                != entry["term"]
                            ):
                                self.state.persistant_state.log = (
                                    self.state.persistant_state.log[: index - 1]
                                )
                                logging.info(
                                    f"Node {self.node_id} deletes conflicting log entries starting at index {index}"
                                )
                                break
                        else:
                            break
                    for entry in entries:
                        index = entry["index"]
                        if len(self.state.persistant_state.log) < index:
                            self.state.persistant_state.log.append(entry)
                            logging.info(
                                f"Node {self.node_id} appends new entry {entry} at index {index}"
                            )

                    if leader_commit > self.state.commit_index:
                        self.state.commit_index = min(
                            leader_commit, len(self.state.persistant_state.log)
                        )
                        self.apply_committed_entries()
                        logging.info(
                            f"Node {self.node_id} updates commit_index to {self.state.commit_index}"
                        )

        response = {
            "term": self.state.persistant_state.current_term,
            "success": success,
        }
        return json.dumps(response).encode()

    def apply_committed_entries(self):
        """
        Applies committed log entries to the key-value store.

        This method iterates through the log entries that have been committed
        but not yet applied to the state machine. It updates the `last_applied`
        index accordingly and performs the specified actions
        (e.g., setting a key-value pair).

        Raises:
            IndexError: If the log index is out of bounds.
        """
        while self.state.last_applied < self.state.commit_index:
            self.state.last_applied += 1
            entry = self.state.persistant_state.log[self.state.last_applied - 1]
            command = entry["command"]
            kv_store = KeyValueStore.instance()
            if command["action"] == "SET":
                kv_store.set(command["key"], command["value"])
            elif command["action"] == "GET":
                pass  # GET commands do not modify the state
            logging.info(
                f"Node {self.node_id} applied command: {command} at index {self.state.last_applied}"
            )

    def client_set(self, key: str, value: str) -> bool:
        """
        Handles client requests to set a key-value pair in the store.

        Only the leader can process `SET` requests by appending the command to
        its log and initiating replication to followers. If the server is not
        the leader, the request fails.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.

        Returns:
            bool: True if the `SET` operation succeeded, False otherwise.
        """
        if self.status != ServerStatus.Leader:
            logging.debug(
                f"Node {self.node_id} is not the leader (current status: {self.status}). Cannot set key."
            )
            return False

        with self.lock:
            new_entry = {
                "term": self.state.persistant_state.current_term,
                "index": len(self.state.persistant_state.log) + 1,
                "command": {"action": "SET", "key": key, "value": value},
            }
            self.state.persistant_state.log.append(new_entry)
            logging.info(
                f"Node {self.node_id} appended SET command to log: {new_entry}"
            )
            self.state.commit_index += 1
            logging.info(
                f"Node {self.node_id} updates commit_index to {self.state.commit_index}"
            )
            self.apply_committed_entries()
            self.replicate_log_entry(new_entry)
            return True

    def replicate_log_entry(self, entry: Dict[str, Any]):
        """
        Initiates the replication of a single log entry to all followers.

        This method spawns separate threads to concurrently send the log entry
        to each peer.

        Args:
            entry (Dict[str, Any]): The log entry to replicate.
        """
        for peer in self.peers:
            threading.Thread(
                target=self.send_append_entries_with_entry,
                args=(peer, entry),
                daemon=True,
            ).start()

    def send_append_entries_with_entry(self, peer_port: int, entry: Dict[str, Any]):
        """
        Sends an AppendEntries RPC containing a specific log entry to a
        designated follower.

        This method constructs the AppendEntries payload with the new entry and
        sends it to the follower. It then processes the response to update
        replication indices or handle term discrepancies.

        Args:
            peer_port (int): The port number of the follower to send the
                             AppendEntries RPC to.
            entry (Dict[str, Any]): The log entry to include in the
                                    AppendEntries RPC.
        """
        with self.lock:
            prev_log_index = entry["index"] - 1
            prev_log_term = (
                self.state.persistant_state.log[prev_log_index - 1]["term"]
                if prev_log_index > 0
                else 0
            )
            payload = {
                "term": self.state.persistant_state.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [entry],
                "leader_commit": self.state.commit_index,
            }
        response = self.node.send_message(
            peer_port, "AppendEntries", json.dumps(payload).encode()
        )
        if response:
            resp = json.loads(response.decode())
            if resp.get("success"):
                with self.lock:
                    self.state.leader_state.match_index[peer_port] = entry["index"]
                    self.state.leader_state.next_index[peer_port] = entry["index"] + 1
                logging.info(
                    f"Node {self.node_id} successfully replicated to peer {peer_port}"
                )
            else:
                if resp.get("term", 0) > self.state.persistant_state.current_term:
                    with self.lock:
                        self.state.persistant_state.current_term = resp["term"]
                        self.status = ServerStatus.Follower
                        self.state.persistant_state.voted_for = None
                        logging.info(
                            f"Node {self.node_id} found higher term {resp['term']} from peer {peer_port}, reverting to Follower"
                        )
        else:
            logging.error(f"Failed to replicate to peer {peer_port}")

    def client_get(self, key: str) -> Optional[str]:
        """
        Handles client requests to retrieve the value associated with a given
        key.

        This method accesses the key-value store to fetch the value and logs
        the operation.

        Args:
            key (str): The key whose value is to be retrieved.

        Returns:
            Optional[str]: The value associated with the key, or None if the
                           key does not exist.
        """
        kv_store = KeyValueStore.instance()
        value = kv_store.get(key)
        logging.info(f"Node {self.node_id} retrieved key '{key}' with value '{value}'")
        return value

    def force_leader(self):
        """
        Manually promotes this server to the Leader state.

        This method increments the current term to reflect a new leadership,
        updates the server's status to Leader, initializes the volatile leader
        state, and sends immediate heartbeats to inform followers of the new
        leadership. This is primarily used for testing purposes.
        """
        with self.lock:
            self.state.persistant_state.current_term += 1
            self.status = ServerStatus.Leader
            self.leader_id = self.node_id
            self.state.leader_state = VolatileLeaderState(
                next_index={
                    peer: len(self.state.persistant_state.log) + 1
                    for peer in self.peers
                },
                match_index={peer: 0 for peer in self.peers},
            )
            logging.info(
                f"Node {self.node_id} has been manually set as Leader for term {self.state.persistant_state.current_term}"
            )
            self.send_heartbeats()
