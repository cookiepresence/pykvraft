import enum
import dataclasses
from typing import Optional, List, Dict

import rpc


class ServerStatus(enum.Enum):
    """
Ref: Figure 4, Section 5 of the Raft Paper.
At any given time each server is in one of three states:
leader, follower, or candidate. In normal operation there
is exactly one leader and all of the other servers are fol-
lowers. Followers are passive: they issue no requests on
their own but simply respond to requests from leaders
and candidates. The leader handles all client requests (if
a client contacts a follower, the follower redirects it to the
leader). The third state, candidate, is used to elect a new
leader as described in Section 5.2.
    """
    # Status on startup.
    Follower = 'Follower'
    Candidate = 'Candidate'
    Leader = 'Leader'


# Separate class so we can serialise and deserialise easily
# instead of worrying about what parts do we need to change
@dataclasses.dataclass
class PersistantServerState:
    # NOTE: All comments are added from Fig 2. of the Raft paper.
    # Any notes of our own will be prepended by a NOTE:

    # latest term server has seen (initialized to 0
    # on first boot, increases monotonically)
    current_term: int = 0
    # candidateId that received vote in current
    # term (or null if none)
    # NOTE: None in Python for denoting no vote
    voted_for: Optional[int] = None
    # log entries; each entry contains command
    # for state machine, and term when entry
    # was received by leader (first index is 1)
    # NOTE: currently a string, would be nice to convert
    #       to nicely typed stuff so we know that there are
    #       no bugs hidden
    log: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class VolatileLeaderState:
    """
    Reinitialised after every election!
    """
    # NOTE: Both next_index and match_index are mappings from
    # server ids to indices respectively. It will be nice to
    # type it out correctly instead of the ad-hoc nonsense that we
    # have right now.

    # for each server, index of the next log entry
    # to send to that server (initialized to leader
    # last log index + 1)
    next_index: Dict[int, int]
    # for each server, index of highest log entry
    # known to be replicated on server
    # (initialized to 0, increases monotonically)
    match_index: Dict[int, int]


@dataclasses.dataclass
class ServerState:
    # (Updated on stable storage before responding to RPCs)
    persistant_state: PersistantServerState
    # (Reinitialized after election)
    # NOTE: We leave it None for any state that is not a leader.
    #       This also implies leaving it undefined on startup, since
    #       by default, every server is a follower on startup
    leader_state: Optional[VolatileLeaderState] = None

    # index of highest log entry known to be
    # committed (initialized to 0, increases
    # monotonically)
    commit_index: int = 0
    # index of highest log entry applied to state
    # machine (initialized to 0, increases
    # monotonically
    last_applied: int = 0


def server_loop():
    # Runs the FSM that controlls the server
    raise NotImplementedError("Yet to implement core server loop")
    pass

