"""
starts up - follower

follower
- if timeout, move to candidate

candidate
- if timeout, restart election
- if win election, become leader
- if discovers current leader or higher term, become follower

leader
- if discover higher term, become follower


follower > candidate
- current_term += 1
- voted_for to self
- initialize current_votes
- hold - commit_index
- set to None - next_index, match_index

candidate > leader
- hold current term
- hold voted_for
- hold current_votes
- hold commit_index
- initialize next_index, match_index

leader > follower, because discover higher term
- set term to new higher term
- set voted_for to None
- set current_votes to None
- reset commit_index because not safe
- reset next_index, match_index to None

candidate > follower, because discover new leader with same term
- hold term
- hold voted_for
- set current_votes to None
- hold commit_index
- set to None - next_index, match_index

candidate > follower, because discover new leader with same term
- increase term
- set voted_for to None
- set current_votes to None
- hold commit_index
- set to None - next_index, match_index


     -1     term            increase when see higher, otherwise hold
None        commit_index    reset only when leader > follower

None self   voted_for       reset only when increase term
None {None} current_votes   only not None when become candidate

None {1}    next_index      only not None as leader, initialize to len(log)
None {None} match_index     only not None as leader, initialize to None
"""
from typing import Optional, Tuple, TypedDict
import enum


class Role(enum.Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"
    TIMER = "TIMER"


class Operation(enum.Enum):
    PASS = "PASS"
    RESET_TO_NONE = "RESET_TO_NONE"
    INITIALIZE = "INITIALIZE"


class StateChange(TypedDict):
    role_change: Optional[Tuple[Role, Role]]
    term: int
    next_index: Operation
    match_index: Operation
    commit_index: Operation
    voted_for: Operation
    current_votes: Operation


def evaluate_role_change(
    source_role: Role, source_term: int, target_role: Role, target_term: int
) -> Tuple[Optional[Tuple[Role, Role]], int, Operation]:
    """
    Enumeration of changes from term comparisons.

    - If target_term higher than source_term, change term and reset voted_for,
      change to follower if not follower
    - If target_term equal to source_term and source is leader and target is
      candidate, then change target to follower. Not change term or voted for.
    - If timer is source, then change target from follower to candidate,
      increase term by one and set voted_for to self.
    """
    match (source_role, target_role):
        # append entry request
        case (Role.LEADER, Role.FOLLOWER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = None

        # append entry request, vote response
        case (Role.LEADER, Role.CANDIDATE):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.CANDIDATE, Role.FOLLOWER)

            elif source_term == target_term:
                target_term = source_term
                voted_for = Operation.PASS
                role_change = (Role.CANDIDATE, Role.FOLLOWER)

        # append entry request
        case (Role.LEADER, Role.LEADER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.LEADER, Role.FOLLOWER)

        # vote request
        case (Role.CANDIDATE, Role.FOLLOWER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = None

        # vote request, vote response
        case (Role.CANDIDATE, Role.CANDIDATE):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.CANDIDATE, Role.FOLLOWER)

        # vote request
        case (Role.CANDIDATE, Role.LEADER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.LEADER, Role.FOLLOWER)

        # response to target when target send pre-change
        case (Role.FOLLOWER, Role.FOLLOWER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = None

        # vote response
        case (Role.FOLLOWER, Role.CANDIDATE):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.CANDIDATE, Role.FOLLOWER)

        # append entry response
        case (Role.FOLLOWER, Role.LEADER):
            if source_term > target_term:
                target_term = source_term
                voted_for = Operation.RESET_TO_NONE
                role_change = (Role.LEADER, Role.FOLLOWER)

        # timeout, relevant
        case (Role.TIMER, Role.FOLLOWER):
            target_term += 1
            voted_for = Operation.INITIALIZE
            role_change = (Role.FOLLOWER, Role.CANDIDATE)

        # timeout, not relevant
        case (Role.TIMER, Role.FOLLOWER):
            target_term = target_term
            voted_for = Operation.PASS
            role_change = None

        # timeout, not relevant
        case (Role.TIMER, Role.LEADER):
            target_term = target_term
            voted_for = Operation.PASS
            role_change = None

        case _:
            raise Exception("Exhaustive switch error.")

    return role_change, target_term, voted_for


def evaluate_operations_from_role_change(
    role_change: Optional[Tuple[Role, Role]]
) -> Tuple[Operation, Operation, Operation, Operation]:
    match role_change:
        case (Role.FOLLOWER, Role.CANDIDATE):
            next_index = Operation.PASS
            match_index = Operation.PASS
            commit_index = Operation.PASS
            current_votes = Operation.INITIALIZE

        case (Role.CANDIDATE, Role.LEADER):
            next_index = Operation.INITIALIZE
            match_index = Operation.INITIALIZE
            commit_index = Operation.PASS
            current_votes = Operation.PASS

        case (Role.LEADER, Role.FOLLOWER):
            next_index = Operation.RESET_TO_NONE
            match_index = Operation.RESET_TO_NONE
            commit_index = Operation.RESET_TO_NONE
            current_votes = Operation.RESET_TO_NONE

        case (Role.CANDIDATE, Role.FOLLOWER):
            next_index = Operation.PASS
            match_index = Operation.PASS
            commit_index = Operation.PASS
            current_votes = Operation.RESET_TO_NONE

        case None:
            next_index = Operation.PASS
            match_index = Operation.PASS
            commit_index = Operation.PASS
            current_votes = Operation.PASS

        case _:
            raise Exception("Invalid state change error.")

    return next_index, match_index, commit_index, current_votes


def enumerate_changes(
    source_role: Role, source_term: int, target_role: Role, target_term: int
) -> StateChange:
    role_change, target_term, voted_for = evaluate_role_change(
        source_role, source_term, target_role, target_term
    )

    (
        next_index,
        match_index,
        commit_index,
        current_votes,
    ) = evaluate_operations_from_role_change(role_change)

    return dict(
        role_change=role_change,
        term=target_term,
        next_index=next_index,
        match_index=match_index,
        commit_index=commit_index,
        voted_for=voted_for,
        current_votes=current_votes,
    )
