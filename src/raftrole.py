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


current_votes


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
     -1     commit_index    reset only when leader > follower

None        voted_for       reset only when increase term
None {None} current_votes   only not None when become candidate

None {1}    next_index      only not None as leader, initialize to len(log)
None {None} match_index     only not None as leader, initialize to None
"""


"""
import enum


class Role(enum.Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"


# Reset is change to None (or -1 for commit_index), initialize for creating
# dictionary to track state.
class Operation(enum.Enum):
    PASS = "PASS"
    SET_TO_NONE = "SET_TO_NONE"
    INITIALIZE = "INITIALIZE"


# only change from term comparisons - voted_for and role_change
def term_based_changes_pre(source_role: Role, source_term: int, target_role: Role, target_term: int): # to TypedDict
    match (source_role, target_role):
        case (Role.LEADER, Role.FOLLOWER):  # append entry request
            if source_term > target_term:
                target_term = source_term  # term increase, no other change
                voted_for_to_none = True
                role_change_to_follower = False

        case (Role.LEADER, Role.CANDIDATE):  # append entry request, vote response
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

            elif source_term == target_term:
                target_term = source_term
                voted_for_to_none = False
                role_change_to_follower = True

        case (Role.LEADER, Role.LEADER):  # append entry request
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

        case (Role.CANDIDATE, Role.FOLLOWER):  # vote request
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = False

        case (Role.CANDIDATE, Role.CANDIDATE):  # vote request, vote response
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

        case (Role.CANDIDATE, Role.LEADER):  # vote request
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

        case (Role.FOLLOWER, Role.FOLLOWER):  # response to target when target send pre-change
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = False

        case (Role.FOLLOWER, Role.CANDIDATE):  # vote response
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

        case (Role.FOLLOWER, Role.LEADER):  # append entry response
            if source_term > target_term:
                target_term = source_term
                voted_for_to_none = True
                role_change_to_follower = True

        case _:
            raise Exception("Exhaustive switch error.")


def term_based_changes(source_role: Role, source_term: int, target_role: Role, target_term: int): # to TypedDict
    if source_term > target_term:
        target_term = source_term
        voted_for_to_none = True

        if target_role != Role.Follower:
            role_change = (target_role, Role.FOLLOWER)
        else:
            role_change = None

    elif source_term == target_term:
        if source_role == Role.LEADER and target_role == Role.CANDIDATE:
            target_term = None
            voted_for_to_none = False
            role_change = (Role.CANDIDATE, Role.FOLLOWER)

    else:
        # target_term = target_term
        voted_for_to_none = False
        role_change = None


def role_based_changes():  # TypedDict
    match role_change:
        case (Role.FOLLOWER, Role.CANDIDATE):
            current_term += 1
            voted_for: Operation.INITIALIZE
            current_votes: Operation.INITIALIZE

        case (Role.CANDIDATE, Role.LEADER):
            next_index: Operation.INITIALIZE
            match_index: Operation.INITIALIZE

        case (Role.LEADER, Role.FOLLOWER):
            next_index: Operation.SET_TO_NONE
            match_index: Operation.SET_TO_NONE

        case (Role.CANDIDATE, Role.FOLLOWER):
            # voted_for: Operation.SET_TO_NONE - in fact only set to None based on term changes
            current_votes: Operation.SET_TO_NONE

        case _:
            raise Exception("Invalid state change error.")
"""
