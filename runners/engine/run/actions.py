"""What an action is: the engine's single description of the five.

Actions are engine-owned, not a consumer value: every branch on an action name
asks a fact declared here, and the interesting one — that provision and destroy
are two directions of ONE state — is the engine's state model.

The names are a `StrEnum`, so a member IS the string it stands for: it compares
equal, hashes the same, indexes the same dicts and serialises the same. What it
adds is that a typo is an `AttributeError` at import rather than a branch that
quietly never fires, and that the set of legal values has one definition instead
of being re-listed at each `in` check.
"""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class Action(StrEnum):
    """The actions a run may perform.

    Declaration order is the order the CLI offers them — the established one,
    rather than the one that would group the mutating pair together.
    """

    PROVISION = "provision"
    PLAN = "plan"
    DESTROY = "destroy"
    READONLY = "readonly"
    MAINTENANCE = "maintenance"


class Group(StrEnum):
    """The status group an action publishes into.

    State is partitioned by group: groups are independent facts and never
    overwrite one another, while provision and destroy are two directions of ONE
    fact and therefore share a group.
    """

    MUTATIVE = "mutative"
    PLAN = "plan"
    READONLY = "readonly"
    MAINTENANCE = "maintenance"


class Direction(StrEnum):
    """An action's side of a forward/reverse pair within one group."""

    FORWARD = "forward"
    REVERSE = "reverse"


class RunType(StrEnum):
    """What kind of thing a run is; the CLI subcommand names it."""

    WORKFLOW = "workflow"
    TARGET = "target"
    PROCEDURE = "procedure"
    MAINTENANCE = "maintenance"
    FAN_OUT = "fan_out"


class ResultKind(StrEnum):
    """What owns state in the central namespace: an instance of one of these.

    Structural names never contain `=`, so a kind can never be mistaken for an
    instance segment when a state path is parsed.
    """

    TARGET = "target"
    WORKFLOW = "workflow"


class MaintenanceAction(StrEnum):
    """Out-of-band operations, which are not runs and hold no run state."""

    UNLOCK_CTL_STATE = "unlock-ctl-state"
    STATUS_SWEEP = "status-sweep"
    HISTORY_PRUNE = "history-prune"
    FORGET = "forget"


@dataclass(frozen=True)
class ActionFacts:
    """What is TRUE of one action. Everything else in this module is derived.

    `mutating` is declared rather than inferred from `direction`, because an
    action may change a resource without having a reverse.
    """

    group: Group
    mutating: bool = False
    direction: Direction | None = None
    previews: Action | None = None


# THE action registry: one row per action, and the only place any of this is
# stated. What was wrong before was not that the actions are hardcoded but that
# they were hardcoded eight times — two verbatim-duplicate name tuples, a group
# map, a group->actions inverse, a group->representative inverse and three
# literal argparse lists, each free to drift from the others.
ACTIONS: dict[Action, ActionFacts] = {
    Action.PROVISION: ActionFacts(Group.MUTATIVE, mutating=True, direction=Direction.FORWARD),
    Action.PLAN: ActionFacts(Group.PLAN, previews=Action.PROVISION),
    Action.DESTROY: ActionFacts(Group.MUTATIVE, mutating=True, direction=Direction.REVERSE),
    Action.READONLY: ActionFacts(Group.READONLY),
    Action.MAINTENANCE: ActionFacts(Group.MAINTENANCE),
}

MAINTENANCE_ACTIONS = tuple(MaintenanceAction)
RUN_TYPES = tuple(RunType)
RESULT_KINDS = tuple(ResultKind)

# Every action name the engine accepts. One tuple, because two of them were
# verbatim copies consulted by different validators.
RUN_ACTIONS = tuple(ACTIONS)
KNOWN_ACTIONS = RUN_ACTIONS

MUTATING_ACTIONS = tuple(name for name, facts in ACTIONS.items() if facts.mutating)

GROUP_BY_ACTION = {name: facts.group for name, facts in ACTIONS.items()}

# Groups in the order their actions are declared, so the tuple is stable.
RESULT_GROUPS = tuple(dict.fromkeys(GROUP_BY_ACTION.values()))

# Group -> the actions publishing into it, the inverse a status filter needs.
STATUS_GROUPS: dict[Group, tuple[Action, ...]] = {
    group: tuple(name for name, g in GROUP_BY_ACTION.items() if g == group)
    for group in RESULT_GROUPS
}

# Kinds that publish history rather than grouped state.
GROUPLESS_KINDS = frozenset({ResultKind.WORKFLOW})

# "What is the strongest thing this workflow does". A composition that provisions
# one target and plans another IS a mutation — the plan does not soften it — so
# mutative wins over everything, and readonly is the weakest claim.
GROUP_PRECEDENCE = (Group.MUTATIVE, Group.MAINTENANCE, Group.PLAN, Group.READONLY)


def action_previewed_by(action: str) -> str:
    """The action whose declarations a preview resolves against.

    `plan` previews `provision`, so a plan run reads the provision variants rather
    than a separate plan block. An action that previews nothing answers itself.
    """

    facts = ACTIONS.get(action)
    return (facts.previews if facts else None) or action


def group_representative_action(group: str) -> str:
    """The one action that speaks for a group.

    A group with a forward/reverse pair is represented by its FORWARD action — the
    state's existence is what a status answers about, and the reverse is that state
    ending. A group with one action is represented by it.
    """

    members = STATUS_GROUPS[group]
    if len(members) == 1:
        return members[0]
    forward = [name for name in members if ACTIONS[name].direction == Direction.FORWARD]
    if len(forward) != 1:
        raise RuntimeError(
            f"❌ group {group!r} holds {len(members)} actions and {len(forward)} "
            "forward direction(s); exactly one action must be the forward side"
        )
    return forward[0]


def action_can_go_stale(action: str) -> bool:
    """Whether a committed record of this action can be outdated by later change.

    Only the forward side of a mutating pair: a destroy record describes an
    instance that is gone, and nothing can make that answer stale.
    """

    facts = ACTIONS.get(action)
    return bool(facts and facts.direction == Direction.FORWARD)


def action_group(action: str) -> str:
    """The group an action publishes into; unknown actions fail loud."""

    group = GROUP_BY_ACTION.get(action)
    if group is None:
        raise RuntimeError(
            f"❌ unknown action {action!r}; expected one of {sorted(GROUP_BY_ACTION)}"
        )
    return group


# One representative action per group, for the compute functions that still take
# an action and derive the group from it. Derived, because a hand-written inverse
# is a second place for the same fact to be wrong — and it WAS wrong: it omitted
# `maintenance`, so `--group maintenance` was refused for rows status produces.
STATUS_GROUP_ACTION = {
    group: group_representative_action(group) for group in RESULT_GROUPS
}


def normalize_result_name(value: str, *, label: str) -> str:
    """Normalize a result key name as a safe relative slash path."""

    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty result name")
    path = Path(value.strip())
    if path.is_absolute() or ".." in path.parts:
        raise RuntimeError(f"❌ {label} must be a relative path without '..': {value}")
    parts = [part for part in path.parts if part not in ("", ".")]
    if not parts:
        raise RuntimeError(f"❌ {label} must contain at least one path segment: {value}")
    return "/".join(parts)
