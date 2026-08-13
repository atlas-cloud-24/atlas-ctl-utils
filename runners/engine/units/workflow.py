"""The workflow a run performs."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from engine.run import actions as run_actions
from engine.units import shared as units_shared
from engine.units import target as units_target


@dataclass(frozen=True, kw_only=True)
class WorkflowMember:
    """One target a workflow names, and the action it names it for."""

    target_key: str
    action: str

    @classmethod
    def from_cfg(cls, member: dict | str, *, default_action: str) -> "WorkflowMember":
        """Build from one resolved target run: a mapping, or a bare target key.

        A member may declare its OWN action; the workflow's `default_action` is
        what it replaces, never an override applied over it. A member declaring
        neither is rejected by the catalog, so the fallback is always real.
        """

        if isinstance(member, str):
            return cls(target_key=member, action=default_action)
        return cls(
            target_key=str(member.get("target") or member.get("id") or ""),
            action=str(member.get("action") or default_action),
        )

    @property
    def is_mutating(self) -> bool:
        """Whether this member's action changes anything."""

        return self.action in run_actions.MUTATING_ACTIONS


class ChildSpawnContext(Protocol):
    """What a workflow needs to spawn one member as its own run."""

    def build_command(self, target_key: str, action: str) -> list[str]:
        """The argv that runs this member as a standalone target run."""

    def child_env(self, target_key: str) -> dict[str, str]:
        """The environment the child runs with, carrying its lock grant."""

    def launch(self, argv: list[str], env: dict[str, str]) -> None:
        """Run the child, streaming its output into this run's log."""

    def log(self, target_key: str) -> None:
        """Announce the child about to be spawned."""

    def mark_mutation_started(self) -> None:
        """Record that this run may now have changed something."""

    def latest_revision(self, target_run: dict, action: str) -> units_shared.Revision | None:
        """The revision the child published, read back under its own action."""


class WorkflowContext(Protocol):
    """What a workflow needs to turn its declared members into running targets."""

    def members(self, target_runs: dict, *, action: str) -> Sequence[units_target.Target]:
        """The members to run, in declaration order."""

    def for_target(self, target: units_target.Target) -> units_target.TargetContext:
        """The context that target runs inside."""


@dataclass(frozen=True, kw_only=True)
class Workflow:
    """One workflow of one run: its members, and what they act as.

    Built from a resolved workflow cfg, so a caller holding one has a workflow
    the catalog already accepted.
    """

    key: str
    action: str
    members: tuple[WorkflowMember, ...] = ()
    instance_params: dict[str, str] | None = None

    @classmethod
    def from_cfg(cls, key: str, workflow_cfg: dict, *, action: str) -> "Workflow":
        """Build from the RESOLVED workflow cfg, whose members are `target_runs`.

        The workflow's own `default_action` is what a member without one takes;
        the run's action stands in only where the cfg declares none, which the
        catalog does not allow for a workflow.
        """

        default_action = str(workflow_cfg.get("default_action") or action or "")
        return cls(
            key=key,
            action=action,
            members=tuple(
                WorkflowMember.from_cfg(member, default_action=default_action)
                for member in (workflow_cfg.get("target_runs") or ())
            ),
            instance_params=workflow_cfg.get("instance_params"),
        )

    @property
    def member_actions(self) -> tuple[str, ...]:
        """Every action its members perform, in declaration order."""

        return tuple(dict.fromkeys(member.action for member in self.members))

    @property
    def is_mutating(self) -> bool:
        """Whether any member changes anything."""

        return any(member.is_mutating for member in self.members)

    def to_document(self) -> dict:
        """Render for the run record."""

        document: dict = {"workflow": self.key, "action": self.action}
        if self.instance_params:
            document["workflow_instance_params"] = self.instance_params
        return document

    def spawn_child(
        self, target_run: dict, *, action: str, context: ChildSpawnContext
    ) -> units_shared.Revision | None:
        """Spawn one member as its own target run, and return its revision.

        The workflow's OWN slot records the mutation: this path spawns and
        returns, so a workflow would otherwise report `mutation_started: false`
        however much its children changed. Marked BEFORE the child runs — from
        here resources may change, and claiming possible damage beats denying it.
        """

        target_key = target_run.get("target")
        argv = context.build_command(target_key, target_run.get("action"))
        context.log(target_key)
        if action in run_actions.MUTATING_ACTIONS:
            context.mark_mutation_started()
        context.launch(argv, context.child_env(target_key))
        return context.latest_revision(target_run, action)

    def run(self, target_runs: dict, *, context: WorkflowContext) -> list[units_shared.Revision]:
        """Run every member in declaration order, collecting their revisions.

        Order is the workflow's own semantics: a member may consume what an
        earlier one produced, so the sequence is not an implementation detail of
        whoever executes them.
        """

        revisions: list[units_shared.Revision] = []
        for target in context.members(target_runs, action=self.action):
            revision = target.run(context=context.for_target(target))
            if revision is not None:
                revisions.append(revision)
        return revisions
