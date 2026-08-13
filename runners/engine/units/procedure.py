"""The procedure a target's repo declares, and the request that asks for one."""

from dataclasses import dataclass
from typing import Protocol

from engine.run import actions as run_actions
from engine.units import step as units_step


class StepProgress(Protocol):
    """What a procedure reports as its steps run."""

    def mark_mutation_started(self) -> None:
        """Record that this run may now have changed something."""

    def banner(self, step_id: str) -> None:
        """Announce the step about to run."""


@dataclass(frozen=True, kw_only=True)
class Procedure:
    """The ordered steps one action's procedure declares in the target's repo.

    A step is only reachable through a procedure: every target names exactly one
    `procedure_key`, and this is the single place a step is run.
    """

    key: str
    action: str
    steps: tuple[units_step.Step, ...] = ()

    @property
    def is_mutating(self) -> bool:
        """Whether this procedure's action changes anything."""

        return self.action in run_actions.MUTATING_ACTIONS

    @property
    def step_ids(self) -> tuple[str, ...]:
        """The steps it runs, in order."""

        return tuple(step.id for step in self.steps)

    def run(
        self,
        base_env: dict[str, str],
        *,
        progress: StepProgress,
        step_context: units_step.StepContext,
    ) -> None:
        """Run every step in order.

        The mutation is marked once and BEFORE the first step: from there
        resources may change, and claiming possible damage beats denying it. A
        procedure declaring no steps therefore marks nothing.
        """

        marked = False
        for step in self.steps:
            if self.is_mutating and not marked:
                progress.mark_mutation_started()
                marked = True
            progress.banner(step.id)
            step.run(base_env, context=step_context)


@dataclass(frozen=True, kw_only=True)
class ProcedureRequest:
    """A `ctl.py procedure` invocation: the synthetic target it asks the run to build.

    A procedure run declares its own source and identity instead of resolving a
    target from ctl cfg, and names the targets it may change.
    """

    key: str
    action: str
    source: str = ""
    ref: str = ""
    domain: str = ""
    execution_provider: str = ""
    execution_account: str = ""
    execution_role: str = ""
    affected_target_keys: tuple[str, ...] = ()

    @classmethod
    def from_args(cls, args) -> "ProcedureRequest":
        """Build from the arguments a procedure run was invoked with."""

        return cls(
            key=str(getattr(args, "procedure", "") or ""),
            action=str(getattr(args, "action", "") or ""),
            source=str(getattr(args, "source", "") or ""),
            ref=str(getattr(args, "ref", "") or ""),
            domain=str(getattr(args, "domain", "") or ""),
            execution_provider=str(getattr(args, "execution_provider", "") or ""),
            execution_account=str(getattr(args, "execution_account", "") or ""),
            execution_role=str(getattr(args, "execution_role", "") or ""),
            affected_target_keys=tuple(getattr(args, "affected_target_keys", ()) or ()),
        )

    @property
    def affects_targets(self) -> bool:
        """Whether this procedure names targets it may change."""

        return bool(self.affected_target_keys)

    def request(self) -> dict:
        """The synthetic-target request a procedure run is made of."""

        return {
            "source": self.source,
            "ref": self.ref,
            "domain": self.domain,
            "procedure": self.key,
            "execution_provider": self.execution_provider,
            "execution_account": self.execution_account,
            "execution_role": self.execution_role,
            "affected_target_keys": list(self.affected_target_keys),
        }
