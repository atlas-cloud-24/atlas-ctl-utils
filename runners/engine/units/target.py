"""The target a run acts on."""

from dataclasses import dataclass, field
from types import TracebackType
from typing import Protocol

from engine.run import actions as run_actions
from engine.units import procedure as units_procedure
from engine.units import shared as units_shared
from engine.units import step as units_step


@dataclass(frozen=True, kw_only=True)
class TargetSource:
    """Where a target's code comes from, and how firmly it is pinned."""

    repo_key: str
    repo_url: str = ""
    repo_path: str = ""
    branch: str = ""
    commit: str = ""
    source_state: str = ""
    ref_policy: str = ""

    @property
    def is_pinned(self) -> bool:
        """Whether the source is fixed to a commit."""

        return bool(self.commit)

    def to_document(self) -> dict:
        """Render for the run record."""

        return {
            key: value
            for key, value in (
                ("repo_key", self.repo_key),
                ("repo_url", self.repo_url),
                ("repo_path", self.repo_path),
                ("branch", self.branch),
                ("commit", self.commit),
                ("source_state", self.source_state),
                ("ref_policy", self.ref_policy),
            )
            if value
        }


@dataclass(frozen=True, kw_only=True)
class PreparedTarget:
    """What a target needs to run its procedure, once its repo and cfg exist.

    `progress` is separate from `TargetContext` because a target that runs
    nothing here owes none of it, and a port nobody can answer is worse than one
    nobody asks.
    """

    procedure: units_procedure.Procedure
    base_env: dict[str, str]
    step_context: units_step.StepContext
    progress: units_procedure.StepProgress


class TargetContext(Protocol):
    """What a target needs from whoever runs it.

    A context manager, because a target's state slot and log open before its
    first step and close however it ends — including by an exception, which the
    slot must record rather than lose.
    """

    def __enter__(self) -> "TargetContext":
        """Open this target's state slot and log."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        """Close the slot, recording `exc` when the target failed."""

    def prepare(self, target: "Target") -> PreparedTarget | None:
        """What the target needs to run, or nothing when it does not run here."""

    def finish(self, target: "Target") -> units_shared.Revision | None:
        """Publish the result and answer with the revision it published."""


@dataclass(frozen=True, kw_only=True)
class Target:
    """One target of one run: what it is, where it runs, and what it acts on.

    Built from a resolved target run, so a caller holding one has a target the
    selection already accepted.
    """

    key: str
    action: str
    domains: tuple[str, ...] = ()
    modules: tuple[str, ...] = ()
    source: TargetSource | None = None
    execution_identities: dict[str, dict] | None = None
    instance_params: dict[str, str] | None = None
    plt_overlays: tuple[str, ...] = ()
    reuse_committed_result: bool = False
    source_run: dict = field(default_factory=dict)

    @classmethod
    def from_target_run(cls, key: str, target_run: dict) -> "Target":
        """Build from the mapping a selection produced."""

        return cls(
            key=key,
            action=str(target_run.get("action") or ""),
            domains=tuple(target_run.get("domains") or ()),
            modules=tuple(target_run.get("modules") or ()),
            source=TargetSource(
                repo_key=str(target_run.get("target") or key),
                repo_url=str(target_run.get("repo_url") or ""),
                repo_path=str(target_run.get("repo_path") or ""),
                branch=str(target_run.get("branch") or ""),
                commit=str(target_run.get("commit") or ""),
                source_state=str(target_run.get("source_state") or ""),
                ref_policy=str(target_run.get("ref_policy") or ""),
            ),
            execution_identities=target_run.get("execution_identities"),
            instance_params=target_run.get("target_instance_params"),
            plt_overlays=tuple(target_run.get("plt_overlays") or ()),
            reuse_committed_result=bool(target_run.get("reuse_committed_result")),
            source_run=dict(target_run),
        )

    @property
    def is_mutating(self) -> bool:
        """Whether this target's action changes anything."""

        return self.action in run_actions.MUTATING_ACTIONS

    def to_document(self) -> dict:
        """Render for the run record."""

        document: dict = {"target": self.key, "action": self.action}
        if self.domains:
            document["domains"] = list(self.domains)
        if self.modules:
            document["modules"] = list(self.modules)
        if self.source:
            document.update(self.source.to_document())
        if self.execution_identities:
            document["execution_identities"] = self.execution_identities
        if self.instance_params:
            document["target_instance_params"] = self.instance_params
        if self.plt_overlays:
            document["plt_overlays"] = list(self.plt_overlays)
        return document

    def run(self, *, context: TargetContext) -> units_shared.Revision | None:
        """Run this target's procedure, and answer with its revision.

        A target that prepares nothing runs nowhere — reused from a committed
        result, or spawned as its own child run — and its context answers with
        the revision that already exists.
        """

        with context:
            prepared = context.prepare(self)
            if prepared is not None:
                prepared.procedure.run(
                    prepared.base_env,
                    progress=prepared.progress,
                    step_context=prepared.step_context,
                )
            return context.finish(self)
