"""The target a run acts on."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass


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
class TargetContext:
    """Ports a target needs to run its steps."""

    step_context: object
    mark_mutation_started: Callable[[], None]
    banner: Callable[[str], None]


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
        )

    @property
    def is_mutating(self) -> bool:
        """Whether this target's action changes anything."""

        from engine.run import actions as run_actions

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

    def run(self, steps: "Iterable", base_env: dict[str, str], *, context) -> None:
        """Run this target's steps in order, marking a mutation once."""

        marked = False
        for step in steps:
            if self.is_mutating and not marked:
                context.mark_mutation_started()
                marked = True
            context.banner(f"[{self.action}] [{self.key}] [{step.id}]")
            step.run(base_env, context=context.step_context)
