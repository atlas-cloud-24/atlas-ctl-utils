"""What one run resolved from cfg, before anything executes.

Not to be confused with the other two things called a selection: a cfg SELECTOR
match (`run/selectors.py`), and a PLT provider selection (`plt/dispatch.py`).
This one is the run's own — which workflow or target it is, under which
execution context, against which resolved cfg.
"""

from dataclasses import dataclass, field, replace

from engine.run import actions as run_actions


@dataclass(frozen=True, kw_only=True)
class RunSelection:
    """The resolved shape of one run: what it is, and everything it resolved.

    Built at the cfg boundary and read everywhere after, so the resolution is
    stated once instead of being re-derived by whoever needs a piece of it.
    """

    kind: str
    key: str | None
    execution_context: dict
    scope_params: dict = field(default_factory=dict)
    require_commit_refs: bool = False
    workflow_cfg: dict = field(default_factory=dict)
    action_cfg: dict = field(default_factory=dict)
    refs: dict = field(default_factory=dict)
    active_target_runs: dict = field(default_factory=dict)
    provider_adapter: object | None = None
    provider_catalogs: dict | None = None

    @property
    def is_workflow(self) -> bool:
        """Whether this run selected a workflow rather than a single target."""

        return self.kind == run_actions.RunType.WORKFLOW

    def with_provider(self, adapter: object, catalogs: dict) -> "RunSelection":
        """The same selection, bound to the provider it resolved.

        A selection may be built without loading provider catalogs — cfg
        validation needs no live provider — so binding one is a second step and
        answers with a new selection rather than filling in the first.
        """

        return replace(self, provider_adapter=adapter, provider_catalogs=catalogs)
