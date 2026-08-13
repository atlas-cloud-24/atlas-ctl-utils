"""What one run was ASKED to do, frozen at the CLI boundary.

The counterpart of `run/selection.py`: a request is the invocation, a selection
is what that invocation resolved. Both are read everywhere and written once.

It exists because the alternative was 33 loose parameters threaded through five
signatures. `commands/pipeline.py` already recorded what that costs, about the
child spec it solved the same way — "passing scattered locals into run_targets is
how a flag gets forgotten and a child silently runs differently".
"""

from dataclasses import dataclass, field, replace
from pathlib import Path


@dataclass(frozen=True, kw_only=True)
class RunRequest:
    """One invocation: where its cfg is, what it runs, and what it may skip."""

    # where cfg comes from
    ctl_cfg_root: Path
    plt_cfg_root: Path | None = None
    guardrails_cfg_root: Path | None = None
    ctl_state_local_root: Path | None = None

    # what to run
    ctl_profile: str
    action: str | None = None
    run_id: str = ""
    workflow_name: str | None = None
    target_name: str | None = None
    procedure_run: dict | None = None

    # how it resolves
    execution_params: dict[str, str] = field(default_factory=dict)
    ctl_ref_policy: str = ""
    execution_runtime_mode: str = ""
    providers: list[str] | tuple[str, ...] = ()
    target_repo_key: str = ""
    require_target_ref: bool = False
    use_local_tooling_cfg: bool = False

    # where it writes
    run_dir: Path | None = None
    artifacts_dir: Path | None = None
    log_file: Path | None = None

    # which provider identity it runs as
    credential_acquisition: str = ""
    provider_options: dict[str, str] | None = None
    execution_access_modes: dict[str, str] | None = None
    credential_refresh_modes: dict | None = None

    # what it is allowed to skip
    agreed_defer_ctl_state_backend_sync: bool = False
    force_skip_ctl_state_backend_sync: bool = False
    force_skip_guardrails: bool = False
    force_skip_full_cfg_validation_gate: bool = False
    force_skip_execution_identity_preflight_check: list[str] | None = None
    skip_children_precheck: bool = False
    skip_up_to_date: bool = False

    # what a parent run already established
    parent_graph_provisions_ctl_state_backend: bool = False
    parent_ctl_state_backend_absence_confirmed: bool = False

    def with_procedure_run(self, procedure_run: dict) -> "RunRequest":
        """The same request, running a repo-local procedure as a synthetic target.

        A procedure names its own source and identity, so the run it asks for is
        only fully known once those arguments are read.
        """

        return replace(self, procedure_run=procedure_run)

    def with_cfg_roots(
        self, *, plt_cfg_root: Path | None, guardrails_cfg_root: Path | None
    ) -> "RunRequest":
        """The same request, once the cfg sources it names have been materialized.

        A workflow or target run DEFERS fetching plt and guardrails cfg until its
        preflight gate has passed, so the request that gate ran under names
        neither of them yet.
        """

        return replace(self, plt_cfg_root=plt_cfg_root, guardrails_cfg_root=guardrails_cfg_root)

    def for_child(
        self,
        *,
        action: str,
        execution_params: dict[str, str],
        workflow_name: str | None = None,
        target_name: str | None = None,
    ) -> "RunRequest":
        """The same request, narrowed to one fan-out child.

        A child differs from its parent in exactly these four: what it runs, and
        the params that pin it. Everything else — cfg roots, skips, provider
        identity — is the batch's and must not drift per child.
        """

        return replace(
            self,
            action=action,
            execution_params=execution_params,
            workflow_name=workflow_name,
            target_name=target_name,
        )

    @classmethod
    def from_args(cls, args, **resolved) -> "RunRequest":
        """Build from parsed CLI arguments, plus what the runner resolved for them.

        Reads every field off `args` by name and lets `resolved` override, so a
        new CLI flag reaches the request without a line here — and a flag that is
        NOT on `args` (a cfg root, a run directory) has to be passed explicitly.
        """

        fields = {
            name: getattr(args, name) for name in cls.__dataclass_fields__ if hasattr(args, name)
        }
        return cls(**{**fields, **resolved})
