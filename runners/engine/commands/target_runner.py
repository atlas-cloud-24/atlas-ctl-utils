"""Runs the members of one run, in this process or as spawned children.

The composition root the units act inside: it implements every port they
declare, so a unit holds behaviour and this holds the wiring. One object rather
than a closure per port — the run facts a port needs are fields, not captured
loop variables.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType

from engine.catalog import workflow as catalog_workflow
from engine.cfg import materialize as cfg_materialize
from engine.cfg import tooling as cfg_tooling
from engine.execution import run_context as execution_run_context
from engine.kernel import process as kernel_process
from engine.plt import dispatch as plt_dispatch
from engine.state import lifecycle as state_lifecycle
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import sync as state_sync
from engine.units import procedure as units_procedure
from engine.units import step as units_step
from engine.units import target as units_target
from engine.units import workflow as units_workflow


@dataclass(frozen=True, kw_only=True)
class TargetStepContext:
    """One target's step ports, bound to the run and the repo that own them."""

    execution_runtime_mode: str
    execution_context_filename: str
    dispatcher: str
    tooling_mode: str
    origin_cfg_path: Path
    target_cfg_dir: Path
    target_artifacts_dir: Path
    runner: "TargetRunner"
    target_run_id: str
    target_run: dict
    repo_path: Path

    def box_name(self, step_id: str) -> str:
        """The name of the box this step runs in."""

        return kernel_process.step_box_name(self.target_run_id, step_id)

    def launch(self, argv: list[str], env: dict[str, str]) -> None:
        """Run the dispatcher from the target's repo, into this run's log."""

        kernel_process.run_and_log(argv, cwd=self.repo_path, env=env)

    def rebind_credentials(self, step: units_step.Step, env: dict[str, str]) -> None:
        """Refresh the step's provider credentials, if this run refreshes per step."""

        self.runner.rebind_step_credentials(
            step, env, target_run_id=self.target_run_id, target_run=self.target_run
        )


@dataclass(frozen=True, kw_only=True)
class ReusedTargetRun:
    """A member whose committed result is reusable: nothing runs, the published revision stands."""

    revision: dict

    def __enter__(self) -> "ReusedTargetRun":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def prepare(self, target: units_target.Target) -> None:
        """Nothing to prepare: this target already has a result."""

        return None

    def finish(self, target: units_target.Target) -> dict:
        """The revision the earlier run published."""

        return self.revision


@dataclass(frozen=True, kw_only=True)
class SpawnedTargetRun:
    """A member run as its own `ctl.py target` child process.

    It executes under this run's ctl-state lock — flock is exclusive and
    non-blocking, so acquiring it again would fail outright.
    """

    runner: "TargetRunner"
    target_run_id: str
    target_run: dict
    member_action: str

    def __enter__(self) -> "SpawnedTargetRun":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def prepare(self, target: units_target.Target) -> None:
        """Nothing to prepare: the child builds its own cfg, context and log."""

        return None

    def finish(self, target: units_target.Target) -> dict | None:
        """Spawn the child and answer with the revision it published."""

        return self.runner.workflow.spawn_child(
            self.target_run, action=self.member_action, context=self
        )

    def build_command(self, target_key: str, action: str) -> list[str]:
        """The argv that runs this member as a standalone target run."""

        return catalog_workflow.WorkflowChildren.build_command(
            self.runner.child_command_spec,
            target_key,
            parent_run_dir=self.runner.run_dir,
            parent_run_id=self.runner.run_id,
            action=action,
        )

    def child_env(self, target_key: str) -> dict[str, str]:
        """The environment the child runs with, carrying its own lock grant."""

        return {
            **os.environ,
            state_run_store.CHILD_LOCK_GRANT_ENV: state_run_store.mint_child_lock_grant(
                Path(self.runner.child_command_spec["ctl_state_local_root"]),
                child_kind="target",
                child_key=target_key,
            ),
        }

    def launch(self, argv: list[str], env: dict[str, str]) -> None:
        """Run the child, streaming its output into this run's log."""

        kernel_process.run_and_log(argv, cwd=str(self.runner.run_dir), env=env)

    def log(self, target_key: str) -> None:
        """Announce the child about to be spawned."""

        logging.info("Spawning child target run: %s", target_key)

    def mark_mutation_started(self) -> None:
        """Record that this run may now have changed something."""

        self.runner.mark_mutation_started(self.target_run_id)

    def latest_revision(self, target_run: dict, action: str) -> dict | None:
        """The revision the child published, read back under its own action."""

        return state_status.latest_child_revision(
            self.runner.run_dir, target_run, self.runner.execution_context, action
        )


@dataclass(kw_only=True)
class InProcessTargetRun:
    """A member run here: its repo is materialized, its steps run, its slot is published."""

    runner: "TargetRunner"
    target_run_id: str
    target_run: dict
    member_action: str
    repo_path: Path = field(init=False)
    target_env: dict = field(init=False)
    procedure_key: str = field(init=False)
    origin_cfg_path: Path = field(init=False)
    target_cfg_dir: Path = field(init=False)
    target_artifacts_dir: Path = field(init=False)
    state_run_dir: Path = field(init=False)
    instance_address: str | None = field(init=False, default=None)
    log_capture: object = field(init=False, default=None)
    copied_execution_context: bool = field(init=False, default=False)

    def __enter__(self) -> "InProcessTargetRun":
        """Materialize the repo and cfg view, open the state slot, and capture the log.

        Nothing here is covered by `__exit__`: Python skips it when `__enter__`
        raises, which is what keeps a failure BEFORE the slot exists out of it.
        """

        runner = self.runner
        self.repo_path, self.target_env = cfg_materialize.prepare_target_repo(
            self.target_run_id,
            self.target_run,
            runner.run_dir,
            runner.tooling_env,
            secret_store=runner.secret_store,
            provider_adapter=runner.provider_adapter,
            provider_catalogs=runner.provider_catalogs,
            execution_context=runner.execution_context,
            credential_acquisition=runner.credential_acquisition,
            execution_access_modes=runner.execution_access_modes,
            provider_options=runner.provider_options,
        )
        procedure_key = self.target_run.get("procedure")
        if not isinstance(procedure_key, str) or not procedure_key:
            raise RuntimeError(
                f"❌ target run {self.target_run_id!r} must define a non-empty procedure"
            )
        self.procedure_key = procedure_key
        self._prepare_cfg_view()
        self.state_run_dir, self.instance_address = state_lifecycle.begin_workflow_target_run(
            runner.run_dir, self.target_run, runner.execution_context
        )
        self.target_artifacts_dir = (
            self.state_run_dir / "artifacts"
            if self.instance_address is not None
            else runner.run_dir / "artifacts" / "targets" / self.target_run_id
        )
        os.makedirs(self.target_artifacts_dir, exist_ok=True)
        self.copied_execution_context = execution_run_context.ensure_repo_execution_context(
            self.repo_path, runner.execution_context_path
        )
        # Everything this target emits also lands in its own log.
        self.log_capture = state_run_store.target_run_log(
            self.state_run_dir if self.instance_address is not None else None
        )
        self.log_capture.__enter__()
        return self

    def _prepare_cfg_view(self) -> None:
        """Resolve where this target reads its cfg from, and make the resolved dir."""

        runner = self.runner
        provider_selection = self.target_run.get("plt_provider")
        target_view_dir = (
            runner.plt_targets_dir_path
            if (runner.plt_targets_dir_path / "input").is_dir()
            or (runner.plt_targets_dir_path / "selection.yaml").is_file()
            else runner.plt_targets_dir_path / self.target_run_id
        )
        self.origin_cfg_path = target_view_dir / "input"
        self.target_cfg_dir = target_view_dir / "resolved"
        if provider_selection is None:
            if not self.origin_cfg_path.is_dir():
                raise RuntimeError(
                    f"❌ target_run input cfg dir not found for target_run "
                    f"{self.target_run_id!r}: {self.origin_cfg_path}"
                )
            os.makedirs(self.target_cfg_dir, exist_ok=True)
        elif runner.plt_provider_dispatch is None:
            raise RuntimeError("❌ PLT provider selection has no provider dispatcher")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Record a failure in the slot, then release the log and the copied context."""

        if exc is not None and self.instance_address is not None:
            state_lifecycle.finish_workflow_target_run(self.state_run_dir, error=exc)
        self.log_capture.__exit__(None, None, None)
        repo_execution_context_path = (
            self.repo_path / execution_run_context.EXECUTION_CONTEXT_FILENAME
        )
        if self.copied_execution_context and repo_execution_context_path.is_file():
            repo_execution_context_path.unlink()
        return None

    def prepare(self, target: units_target.Target) -> units_target.PreparedTarget:
        """Read the repo's procedure for this action, and bind the ports it runs with."""

        runner = self.runner
        _, repo_steps = cfg_materialize.get_repo_local_steps(
            self.repo_path, self.member_action, self.procedure_key
        )
        procedure = units_procedure.Procedure(
            key=self.procedure_key, action=self.member_action, steps=tuple(repo_steps)
        )
        logging.info(json.dumps(self._run_manifest(procedure.step_ids), indent=4))
        return units_target.PreparedTarget(
            procedure=procedure,
            base_env=self.target_env,
            progress=self,
            step_context=TargetStepContext(
                execution_runtime_mode=runner.execution_runtime_mode,
                execution_context_filename=execution_run_context.EXECUTION_CONTEXT_FILENAME,
                dispatcher=runner.runtime_dispatcher,
                tooling_mode=runner.tooling_mode,
                origin_cfg_path=self.origin_cfg_path,
                target_cfg_dir=self.target_cfg_dir,
                target_artifacts_dir=self.target_artifacts_dir,
                runner=runner,
                target_run_id=self.target_run_id,
                target_run=self.target_run,
                repo_path=self.repo_path,
            ),
        )

    def _run_manifest(self, step_ids: tuple[str, ...]) -> dict:
        """What this target is about to run, for the log."""

        runner = self.runner
        return {
            "run_id": runner.run_id,
            "branch": self.target_run.get("branch"),
            "commit": self.target_run.get("commit"),
            "action": self.member_action,
            "procedure": self.procedure_key,
            "active_steps": list(step_ids),
            **(
                {"origin_cfg": str(self.origin_cfg_path)}
                if self.target_run.get("plt_provider") is None
                else {}
            ),
            "execution_context_file": str(runner.execution_context_path),
            "execution_context_keys": sorted(runner.execution_context),
        }

    def mark_mutation_started(self) -> None:
        """Record that this run may now have changed something."""

        self.runner.mark_mutation_started(self.target_run_id)

    def banner(self, step_id: str) -> None:
        """Announce the step about to run."""

        state_lifecycle.log_target_run_banner(
            f"[{self.member_action}] [{self.target_run_id}] [{step_id}]", ch="-"
        )

    def finish(self, target: units_target.Target) -> dict | None:
        """Publish this target's slice and result, and answer with its revision."""

        state_sync.PUBLICATION.push(f"target_run {self.target_run_id} completed")
        if self.instance_address is None:
            return None
        # Fill the child's target-level slice (cfg, execution context, source
        # refs) now that resolved cfg exists — before the child pointer is published.
        catalog_workflow.WorkflowChildren.populate_slice(
            self.state_run_dir,
            self.target_run,
            self.target_run_id,
            self.runner.plt_targets_dir_path,
            self.runner.execution_context,
        )
        return state_lifecycle.finish_workflow_target_run(self.state_run_dir)


@dataclass(kw_only=True)
class TargetRunner:
    """Runs every member of one run, and answers the ports its units declare."""

    active_target_runs: dict
    run_dir: Path
    plt_targets_dir_path: Path
    execution_context_path: Path
    action: str
    execution_context: dict[str, object]
    run_id: str
    tooling_refs: dict
    use_local_tooling_cfg: bool
    provider_adapter: object
    provider_catalogs: dict
    credential_acquisition: str
    execution_runtime_mode: str
    execution_access_modes: dict[str, str] | None = None
    provider_options: dict[str, str] | None = None
    skip_up_to_date: bool = False
    child_command_spec: dict | None = None
    credential_refresh_modes: dict | None = None
    secret_store: object = None
    plt_provider_dispatch: plt_dispatch.ProviderDispatch | None = None
    tooling_env: dict = field(init=False, default_factory=dict)
    tooling_mode: str = field(init=False, default="")
    runtime_dispatcher: str = field(init=False, default="")
    workflow: units_workflow.Workflow | None = field(init=False, default=None)
    mutation_marked: bool = field(init=False, default=False)

    def run(self) -> None:
        """Clone and run every active target run."""

        os.chdir(self.run_dir)
        self.tooling_env = cfg_tooling.build_tooling_env(self.tooling_refs)
        # CTL owns the execution box. It invokes the ctl-owned runtime dispatcher
        # (run_step.sh) — never a per-target_run run script — passing the box spec
        # the target_run declared (image / docker_build) plus the active runtime
        # and tooling source. The target_run carries only src/step.sh + step.yaml.
        self.runtime_dispatcher = str(
            cfg_materialize.materialize_step_utils(self.run_dir) / "run_step.sh"
        )
        self.tooling_mode = "repo_path" if self.use_local_tooling_cfg else "repo_url"
        self.workflow = units_workflow.Workflow.from_cfg(
            str(self.run_id),
            {"target_runs": list(self.active_target_runs.values())},
            action=self.action,
        )
        revisions = self.workflow.run(self.active_target_runs, context=self)
        if revisions:
            state_run_store.update_run_metadata(self.run_dir, {"child_revisions": revisions})

    def members(self, target_runs: dict, *, action: str) -> list[units_target.Target]:
        """The targets to run, in declaration order.

        A member may declare its OWN action; the run's is the default it
        replaces, never an override applied over it.
        """

        return [
            units_target.Target.from_target_run(
                key, {**target_run, "action": target_run.get("action") or action}
            )
            for key, target_run in target_runs.items()
        ]

    def for_target(self, target: units_target.Target):
        """How this target runs: from its committed result, as a child, or here."""

        target_run = dict(target.source_run)
        state_lifecycle.log_target_run_banner(f"[{target.action}] [{target.key}]")
        if self.skip_up_to_date:
            revision = state_status.up_to_date_child_revision(
                self.run_dir, target_run, self.execution_context, target.action
            )
            if revision is not None:
                logging.info(
                    "Skipping committed target instance %s (published result is reusable)",
                    revision["address"],
                )
                return ReusedTargetRun(revision=revision)
        # A WORKFLOW spawns `ctl.py target` per child, so a target runs by exactly
        # the same path standalone and inside a workflow.
        if (
            self.child_command_spec is not None
            and state_run_store.load_run_metadata(self.run_dir).get("run_type") == "workflow"
        ):
            return SpawnedTargetRun(
                runner=self,
                target_run_id=target.key,
                target_run=target_run,
                member_action=target.action,
            )
        return InProcessTargetRun(
            runner=self,
            target_run_id=target.key,
            target_run=target_run,
            member_action=target.action,
        )

    def mark_mutation_started(self, target_run_id: str) -> None:
        """Record that this run may have changed something. One run marks once."""

        if self.mutation_marked:
            return
        self.mutation_marked = True
        state_lifecycle.mark_mutation_started(self.run_dir, target_run_id)

    def rebind_step_credentials(
        self,
        step: units_step.Step,
        step_env: dict[str, str],
        *,
        target_run_id: str,
        target_run: dict,
    ) -> None:
        """Refresh the step's provider credentials, if this run refreshes per step."""

        provider_name = getattr(self.provider_adapter, "PROVIDER_NAME", "")
        if (self.credential_refresh_modes or {}).get(provider_name) != "per_step":
            return
        cfg_materialize.rebind_step_credentials(
            list(step.providers),
            target_run_id=target_run_id,
            target_run=target_run,
            step_env=step_env,
            provider_adapter=self.provider_adapter,
            provider_catalogs=self.provider_catalogs,
            execution_context=self.execution_context,
            credential_acquisition=self.credential_acquisition,
            execution_access_modes=self.execution_access_modes,
            provider_options=self.provider_options,
        )
