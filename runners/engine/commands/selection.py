"""Turning CLI arguments into a validated, ready-to-run selection.

Shared by every run type: what to run, under which identity, in which
directories, having passed which gates. The command modules above it decide what
to DO with a selection; none of them re-derive one."""

import abc
import argparse
import logging
import logging.handlers
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml

from engine.catalog import target_catalog
from engine.catalog import workflow as catalog_workflow
from engine.cfg import materialize as cfg_materialize
from engine.cfg import overlays as cfg_overlays
from engine.cfg import tooling as cfg_tooling
from engine.cfg import validate as cfg_validate
from engine.execution import providers as execution_providers
from engine.execution import run_context as execution_run_context
from engine.kernel import ids as kernel_ids
from engine.kernel import yaml_io as kernel_yaml_io
from engine.preflight import checks as preflight_checks
from engine.preflight import reports as preflight_reports
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.run import policy as run_policy
from engine.run import request as run_request
from engine.run import selection as run_selection
from engine.state import run_store as state_run_store
from engine.state import sync as state_sync
from engine.units import procedure as units_procedure
from engine.units import workflow as units_workflow

SYNTHETIC_TARGET_FIELDS = (
    "source",
    "ref",
    "domain",
    "procedure",
    "execution_provider",
    "execution_account",
    "execution_role",
    "affected_target_keys",
)


class RunArguments(abc.ABC):
    """The argument rules of ONE run type.

    Each run type is reached by exactly one runner and states which arguments it
    requires and which belong elsewhere. A new run type is a new subclass: no
    existing rule is edited to admit it.
    """

    run_type: str

    @abc.abstractmethod
    def validate(self, args: argparse.Namespace) -> None:
        """Refuse arguments this run type cannot act on."""

    def _refuse_synthetic_target_args(self, args: argparse.Namespace) -> None:
        """Procedure-only arguments, which describe a target no cfg declares."""

        if any(getattr(args, field, None) for field in SYNTHETIC_TARGET_FIELDS):
            raise RuntimeError(
                f"❌ {self.run_type} runner does not accept procedure synthetic target args"
            )


class WorkflowArguments(RunArguments):
    run_type = "workflow"

    def validate(self, args: argparse.Namespace) -> None:

        if not getattr(args, "workflow", None):
            raise RuntimeError(f"❌ {self.run_type} runner requires --workflow")
        if getattr(args, "target", None):
            raise RuntimeError(f"❌ {self.run_type} runner does not accept --target")
        if any(
            getattr(args, field, None)
            for field in (
                "source",
                "ref",
                "domain",
                "procedure",
                "execution_provider",
                "execution_account",
                "execution_role",
                "affected_target_keys",
            )
        ):
            raise RuntimeError("❌ workflow runner does not accept procedure synthetic target args")


class TargetArguments(RunArguments):
    run_type = "target"

    def validate(self, args: argparse.Namespace) -> None:

        if not getattr(args, "target", None):
            raise RuntimeError(f"❌ {self.run_type} runner requires --target")
        if getattr(args, "workflow", None):
            raise RuntimeError(f"❌ {self.run_type} runner does not accept --workflow")
        if any(
            getattr(args, field, None)
            for field in (
                "source",
                "ref",
                "domain",
                "procedure",
                "execution_provider",
                "execution_account",
                "execution_role",
                "affected_target_keys",
            )
        ):
            raise RuntimeError("❌ target runner does not accept procedure synthetic target args")


class MaintenanceArguments(RunArguments):
    run_type = "maintenance"

    def validate(self, args: argparse.Namespace) -> None:

        if any(
            getattr(args, field, None)
            for field in (
                "source",
                "ref",
                "domain",
                "procedure",
                "execution_provider",
                "execution_account",
                "execution_role",
                "affected_target_keys",
            )
        ):
            raise RuntimeError("❌ maintenance runner does not accept synthetic target args")
        action = getattr(args, "maintenance_action", None)
        if not action:
            raise RuntimeError("❌ --maintenance-action is required for maintenance")
        if action == "unlock-ctl-state":
            # Which of the two ctl-state locks. `both` is the default because a run
            # that dies holds both, and clearing one alone only moves where the next
            # run is refused. It means the remote lock and THIS machine's local one:
            # Remote is namespace-wide, local is one directory, so `both` is not a
            # claim to have cleared every local lock everywhere.
            scope = getattr(args, "unlock_scope", None) or "both"
            args.unlock_scope = scope
            if scope in ("local", "both") and not getattr(args, "ctl_state_local_root", None):
                raise RuntimeError(
                    f"❌ --scope {scope} releases the local lock and requires "
                    "--ctl-state-local-root"
                )
            if not getattr(args, "lock_id", None):
                raise RuntimeError(
                    "❌ --lock-id is required for --maintenance-action=unlock-ctl-state"
                )
            return
        if getattr(args, "target", None):
            raise RuntimeError(f"❌ --target is not valid for {action}")
        if action == "forget":
            missing = [
                flag
                for flag, value in (
                    ("--older-than", getattr(args, "older_than", None)),
                    ("--address", getattr(args, "forget_address", None)),
                )
                if not value
            ]
            if missing:
                raise RuntimeError(
                    f"❌ forget requires {' and '.join(missing)}: both filters are "
                    "always stated, so nothing is removed on one the caller did not write"
                )
            args.forget_scope = getattr(args, "unlock_scope", None) or "both"
            if args.forget_scope in ("local", "both") and not getattr(
                args, "ctl_state_local_root", None
            ):
                raise RuntimeError(
                    f"❌ --scope {args.forget_scope} forgets local records and requires "
                    "--ctl-state-local-root"
                )
            return
        if action == "status-sweep":
            return
        if action == "history-prune":
            if not args.prune_run_id and not args.prune_before:
                raise RuntimeError("❌ history-prune requires --prune-run-id or --prune-before")
            if args.apply_history_prune != args.agree_history_prune:
                raise RuntimeError(
                    "❌ applying history prune requires both --apply-history-prune "
                    "and --agree-history-prune"
                )
            return
        raise RuntimeError(f"❌ unsupported maintenance action: {action}")


class ProcedureArguments(RunArguments):
    run_type = "procedure"

    def validate(self, args: argparse.Namespace) -> None:

        if getattr(args, "workflow", None) or getattr(args, "target", None):
            raise RuntimeError("❌ procedure runner does not accept --workflow or --target")
        missing = [
            f for f in ("source", "ref", "domain", "procedure") if not getattr(args, f, None)
        ]
        if missing:
            raise RuntimeError(
                "❌ procedure needs " + ", ".join(f"--{m.replace('_', '-')}" for m in missing)
            )
        execution_fields = ("execution_provider", "execution_account", "execution_role")
        supplied = [f for f in execution_fields if getattr(args, f, None)]
        if supplied and len(supplied) != len(execution_fields):
            missing_execution = [f for f in execution_fields if f not in supplied]
            raise RuntimeError(
                "❌ a synthetic target's execution is declared in full or not at all; missing "
                + ", ".join(f"--{m.replace('_', '-')}" for m in missing_execution)
            )
        procedure = units_procedure.ProcedureRequest.from_args(args)
        affected_target_keys = list(procedure.affected_target_keys)
        if affected_target_keys:
            args.affected_target_keys = run_addressing.normalize_target_keys(
                affected_target_keys, label="--affected-target-key"
            )
        if args.action in run_actions.MUTATING_ACTIONS and not procedure.affects_targets:
            raise RuntimeError(
                "❌ mutating procedure runs require at least one --affected-target-key"
            )


RUN_ARGUMENTS: dict[str, RunArguments] = {
    rules.run_type: rules
    for rules in (
        WorkflowArguments(),
        TargetArguments(),
        MaintenanceArguments(),
        ProcedureArguments(),
    )
}


def validate_run_args(run_type: str, args: argparse.Namespace) -> None:
    """Apply one run type's argument rules, by name."""

    rules = RUN_ARGUMENTS.get(run_type)
    if rules is None:
        raise RuntimeError(f"❌ unknown run type {run_type!r}; known: {sorted(RUN_ARGUMENTS)}")
    rules.validate(args)


@dataclass(frozen=True, kw_only=True)
class RunLocation:
    """Where one run's directories live, and what identifies it there.

    Both ways of creating a run — full and preflight-only — compose the same
    path and write the same metadata; only the tooling and cfg they materialize
    differ. This is the part they share, stated once.
    """

    run_id: str
    action: str | None
    run_type: str
    result_name: str
    ctl_state_local_root: Path
    locator_segments: list[str]
    label: str | None = None
    parent_fan_out_run_id: str | None = None
    parent_workflow_run_id: str | None = None
    parent_workflow_instance_address: str | None = None
    instance_segments: list[str] | None = None
    instance_address: str | None = None
    target_addresses: list[str] | None = None
    identity_doc: dict | None = None
    execution_access_modes: str | None = None

    @property
    def ctl_state_dir(self) -> Path:
        """The instance directory this run writes under.

        Composed, never hand-assembled: hand-building `/ action / run_type /` is
        what kept every real run on the action-prefixed layout after the readers
        had moved off it — the two agreed with each other and with nothing.
        """

        return Path(self.ctl_state_local_root).joinpath(
            *self.locator_segments
        ) / run_addressing.compose_state_relpath(
            self.run_type,
            run_actions.normalize_result_name(self.result_name, label="ctl result name"),
            list(self.instance_segments or []),
        )

    def write_identity(self) -> None:
        """Write the instance's authoritative identity.yaml, before any run content."""

        if not (self.instance_segments and self.identity_doc is not None):
            return
        identity_path = self.ctl_state_dir / "identity.yaml"
        if not identity_path.exists():
            self.ctl_state_dir.mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(identity_path, self.identity_doc)

    def metadata(self, *, run_dir: Path, log_file: Path, **extra) -> dict:
        """The run record both creation paths write."""

        result_name = run_actions.normalize_result_name(self.result_name, label="ctl result name")
        namespace = self.locator_segments[0] if self.locator_segments else None
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "result_name": result_name,
            **(
                {
                    "action": self.action,
                    "result_key": f"{self.action}/{self.run_type}/{result_name}",
                }
                if self.action is not None
                else {}
            ),
            "ctl_state_local_root": str(Path(self.ctl_state_local_root)),
            "ctl_state_locator": list(self.locator_segments),
            "ctl_state_dir": str(self.ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
            **extra,
            # Degraded-mode audit: each provider's access mode is persisted
            # structurally (not only in the logged command) so an audit of
            # committed run records can tell which runs escalated, and where.
            **(
                {"execution_access_modes": self.execution_access_modes}
                if self.execution_access_modes
                else {}
            ),
            # Instance identity + namespace facts of this run.
            **(
                {"ctl_state_namespace": namespace}
                if namespace and namespace != state_run_store.LOCAL_ONLY_LOCATOR[0]
                else {}
            ),
            **({"instance": list(self.instance_segments)} if self.instance_segments else {}),
            **({"instance_address": self.instance_address} if self.instance_address else {}),
            **({"target_addresses": list(self.target_addresses)} if self.target_addresses else {}),
            # The operator's name for the invocation this run belongs to,
            # inherited from the parent rather than minted here. Metadata: it is
            # denormalized onto the committed pointer for reading, and absent
            # from every identity and reuse comparison — see _COMMITTED_FACT_KEYS.
            **({"label": self.label} if self.label else {}),
            # The stateless fan-out's batch audit record — "these runs were one
            # invocation" lives only in child metadata.
            **(
                {"fan_out_run_id": self.parent_fan_out_run_id} if self.parent_fan_out_run_id else {}
            ),
            # A child spawned by a workflow records its parent, so the namespace
            # mutation lock can tell "my parent holds it" from contention.
            **(
                {"parent_workflow_run_id": self.parent_workflow_run_id}
                if self.parent_workflow_run_id
                else {}
            ),
            **(
                {"parent_workflow_instance_address": self.parent_workflow_instance_address}
                if self.parent_workflow_instance_address
                else {}
            ),
        }


def _attach_run_log(run_dir: Path, memory_handler: logging.handlers.MemoryHandler) -> Path:
    """Open this run's log file and redirect buffered logging into it."""

    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logs_run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex[:6]
    log_file = logs_dir / f"{kernel_ids.SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)
    memory_handler.setTarget(file_handler)
    memory_handler.flush()
    logging.getLogger().removeHandler(memory_handler)
    return log_file


def setup_run_dirs(
    location: RunLocation, memory_handler: logging.handlers.MemoryHandler
) -> tuple[Path, Path, Path]:
    """Create a run's directories, materialize the ctl runtime, and start file logging.

    Results nest under the resolved ctl-state NAMESPACE tree (`_local` for
    stateless runs), with the instance layer between the key and `runs/`:
      <root>/<namespace>/<run_type>/<key>[/instances/<seg>...]/runs/<id>
    """

    ctl_state_dir = location.ctl_state_dir
    run_dir = ctl_state_dir / "runs" / location.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    location.write_identity()
    logging.info(f"Using ctl_state_dir: {ctl_state_dir}")
    logging.info(f"Using run_dir: {run_dir}")

    # Materialize the pinned ctl runtime once, up front — it is a run-scoped
    # precondition, not a per-target step. Idempotent thereafter.
    step_utils_dir = cfg_materialize.materialize_step_utils(run_dir)
    logging.info(f"Using ctl target_run runtime: {step_utils_dir}")

    # artifacts/ splits into general/ (run-level reports + metadata) and
    # target_runs/<target_run>/. Logs are a top-level run concern, sibling of
    # cfg/ — not buried under artifacts/.
    artifacts_dir = run_dir / "artifacts" / "general"
    os.makedirs(artifacts_dir, exist_ok=True)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    os.makedirs(cfg_dir)

    log_file = _attach_run_log(run_dir, memory_handler)
    state_run_store.write_run_metadata(
        run_dir, location.metadata(run_dir=run_dir, log_file=log_file)
    )
    logging.info(f"Using artifacts_dir: {artifacts_dir}")
    logging.info(f"Logging to: {log_file}")
    return run_dir, artifacts_dir, log_file


def setup_run_workspace(run_dir: Path) -> Path:
    """Materialize the target_run runtime and mutable cfg workspace after preflight."""

    step_utils_dir = cfg_materialize.materialize_step_utils(run_dir)
    logging.info("Using ctl target_run runtime: %s", step_utils_dir)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    cfg_dir.mkdir(parents=True)

    return cfg_dir


def setup_preflight_run_dirs(
    location: RunLocation,
    memory_handler: logging.handlers.MemoryHandler,
    *,
    check_only: bool = True,
) -> tuple[Path, Path, Path]:
    """Create a preflight result without target_run tooling or companion cfg."""

    location.write_identity()
    run_dir = location.ctl_state_dir / "runs" / location.run_id
    artifacts_dir = run_dir / "artifacts" / "general"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    log_file = _attach_run_log(run_dir, memory_handler)
    state_run_store.write_run_metadata(
        run_dir,
        location.metadata(
            run_dir=run_dir,
            log_file=log_file,
            execution_identity_preflight_check_only=bool(check_only),
        ),
    )
    logging.info("Using preflight run_dir: %s", run_dir)
    logging.info("Using artifacts_dir: %s", artifacts_dir)
    return run_dir, artifacts_dir, log_file


def prepare_pipeline_cfg(
    plt_cfg_root: Path,
    workflow_cfg: dict,
    action_cfg: dict,
    artifacts_dir: Path,
    ctl_profile: str,
    scope_params: dict[str, str] | None = None,
    execution_context: dict[str, object] | None = None,
    target_repo_key: str = "repo_url",
    require_target_ref: bool = True,
    require_commit_refs: bool = False,
    refs: dict | None = None,
    active_target_runs: dict | None = None,
) -> tuple[dict, Path, list[str]]:
    """Build active target_runs, resolve per-target overlays, and write pipeline_run_cfg.

    Each target derives its own cfg (`prepare_target_cfg_view`), so there is no
    run-wide merged tree to build here.

    Returns:
        tuple: (active_target_runs, pipeline_run_cfg_path, final_plt_overlays)
    """

    if active_target_runs is None:
        active_target_runs = target_catalog.ActiveTargetRuns.build(
            workflow_cfg,
            action_cfg,
            repo_key=target_repo_key,
            require_branch_or_commit=require_target_ref,
            refs=refs,
            execution_context=execution_context,
            require_commit_refs=require_commit_refs,
        )

    # Overlays are a PER-TARGET declaration and nothing else — a run carries no
    # explicit list, so a target's cfg is merged with exactly the overlays it
    # asked for. The run-wide total is recorded for provenance only.
    final_plt_overlays = cfg_overlays.resolve_run_plt_overlays(
        plt_cfg_root,
        [],
        active_target_runs,
        execution_context=execution_context or {},
    )
    for target_run in active_target_runs.values():
        target_run["plt_overlays"] = cfg_overlays.resolve_target_plt_overlays(
            plt_cfg_root,
            [],
            target_run,
            execution_context=execution_context or {},
        )
    target_catalog.ActiveTargetRuns.attach_definition_facts(active_target_runs)

    catalog_workflow.WorkflowArtifacts.write_target_run_flow(
        artifacts_dir / "resolved_target_runs_flow.yaml",
        workflow_cfg.get("meta"),
        active_target_runs,
    )

    # Create and write pipeline_run_cfg
    pipeline_run_cfg = {"meta": workflow_cfg.get("meta"), "target_runs": active_target_runs}
    pipeline_run_cfg_path = artifacts_dir / "pipeline_run_cfg.yaml"
    with pipeline_run_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline_run_cfg, f, sort_keys=False)

    return active_target_runs, pipeline_run_cfg_path, final_plt_overlays


def require_unique_fan_out_namespace(
    ctl_cfg_root: Path,
    children: list[dict],
    *,
    action: str | None,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    providers: list[str] | tuple[str, ...] = (),
) -> str:
    """3: a fan-out first expands, then resolves the namespace
    for EVERY child execution context and requires the unique set to contain
    exactly one member. Cross-namespace expansions are hard errors and must be
    partitioned into separate invocations. The fan-out runner never names or
    interprets selector parameters — it only compares resolved keys."""
    namespace_by_child: dict[str, str] = {}
    for child in children:
        child_params = dict(execution_params)
        child_params.update(child["params"])
        child_context = execution_run_context.build_execution_context(
            ctl_cfg_root,
            action=action,
            ctl_profile=ctl_profile,
            execution_params=child_params,
            providers=providers,
            execution_runtime_mode=execution_runtime_mode,
        )
        namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
            ctl_cfg_root, child_context
        )
        namespace_by_child[child["label"]] = namespace_key
    unique = sorted(set(namespace_by_child.values()))
    if len(unique) != 1:
        detail = ", ".join(f"{label} -> {ns}" for label, ns in sorted(namespace_by_child.items()))
        raise RuntimeError(
            f"❌ fan-out children resolve {len(unique)} ctl-state namespaces ({detail}); "
            "one invocation must not cross namespaces — partition the fan-out"
        )
    return unique[0]


def resolve_run_locator_segments(
    ctl_cfg_root: Path,
    *,
    run_type: str,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    workflow_name: str | None = None,
    target_name: str | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> list[str]:
    """Resolve a run's local ctl-state locator BEFORE its dirs exist.

    Pure cfg resolution: the run's single ctl-state namespace maps through the
    provider adapter to the backend mirror tree the run lives in. The same
    namespace is re-resolved when the syncer is armed and must agree. Fan-out
    and namespace-less runs land under the reserved `_local` tree."""

    if run_type in ("fan_out", "procedure"):
        # fan-outs are stateless (local artifacts only) and
        # procedure runs are synthetic dev-loop records — neither has a
        # bucket presence.
        return list(state_run_store.LOCAL_ONLY_LOCATOR)
    if run_type == "maintenance" and not target_name:
        return list(state_run_store.LOCAL_ONLY_LOCATOR)
    if run_type not in ("target", "workflow", "maintenance"):
        raise RuntimeError(f"❌ unknown run_type {run_type!r} for locator resolution")
    # target/workflow state lives in the ONE resolved ctl-state
    # namespace tree — the local root scopes by namespace key, the synchronized
    # relative tree carries no provider locator segments.
    execution_context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=None if run_type == "workflow" else action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        execution_runtime_mode=execution_runtime_mode,
    )
    namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
        ctl_cfg_root, execution_context
    )
    return [namespace_key]


def resolve_run_instance_identity(
    ctl_cfg_root: Path,
    *,
    run_type: str,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    workflow_name: str | None = None,
    target_name: str | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> dict | None:
    """Resolve a run's target-instance identity BEFORE its dirs exist.

    Both kinds resolve DECLARED instance params to Hive segments: a target's
    `target_instance_params`, a workflow's `workflow_instance_params` (—
    a workflow publishes history, not state, so it carries no composition digest).
    Returns {instance_segments, address, target_addresses, identity_doc?} or None
    for run types without instance identity (fan_out/procedure/maintenance)."""

    if run_type not in ("target", "workflow"):
        return None
    execution_context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=None if run_type == "workflow" else action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        execution_runtime_mode=execution_runtime_mode,
    )
    if run_type == "workflow":
        workflow_cfg = catalog_workflow.WorkflowCatalog.workflow_cfg(
            ctl_cfg_root, ctl_profile, action, workflow_name, execution_context
        )
        action_cfg = target_catalog.TargetCatalog.action_cfg(
            ctl_cfg_root,
            action,
            execution_context,
            member_actions=units_workflow.Workflow.from_cfg(
                workflow_name or "", workflow_cfg, action=action
            ).member_actions,
        )
    else:
        workflow_cfg = None
        action_cfg = target_catalog.TargetCatalog.action_cfg(
            ctl_cfg_root, action, execution_context
        )
    targets = action_cfg.get("targets", {})

    def target_segments(name: str) -> list[str]:
        target_def = targets.get(name) or {}
        return run_addressing.resolve_target_instance_segments(
            target_def.get("target_instance_params"),
            execution_context,
            label=f"target {name}",
        )

    if run_type == "target":
        if not target_name:
            return None
        segments = target_segments(target_name)
        address = run_addressing.target_instance_address(target_name, segments)
        resolved_params = {
            key: str(execution_context[f"execution_context.params.{key}"])
            for key in (targets.get(target_name) or {}).get("target_instance_params", [])
        }
        return {
            "instance_segments": segments,
            "address": address,
            "target_addresses": [address],
            "identity_doc": {
                "target_instance": {
                    "target": target_name,
                    "resolved_params": resolved_params,
                }
            },
        }
    assert workflow_cfg is not None
    addresses: list[str] = []
    member_actions: list = []
    for entry in workflow_cfg.get("target_runs", []):
        name = entry if isinstance(entry, str) else entry.get("target")
        if not name:
            continue
        addresses.append(run_addressing.target_instance_address(name, target_segments(name)))
        # The member's ACTION is part of its identity, so a teardown of
        # A target and a deploy of the same target are different compositions.
        member_actions.append(entry.get("action") if isinstance(entry, dict) else None)
    if not addresses:
        raise RuntimeError(f"❌ workflow {workflow_name!r} resolves no target addresses")
    # A workflow publishes HISTORY, not state — no composition digest,
    # no committed pointer. that history is PARTITIONED by the axes its
    # members vary over, so one key fanned across environments keeps a separate
    # `last_run` per environment instead of one row for whichever finished last.
    # Params, not a hash: params ADDRESS (readable, predictable from cfg, stable
    # across cfg edits) where a hash IDENTIFIES, and identity is the question
    # phase 73 decided ctl must not answer.
    instance_params = catalog_workflow.WorkflowInstanceParams.validate(
        workflow_cfg.get("workflow_instance_params"),
        workflow_cfg,
        targets,
        label=f"workflow {workflow_name!r}",
        execution_context=execution_context,
    )
    segments = run_addressing.resolve_target_instance_segments(
        instance_params, execution_context, label=f"workflow {workflow_name!r}"
    )
    return {
        "instance_segments": segments,
        "address": run_addressing.workflow_instance_address(workflow_name, segments),
        "target_addresses": addresses,
        "member_actions": member_actions,
        "identity_doc": None,
    }


def resolve_pipeline_selection(
    request: run_request.RunRequest,
    *,
    enforce_ctl_policy: bool = True,
    load_provider_catalogs: bool = True,
) -> run_selection.RunSelection:
    """Resolve a run through active target_runs without touching state or plt cfg.

    Policy-free resolution is used only to produce independent ctl-policy and
    execution-identity preflight artifacts. Callers must enforce both reports
    before executing the returned selection.

    With `load_provider_catalogs=False` the provider adapter and its runtime
    catalogs are NOT loaded (`provider_adapter`/`provider_catalogs` come back
    None). The cfg-level result is enough for the provider-independent ctl-policy
    preflight; call `load_selection_provider_catalogs` afterwards for the
    execution-identity preflight, which does need catalogs. This split keeps a
    provider-catalog failure (e.g. a malformed account id) from masquerading as a
    ctl-policy failure.
    """
    execution_context = execution_run_context.build_execution_context(
        request.ctl_cfg_root,
        action=None
        if request.workflow_name and not request.target_name and not request.procedure_run
        else request.action,
        ctl_profile=request.ctl_profile,
        execution_params=request.execution_params,
        providers=request.providers,
        agreed_defer_ctl_state_backend_sync=request.agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=request.force_skip_ctl_state_backend_sync,
        force_skip_guardrails=request.force_skip_guardrails,
        force_skip_full_cfg_validation_gate=request.force_skip_full_cfg_validation_gate,
        execution_access_modes=request.execution_access_modes,
        execution_runtime_mode=request.execution_runtime_mode,
        force_skip_execution_identity_preflight_check=(
            request.force_skip_execution_identity_preflight_check
        ),
    )
    if enforce_ctl_policy:
        execution_run_context.validate_execution_context_constraints(
            request.ctl_cfg_root, execution_context
        )
    require_commit_refs = run_policy.ref_policy_requires_commits(request.ctl_ref_policy)

    if request.procedure_run:
        workflow_cfg, action_cfg = catalog_workflow.WorkflowCatalog.procedure_cfg(
            request.ctl_cfg_root,
            request.action,
            source=request.procedure_run["source"],
            ref=request.procedure_run["ref"],
            domain_name=request.procedure_run["domain"],
            procedure=request.procedure_run["procedure"],
            execution_provider=request.procedure_run.get("execution_provider"),
            execution_account=request.procedure_run.get("execution_account"),
            execution_role=request.procedure_run.get("execution_role"),
        )
        selection_kind = "procedure"
        selection_key = request.procedure_run["procedure"]
    elif request.target_name:
        # A standalone target run has no members, so the action is filtered by
        # the invoked action alone.
        action_cfg = target_catalog.TargetCatalog.action_cfg(
            request.ctl_cfg_root, request.action, execution_context
        )
        workflow_cfg = {
            "meta": {
                "name": f"{request.ctl_profile}/{request.action}/{request.target_name}",
                "action": request.action,
            },
            "target_runs": [request.target_name],
        }
        selection_kind = "target"
        selection_key = request.target_name
    else:
        workflow_cfg = catalog_workflow.WorkflowCatalog.workflow_cfg(
            request.ctl_cfg_root,
            request.ctl_profile,
            request.action,
            request.workflow_name,
            execution_context,
        )
        action_cfg = target_catalog.TargetCatalog.action_cfg(
            request.ctl_cfg_root,
            request.action,
            execution_context,
            member_actions=units_workflow.Workflow.from_cfg(
                request.workflow_name or "", workflow_cfg, action=request.action
            ).member_actions,
        )
        selection_kind = "workflow"
        selection_key = request.workflow_name

    if not request.procedure_run:
        target_catalog.TargetEntries.validate_selectors(workflow_cfg, action_cfg, execution_context)
    if enforce_ctl_policy:
        run_policy.validate_target_policy_constraints(
            request.ctl_cfg_root, request.ctl_profile, workflow_cfg, action_cfg
        )
        run_policy.validate_execution_access(
            request.ctl_cfg_root,
            request.ctl_profile,
            workflow_cfg,
            action_cfg,
            execution_context=execution_context,
            agreed_defer_ctl_state_backend_sync=request.agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=request.force_skip_ctl_state_backend_sync,
            execution_access_modes=request.execution_access_modes,
            provider_options=request.provider_options,
            force_skip_execution_identity_preflight_check=(
                request.force_skip_execution_identity_preflight_check
            ),
        )
        run_policy.validate_execution_runtime_mode(
            request.ctl_cfg_root, request.ctl_profile, request.execution_runtime_mode
        )

    refs = cfg_tooling.load_refs_cfg(request.ctl_cfg_root)
    active_target_runs = target_catalog.ActiveTargetRuns.build(
        workflow_cfg,
        action_cfg,
        repo_key=request.target_repo_key,
        require_branch_or_commit=request.require_target_ref,
        refs=refs,
        execution_context=execution_context,
        require_commit_refs=require_commit_refs if enforce_ctl_policy else False,
    )
    if enforce_ctl_policy:
        cfg_validate.CommitPinning(request.ctl_ref_policy).check_target_runs(active_target_runs)
    provider_adapter = None
    provider_catalogs = None
    if load_provider_catalogs:
        provider_adapter = execution_providers.run_provider_adapter(execution_context)
        provider_catalogs = provider_adapter.load_runtime_catalogs(
            request.ctl_cfg_root, execution_context=execution_context
        )
    return run_selection.RunSelection(
        kind=selection_kind,
        key=selection_key,
        execution_context=execution_context,
        scope_params=execution_run_context.scope_params_from_context(execution_context),
        require_commit_refs=require_commit_refs,
        workflow_cfg=workflow_cfg,
        action_cfg=action_cfg,
        refs=refs,
        active_target_runs=active_target_runs,
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
    )


def resolve_and_preflight_execution_identities(
    request: run_request.RunRequest,
) -> tuple[run_selection.RunSelection, dict]:
    """Single-runner (workflow/target/procedure) preflight: the same four

    validation reports the fan-out produces, for this one selection.
    """

    gates_dir = cfg_materialize.run_gates_dir(request.run_dir)
    selection = resolve_pipeline_selection(
        request,
        enforce_ctl_policy=False,
        load_provider_catalogs=False,
    )
    cfg_report = preflight_checks.CFG_VALIDATION.build(
        preflight_reports.collect_provider_cfg_findings(
            request.ctl_cfg_root, selection.execution_context
        )
    )
    preflight_checks.CFG_VALIDATION.apply_gate(
        cfg_report, force_skip=request.force_skip_full_cfg_validation_gate
    )
    outcome = preflight_checks.build_selection_validation_reports(
        selection,
        preflight_checks.PreflightInputs(
            ctl_cfg_root=request.ctl_cfg_root,
            ctl_profile=request.ctl_profile,
            ctl_ref_policy=request.ctl_ref_policy,
            execution_runtime_mode=request.execution_runtime_mode,
            execution_access_modes=request.execution_access_modes,
            provider_options=request.provider_options,
            credential_acquisition=request.credential_acquisition,
            force_skip_execution_identity_preflight_check=(
                request.force_skip_execution_identity_preflight_check
            ),
            agreed_defer_ctl_state_backend_sync=request.agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=request.force_skip_ctl_state_backend_sync,
        ),
    )
    reports = {preflight_checks.CFG_VALIDATION.name: cfg_report, **outcome["reports"]}
    for check in preflight_checks.PREFLIGHT_CHECKS:
        check.write_artifacts(gates_dir, reports[check.name])
    # Full cfg health is always rendered. The authorized force flag skips only
    # this aggregate gate; structural and selected-run validation still block.
    for check in preflight_checks.PREFLIGHT_CHECKS:
        check.assert_accepted(reports[check.name])
    return (
        outcome["selection"],
        reports[preflight_checks.EXECUTION_IDENTITY_PREFLIGHT.name],
    )
