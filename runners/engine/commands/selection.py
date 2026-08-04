"""Turning CLI arguments into a validated, ready-to-run selection.

Shared by every run type: what to run, under which identity, in which
directories, having passed which gates. The command modules above it decide what
to DO with a selection; none of them re-derive one."""

import argparse
import logging
import os
import shutil
import uuid
import logging.handlers

from datetime import UTC, datetime
from pathlib import Path
import yaml

from engine.catalog import targets as catalog_targets
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
from engine.state import run_store as state_run_store
from engine.state import sync as state_sync

def validate_workflow_args(args: argparse.Namespace) -> None:
    """

    validate args for a declared workflow run."""

    if not getattr(args, "workflow", None):
        raise RuntimeError("❌ workflow runner requires --workflow")
    if getattr(args, "target", None):
        raise RuntimeError("❌ workflow runner does not accept --target")
    if any(getattr(args, field, None) for field in ("source", "ref", "domain", "procedure", "execution_provider", "execution_account", "execution_role", "affected_target_keys")):
        raise RuntimeError("❌ workflow runner does not accept procedure synthetic target args")


def validate_target_args(args: argparse.Namespace) -> None:
    """

    validate args for a declared single-target run."""

    if not getattr(args, "target", None):
        raise RuntimeError("❌ target runner requires --target")
    if getattr(args, "workflow", None):
        raise RuntimeError("❌ target runner does not accept --workflow")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for target runs")
    if any(getattr(args, field, None) for field in ("source", "ref", "domain", "procedure", "execution_provider", "execution_account", "execution_role", "affected_target_keys")):
        raise RuntimeError("❌ target runner does not accept procedure synthetic target args")


def validate_maintenance_args(args: argparse.Namespace) -> None:
    """

    validate args for one explicit maintenance operation."""

    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for maintenance")
    if any(
        getattr(args, field, None)
        for field in (
            "source", "ref", "domain", "procedure",
            "execution_provider", "execution_account", "execution_role",
            "affected_target_keys",
        )
    ):
        raise RuntimeError(
            "❌ maintenance runner does not accept synthetic target args"
        )
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
        if not state_run_store.ctl_state_lock_matches(args.ctl_state_local_root, args.lock_id):
            raise RuntimeError(
                f"❌ --lock-id {args.lock_id!r} does not hold the ctl-state lock"
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
            raise RuntimeError(
                "❌ history-prune requires --prune-run-id or --prune-before"
            )
        if args.apply_history_prune != args.agree_history_prune:
            raise RuntimeError(
                "❌ applying history prune requires both --apply-history-prune "
                "and --agree-history-prune"
            )
        return
    raise RuntimeError(f"❌ unsupported maintenance action: {action}")


def validate_procedure_args(args: argparse.Namespace) -> None:
    """

    validate args for a synthetic repo-local procedure run."""

    if getattr(args, "workflow", None) or getattr(args, "target", None):
        raise RuntimeError("❌ procedure runner does not accept --workflow or --target")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for procedure runs")
    missing = [f for f in ("source", "ref", "domain", "procedure") if not getattr(args, f, None)]
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
    affected_target_keys = getattr(args, "affected_target_keys", None) or []
    if affected_target_keys:
        args.affected_target_keys = run_addressing.normalize_target_keys(affected_target_keys, label="--affected-target-key")
    if args.action in run_actions.MUTATING_ACTIONS and not getattr(args, "affected_target_keys", None):
        raise RuntimeError("❌ mutating procedure runs require at least one --affected-target-key")


def setup_run_dirs(
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    ctl_state_local_root: Path,
    memory_handler: logging.handlers.MemoryHandler,
    *,
    locator_segments: list[str],
    parent_fan_out_run_id: str | None = None,
    parent_workflow_run_id: str | None = None,
    parent_workflow_instance_address: str | None = None,
    instance_segments: list[str] | None = None,
    instance_address: str | None = None,
    target_addresses: list[str] | None = None,
    identity_doc: dict | None = None,
    execution_access_modes: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create run directories under the stable ctl result key and setup file logging.

    results nest under the resolved ctl-state NAMESPACE tree
    (`_local` for stateless/synthetic runs), with the target/workflow instance
    layer between the key and `runs/`:
      <root>/<namespace>/<run_type>/<key>[/instances/<seg>...]/runs/<id>
    A parameterized instance writes its authoritative identity.yaml
    (manifest-first ordering, Q2) before any run content."""
    result_name = run_actions.normalize_result_name(result_name, label="ctl result name")
    # composed, never hand-assembled. Building `/ action / run_type /`
    # Here is what kept every real run on the action-prefixed layout while the
    # readers had already moved — the two agreed with each other and with nothing.
    ctl_state_dir = Path(ctl_state_local_root).joinpath(
        *locator_segments
    ) / run_addressing.compose_state_relpath(run_type, result_name, list(instance_segments or []))
    runs_dir = ctl_state_dir / "runs"
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    if instance_segments and identity_doc is not None:
        identity_path = ctl_state_dir / "identity.yaml"
        if not identity_path.exists():
            kernel_yaml_io.write_yaml_file(identity_path, identity_doc)
    logging.info(f"Using ctl_state_dir: {ctl_state_dir}")
    logging.info(f"Using run_dir: {run_dir}")

    # Materialize the pinned ctl target_run runtime once, up front — it is a run-scoped
    # (workspace-scoped) precondition, not a per-target_run step. Idempotent thereafter.
    step_utils_dir = cfg_materialize.materialize_step_utils(run_dir)
    logging.info(f"Using ctl target_run runtime: {step_utils_dir}")

    # artifacts/ splits into general/ (run-level validation reports + metadata)
    # And target_runs/<target_run>/ (per-target_run outputs, created when target_runs run).
    # Logs are a top-level run concern (run_dir/logs/), sibling of cfg/ — not buried
    # under artifacts/.
    artifacts_dir = run_dir / "artifacts" / "general"
    os.makedirs(artifacts_dir, exist_ok=True)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    os.makedirs(cfg_dir)

    logs_dir = run_dir / "logs"
    os.makedirs(logs_dir, exist_ok=True)
    logs_run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex[:6]
    log_file = logs_dir / f"{kernel_ids.SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)

    memory_handler.setTarget(file_handler)
    memory_handler.flush()
    logging.getLogger().removeHandler(memory_handler)

    state_run_store.write_run_metadata(
        run_dir,
        {
            "run_id": run_id,
            "action": action,
            "run_type": run_type,
            "result_name": result_name,
            "result_key": f"{action}/{run_type}/{result_name}",
            "ctl_state_local_root": str(Path(ctl_state_local_root)),
            "ctl_state_locator": list(locator_segments),
            "ctl_state_dir": str(ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
            # Degraded-mode audit: each provider's access mode is persisted
            # structurally (not only in the logged command) so an audit of
            # committed run records can tell which runs escalated, and where.
            **({"execution_access_modes": execution_access_modes}
               if execution_access_modes else {}),
            # Instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != state_run_store.LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            # 8: the stateless fan-out's batch audit record —
            # "these runs were one invocation" lives only in child metadata.
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
            # A child spawned by a workflow records its parent, so the
            # namespace mutation lock can tell "my parent holds it" from contention.
            **({"parent_workflow_run_id": parent_workflow_run_id}
               if parent_workflow_run_id else {}),
            **({"parent_workflow_instance_address": parent_workflow_instance_address}
               if parent_workflow_instance_address else {}),
        },
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
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    ctl_state_local_root: Path,
    memory_handler: logging.handlers.MemoryHandler,
    *,
    locator_segments: list[str],
    check_only: bool = True,
    instance_segments: list[str] | None = None,
    instance_address: str | None = None,
    target_addresses: list[str] | None = None,
    identity_doc: dict | None = None,
    parent_fan_out_run_id: str | None = None,
    parent_workflow_run_id: str | None = None,
    parent_workflow_instance_address: str | None = None,
    execution_access_modes: str | None = None,
) -> tuple[Path, Path, Path]:
    """Create a preflight result without target_run tooling or companion cfg."""
    result_name = run_actions.normalize_result_name(result_name, label="ctl result name")
    # composed, never hand-assembled. Building `/ action / run_type /`
    # Here is what kept every real run on the action-prefixed layout while the
    # readers had already moved — the two agreed with each other and with nothing.
    ctl_state_dir = Path(ctl_state_local_root).joinpath(
        *locator_segments
    ) / run_addressing.compose_state_relpath(run_type, result_name, list(instance_segments or []))
    if instance_segments and identity_doc is not None:
        identity_path = ctl_state_dir / "identity.yaml"
        if not identity_path.exists():
            ctl_state_dir.mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(identity_path, identity_doc)
    run_dir = ctl_state_dir / "runs" / run_id
    artifacts_dir = run_dir / "artifacts" / "general"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    logs_run_id = (
        datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        + "_"
        + uuid.uuid4().hex[:6]
    )
    log_file = logs_dir / f"{kernel_ids.SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logging.getLogger().addHandler(file_handler)
    memory_handler.setTarget(file_handler)
    memory_handler.flush()
    logging.getLogger().removeHandler(memory_handler)

    state_run_store.write_run_metadata(
        run_dir,
        {
            "run_id": run_id,
            "action": action,
            "run_type": run_type,
            "result_name": result_name,
            "result_key": f"{action}/{run_type}/{result_name}",
            "ctl_state_local_root": str(Path(ctl_state_local_root)),
            "ctl_state_locator": list(locator_segments),
            "ctl_state_dir": str(ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
            "execution_identity_preflight_check_only": bool(check_only),
            # Degraded-mode audit (see setup_run_dirs).
            **({"execution_access_modes": execution_access_modes}
               if execution_access_modes else {}),
            # Instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != state_run_store.LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
            # A child spawned by a workflow records its parent, so the
            # namespace mutation lock can tell "my parent holds it" from contention.
            **({"parent_workflow_run_id": parent_workflow_run_id}
               if parent_workflow_run_id else {}),
            **({"parent_workflow_instance_address": parent_workflow_instance_address}
               if parent_workflow_instance_address else {}),
        },
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
    plt_overlays: list[str],
    scope_params: dict[str, str] | None = None,
    execution_context: dict[str, object] | None = None,
    target_repo_key: str = "repo_url",
    require_target_ref: bool = True,
    require_commit_refs: bool = False,
    refs: dict | None = None,
    active_target_runs: dict | None = None,
) -> tuple[dict, Path, list[str]]:
    """Build active target_runs, resolve per-target overlays, and write pipeline_run_cfg.

    this no longer merges anything. Each target derives its own cfg
    (`prepare_target_cfg_view`), so there is no run-wide merged tree to build here.

    Returns:
        tuple: (active_target_runs, pipeline_run_cfg_path, final_plt_overlays)
    """

    if active_target_runs is None:
        active_target_runs = catalog_targets.build_active_target_runs(
            workflow_cfg,
            action_cfg,
            repo_key=target_repo_key,
            require_branch_or_commit=require_target_ref,
            refs=refs,
            execution_context=execution_context,
            require_commit_refs=require_commit_refs,
        )

    # Overlays are a PER-TARGET declaration, so each target_run gets
    # exactly the overlays it asked for plus the run's explicit ones. The former
    # run-wide union meant a target that never declared an overlay still had its cfg
    # merged with it — `requires_plt_overlays` now means what it says.
    final_plt_overlays = cfg_overlays.resolve_run_plt_overlays(
        plt_cfg_root,
        plt_overlays,
        active_target_runs,
        execution_context=execution_context or {},
    )
    for target_run in active_target_runs.values():
        target_run["plt_overlays"] = cfg_overlays.resolve_target_plt_overlays(
            plt_cfg_root,
            plt_overlays,
            target_run,
            execution_context=execution_context or {},
        )
    catalog_targets.attach_target_definition_facts(active_target_runs)

    catalog_workflow.write_target_run_flow_artifact(
        artifacts_dir / "resolved_target_runs_flow.yaml",
        workflow_cfg.get("meta"),
        active_target_runs,
    )

    # Create and write pipeline_run_cfg
    pipeline_run_cfg = {
        "meta": workflow_cfg.get("meta"),
        "target_runs": active_target_runs
    }
    pipeline_run_cfg_path = artifacts_dir / "pipeline_run_cfg.yaml"
    with pipeline_run_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline_run_cfg, f, sort_keys=False)

    return active_target_runs, pipeline_run_cfg_path, final_plt_overlays


def require_unique_fan_out_namespace(
    ctl_cfg_root: Path,
    children: list[dict],
    *,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    providers: list[str] | tuple[str, ...] = (),
) -> str:
    """ 3: a fan-out first expands, then resolves the namespace
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
        namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(ctl_cfg_root, child_context)
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
    ctl_variants: list[str] | tuple[str, ...] = (),
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
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        execution_runtime_mode=execution_runtime_mode,
    )
    namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(ctl_cfg_root, execution_context)
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
    ctl_variants: list[str] | tuple[str, ...] = (),
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
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        execution_runtime_mode=execution_runtime_mode,
    )
    action_cfg = catalog_targets.load_action_cfg(ctl_cfg_root, action, execution_context)
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
    workflow_cfg = catalog_workflow.load_workflow_cfg(ctl_cfg_root, ctl_profile, action, workflow_name, execution_context)
    workflow_cfg = catalog_workflow.apply_ctl_variants_to_workflow_cfg(
        ctl_cfg_root,
        workflow_cfg,
        action_cfg,
        execution_context=execution_context,
        action=action,
        workflow_name=workflow_name,
        ctl_variants=list(ctl_variants),
    )
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
    instance_params = catalog_workflow.validate_workflow_instance_params(
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
    ctl_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    action: str,
    workflow_name: str | None,
    *,
    ctl_variants: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    execution_runtime_mode: str,
    provider_options: dict[str, str] | None,
    execution_access_modes: dict[str, str],
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    enforce_ctl_policy: bool = True,
    load_provider_catalogs: bool = True,
    providers: list[str] | tuple[str, ...] = (),
) -> dict:
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
        ctl_cfg_root,
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        execution_access_modes=execution_access_modes,
        execution_runtime_mode=execution_runtime_mode,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
    )
    if enforce_ctl_policy:
        execution_run_context.validate_execution_context_constraints(ctl_cfg_root, execution_context)
    require_commit_refs = run_policy.ref_policy_requires_commits(ctl_ref_policy)

    if procedure_run:
        workflow_cfg, action_cfg = catalog_workflow.build_procedure_cfg(
            ctl_cfg_root,
            action,
            source=procedure_run["source"],
            ref=procedure_run["ref"],
            domain_name=procedure_run["domain"],
            procedure=procedure_run["procedure"],
            execution_provider=procedure_run.get("execution_provider"),
            execution_account=procedure_run.get("execution_account"),
            execution_role=procedure_run.get("execution_role"),
        )
        selection_kind = "procedure"
        selection_key = procedure_run["procedure"]
    elif target_name:
        # A standalone target run has no members, so the action is filtered by
        # the invoked action alone.
        action_cfg = catalog_targets.load_action_cfg(
            ctl_cfg_root, action, execution_context
        )
        workflow_cfg = {
            "meta": {
                "name": f"{ctl_profile}/{action}/{target_name}",
                "action": action,
            },
            "target_runs": [target_name],
        }
        selection_kind = "target"
        selection_key = target_name
    else:
        workflow_cfg = catalog_workflow.load_workflow_cfg(
            ctl_cfg_root,
            ctl_profile,
            action,
            workflow_name,
            execution_context,
        )
        action_cfg = catalog_targets.load_action_cfg(
            ctl_cfg_root, action, execution_context,
            member_actions=catalog_workflow.workflow_member_actions(workflow_cfg),
        )
        workflow_cfg = catalog_workflow.apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            workflow_cfg,
            action_cfg,
            execution_context=execution_context,
            action=action,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
        )
        selection_kind = "workflow"
        selection_key = workflow_name

    if not procedure_run:
        catalog_targets.validate_workflow_target_selectors(
            workflow_cfg, action_cfg, execution_context
        )
    if enforce_ctl_policy:
        run_policy.validate_target_policy_constraints(
            ctl_cfg_root, ctl_profile, workflow_cfg, action_cfg
        )
        run_policy.validate_execution_access(
            ctl_cfg_root,
            ctl_profile,
            workflow_cfg,
            action_cfg,
            execution_context=execution_context,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        )
        run_policy.validate_execution_runtime_mode(ctl_cfg_root, ctl_profile, execution_runtime_mode)

    refs = cfg_tooling.load_refs_cfg(ctl_cfg_root)
    active_target_runs = catalog_targets.build_active_target_runs(
        workflow_cfg,
        action_cfg,
        repo_key=target_repo_key,
        require_branch_or_commit=require_target_ref,
        refs=refs,
        execution_context=execution_context,
        require_commit_refs=require_commit_refs if enforce_ctl_policy else False,
    )
    if enforce_ctl_policy:
        cfg_validate.CommitPinning(ctl_ref_policy).check_target_runs(active_target_runs)
    provider_adapter = None
    provider_catalogs = None
    if load_provider_catalogs:
        provider_adapter = execution_providers.run_provider_adapter(execution_context)
        provider_catalogs = provider_adapter.load_runtime_catalogs(
            ctl_cfg_root, execution_context=execution_context
        )
    return {
        "selection_kind": selection_kind,
        "selection_key": selection_key,
        "execution_context": execution_context,
        "scope_params": execution_run_context.scope_params_from_context(execution_context),
        "require_commit_refs": require_commit_refs,
        "workflow_cfg": workflow_cfg,
        "action_cfg": action_cfg,
        "refs": refs,
        "active_target_runs": active_target_runs,
        "provider_adapter": provider_adapter,
        "provider_catalogs": provider_catalogs,
    }


def resolve_and_preflight_execution_identities(
    ctl_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    action: str,
    workflow_name: str | None,
    *,
    ctl_variants: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    provider_implementation_key: str,
    execution_runtime_mode: str,
    provider_options: dict[str, str] | None,
    execution_access_modes: dict[str, str],
    artifacts_dir: Path,
    gates_dir: Path,
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> tuple[dict, dict]:
    """

    single-runner (workflow/target/procedure) preflight: the same four
    validation reports the fan-out produces, for this one selection."""

    selection = resolve_pipeline_selection(
        ctl_cfg_root,
        ctl_profile,
        execution_params,
        ctl_ref_policy,
        action,
        workflow_name,
        ctl_variants=ctl_variants,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        execution_runtime_mode=execution_runtime_mode,
        provider_options=provider_options,
        execution_access_modes=execution_access_modes,
        target_name=target_name,
        procedure_run=procedure_run,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
        enforce_ctl_policy=False,
        load_provider_catalogs=False,
        providers=providers,
    )
    cfg_report = preflight_checks.CFG_VALIDATION.build(
        preflight_reports.collect_provider_cfg_findings(ctl_cfg_root, selection["execution_context"])
    )
    preflight_checks.CFG_VALIDATION.apply_gate(
        cfg_report, force_skip=force_skip_full_cfg_validation_gate
    )
    outcome = preflight_checks.build_selection_validation_reports(
        selection,
        preflight_checks.PreflightInputs(
            ctl_cfg_root=ctl_cfg_root,
            ctl_profile=ctl_profile,
            ctl_ref_policy=ctl_ref_policy,
            execution_runtime_mode=execution_runtime_mode,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            implementation_key=provider_implementation_key,
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
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
