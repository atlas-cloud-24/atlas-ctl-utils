"""Operator maintenance operations.

Target-backed maintenance uses the dedicated maintenance result owner and the
normal lifecycle. Ctl-state-only maintenance has no target or workflow and owns
its command report or audit manifest. These are command entry points rather than
state primitives, so they live here instead of in ``state/``.
"""

import argparse
import logging
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import yaml

from engine.catalog import targets as catalog_targets
from engine.cfg import materialize as cfg_materialize
from engine.cfg import secrets as cfg_secrets
from engine.cfg import tooling as cfg_tooling
from engine.cfg import validate as cfg_validate
from engine.cfg import views as cfg_views
from engine.commands import selection as commands_selection
from engine.execution import adapters as execution_adapters
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.guardrails import verify as guardrails_verify
from engine.kernel import ids as kernel_ids
from engine.kernel import process as kernel_process
from engine.kernel import yaml_io as kernel_yaml_io
from engine.preflight import checks as preflight_checks
from engine.preflight import reports as preflight_reports
from engine.run import policy as run_policy
from engine.state import lifecycle as state_lifecycle
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import sync as state_sync


def inspect_selected_graph_ctl_state_backend(
    selections: list[dict],
    ctl_cfg_root: Path,
    *,
    implementation_key: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
) -> dict[str, object]:
    """Find the one backend provisioner and classify the selected backend."""
    provisioners: list[tuple[dict, str, dict]] = []
    for selection in selections:
        for target_run_id, target_run in selection["active_target_runs"].items():
            if target_run.get("provisions_ctl_state_backend") is True:
                provisioners.append((selection, target_run_id, target_run))
    if len(provisioners) != 1:
        raise RuntimeError(
            "❌ agreed ctl-state defer requires exactly one backend provisioner "
            f"in the complete selected graph; found {len(provisioners)}"
        )

    selection, target_run_id, target_run = provisioners[0]
    namespace_key, entry = state_sync.CtlStateBackends.resolve_namespace(
        ctl_cfg_root, selection["execution_context"]
    )
    adapter = execution_adapters.get_adapter(entry["provider"])
    adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
    bucket_name = str(
        execution_references.resolve_runtime_scalar(
            entry["bucket_name"],
            selection["execution_context"],
            label=f"ctl_state_backends.{namespace_key}.bucket_name",
        )
    )
    bucket_region = str(entry["bucket_region"])
    probe_access_mode, probe_options = execution_providers.provider_inputs(
        str(entry["provider"]), execution_access_modes, provider_options
    )
    credential = adapter.resolve_state_backend_probe_credential(
        target_run,
        selection["provider_catalogs"],
        execution_context=selection["execution_context"],
        implementation_key=implementation_key,
        execution_access_mode=probe_access_mode,
        provider_options=probe_options,
    )
    probe = adapter.probe_state_backend(bucket_name, bucket_region, credential)
    status = probe.get("status")
    if status not in {"ready", "absent"}:
        raise RuntimeError(
            f"❌ ctl-state backend readiness probe for {namespace_key!r} "
            f"returned {status!r}: {probe.get('detail') or 'no detail'}"
        )
    return {
        "namespace": namespace_key,
        "bucket_name": bucket_name,
        "bucket_region": bucket_region,
        "provisioner_target_run_id": target_run_id,
        "status": status,
        "detail": probe.get("detail"),
    }


def run_ctl_state_status_sweep(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        providers=getattr(args, "providers", ()),
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    # The sweep is a QUERY over bucket truth (its only output, an
    # advisory status_cache.yaml, belongs to the bucket). It hydrates every
    # pointer in the namespace, so running it against the real local root would
    # clobber local-only records wholesale — it works in a throwaway root and
    # pushes the caches from there.
    with tempfile.TemporaryDirectory(prefix="atlas-ctl-state-sweep-") as scratch:
        return _run_ctl_state_status_sweep_in(
            ctl_cfg_root,
            args,
            context,
            Path(scratch),
            provider_implementation_key=provider_implementation_key,
        )


def _run_ctl_state_status_sweep_in(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    context: dict[str, object],
    ctl_state_root: Path,
    *,
    provider_implementation_key: str,
) -> dict:
    namespace_key, namespace_root, reader = state_sync.CtlStateAccess.arm_operation(
        ctl_cfg_root,
        context,
        ctl_state_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
    )
    state_run_store.hydrate_ctl_state_index(reader)
    #.10: ONE lean root-level map (advisory, bucket-owned), replacing
    # the old per-workflow-instance verbose docs. Flat address -> verdict over
    # every target and workflow instance, lifecycle-collapsed.
    instances = state_status.compute_namespace_status_map(namespace_root)
    #.10: same self-describing shape status.py --write-cache emits, so
    # A reader never has to guess which view / when produced this snapshot.
    cache = {
        "advisory": True,
        "source": "ctl-state self-consistency sweep",
        "namespace": namespace_key,
        "scope": "remote",
        "computed_at": kernel_ids.utc_timestamp(),
        **instances,
    }
    cache_path = namespace_root / "status_cache.yaml"
    kernel_yaml_io.write_yaml_file(cache_path, cache)
    cache_key = cache_path.relative_to(namespace_root).as_posix()
    _, _, writer = state_sync.CtlStateAccess.arm_operation(
        ctl_cfg_root,
        context,
        ctl_state_root,
        operation="sync",
        object_keys=[cache_key],
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
    )
    writer.put_object(cache_key, cache_path)
    report = {
        "operation": "status-sweep",
        "namespace": namespace_key,
        **instances,
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def run_ctl_state_forget(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    """Remove ctl-state records selected by AGE and ADDRESS.

    A dry run by default, so the safe form is also the discovery form: it lists
    what would go and removes nothing. `--apply` is the only flag that means
    "do it".

    A record missing in one scope is a SKIP, never a failure and never silent.
    The scopes diverge legitimately — a force-skipped run is local-only and
    permanently absent from remote, a record made on another machine is
    remote-only from here — so erroring on absence would make `--scope both`
    unusable in exactly the case it exists for.
    """

    if not run_policy.Permissions.CTL_STATE_FORGET.granted(ctl_cfg_root, args.ctl_profile):
        raise RuntimeError(
            f"❌ ctl profile {args.ctl_profile!r} does not grant allow_ctl_state_forget"
        )
    wide = args.older_than == "any" and args.forget_address == ["all"]
    if wide and not getattr(args, "accept_forget_everything", False):
        raise RuntimeError(
            "❌ --older-than any with --address all forgets EVERY record; "
            "pass --accept-forget-everything"
        )

    context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        providers=getattr(args, "providers", ()),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(ctl_cfg_root, context)
    scope = getattr(args, "forget_scope", None) or "both"
    apply = getattr(args, "apply_forget", False)

    results: dict[str, dict[str, str]] = {}
    roots: dict[str, Path] = {}
    if scope in ("local", "both"):
        roots["local"] = Path(args.ctl_state_local_root) / namespace_key
    scratch = None
    if scope in ("remote", "both"):
        scratch = tempfile.TemporaryDirectory(prefix="atlas-ctl-state-forget-")
        _, remote_root, syncer = state_sync.CtlStateAccess.arm_operation(
            ctl_cfg_root,
            context,
            Path(scratch.name),
            operation="maintenance",
            provider_implementation_key=provider_implementation_key,
            execution_access_modes=args.execution_access_modes,
            provider_options=args.provider_options,
        )
        state_run_store.hydrate_ctl_state_index(syncer)
        roots["remote"] = remote_root

    try:
        agree_active = getattr(args, "accept_orphaned_resources", False)
        cascade = getattr(args, "cascade", False)
        refused = 0
        for where, root in roots.items():
            if not root.is_dir():
                continue
            referenced_by = state_status.workflow_references(root)
            for item in state_status.forget_selection(root, args.older_than, args.forget_address):
                row = results.setdefault(item["address"], {})
                refusal = state_status.forget_guard(
                    root,
                    item["address"],
                    accept_orphans=agree_active,
                    cascade=cascade,
                    referenced_by=referenced_by,
                )
                if refusal:
                    row[where] = f"refused — {refusal}"
                    refused += 1
                    continue
                row[where] = "removed" if apply else "would remove"
                if apply:
                    shutil.rmtree(item["dir"], ignore_errors=True)
        # An address the caller named that no scope held is stated, not inferred
        # from a count: "removed nothing" and "was not there" must not look alike.
        if args.forget_address != ["all"]:
            for named in args.forget_address:
                row = results.setdefault(named.strip("/"), {})
                for where in roots:
                    row.setdefault(where, "not present — skipped")
    finally:
        if scratch is not None:
            scratch.cleanup()

    report = {
        "operation": "forget",
        "namespace": namespace_key,
        "scope": scope,
        "older_than": args.older_than,
        "address": args.forget_address,
        "applied": apply,
        **({"refused": refused} if refused else {}),
        "instances": dict(sorted(results.items())),
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def run_ctl_state_history_prune(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    if not run_policy.Permissions.CTL_STATE_HISTORY_MAINTENANCE.granted(
        ctl_cfg_root, args.ctl_profile
    ):
        raise RuntimeError(
            f"❌ ctl profile {args.ctl_profile!r} does not grant "
            "allow_ctl_state_history_maintenance"
        )
    context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        providers=getattr(args, "providers", ()),
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, namespace_root, reader = state_sync.CtlStateAccess.arm_operation(
        ctl_cfg_root,
        context,
        args.ctl_state_local_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
    )
    keys = state_run_store.hydrate_ctl_state_index(reader)
    selected_ids = set(args.prune_run_id or [])
    cutoff = None
    if args.prune_before:
        try:
            cutoff = datetime.fromisoformat(args.prune_before)
        except ValueError as error:
            raise RuntimeError("❌ --prune-before must be ISO-8601") from error
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=UTC)

    run_keys: dict[str, list[str]] = {}
    run_kinds: dict[str, str] = {}
    for key in keys:
        match = re.search(r"/(target|workflow)/.+?/runs/([^/]+)/", "/" + key)
        if not match:
            continue
        kind, run_id = match.group(1), match.group(2)
        run_keys.setdefault(run_id, []).append(key)
        run_kinds[run_id] = kind
        when = kernel_ids._uuid7_datetime(run_id)
        if cutoff is not None and when is not None and when < cutoff:
            selected_ids.add(run_id)
    if args.prune_kind:
        selected_ids = {
            run_id
            for run_id in selected_ids
            if run_kinds.get(run_id) == args.prune_kind
        }
    unknown = sorted(selected_ids - set(run_keys))
    if unknown:
        raise RuntimeError(
            "❌ selected prune run ids are not present in this namespace: "
            + ", ".join(unknown)
        )

    current_ids = set()
    for pointer_path in namespace_root.rglob("committed/*.yaml"):
        pointer = state_run_store.read_committed_pointer(pointer_path.parent)
        if pointer and pointer.get("run_id"):
            current_ids.add(str(pointer["run_id"]))
    protected = sorted(selected_ids & current_ids)
    if protected:
        raise RuntimeError(
            "❌ current committed revisions cannot be pruned: "
            + ", ".join(protected)
        )

    references: dict[str, set[str]] = {}
    for snapshot_path in namespace_root.rglob(state_run_store.RUN_METADATA_FILENAME):
        snapshot = kernel_yaml_io.load_yaml(snapshot_path) or {}
        workflow_run = str(
            snapshot.get("run_id") or snapshot_path.parent.name
        )
        for child in snapshot.get("child_revisions") or []:
            if isinstance(child, dict) and child.get("run_id"):
                references.setdefault(str(child["run_id"]), set()).add(workflow_run)

    candidates = set(selected_ids)
    changed = True
    while changed:
        changed = False
        for run_id in list(candidates):
            referrers = references.get(run_id, set()) - candidates
            if not referrers:
                continue
            if not args.cascade:
                raise RuntimeError(
                    f"❌ run {run_id} is referenced by retained workflow runs: "
                    + ", ".join(sorted(referrers))
                )
            current_referrers = referrers & current_ids
            if current_referrers:
                raise RuntimeError(
                    "❌ cascade would prune current workflow revisions: "
                    + ", ".join(sorted(current_referrers))
                )
            candidates.update(referrers)
            changed = True

    deletion_keys = sorted(
        key for run_id in candidates for key in run_keys.get(run_id, [])
    )
    maintenance_id = kernel_ids.generate_uuid7()
    report = {
        "operation": "history-prune",
        "namespace": namespace_key,
        "maintenance_id": maintenance_id,
        "dry_run": not args.apply_history_prune,
        "selection": {
            "run_ids": sorted(args.prune_run_id or []),
            "before": args.prune_before,
            "kind": args.prune_kind,
            "cascade": bool(args.cascade),
        },
        "candidate_run_ids": sorted(candidates),
        "object_keys": deletion_keys,
        "delete_object_versions": False,
        "created_at": kernel_ids.utc_timestamp(),
    }
    manifest_path = (
        namespace_root
        / "_maintenance"
        / "history-prune"
        / maintenance_id
        / "manifest.yaml"
    )
    kernel_yaml_io.write_yaml_file(manifest_path, report)
    manifest_key = manifest_path.relative_to(namespace_root).as_posix()
    _, _, maintainer = state_sync.CtlStateAccess.arm_operation(
        ctl_cfg_root,
        context,
        args.ctl_state_local_root,
        operation="maintenance",
        object_keys=[manifest_key, *deletion_keys],
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
    )
    maintainer.put_object(manifest_key, manifest_path)
    if args.apply_history_prune:
        maintainer.delete_object_keys(deletion_keys)
        report["applied_at"] = kernel_ids.utc_timestamp()
        kernel_yaml_io.write_yaml_file(manifest_path, report)
        maintainer.put_object(manifest_key, manifest_path)
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def run_ctl_state_maintenance_command(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    commands_selection.validate_maintenance_args(args)
    context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        providers=getattr(args, "providers", ()),
        execution_access_modes=args.execution_access_modes,
        agreed_defer_ctl_state_backend_sync=args.agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=args.force_skip_ctl_state_backend_sync,
        force_skip_guardrails=args.force_skip_guardrails,
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
        force_skip_execution_identity_preflight_check=getattr(
            args, "force_skip_execution_identity_preflight_check", False
        ),
    )
    run_policy.validate_force_skip_full_cfg_validation_gate_policy(
        ctl_cfg_root,
        args.ctl_profile,
        args.force_skip_full_cfg_validation_gate,
    )
    cfg_report = preflight_checks.CFG_VALIDATION.build(
        preflight_reports.collect_provider_cfg_findings(ctl_cfg_root, context)
    )
    preflight_checks.CFG_VALIDATION.apply_gate(
        cfg_report, force_skip=args.force_skip_full_cfg_validation_gate
    )
    logging.info(
        "\n%s", "\n".join(preflight_checks.CFG_VALIDATION.render_lines(cfg_report))
    )
    preflight_checks.CFG_VALIDATION.assert_accepted(cfg_report)
    if args.maintenance_action == "forget":
        return run_ctl_state_forget(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    if args.maintenance_action == "status-sweep":
        return run_ctl_state_status_sweep(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    if args.maintenance_action == "history-prune":
        return run_ctl_state_history_prune(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    raise RuntimeError(
        f"❌ {args.maintenance_action!r} is not a ctl-state-only maintenance operation"
    )


def run_maintenance(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    ctl_state_local_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    action: str,
    maintenance_action: str,
    target_key: str,
    lock_id: str,
    run_id: str,
    target_repo_key: str,
    require_target_ref: bool,
    use_local_tooling_cfg: bool,
    provider_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    log_file: Path,
    provider_options: dict[str, str] | None,
    execution_runtime_mode: str,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_modes: dict[str, str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
    unlock_scope: str | None = None,
) -> None:
    """

    run a maintenance action against a single target_run target."""

    if maintenance_action == "unlock-ctl-state":
        # Two locks with different reach. `both` clears the remote lock and THIS
        # machine's local one; remote is namespace-wide, local is one directory,
        # so `both` is not a claim to have cleared every local lock everywhere.
        # A lock missing in one scope is a SKIP: the scopes diverge legitimately.
        scope = unlock_scope or "both"
        outcome: dict[str, str] = {}
        if scope in ("local", "both"):
            outcome["local"] = (
                "released"
                if state_lifecycle.force_unlock_ctl_state_lock(ctl_state_local_root, lock_id, run_dir)
                else "not present — skipped"
            )
        if scope in ("remote", "both"):
            with tempfile.TemporaryDirectory(prefix="atlas-ctl-state-unlock-") as scratch:
                context = execution_run_context.build_execution_context(
                    ctl_cfg_root,
                    action=action,
                    ctl_profile=ctl_profile,
                    execution_params=execution_params,
                    providers=providers,
                    execution_runtime_mode=execution_runtime_mode,
                )
                _, _, syncer = state_sync.CtlStateAccess.arm_operation(
                    ctl_cfg_root,
                    context,
                    Path(scratch),
                    operation="maintenance",
                    provider_implementation_key=provider_implementation_key,
                    execution_access_modes=execution_access_modes,
                    provider_options=provider_options,
                )
                outcome["remote"] = state_sync.release_remote_mutation_lock(syncer, lock_id)
        print(yaml.safe_dump(
            {"operation": "unlock-ctl-state", "scope": scope,
             "lock_id": lock_id, "locks": outcome},
            sort_keys=False).rstrip())
        state_lifecycle.print_run_summary(run_id, log_file)
        return

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
    )
    scope_params = execution_run_context.scope_params_from_context(execution_context)
    execution_run_context.validate_execution_context_constraints(ctl_cfg_root, execution_context)
    action_cfg = catalog_targets.load_action_cfg(ctl_cfg_root, action, execution_context)
    maintenance_workflow_cfg = {"target_runs": [{"target": target_key}]}
    run_policy.validate_target_policy_constraints(ctl_cfg_root, ctl_profile, maintenance_workflow_cfg, action_cfg)
    run_policy.validate_execution_access(
        ctl_cfg_root,
        ctl_profile,
        maintenance_workflow_cfg,
        action_cfg,
        execution_context=execution_context,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
    )
    cfg_report = preflight_checks.CFG_VALIDATION.build(
        preflight_reports.collect_provider_cfg_findings(ctl_cfg_root, execution_context)
    )
    preflight_checks.CFG_VALIDATION.apply_gate(
        cfg_report, force_skip=force_skip_full_cfg_validation_gate
    )
    preflight_checks.CFG_VALIDATION.write_artifacts(
        cfg_materialize.run_gates_dir(run_dir), cfg_report
    )
    preflight_checks.CFG_VALIDATION.assert_accepted(cfg_report)
    ctl_state_namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
        ctl_cfg_root, execution_context
    )
    guardrails_verify.verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )
    state_sync.PUBLICATION.configure(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_namespace_key,
        execution_context,
        run_dir,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
        provider_implementation_key=provider_implementation_key,
    )
    execution_run_context.write_execution_context_artifact(run_dir, execution_context)
    require_commit_refs = run_policy.ref_policy_requires_commits(ctl_ref_policy)

    refs = cfg_tooling.load_refs_cfg(ctl_cfg_root)
    if use_local_tooling_cfg:
        tooling_refs = cfg_tooling.load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        cfg_validate.CommitPinning(ctl_ref_policy).check_tooling_refs(tooling_refs)

    logging.info(f"Selector policy validation passed: ctl_profile={ctl_profile}")

    workflow_cfg = {
        "meta": {
            "name": f"{ctl_profile}/{action}/maintenance/{maintenance_action}/{target_key}",
            "action": action,
        },
        "target_runs": [
            {
                "id": target_key,
                "target": target_key,
            }
        ],
    }
    catalog_targets.validate_workflow_target_selectors(workflow_cfg, action_cfg, execution_context)

    active_target_runs, pipeline_run_cfg_path, final_plt_overlays = commands_selection.prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        action_cfg,
        artifacts_dir,
        ctl_profile,
        scope_params=scope_params,
        execution_context=execution_context,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )
    state_run_store.update_run_metadata(run_dir, {"plt_overlays": final_plt_overlays})
    state_run_store.record_run_target_keys(run_dir, catalog_targets.target_keys_from_active_target_runs(active_target_runs))
    # per-target derivation, same as run_pipeline.
    run_type_now = str(state_run_store.load_run_metadata(run_dir).get("run_type"))
    plt_targets_dir_path = cfg_views.target_cfg_views_root(run_dir, run_type_now)
    for target_run_id, target_run in active_target_runs.items():
        if not target_run.get("domains"):
            continue
        target_context = execution_run_context.build_target_execution_context(
            target_run_id, target_run, execution_context
        )
        target_rendered_dir = cfg_views.prepare_target_cfg_view(
            target_run_id, target_run,
            plt_cfg_root=plt_cfg_root,
            target_cfg_dir=cfg_views.target_cfg_view_dir(run_dir, run_type_now, target_run_id),
            ctl_profile=ctl_profile,
            scope_params=execution_run_context.scope_params_from_context(target_context),
            execution_context=target_context,
        )
        guardrails_verify.verify_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            guardrails_cfg_root,
            target_rendered_dir,
            target_context,
            execution_run_context.scope_params_from_context(target_context),
        )

    cfg_validate.CommitPinning(ctl_ref_policy).check_target_runs(active_target_runs)
    provider_adapter = execution_providers.run_provider_adapter(execution_context)
    provider_catalogs = provider_adapter.load_runtime_catalogs(
        ctl_cfg_root, execution_context=execution_context
    )
    adapter_access_mode, adapter_options = execution_providers.provider_inputs(
        execution_providers.run_provider(execution_context), execution_access_modes, provider_options
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )
    cfg_materialize.write_git_metas(ctl_cfg_root, plt_cfg_root, guardrails_cfg_root, artifacts_dir)
    plt_targets_dir_path = cfg_materialize.run_cfg_distribution(
        pipeline_run_cfg_path, plt_targets_dir_path, run_type_now
    )
    cfg_views.finalize_target_cfg_view_facts(
        active_target_runs, plt_targets_dir_path, pipeline_run_cfg_path
    )

    os.chdir(run_dir)
    tooling_env = cfg_tooling.build_tooling_env(tooling_refs)
    if len(active_target_runs) != 1:
        raise RuntimeError(
            f"❌ maintenance action '{maintenance_action}' expected exactly one active target_run, got: {list(active_target_runs)}"
        )

    target_run_id, target_run = next(iter(active_target_runs.items()))
    state_lifecycle.log_target_run_banner(f"[{action}] [maintenance/{maintenance_action}/{target_run_id}]")
    repo_path, target_env = cfg_materialize.prepare_target_repo(
        target_run_id,
        target_run,
        run_dir,
        tooling_env,
        secret_store=cfg_secrets.SecretStore(ctl_cfg_root),
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
        execution_context=execution_context,
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
    )
    assertion_argv = provider_adapter.target_assertion_argv(cfg_materialize.materialize_step_utils(run_dir))
    if assertion_argv:
        kernel_process.run_and_log(assertion_argv, cwd=repo_path, env=target_env)

    target_cfg_dir = (
        plt_targets_dir_path
        if (plt_targets_dir_path / "input").is_dir()
        else plt_targets_dir_path / target_run_id
    ) / "input"
    if not target_cfg_dir.is_dir():
        raise RuntimeError(f"❌ target_run input cfg dir not found for target_run '{target_run_id}': {target_cfg_dir}")

    # A tool's own state lock is released by the target repo's declared `unlock`
    # procedure, not here: only the repo knows which tool it runs and where
    # that tool keeps its state. The engine used to author the script itself,
    # which meant reading step SOURCE to find a project path — the one place it
    # reached inside a step instead of going through the step contract.
    raise RuntimeError(
        f"❌ maintenance action {maintenance_action!r} does not operate on a target.\n"
        "To release a tool's state lock, run the target repo's declared `unlock` "
        "procedure:\n"
        "  ./ctl.py procedure --procedure unlock --action destroy "
        "--target <key> --lock-id <id>"
    )

    state_lifecycle.print_run_summary(run_id, log_file)
